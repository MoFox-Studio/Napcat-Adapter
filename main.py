import asyncio
import sys
import json
import websockets as Server
from src.logger import logger
from src.recv_handler.message_handler import message_handler
from src.recv_handler.meta_event_handler import meta_event_handler
from src.recv_handler.notice_handler import notice_handler
from src.recv_handler.message_sending import message_send_instance
from src.send_handler import send_handler
from src.config import global_config
from src.config.features_config import features_manager
from src.config.migrate_features import auto_migrate_features
from src.mmc_com_layer import mmc_start_com, mmc_stop_com, router
from src.response_pool import put_response, check_timeout_response
from src.websocket_manager import websocket_manager
from src.message_chunker import chunker, reassembler

message_queue = asyncio.Queue()


async def message_recv(server_connection: Server.ServerConnection):
    await message_handler.set_server_connection(server_connection)
    asyncio.create_task(notice_handler.set_server_connection(server_connection))
    await send_handler.set_server_connection(server_connection)
    async for raw_message in server_connection:
        logger.debug(f"{raw_message[:1500]}..." if (len(raw_message) > 1500) else raw_message)
        
        try:
            # 首先尝试解析原始消息
            decoded_raw_message: dict = json.loads(raw_message)
            
            # 检查是否是切片消息 (来自 MMC)
            if chunker.is_chunk_message(decoded_raw_message):
                logger.debug("接收到切片消息，尝试重组")
                # 尝试重组消息
                reassembled_message = await reassembler.add_chunk(decoded_raw_message)
                if reassembled_message:
                    # 重组完成，处理完整消息
                    logger.debug("消息重组完成，处理完整消息")
                    decoded_raw_message = reassembled_message
                else:
                    # 切片尚未完整，继续等待更多切片
                    logger.debug("等待更多切片...")
                    continue
            
            # 处理完整消息（可能是重组后的，也可能是原本就完整的）
            post_type = decoded_raw_message.get("post_type")
            if post_type in ["meta_event", "message", "notice"]:
                await message_queue.put(decoded_raw_message)
            elif post_type is None:
                await put_response(decoded_raw_message)
                
        except json.JSONDecodeError as e:
            logger.error(f"消息解析失败: {e}")
            logger.debug(f"原始消息: {raw_message[:500]}...")
        except Exception as e:
            logger.error(f"处理消息时出错: {e}")
            logger.debug(f"原始消息: {raw_message[:500]}...")


async def message_process():
    """消息处理主循环"""
    logger.info("消息处理器已启动")
    try:
        while True:
            try:
                # 使用超时等待，以便能够响应取消请求
                message = await asyncio.wait_for(message_queue.get(), timeout=1.0)
                
                post_type = message.get("post_type")
                if post_type == "message":
                    await message_handler.handle_raw_message(message)
                elif post_type == "meta_event":
                    await meta_event_handler.handle_meta_event(message)
                elif post_type == "notice":
                    await notice_handler.handle_notice(message)
                else:
                    logger.warning(f"未知的post_type: {post_type}")
                
                message_queue.task_done()
                await asyncio.sleep(0.05)
                
            except asyncio.TimeoutError:
                # 超时是正常的，继续循环
                continue
            except asyncio.CancelledError:
                logger.info("消息处理器收到取消信号")
                break
            except Exception as e:
                logger.error(f"处理消息时出错: {e}")
                # 即使出错也标记任务完成，避免队列阻塞
                try:
                    message_queue.task_done()
                except ValueError:
                    pass
                await asyncio.sleep(0.1)
                
    except asyncio.CancelledError:
        logger.info("消息处理器已停止")
        raise
    except Exception as e:
        logger.error(f"消息处理器异常: {e}")
        raise
    finally:
        logger.info("消息处理器正在清理...")
        # 清空剩余的队列项目
        try:
            while not message_queue.empty():
                try:
                    message_queue.get_nowait()
                    message_queue.task_done()
                except asyncio.QueueEmpty:
                    break
        except Exception as e:
            logger.debug(f"清理消息队列时出错: {e}")


async def main():
    # 执行功能配置迁移（如果需要）
    logger.info("检查功能配置迁移...")
    auto_migrate_features()
    
    # 初始化功能管理器
    logger.info("正在初始化功能管理器...")
    features_manager.load_config()
    await features_manager.start_file_watcher(check_interval=2.0)
    logger.info("功能管理器初始化完成")
    
    # 启动消息重组器的清理任务
    logger.info("启动消息重组器...")
    await reassembler.start_cleanup_task()
    
    message_send_instance.maibot_router = router
    _ = await asyncio.gather(napcat_server(), mmc_start_com(), message_process(), check_timeout_response())


async def napcat_server():
    """启动 Napcat WebSocket 连接（支持正向和反向连接）"""
    mode = global_config.napcat_server.mode
    logger.info(f"正在启动 adapter，连接模式: {mode}")
    
    try:
        await websocket_manager.start_connection(message_recv)
    except Exception as e:
        logger.error(f"启动 WebSocket 连接失败: {e}")
        raise


async def graceful_shutdown():
    """优雅关闭所有组件"""
    try:
        logger.info("正在关闭adapter...")
        
        # 停止消息重组器的清理任务
        try:
            await reassembler.stop_cleanup_task()
        except Exception as e:
            logger.warning(f"停止消息重组器清理任务时出错: {e}")
        
        # 停止功能管理器文件监控
        try:
            await features_manager.stop_file_watcher()
        except Exception as e:
            logger.warning(f"停止功能管理器文件监控时出错: {e}")
        
        # 关闭消息处理器（包括消息缓冲器）
        try:
            await message_handler.shutdown()
        except Exception as e:
            logger.warning(f"关闭消息处理器时出错: {e}")
        
        # 关闭 WebSocket 连接
        try:
            await websocket_manager.stop_connection()
        except Exception as e:
            logger.warning(f"关闭WebSocket连接时出错: {e}")
        
        # 关闭 MaiBot 连接
        try:
            await mmc_stop_com()
        except Exception as e:
            logger.warning(f"关闭MaiBot连接时出错: {e}")
        
        # 取消所有剩余任务
        current_task = asyncio.current_task()
        tasks = [t for t in asyncio.all_tasks() if t is not current_task and not t.done()]
        
        if tasks:
            logger.info(f"正在取消 {len(tasks)} 个剩余任务...")
            for task in tasks:
                task.cancel()
            
            # 等待任务取消完成，忽略 CancelledError
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True), 
                    timeout=10
                )
            except asyncio.TimeoutError:
                logger.warning("部分任务取消超时")
            except Exception as e:
                logger.debug(f"任务取消过程中的异常（可忽略）: {e}")
        
        logger.info("Adapter已成功关闭")
        
    except Exception as e:
        logger.error(f"Adapter关闭中出现错误: {e}")
    finally:
        # 确保消息队列被清空
        try:
            while not message_queue.empty():
                try:
                    message_queue.get_nowait()
                    message_queue.task_done()
                except asyncio.QueueEmpty:
                    break
        except Exception:
            pass


async def run_with_graceful_shutdown():
    """运行主程序并处理优雅关闭"""
    try:
        await main()
    except KeyboardInterrupt:
        logger.warning("收到中断信号，正在优雅关闭...")
        await graceful_shutdown()
    except Exception as e:
        logger.exception(f"主程序异常: {str(e)}")
        await graceful_shutdown()
        raise


if __name__ == "__main__":
    try:
        asyncio.run(run_with_graceful_shutdown())
    except KeyboardInterrupt:
        logger.info("程序已被用户中断")
    except Exception as e:
        logger.exception(f"程序运行失败: {str(e)}")
        sys.exit(1)
