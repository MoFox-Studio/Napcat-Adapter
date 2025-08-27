from src.logger import logger
from src.message_chunker import chunker
from src.config import global_config
from maim_message import MessageBase, Router
import asyncio


class MessageSending:
    """
    负责把消息发送到麦麦
    """

    maibot_router: Router = None

    def __init__(self):
        pass

    async def message_send(self, message_base: MessageBase) -> bool:
        """
        发送消息（Ada -> MMC 方向，需要实现切片）
        Parameters:
            message_base: MessageBase: 消息基类，包含发送目标和消息内容等信息
        """
        try:
            # 检查是否需要切片发送
            message_dict = message_base.to_dict()
            
            if chunker.should_chunk_message(message_dict):
                logger.info("消息过大，进行切片发送到 MaiBot")
                
                # 切片消息
                chunks = chunker.chunk_message(message_dict)
                
                # 逐个发送切片
                for i, chunk in enumerate(chunks):
                    logger.debug(f"发送切片 {i+1}/{len(chunks)} 到 MaiBot")
                    
                    # 获取对应的客户端并发送切片
                    platform = message_base.message_info.platform
                    if platform not in self.maibot_router.clients:
                        logger.error(f"平台 {platform} 未连接")
                        return False
                    
                    client = self.maibot_router.clients[platform]
                    send_status = await client.send_message(chunk)
                    
                    if not send_status:
                        logger.error(f"发送切片 {i+1}/{len(chunks)} 失败")
                        return False
                    
                    # 使用配置中的延迟时间
                    if i < len(chunks) - 1:
                        delay_seconds = global_config.slicing.delay_ms / 1000.0
                        logger.debug(f"切片发送延迟: {global_config.slicing.delay_ms}毫秒")
                        await asyncio.sleep(delay_seconds)
                
                logger.debug("所有切片发送完成")
                return True
            else:
                # 直接发送小消息
                send_status = await self.maibot_router.send_message(message_base)
                if not send_status:
                    raise RuntimeError("可能是路由未正确配置或连接异常")
                return send_status
                
        except Exception as e:
            logger.error(f"发送消息失败: {str(e)}")
            logger.error("请检查与MaiBot之间的连接")
            return False


message_send_instance = MessageSending()
