import asyncio
import grpc
import json
import uuid
from typing import Optional, Callable, Any, Dict
from datetime import datetime
import traceback

from proto.mmc_message_pb2 import Message, MessageType, ResponseStatus
from proto.mmc_message_pb2_grpc import MessageServiceStub, add_MessageServiceServicer_to_server

from .config import global_config
from .logger import logger


class GrpcManager:
    """gRPC 连接管理器（客户端模式）"""
    
    def __init__(self):
        self.stub: Optional[MessageServiceStub] = None
        self.channel: Optional[grpc.aio.Channel] = None
        self.is_running = False
        self.reconnect_interval = 5  # 重连间隔（秒）
        self.max_reconnect_attempts = 10  # 最大重连次数
        self.message_handler: Optional[Callable] = None
        self.response_futures: Dict[str, asyncio.Future] = {}
        self.platform_name = global_config.maibot_server.platform_name
        self.send_queue: asyncio.Queue = asyncio.Queue()  # 预先初始化队列
        
    async def start_connection(self, message_handler: Callable[[dict], Any]) -> None:
        """启动 gRPC 连接"""
        self.message_handler = message_handler
        await self._start_client_connection()
    
    async def _start_client_connection(self) -> None:
        """启动客户端连接"""
        host = global_config.maibot_server.host
        port = global_config.maibot_server.port
        target = f'{host}:{port}'
        
        logger.info(f"正在启动 gRPC 客户端连接，目标地址: {target}")
        
        reconnect_count = 0
        
        while reconnect_count < self.max_reconnect_attempts:
            try:
                logger.info(f"尝试连接到 MaiBot gRPC 服务器: {target}")
                
                # 创建 gRPC 通道
                self.channel = grpc.aio.insecure_channel(target)
                self.stub = MessageServiceStub(self.channel)
                
                # 测试连接
                await self.channel.channel_ready()
                
                self.is_running = True
                reconnect_count = 0  # 重置重连计数
                
                logger.success(f"成功连接到 MaiBot gRPC 服务器: {target}")
                
                # 启动消息流处理
                await self._handle_message_stream()
                    
            except grpc.aio.AioRpcError as e:
                reconnect_count += 1
                logger.warning(f"gRPC 连接失败 ({reconnect_count}/{self.max_reconnect_attempts}): {e}")
                
                if reconnect_count < self.max_reconnect_attempts:
                    logger.info(f"将在 {self.reconnect_interval} 秒后重试连接...")
                    await asyncio.sleep(self.reconnect_interval)
                else:
                    logger.error("已达到最大重连次数，停止重连")
                    raise
            except Exception as e:
                logger.error(f"gRPC 连接时发生未知错误: {e}")
                logger.error(traceback.format_exc())
                raise
            finally:
                self.is_running = False
                if self.channel:
                    await self.channel.close()
                    self.channel = None
                    self.stub = None
    
    async def _handle_message_stream(self):
        """处理消息流"""
        try:
            # 创建消息流
            message_stream = self.stub.MessageStream(self._generate_messages())
            
            # 发送握手消息
            handshake_msg = Message(
                message_id=str(uuid.uuid4()),
                type=MessageType.HANDSHAKE,
                sender_platform=self.platform_name,
                payload=json.dumps({"action": "connect"}),
                timestamp=int(datetime.now().timestamp()),
                require_ack=True
            )
            await self.send_queue.put(handshake_msg)
            
            # 处理接收到的消息
            async for message in message_stream:
                await self._handle_received_message(message)
                
        except grpc.aio.AioRpcError as e:
            logger.error(f"消息流处理错误: {e}")
            raise
        except Exception as e:
            logger.error(f"消息流处理异常: {e}")
            logger.error(traceback.format_exc())
            raise
    
    async def _generate_messages(self):
        """生成消息流（用于客户端发送）"""
        while self.is_running:
            try:
                # 等待消息队列中的消息
                try:
                    message = await asyncio.wait_for(self.send_queue.get(), timeout=1.0)
                    yield message
                except asyncio.TimeoutError:
                    # 发送心跳消息保持连接
                    heartbeat = Message(
                        message_id=str(uuid.uuid4()),
                        type=MessageType.HEARTBEAT,
                        sender_platform=self.platform_name,
                        timestamp=int(datetime.now().timestamp())
                    )
                    yield heartbeat
            except Exception as e:
                logger.error(f"生成消息时出错: {e}")
                break
    
    async def _handle_received_message(self, message: Message):
        """处理接收到的消息"""
        try:
            logger.debug(f"收到消息: {message.message_id}, 类型: {message.type}")
            
            # 如果是响应消息，处理 Future
            if message.message_id in self.response_futures:
                future = self.response_futures.pop(message.message_id)
                if not future.cancelled():
                    future.set_result(message)
                return
            
            # 解析消息负载
            try:
                payload = json.loads(message.payload) if message.payload else {}
            except json.JSONDecodeError:
                logger.error(f"无法解析消息负载: {message.payload}")
                return
            
            # 构造消息数据结构（保持与原来的格式兼容）
            message_data = {
                "message_id": message.message_id,
                "type": message.type,
                "sender_platform": message.sender_platform,
                "target_platform": message.target_platform,
                "payload": payload,
                "timestamp": message.timestamp,
                "require_ack": message.require_ack
            }
            
            # 调用消息处理器
            if self.message_handler:
                await self.message_handler(message_data)
                
            # 如果需要确认，发送确认消息
            if message.require_ack:
                ack_message = Message(
                    message_id=message.message_id,
                    type=MessageType.MESSAGE_ACK,
                    sender_platform=self.platform_name,
                    target_platform=message.sender_platform,
                    payload=json.dumps({"status": "received"}),
                    timestamp=int(datetime.now().timestamp()),
                    status=ResponseStatus.SUCCESS
                )
                await self.send_queue.put(ack_message)
                
        except Exception as e:
            logger.error(f"处理接收消息时出错: {e}")
            logger.error(traceback.format_exc())
    
    async def send_message(self, message_data: dict) -> Optional[dict]:
        """发送消息"""
        try:
            # 转换为 gRPC 消息格式
            if isinstance(message_data, dict):
                grpc_message = Message(
                    message_id=message_data.get("message_id", str(uuid.uuid4())),
                    type=MessageType.NORMAL_MESSAGE,  # 默认类型
                    sender_platform=self.platform_name,
                    target_platform=message_data.get("target_platform", ""),
                    payload=json.dumps(message_data.get("payload", {})),
                    timestamp=int(datetime.now().timestamp()),
                    require_ack=message_data.get("require_ack", False)
                )
            else:
                grpc_message = message_data
            
            # 发送消息并等待响应
            if grpc_message.require_ack:
                future = asyncio.Future()
                self.response_futures[grpc_message.message_id] = future
                
                # 发送消息
                await self.send_queue.put(grpc_message)
                
                # 等待响应
                try:
                    response = await asyncio.wait_for(future, timeout=30.0)
                    return {
                        "message_id": response.message_id,
                        "status": response.status,
                        "payload": json.loads(response.payload) if response.payload else {}
                    }
                except asyncio.TimeoutError:
                    self.response_futures.pop(grpc_message.message_id, None)
                    logger.error(f"消息 {grpc_message.message_id} 响应超时")
                    return None
            else:
                # 不需要确认的消息
                await self.send_queue.put(grpc_message)
                return None
                
        except Exception as e:
            logger.error(f"发送消息时出错: {e}")
            logger.error(traceback.format_exc())
            return None
    
    async def stop_connection(self) -> None:
        """停止 gRPC 连接"""
        self.is_running = False
        
        # 取消所有等待的响应
        for future in self.response_futures.values():
            if not future.cancelled():
                future.cancel()
        self.response_futures.clear()
        
        if self.channel:
            try:
                await self.channel.close()
                logger.info("gRPC 客户端连接已关闭")
            except Exception as e:
                logger.error(f"关闭 gRPC 客户端连接时出错: {e}")
            finally:
                self.channel = None
                self.stub = None
    
    def is_connected(self) -> bool:
        """检查是否已连接"""
        return self.is_running and self.stub is not None


# 全局 gRPC 管理器实例
grpc_manager = GrpcManager()
