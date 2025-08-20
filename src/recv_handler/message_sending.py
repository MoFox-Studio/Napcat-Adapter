from src.logger import logger
from maim_message import MessageBase
from src.grpc_manager import grpc_manager
import json


class MessageSending:
    """
    负责把消息发送到麦麦
    """

    def __init__(self):
        pass

    async def message_send(self, message_base: MessageBase) -> bool:
        """
        发送消息
        Parameters:
            message_base: MessageBase: 消息基类，包含发送目标和消息内容等信息
        """
        try:
            # 将 MessageBase 转换为字典格式
            message_dict = message_base.to_dict()
            
            # 构造 gRPC 消息
            grpc_message_data = {
                "payload": message_dict,
                "require_ack": False
            }
            
            # 通过 gRPC 管理器发送消息
            result = await grpc_manager.send_message(grpc_message_data)
            
            if result is None:  # 不需要确认的消息返回 None 表示成功
                return True
            elif isinstance(result, dict) and result.get("status") == "SUCCESS":
                return True
            else:
                logger.warning(f"发送消息失败，返回: {result}")
                return False
                
        except Exception as e:
            logger.error(f"发送消息失败: {str(e)}")
            logger.error("请检查与MaiBot之间的连接")
            return False


message_send_instance = MessageSending()
