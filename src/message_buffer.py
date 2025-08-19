import asyncio
import time
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from src.logger import logger
from src.config.features_config import features_manager
from src.recv_handler import RealMessageType


@dataclass
class TextMessage:
    """文本消息"""
    text: str
    timestamp: float = field(default_factory=time.time)


@dataclass
class BufferedSession:
    """缓冲会话数据"""
    session_id: str
    messages: List[TextMessage] = field(default_factory=list)
    timer_task: Optional[asyncio.Task] = None
    delay_task: Optional[asyncio.Task] = None
    original_event: Any = None
    created_at: float = field(default_factory=time.time)


class SimpleMessageBuffer:
    
    def __init__(self, merge_callback=None):
        """
        初始化消息缓冲器
        
        Args:
            merge_callback: 消息合并后的回调函数，接收(session_id, merged_text, original_event)参数
        """
        self.buffer_pool: Dict[str, BufferedSession] = {}
        self.lock = asyncio.Lock()
        self.merge_callback = merge_callback
        self._shutdown = False
        
    def get_session_id(self, event_data: Dict[str, Any]) -> str:
        """根据事件数据生成会话ID"""
        message_type = event_data.get("message_type", "unknown")
        user_id = event_data.get("user_id", "unknown")
        
        if message_type == "private":
            return f"private_{user_id}"
        elif message_type == "group":
            group_id = event_data.get("group_id", "unknown")
            return f"group_{group_id}_{user_id}"
        else:
            return f"{message_type}_{user_id}"
    
    def extract_text_from_message(self, message: List[Dict[str, Any]]) -> Optional[str]:
        """从OneBot消息中提取纯文本，如果包含非文本内容则返回None"""
        text_parts = []
        has_non_text = False
        
        logger.debug(f"正在提取消息文本，消息段数量: {len(message)}")
        
        for msg_seg in message:
            msg_type = msg_seg.get("type", "")
            logger.debug(f"处理消息段类型: {msg_type}")
            
            if msg_type == RealMessageType.text:
                text = msg_seg.get("data", {}).get("text", "").strip()
                if text:
                    text_parts.append(text)
                    logger.debug(f"提取到文本: {text[:50]}...")
            else:
                # 发现非文本消息段，标记为包含非文本内容
                has_non_text = True
                logger.debug(f"发现非文本消息段: {msg_type}，跳过缓冲")
        
        # 如果包含非文本内容，则不进行缓冲
        if has_non_text:
            logger.debug("消息包含非文本内容，不进行缓冲")
            return None
            
        if text_parts:
            combined_text = " ".join(text_parts).strip()
            logger.debug(f"成功提取纯文本: {combined_text[:50]}...")
            return combined_text
        
        logger.debug("没有找到有效的文本内容")
        return None
    
    def should_skip_message(self, text: str) -> bool:
        """判断消息是否应该跳过缓冲"""
        if not text or not text.strip():
            return True
            
        # 检查屏蔽前缀
        config = features_manager.get_config()
        block_prefixes = tuple(config.message_buffer_block_prefixes)
        
        text = text.strip()
        if text.startswith(block_prefixes):
            logger.debug(f"消息以屏蔽前缀开头，跳过缓冲: {text[:20]}...")
            return True
            
        return False
    
    async def add_text_message(self, event_data: Dict[str, Any], message: List[Dict[str, Any]], 
                              original_event: Any = None) -> bool:
        """
        添加文本消息到缓冲区
        
        Args:
            event_data: 事件数据
            message: OneBot消息数组
            original_event: 原始事件对象
            
        Returns:
            是否成功添加到缓冲区
        """
        if self._shutdown:
            return False
            
        config = features_manager.get_config()
        if not config.enable_message_buffer:
            return False
        
        # 检查是否启用对应类型的缓冲
        message_type = event_data.get("message_type", "")
        if message_type == "group" and not config.message_buffer_enable_group:
            return False
        elif message_type == "private" and not config.message_buffer_enable_private:
            return False
        
        # 提取文本
        text = self.extract_text_from_message(message)
        if not text:
            return False
            
        # 检查是否应该跳过
        if self.should_skip_message(text):
            return False
        
        session_id = self.get_session_id(event_data)
        
        async with self.lock:
            # 获取或创建会话
            if session_id not in self.buffer_pool:
                self.buffer_pool[session_id] = BufferedSession(
                    session_id=session_id,
                    original_event=original_event
                )
            
            session = self.buffer_pool[session_id]
            
            # 检查是否超过最大组件数量
            if len(session.messages) >= config.message_buffer_max_components:
                logger.info(f"会话 {session_id} 消息数量达到上限，强制合并")
                await self._force_merge_session(session_id)
                return False
            
            # 添加文本消息
            session.messages.append(TextMessage(text=text))
            session.original_event = original_event  # 更新事件
            
            # 取消之前的定时器
            await self._cancel_session_timers(session)
            
            # 设置新的延迟任务
            session.delay_task = asyncio.create_task(
                self._wait_and_start_merge(session_id)
            )
            
            logger.debug(f"文本消息已添加到缓冲器 {session_id}: {text[:50]}...")
            return True
    
    async def _cancel_session_timers(self, session: BufferedSession):
        """取消会话的所有定时器"""
        for task_name in ['timer_task', 'delay_task']:
            task = getattr(session, task_name)
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                setattr(session, task_name, None)
    
    async def _wait_and_start_merge(self, session_id: str):
        """等待初始延迟后开始合并定时器"""
        config = features_manager.get_config()
        await asyncio.sleep(config.message_buffer_initial_delay)
        
        async with self.lock:
            session = self.buffer_pool.get(session_id)
            if session and session.messages:
                # 取消旧的定时器
                if session.timer_task and not session.timer_task.done():
                    session.timer_task.cancel()
                    try:
                        await session.timer_task
                    except asyncio.CancelledError:
                        pass
                
                # 设置合并定时器
                session.timer_task = asyncio.create_task(
                    self._wait_and_merge(session_id)
                )
    
    async def _wait_and_merge(self, session_id: str):
        """等待合并间隔后执行合并"""
        config = features_manager.get_config()
        await asyncio.sleep(config.message_buffer_interval)
        await self._merge_session(session_id)
    
    async def _force_merge_session(self, session_id: str):
        """强制合并会话（不等待定时器）"""
        await self._merge_session(session_id, force=True)
    
    async def _merge_session(self, session_id: str, force: bool = False):
        """合并会话中的消息"""
        async with self.lock:
            session = self.buffer_pool.get(session_id)
            if not session or not session.messages:
                self.buffer_pool.pop(session_id, None)
                return
            
            try:
                # 合并文本消息
                text_parts = []
                for msg in session.messages:
                    if msg.text.strip():
                        text_parts.append(msg.text.strip())
                
                if not text_parts:
                    self.buffer_pool.pop(session_id, None)
                    return
                
                merged_text = "，".join(text_parts)  # 使用中文逗号连接
                message_count = len(session.messages)
                
                logger.info(f"合并会话 {session_id} 的 {message_count} 条文本消息: {merged_text[:100]}...")
                
                # 调用回调函数
                if self.merge_callback:
                    try:
                        if asyncio.iscoroutinefunction(self.merge_callback):
                            await self.merge_callback(session_id, merged_text, session.original_event)
                        else:
                            self.merge_callback(session_id, merged_text, session.original_event)
                    except Exception as e:
                        logger.error(f"消息合并回调执行失败: {e}")
                
            except Exception as e:
                logger.error(f"合并会话 {session_id} 时出错: {e}")
            finally:
                # 清理会话
                await self._cancel_session_timers(session)
                self.buffer_pool.pop(session_id, None)
    
    async def flush_session(self, session_id: str):
        """强制刷新指定会话的缓冲区"""
        await self._force_merge_session(session_id)
    
    async def flush_all(self):
        """强制刷新所有会话的缓冲区"""
        session_ids = list(self.buffer_pool.keys())
        for session_id in session_ids:
            await self._force_merge_session(session_id)
    
    async def get_buffer_stats(self) -> Dict[str, Any]:
        """获取缓冲区统计信息"""
        async with self.lock:
            stats = {
                "total_sessions": len(self.buffer_pool),
                "sessions": {}
            }
            
            for session_id, session in self.buffer_pool.items():
                stats["sessions"][session_id] = {
                    "message_count": len(session.messages),
                    "created_at": session.created_at,
                    "age": time.time() - session.created_at
                }
            
            return stats
    
    async def clear_expired_sessions(self, max_age: float = 300.0):
        """清理过期的会话"""
        current_time = time.time()
        expired_sessions = []
        
        async with self.lock:
            for session_id, session in self.buffer_pool.items():
                if current_time - session.created_at > max_age:
                    expired_sessions.append(session_id)
        
        for session_id in expired_sessions:
            logger.info(f"清理过期会话: {session_id}")
            await self._force_merge_session(session_id)
    
    async def shutdown(self):
        """关闭消息缓冲器"""
        self._shutdown = True
        logger.info("正在关闭简化消息缓冲器...")
        
        # 刷新所有缓冲区
        await self.flush_all()
        
        # 确保所有任务都被取消
        async with self.lock:
            for session in list(self.buffer_pool.values()):
                await self._cancel_session_timers(session)
            self.buffer_pool.clear()
        
        logger.info("简化消息缓冲器已关闭")
