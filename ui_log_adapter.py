"""
Adapter服务UI日志适配器
在最小侵入的情况下捕获Adapter的日志并发送到UI
"""
import sys
import os
import logging
import threading
import time

# 添加MoFox-UI路径以导入ui_logger
ui_path = os.path.join(os.path.dirname(__file__), '..', 'MoFox-UI')
if os.path.exists(ui_path):
    sys.path.insert(0, ui_path)
    try:
        from ui_logger import get_ui_logger
        ui_logger = get_ui_logger("Adapter")
        UI_LOGGER_AVAILABLE = True
    except ImportError:
        UI_LOGGER_AVAILABLE = False
else:
    UI_LOGGER_AVAILABLE = False

class UILogHandler(logging.Handler):
    """自定义日志处理器，将日志发送到UI"""
    
    def __init__(self):
        super().__init__()
        self.ui_logger = ui_logger if UI_LOGGER_AVAILABLE else None
        
    def emit(self, record):
        if not self.ui_logger:
            return
            
        try:
            msg = self.format(record)
            level_mapping = {
                'DEBUG': 'debug',
                'INFO': 'info', 
                'WARNING': 'warning',
                'ERROR': 'error',
                'CRITICAL': 'error'
            }
            ui_level = level_mapping.get(record.levelname, 'info')
            
            # 过滤掉过于频繁的调试信息
            if record.levelname == 'DEBUG':
                return
                
            # 添加emoji前缀让日志更清晰
            # emoji_map = {
            #     'info': '📝',
            #     'warning': '⚠️',
            #     'error': '❌',
            #     'debug': '🔍'
            # }

            # formatted_msg = f"{emoji_map.get(ui_level, '📝')} {msg}"
            formatted_msg = f"{msg}"

            print(f"[UI日志适配器] 正在发送日志: {ui_level} - {formatted_msg[:50]}...")
            
            if ui_level == 'info':
                self.ui_logger.info(formatted_msg)
            elif ui_level == 'warning':
                self.ui_logger.warning(formatted_msg)
            elif ui_level == 'error':
                self.ui_logger.error(formatted_msg)
            elif ui_level == 'debug':
                self.ui_logger.debug(formatted_msg)
                
        except Exception as e:
            print(f"[UI日志适配器] emit失败: {e}")
            # 静默失败，不影响主程序
            pass

def setup_ui_logging():
    """设置UI日志处理器"""
    if not UI_LOGGER_AVAILABLE:
        print("[UI日志适配器] UI Logger不可用，跳过设置")
        return
        
    try:
        print("[UI日志适配器] 开始设置UI日志处理器...")
        
        # 获取Adapter的根日志器
        root_logger = logging.getLogger()
        
        # 检查是否已经添加过UI处理器
        for handler in root_logger.handlers:
            if isinstance(handler, UILogHandler):
                print("[UI日志适配器] UI日志处理器已存在，跳过重复添加")
                return
        
        # 创建UI日志处理器
        ui_handler = UILogHandler()
        ui_handler.setLevel(logging.INFO)  # 只捕获INFO及以上级别
        
        # 添加到根日志器
        root_logger.addHandler(ui_handler)
        
        print(f"[UI日志适配器] UI日志处理器已添加到根日志器，当前处理器数量: {len(root_logger.handlers)}")
        
        # 发送启动信息
        if UI_LOGGER_AVAILABLE:
            ui_logger.info("Adapter服务日志适配器已启动")
            print("[UI日志适配器] 启动信息已发送到UI")
            
    except Exception as e:
        print(f"[UI日志适配器] 设置失败: {e}")
        # 静默失败
        pass

# 自动设置
if __name__ != "__main__":
    print("[UI日志适配器] 模块被导入，准备设置UI日志...")
    
    # 立即尝试设置，如果日志系统还未初始化则延迟执行
    try:
        setup_ui_logging()
    except Exception as e:
        print(f"[UI日志适配器] 立即设置失败，将延迟执行: {e}")
        
        # 延迟执行，确保主程序日志系统已初始化
        def delayed_setup():
            time.sleep(1.0)  # 延迟1秒
            print("[UI日志适配器] 执行延迟设置...")
            setup_ui_logging()
        
        threading.Thread(target=delayed_setup, daemon=True).start()
