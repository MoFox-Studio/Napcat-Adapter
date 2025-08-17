import asyncio
from dataclasses import dataclass, field
from typing import Literal, Optional
from pathlib import Path
import tomlkit
from src.logger import logger
from .config_base import ConfigBase
from .config_utils import create_config_from_template, create_default_config_dict


@dataclass
class FeaturesConfig(ConfigBase):
    """功能配置类"""
    
    group_list_type: Literal["whitelist", "blacklist"] = "whitelist"
    """群聊列表类型 白名单/黑名单"""
    
    group_list: list[int] = field(default_factory=list)
    """群聊列表"""
    
    private_list_type: Literal["whitelist", "blacklist"] = "whitelist"
    """私聊列表类型 白名单/黑名单"""
    
    private_list: list[int] = field(default_factory=list)
    """私聊列表"""
    
    ban_user_id: list[int] = field(default_factory=list)
    """被封禁的用户ID列表，封禁后将无法与其进行交互"""
    
    ban_qq_bot: bool = False
    """是否屏蔽QQ官方机器人，若为True，则所有QQ官方机器人将无法与MaiMCore进行交互"""
    
    enable_poke: bool = True
    """是否启用戳一戳功能"""
    
    ignore_non_self_poke: bool = False
    """是否无视不是针对自己的戳一戳"""
    
    poke_debounce_seconds: int = 3
    """戳一戳防抖时间（秒），在指定时间内第二次针对机器人的戳一戳将被忽略"""

    enable_reply_at: bool = True
    """是否启用引用回复时艾特用户的功能"""

    reply_at_rate: float = 0.5
    """引用回复时艾特用户的几率 (0.0 ~ 1.0)"""
    
    enable_video_analysis: bool = True
    """是否启用视频识别功能"""
    
    max_video_size_mb: int = 100
    """视频文件最大大小限制（MB）"""
    
    download_timeout: int = 60
    """视频下载超时时间（秒）"""
    
    supported_formats: list[str] = field(default_factory=lambda: ["mp4", "avi", "mov", "mkv", "flv", "wmv", "webm"])
    """支持的视频格式"""


class FeaturesManager:
    """功能管理器，支持热重载"""
    
    def __init__(self, config_path: str = "config/features.toml"):
        self.config_path = Path(config_path)
        self.config: Optional[FeaturesConfig] = None
        self._file_watcher_task: Optional[asyncio.Task] = None
        self._last_modified: Optional[float] = None
        self._callbacks: list = []
        
    def add_reload_callback(self, callback):
        """添加配置重载回调函数"""
        self._callbacks.append(callback)
        
    def remove_reload_callback(self, callback):
        """移除配置重载回调函数"""
        if callback in self._callbacks:
            self._callbacks.remove(callback)
    
    async def _notify_callbacks(self):
        """通知所有回调函数配置已重载"""
        for callback in self._callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(self.config)
                else:
                    callback(self.config)
            except Exception as e:
                logger.error(f"配置重载回调执行失败: {e}")
    
    def load_config(self) -> FeaturesConfig:
        """加载功能配置文件"""
        try:
            # 检查配置文件是否存在，如果不存在则创建并退出程序
            if not self.config_path.exists():
                logger.info(f"功能配置文件不存在: {self.config_path}")
                self._create_default_config()
                # 配置文件创建后程序应该退出，让用户检查配置
                logger.info("程序将退出，请检查功能配置文件后重启")
                quit(0)
            
            with open(self.config_path, "r", encoding="utf-8") as f:
                config_data = tomlkit.load(f)
            
            self.config = FeaturesConfig.from_dict(config_data)
            self._last_modified = self.config_path.stat().st_mtime
            logger.info(f"功能配置加载成功: {self.config_path}")
            return self.config
            
        except Exception as e:
            logger.error(f"功能配置加载失败: {e}")
            logger.critical("无法加载功能配置文件，程序退出")
            quit(1)
    
    def _create_default_config(self):
        """创建默认功能配置文件"""
        template_path = "template/features_template.toml"
        
        # 尝试从模板创建配置文件
        if create_config_from_template(
            str(self.config_path),
            template_path,
            "功能配置文件",
            should_exit=False  # 不在这里退出，由调用方决定
        ):
            return
        
        # 如果模板文件不存在，创建基本配置
        logger.info("模板文件不存在，创建基本功能配置")
        default_config = {
            "group_list_type": "whitelist",
            "group_list": [],
            "private_list_type": "whitelist",
            "private_list": [],
            "ban_user_id": [],
            "ban_qq_bot": False,
            "enable_poke": True,
            "ignore_non_self_poke": False,
            "poke_debounce_seconds": 3,
            "enable_reply_at": True,
            "reply_at_rate": 0.5,
            "enable_video_analysis": True,
            "max_video_size_mb": 100,
            "download_timeout": 60,
            "supported_formats": ["mp4", "avi", "mov", "mkv", "flv", "wmv", "webm"]
        }
        
        if not create_default_config_dict(default_config, str(self.config_path), "功能配置文件"):
            logger.critical("无法创建功能配置文件")
            quit(1)
    
    async def reload_config(self) -> bool:
        """重新加载配置文件"""
        try:
            if not self.config_path.exists():
                logger.warning(f"功能配置文件不存在，无法重载: {self.config_path}")
                return False
            
            current_modified = self.config_path.stat().st_mtime
            if self._last_modified and current_modified <= self._last_modified:
                return False  # 文件未修改
            
            old_config = self.config
            new_config = self.load_config()
            
            # 检查配置是否真的发生了变化
            if old_config and self._configs_equal(old_config, new_config):
                return False
            
            logger.info("功能配置已重载")
            await self._notify_callbacks()
            return True
            
        except Exception as e:
            logger.error(f"功能配置重载失败: {e}")
            return False
    
    def _configs_equal(self, config1: FeaturesConfig, config2: FeaturesConfig) -> bool:
        """比较两个配置是否相等"""
        return (
            config1.group_list_type == config2.group_list_type and
            set(config1.group_list) == set(config2.group_list) and
            config1.private_list_type == config2.private_list_type and
            set(config1.private_list) == set(config2.private_list) and
            set(config1.ban_user_id) == set(config2.ban_user_id) and
            config1.ban_qq_bot == config2.ban_qq_bot and
            config1.enable_poke == config2.enable_poke and
            config1.ignore_non_self_poke == config2.ignore_non_self_poke and
            config1.poke_debounce_seconds == config2.poke_debounce_seconds and
            config1.enable_reply_at == config2.enable_reply_at and
            config1.reply_at_rate == config2.reply_at_rate and
            config1.enable_video_analysis == config2.enable_video_analysis and
            config1.max_video_size_mb == config2.max_video_size_mb and
            config1.download_timeout == config2.download_timeout and
            set(config1.supported_formats) == set(config2.supported_formats)
        )
    
    async def start_file_watcher(self, check_interval: float = 1.0):
        """启动文件监控，定期检查配置文件变化"""
        if self._file_watcher_task and not self._file_watcher_task.done():
            logger.warning("文件监控已在运行")
            return
        
        self._file_watcher_task = asyncio.create_task(
            self._file_watcher_loop(check_interval)
        )
        logger.info(f"功能配置文件监控已启动，检查间隔: {check_interval}秒")
    
    async def stop_file_watcher(self):
        """停止文件监控"""
        if self._file_watcher_task and not self._file_watcher_task.done():
            self._file_watcher_task.cancel()
            try:
                await self._file_watcher_task
            except asyncio.CancelledError:
                pass
            logger.info("功能配置文件监控已停止")
    
    async def _file_watcher_loop(self, check_interval: float):
        """文件监控循环"""
        while True:
            try:
                await asyncio.sleep(check_interval)
                await self.reload_config()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"文件监控循环出错: {e}")
                await asyncio.sleep(check_interval)
    
    def get_config(self) -> FeaturesConfig:
        """获取当前功能配置"""
        if self.config is None:
            return self.load_config()
        return self.config
    
    def is_group_allowed(self, group_id: int) -> bool:
        """检查群聊是否被允许"""
        config = self.get_config()
        if config.group_list_type == "whitelist":
            return group_id in config.group_list
        else:  # blacklist
            return group_id not in config.group_list
    
    def is_private_allowed(self, user_id: int) -> bool:
        """检查私聊是否被允许"""
        config = self.get_config()
        if config.private_list_type == "whitelist":
            return user_id in config.private_list
        else:  # blacklist
            return user_id not in config.private_list
    
    def is_user_banned(self, user_id: int) -> bool:
        """检查用户是否被全局禁止"""
        config = self.get_config()
        return user_id in config.ban_user_id
    
    def is_qq_bot_banned(self) -> bool:
        """检查是否禁止QQ官方机器人"""
        config = self.get_config()
        return config.ban_qq_bot
    
    def is_poke_enabled(self) -> bool:
        """检查戳一戳功能是否启用"""
        config = self.get_config()
        return config.enable_poke
    
    def is_non_self_poke_ignored(self) -> bool:
        """检查是否忽略非自己戳一戳"""
        config = self.get_config()
        return config.ignore_non_self_poke


# 全局功能管理器实例
features_manager = FeaturesManager()