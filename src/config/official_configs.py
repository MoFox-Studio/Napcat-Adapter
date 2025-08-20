from dataclasses import dataclass, field
from typing import Literal

from src.config.config_base import ConfigBase

"""
须知：
1. 本文件中记录了所有的配置项
2. 所有新增的class都需要继承自ConfigBase
3. 所有新增的class都应在config.py中的Config类中添加字段
4. 对于新增的字段，若为可选项，则应在其后添加field()并设置default_factory或default
"""

ADAPTER_PLATFORM = "qq"


@dataclass
class NicknameConfig(ConfigBase):
    nickname: str
    """机器人昵称"""


@dataclass
class NapcatServerConfig(ConfigBase):
    mode: Literal["reverse", "forward"] = "reverse"
    """连接模式：reverse=反向连接(作为服务器), forward=正向连接(作为客户端)"""

    host: str = "localhost"
    """主机地址"""

    port: int = 8095
    """端口号"""

    url: str = ""
    """正向连接时的完整WebSocket URL，如 ws://localhost:8080/ws"""

    access_token: str = ""
    """WebSocket 连接的访问令牌，用于身份验证"""

    heartbeat_interval: int = 30
    """心跳间隔时间，单位为秒"""


@dataclass
class MaiBotServerConfig(ConfigBase):
    host: str = "localhost"
    """MaiBot 服务器的主机地址"""

    port: int = 8090
    """MaiBot 服务器的端口号（gRPC 端口）"""
    
    @property
    def platform_name(self) -> str:
        """平台名称，"qq" """
        return ADAPTER_PLATFORM


@dataclass
class VoiceConfig(ConfigBase):
    use_tts: bool = False
    """是否启用TTS功能"""


@dataclass
class DebugConfig(ConfigBase):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    """日志级别，默认为INFO"""
