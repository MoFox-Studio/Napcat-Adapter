import os
from dataclasses import dataclass
from datetime import datetime

import tomlkit
import shutil

from tomlkit import TOMLDocument
from tomlkit.items import Table
from ..logger import logger
from rich.traceback import install

from src.config.config_base import ConfigBase
from src.config.official_configs import (
    DebugConfig,
    MaiBotServerConfig,
    NapcatServerConfig,
    NicknameConfig,
    VoiceConfig,
)

install(extra_lines=3)

TEMPLATE_DIR = "template"
CONFIG_DIR = "config"
OLD_CONFIG_DIR = "config/old"


def ensure_config_directories():
    """确保配置目录存在"""
    os.makedirs(CONFIG_DIR, exist_ok=True)
    os.makedirs(OLD_CONFIG_DIR, exist_ok=True)


def update_config():
    """更新配置文件，统一使用 config/old 目录进行备份"""
    # 确保目录存在
    ensure_config_directories()
    
    # 定义文件路径
    template_path = f"{TEMPLATE_DIR}/template_config.toml"
    config_path = f"{CONFIG_DIR}/config.toml"

    # 检查配置文件是否存在
    if not os.path.exists(config_path):
        logger.info("主配置文件不存在，从模板创建新配置")
        shutil.copy2(template_path, config_path)
        logger.info(f"已创建新配置文件: {config_path}")
        logger.info("程序将退出，请检查配置文件后重启")

    # 读取配置文件和模板文件
    with open(config_path, "r", encoding="utf-8") as f:
        old_config = tomlkit.load(f)
    with open(template_path, "r", encoding="utf-8") as f:
        new_config = tomlkit.load(f)

    # 检查version是否相同
    if old_config and "inner" in old_config and "inner" in new_config:
        old_version = old_config["inner"].get("version")
        new_version = new_config["inner"].get("version")
        if old_version and new_version and old_version == new_version:
            logger.info(f"检测到配置文件版本号相同 (v{old_version})，跳过更新")
            return
        else:
            logger.info(f"检测到版本号不同: 旧版本 v{old_version} -> 新版本 v{new_version}")
    else:
        logger.info("已有配置文件未检测到版本号，可能是旧版本。将进行更新")

    # 创建备份文件
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(OLD_CONFIG_DIR, f"config.toml.bak.{timestamp}")

    # 备份旧配置文件
    shutil.copy2(config_path, backup_path)
    logger.info(f"已备份旧配置文件到: {backup_path}")

    # 复制模板文件到配置目录
    shutil.copy2(template_path, config_path)
    logger.info(f"已创建新配置文件: {config_path}")

    def update_dict(target: TOMLDocument | dict, source: TOMLDocument | dict):
        """将source字典的值更新到target字典中（如果target中存在相同的键）"""
        for key, value in source.items():
            # 跳过version字段的更新
            if key == "version":
                continue
            if key in target:
                if isinstance(value, dict) and isinstance(target[key], (dict, Table)):
                    update_dict(target[key], value)
                else:
                    try:
                        # 对数组类型进行特殊处理
                        if isinstance(value, list):
                            # 如果是空数组，确保它保持为空数组
                            target[key] = tomlkit.array(str(value)) if value else tomlkit.array()
                        else:
                            # 其他类型使用item方法创建新值
                            target[key] = tomlkit.item(value)
                    except (TypeError, ValueError):
                        # 如果转换失败，直接赋值
                        target[key] = value

    # 将旧配置的值更新到新配置中
    logger.info("开始合并新旧配置...")
    update_dict(new_config, old_config)

    # 保存更新后的配置（保留注释和格式）
    with open(config_path, "w", encoding="utf-8") as f:
        f.write(tomlkit.dumps(new_config))
    logger.info("配置文件更新完成，建议检查新配置文件中的内容，以免丢失重要信息")


@dataclass
class Config(ConfigBase):
    """总配置类"""

    nickname: NicknameConfig
    napcat_server: NapcatServerConfig
    maibot_server: MaiBotServerConfig
    voice: VoiceConfig
    debug: DebugConfig


def load_config(config_path: str) -> Config:
    """
    加载配置文件
    :param config_path: 配置文件路径
    :return: Config对象
    """
    # 读取配置文件
    with open(config_path, "r", encoding="utf-8") as f:
        config_data = tomlkit.load(f)

    # 创建Config对象
    try:
        return Config.from_dict(config_data)
    except Exception as e:
        logger.critical("配置文件解析失败")
        raise e


# 更新配置
update_config()

logger.info("正在品鉴配置文件...")
global_config = load_config(config_path=f"{CONFIG_DIR}/config.toml")
logger.info("非常的新鲜，非常的美味！")
