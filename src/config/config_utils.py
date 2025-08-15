"""
配置文件工具模块
提供统一的配置文件生成和管理功能
"""
import os
import shutil
from pathlib import Path
from datetime import datetime
from typing import Optional

from ..logger import logger


def ensure_config_directories():
    """确保配置目录存在"""
    os.makedirs("config", exist_ok=True)
    os.makedirs("config/old", exist_ok=True)


def create_config_from_template(
    config_path: str,
    template_path: str,
    config_name: str = "配置文件",
    should_exit: bool = True
) -> bool:
    """
    从模板创建配置文件的统一函数
    
    Args:
        config_path: 配置文件路径
        template_path: 模板文件路径
        config_name: 配置文件名称（用于日志显示）
        should_exit: 创建后是否退出程序
        
    Returns:
        bool: 是否成功创建配置文件
    """
    try:
        # 确保配置目录存在
        ensure_config_directories()
        
        config_path_obj = Path(config_path)
        template_path_obj = Path(template_path)
        
        # 检查配置文件是否存在
        if config_path_obj.exists():
            return False  # 配置文件已存在，无需创建
        
        logger.info(f"{config_name}不存在，从模板创建新配置")
        
        # 检查模板文件是否存在
        if not template_path_obj.exists():
            logger.error(f"模板文件不存在: {template_path}")
            if should_exit:
                logger.critical("无法创建配置文件，程序退出")
                quit(1)
            return False
        
        # 确保配置文件目录存在
        config_path_obj.parent.mkdir(parents=True, exist_ok=True)
        
        # 复制模板文件到配置目录
        shutil.copy2(template_path_obj, config_path_obj)
        logger.info(f"已创建新{config_name}: {config_path}")
        
        if should_exit:
            logger.info("程序将退出，请检查配置文件后重启")
            quit(0)
        
        return True
        
    except Exception as e:
        logger.error(f"创建{config_name}失败: {e}")
        if should_exit:
            logger.critical("无法创建配置文件，程序退出")
            quit(1)
        return False


def create_default_config_dict(default_values: dict, config_path: str, config_name: str = "配置文件") -> bool:
    """
    创建默认配置文件（使用字典数据）
    
    Args:
        default_values: 默认配置值字典
        config_path: 配置文件路径
        config_name: 配置文件名称（用于日志显示）
        
    Returns:
        bool: 是否成功创建配置文件
    """
    try:
        import tomlkit
        
        config_path_obj = Path(config_path)
        
        # 确保配置文件目录存在
        config_path_obj.parent.mkdir(parents=True, exist_ok=True)
        
        # 写入默认配置
        with open(config_path_obj, "w", encoding="utf-8") as f:
            tomlkit.dump(default_values, f)
        
        logger.info(f"已创建默认{config_name}: {config_path}")
        return True
        
    except Exception as e:
        logger.error(f"创建默认{config_name}失败: {e}")
        return False


def backup_config_file(config_path: str, backup_dir: str = "config/old") -> Optional[str]:
    """
    备份配置文件
    
    Args:
        config_path: 要备份的配置文件路径
        backup_dir: 备份目录
        
    Returns:
        Optional[str]: 备份文件路径，失败时返回None
    """
    try:
        config_path_obj = Path(config_path)
        if not config_path_obj.exists():
            return None
        
        # 确保备份目录存在
        backup_dir_obj = Path(backup_dir)
        backup_dir_obj.mkdir(parents=True, exist_ok=True)
        
        # 创建备份文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"{config_path_obj.stem}.toml.bak.{timestamp}"
        backup_path = backup_dir_obj / backup_filename
        
        # 备份文件
        shutil.copy2(config_path_obj, backup_path)
        logger.info(f"已备份配置文件到: {backup_path}")
        
        return str(backup_path)
        
    except Exception as e:
        logger.error(f"备份配置文件失败: {e}")
        return None