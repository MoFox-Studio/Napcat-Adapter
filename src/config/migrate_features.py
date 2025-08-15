"""
功能配置迁移脚本
用于将旧的配置文件中的聊天、权限、视频处理等设置迁移到新的独立功能配置文件
"""

import os
import shutil
from pathlib import Path
import tomlkit
from ..logger import logger


def migrate_features_from_config(old_config_path: str = "config/config.toml",
                                new_features_path: str = "config/features.toml",
                                template_path: str = "template/features_template.toml"):
    """
    从旧配置文件迁移功能设置到新的功能配置文件
    
    Args:
        old_config_path: 旧配置文件路径
        new_features_path: 新功能配置文件路径
        template_path: 功能配置模板路径
    """
    try:
        # 检查旧配置文件是否存在
        if not os.path.exists(old_config_path):
            logger.warning(f"旧配置文件不存在: {old_config_path}")
            return False
        
        # 读取旧配置文件
        with open(old_config_path, "r", encoding="utf-8") as f:
            old_config = tomlkit.load(f)
        
        # 检查是否有chat配置段和video配置段
        chat_config = old_config.get("chat", {})
        video_config = old_config.get("video", {})
        
        # 检查是否有权限相关配置
        permission_keys = ["group_list_type", "group_list", "private_list_type", 
                          "private_list", "ban_user_id", "ban_qq_bot", 
                          "enable_poke", "ignore_non_self_poke", "poke_debounce_seconds"]
        video_keys = ["enable_video_analysis", "max_video_size_mb", "download_timeout", "supported_formats"]
        
        has_permission_config = any(key in chat_config for key in permission_keys)
        has_video_config = any(key in video_config for key in video_keys)
        
        if not has_permission_config and not has_video_config:
            logger.info("旧配置文件中没有找到功能相关配置，无需迁移")
            return False
        
        # 确保新功能配置目录存在
        new_features_dir = Path(new_features_path).parent
        new_features_dir.mkdir(parents=True, exist_ok=True)
        
        # 如果新功能配置文件已存在，先备份
        if os.path.exists(new_features_path):
            backup_path = f"{new_features_path}.backup"
            shutil.copy2(new_features_path, backup_path)
            logger.info(f"已备份现有功能配置文件到: {backup_path}")
        
        # 创建新的功能配置
        new_features_config = {
            "group_list_type": chat_config.get("group_list_type", "whitelist"),
            "group_list": chat_config.get("group_list", []),
            "private_list_type": chat_config.get("private_list_type", "whitelist"),
            "private_list": chat_config.get("private_list", []),
            "ban_user_id": chat_config.get("ban_user_id", []),
            "ban_qq_bot": chat_config.get("ban_qq_bot", False),
            "enable_poke": chat_config.get("enable_poke", True),
            "ignore_non_self_poke": chat_config.get("ignore_non_self_poke", False),
            "poke_debounce_seconds": chat_config.get("poke_debounce_seconds", 3),
            "enable_video_analysis": video_config.get("enable_video_analysis", True),
            "max_video_size_mb": video_config.get("max_video_size_mb", 100),
            "download_timeout": video_config.get("download_timeout", 60),
            "supported_formats": video_config.get("supported_formats", ["mp4", "avi", "mov", "mkv", "flv", "wmv", "webm"])
        }
        
        # 写入新的功能配置文件
        with open(new_features_path, "w", encoding="utf-8") as f:
            tomlkit.dump(new_features_config, f)
        
        logger.info(f"功能配置已成功迁移到: {new_features_path}")
        
        # 显示迁移的配置内容
        logger.info("迁移的配置内容:")
        for key, value in new_features_config.items():
            logger.info(f"  {key}: {value}")
        
        return True
        
    except Exception as e:
        logger.error(f"功能配置迁移失败: {e}")
        return False


def remove_features_from_old_config(config_path: str = "config/config.toml"):
    """
    从旧配置文件中移除功能相关配置，并将旧配置移动到 config/old/ 目录
    
    Args:
        config_path: 配置文件路径
    """
    try:
        if not os.path.exists(config_path):
            logger.warning(f"配置文件不存在: {config_path}")
            return False
        
        # 确保 config/old 目录存在
        old_config_dir = "config/old"
        os.makedirs(old_config_dir, exist_ok=True)
        
        # 备份原配置文件到 config/old 目录
        old_config_path = os.path.join(old_config_dir, "config_with_features.toml")
        shutil.copy2(config_path, old_config_path)
        logger.info(f"已备份包含功能配置的原文件到: {old_config_path}")
        
        # 读取配置文件
        with open(config_path, "r", encoding="utf-8") as f:
            config = tomlkit.load(f)
        
        # 移除chat段中的功能相关配置
        removed_keys = []
        if "chat" in config:
            chat_config = config["chat"]
            permission_keys = ["group_list_type", "group_list", "private_list_type",
                              "private_list", "ban_user_id", "ban_qq_bot",
                              "enable_poke", "ignore_non_self_poke", "poke_debounce_seconds"]
            
            for key in permission_keys:
                if key in chat_config:
                    del chat_config[key]
                    removed_keys.append(key)
            
            if removed_keys:
                logger.info(f"已从chat配置段中移除功能相关配置: {removed_keys}")
        
        # 移除video段中的配置
        if "video" in config:
            video_config = config["video"]
            video_keys = ["enable_video_analysis", "max_video_size_mb", "download_timeout", "supported_formats"]
            
            video_removed_keys = []
            for key in video_keys:
                if key in video_config:
                    del video_config[key]
                    video_removed_keys.append(key)
            
            if video_removed_keys:
                logger.info(f"已从video配置段中移除配置: {video_removed_keys}")
                removed_keys.extend(video_removed_keys)
            
            # 如果video段为空，则删除整个段
            if not video_config:
                del config["video"]
                logger.info("已删除空的video配置段")
        
        if removed_keys:
            logger.info(f"总共移除的配置项: {removed_keys}")
        
        # 写回配置文件
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(tomlkit.dumps(config))
        
        logger.info(f"已更新配置文件: {config_path}")
        return True
        
    except Exception as e:
        logger.error(f"移除功能配置失败: {e}")
        return False


def auto_migrate_features():
    """
    自动执行功能配置迁移
    """
    logger.info("开始自动功能配置迁移...")
    
    # 执行迁移
    if migrate_features_from_config():
        logger.info("功能配置迁移成功")
        
        # 询问是否要从旧配置文件中移除功能配置
        logger.info("功能配置已迁移到独立文件，建议从主配置文件中移除相关配置")
        # 在实际使用中，这里可以添加用户确认逻辑
        # 为了自动化，这里直接执行移除
        remove_features_from_old_config()
        
    else:
        logger.info("功能配置迁移跳过或失败")


if __name__ == "__main__":
    auto_migrate_features()