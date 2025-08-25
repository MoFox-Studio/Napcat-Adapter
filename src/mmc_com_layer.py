from maim_message import Router, RouteConfig, TargetConfig
from .config import global_config
from .logger import logger, custom_logger
from .send_handler import send_handler

route_config = RouteConfig(
    route_config={
        global_config.maibot_server.platform_name: TargetConfig(
            url=f"ws://{global_config.maibot_server.host}:{global_config.maibot_server.port}/ws",
            token=None,
        )
    }
)
router = Router(route_config, custom_logger)


async def mmc_start_com():
    logger.info("正在连接MaiBot")
    router.register_class_handler(send_handler.handle_message)
    await router.run()


async def mmc_stop_com():
    """停止 MaiBot 通信连接"""
    try:
        await router.stop()
        logger.info("MaiBot 连接已关闭")
    except Exception as e:
        logger.warning(f"关闭 MaiBot 连接时出错: {e}")
        # 不重新抛出异常，允许优雅关闭继续进行
