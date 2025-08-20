from .grpc_manager import grpc_manager
from .config import global_config
from .logger import logger
from .send_handler import send_handler


async def mmc_start_com():
    logger.info("正在连接MaiBot")
    await grpc_manager.start_connection(send_handler.handle_message)


async def mmc_stop_com():
    await grpc_manager.stop_connection()
