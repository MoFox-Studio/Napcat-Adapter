#!/usr/bin/env python3
"""Adapter项目的主入口点"""

if __name__ == "__main__":
    import sys
    import asyncio
    from pathlib import Path
    
    # 添加当前目录到Python路径，这样可以识别src包
    current_dir = Path(__file__).parent
    sys.path.insert(0, str(current_dir))
    
    # 导入main模块
    import main
    
    # 手动执行main函数
    try:
        asyncio.run(main.main())
    except KeyboardInterrupt:
        from src.logger import logger
        logger.warning("收到中断信号，正在优雅关闭...")
        # 在KeyboardInterrupt时不需要额外调用graceful_shutdown，
        # 因为main.main()中的asyncio.gather被取消时会自然触发清理
    except Exception as e:
        from src.logger import logger
        logger.exception(f"主程序异常: {str(e)}")
        sys.exit(1)


# 这个文件是为了适配一键包使用的，在一键包项目之外没有用