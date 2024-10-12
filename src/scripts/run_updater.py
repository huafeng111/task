import asyncio
from core.updater import update_speeches
from utils.logger import get_logger

logger = get_logger(__name__)

if __name__ == "__main__":
    logger.info("Starting speech update process...")
    asyncio.run(update_speeches())
    logger.info("Speech update process completed.")
