import asyncio

from downloader import fetch_and_save_speeches
from logger import get_logger

logger = get_logger(__name__)

if __name__ == "__main__":
    logger.info("Starting speech download process...")
    asyncio.run(fetch_and_save_speeches())  # Asynchronously fetch and save speeches
    logger.info("Speech download process completed.")
