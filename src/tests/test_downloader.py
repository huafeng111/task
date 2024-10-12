import sys
import asyncio
import sys
import os

# Add the directory that contains 'core' to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from downloader import fetch_and_save_speeches
from logger import get_logger
import os

logger = get_logger(__name__)

def test_fetch_and_save_speeches():
    # Step 1: Start the downloader
    logger.info("Starting speech download test...")

    # Step 2: Run the asynchronous function
    asyncio.run(fetch_and_save_speeches())

    # Step 3: Check if the files are saved
    pdf_files = os.listdir('data/pdfs')

    if pdf_files:
        logger.info(f"Download successful. Files saved: {pdf_files}")
        print(f"Downloaded files: {pdf_files}")
    else:
        logger.error("No files downloaded.")
        print("No files downloaded.")

if __name__ == "__main__":
    test_fetch_and_save_speeches()
