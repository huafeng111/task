import asyncio
import aiohttp
import sys
import os

# Add the directory that contains 'core' to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import FEDERAL_RESERVE_URLS, PDF_DIR
from logger import get_logger

logger = get_logger(__name__)

# Async function to fetch content from a URL
async def fetch_speech(session, url):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                content = await response.read()
                return content
            else:
                logger.error(f"Failed to fetch {url} with status: {response.status}")
                return None
    except Exception as e:
        logger.error(f"Error fetching {url}: {str(e)}")
        return None

# Download speeches from all URLs defined in the config
async def download_speeches():
    tasks = []
    async with aiohttp.ClientSession() as session:
        for url in FEDERAL_RESERVE_URLS.values():
            tasks.append(fetch_speech(session, url))
        speeches = await asyncio.gather(*tasks)
        return speeches

# Save PDF content to a file
def save_pdf(content, filename):
    filepath = os.path.join(PDF_DIR, filename)
    os.makedirs(PDF_DIR, exist_ok=True)  # Ensure the directory exists
    with open(filepath, 'wb') as f:
        f.write(content)
    logger.info(f"Saved PDF: {filename}")

# Function to fetch and save speeches
async def fetch_and_save_speeches():
    speeches = await download_speeches()
    for idx, speech in enumerate(speeches):
        if speech:
            filename = f"speech_{idx}.pdf"  # Customize filenames as needed
            save_pdf(speech, filename)
