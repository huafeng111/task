# C:\Users\admin\Documents\GitHub\task\src\core\BaseDownloader.py
import os
import requests
import logging
import time
from requests.exceptions import HTTPError
from bs4 import BeautifulSoup

# Configure logging for base downloader
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BaseDownloader:
    def __init__(self, base_folder):
        self.base_folder = base_folder
        self.metadata = []

        # Ensure base folder exists
        self.create_directory_if_not_exists(self.base_folder)

    def create_directory_if_not_exists(self, directory):
        if not os.path.exists(directory):
            os.makedirs(directory)
            logger.info(f"Created directory: {directory}")

    def fetch_with_retries(self, url, retries=3, delay=5):
        for i in range(retries):
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                return response
            except (HTTPError, requests.exceptions.ConnectionError) as e:
                logger.warning(f"Attempt {i+1} failed for {url}: {e}. Retrying in {delay} seconds...")
                time.sleep(delay)
        return None

    def _get_soup_from_url(self, url):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")
        except Exception as err:
            logger.error(f"Error occurred: {err}")
        return None

    # Abstract method: Must be implemented by child classes
    def download(self):
        raise NotImplementedError("Subclasses must implement this method.")
