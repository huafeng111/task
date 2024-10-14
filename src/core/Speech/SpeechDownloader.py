import importlib
import os
import re
import logging
import sys
import time
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from logging.handlers import RotatingFileHandler
from threading import Lock

import requests
from tqdm import tqdm

from SpeechUpdater import SpeechUpdater  # Import the updater module
import SpeechParser  # Import the parser module

def dynamic_import(module_name, module_path):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

# Assuming the relative path to config.py from the current script
config_module_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "config", "config.py")
config = dynamic_import("config", config_module_path)

# Configure logging to file and console
log_filename = os.path.join(os.path.dirname(__file__), config.LOG_FILE)  # Use relative path for log file
handler = RotatingFileHandler(log_filename, maxBytes=config.LOG_MAX_BYTES,
                              backupCount=config.LOG_BACKUP_COUNT)  # Max file size 5MB, keep 3 backups
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Set handler and formatter
handler.setFormatter(formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Create a global logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Check if logger already has handlers to avoid duplicate logs
if not logger.hasHandlers():
    logger.addHandler(handler)
    logger.addHandler(console_handler)

class SpeechDownloader:
    """
    This class is responsible for downloading Federal Reserve speeches and saving them locally.
    It will attempt to download speeches from the specified start year to the present, retrying failed downloads.
    """

    STATE_FILE = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'pdfs', 'download_state.json')

    def __init__(self, base_folder=os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'pdfs'), start_year=config.START_YEAR):
        self.base_folder = os.path.abspath(base_folder)
        self.start_year = start_year
        self.speech_metadata = []
        self.downloaded_files = set()
        self.lock = Lock()

        # Ensure the base folder exists
        create_directory_if_not_exists(self.base_folder)

        # Initialize downloaded_files with existing files
        for root, dirs, files in os.walk(self.base_folder):
            for file in files:
                if file.endswith('.pdf'):
                    self.downloaded_files.add(os.path.join(root, file))

        # Load the last download state
        self.last_year = self.load_last_year()

    def load_last_year(self):
        """
        Load the last downloaded year from the state file.
        If the file does not exist, return the start_year from the config.
        """
        if os.path.exists(self.STATE_FILE):
            try:
                with open(self.STATE_FILE, 'r') as f:
                    state = json.load(f)
                    return state.get('last_year', self.start_year)
            except json.JSONDecodeError:
                logger.error("Failed to decode state file. Starting from configured start_year.")
                return self.start_year
        return self.start_year

    def save_last_year(self, year):
        """
        Save the last successfully downloaded year to the state file.
        """
        with open(self.STATE_FILE, 'w') as f:
            json.dump({'last_year': year}, f)

    def download_speeches_parallel(self):
        try:
            logger.info("Starting download_speeches_parallel")
            with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
                current_year = datetime.now().year
                years = range(self.last_year, current_year + 1)  # Start from the last downloaded year
                with tqdm(total=len(years)) as pbar:
                    for year in years:
                        year_folder = os.path.join(self.base_folder, str(year))
                        create_directory_if_not_exists(year_folder)
                        executor.submit(self._process_year, year, year_folder)
                        pbar.update(1)
                        self.save_last_year(year)  # Save the progress after processing each year
        except Exception as e:
            logger.error(f"Unexpected error during parallel speech download: {e}", exc_info=True)

    def _process_year(self, year, year_folder):
        try:
            speech_page_links = SpeechParser.fetch_speech_links_for_year(year)
            total_links = len(speech_page_links)  # 获取该年份下的URL数量
            logger.info(f"Year {year} has {total_links} speech page links.")

            if total_links == 0:
                logger.info(f"No speech page links found for year {year}.")
                return

            for speech_page_url in speech_page_links:
                full_page_url = f"https://www.federalreserve.gov{speech_page_url}"
                self._process_speech_page(full_page_url, year, year_folder)
        except Exception as e:
            error_message = f"Failed to fetch speech links for year {year}: {e}"
            logger.error(error_message, exc_info=True)
            log_error("Fetch speech links error", error_message, url=f"https://www.federalreserve.gov/speeches-{year}.htm")


    def _process_speech_page(self, page_url, year, year_folder):
        try:
            pdf_links, title, author, date = SpeechParser.fetch_pdf_links_from_speech_page(page_url)
            if not pdf_links:
                error_message = f"No PDF links found for page: {page_url}"
                logger.info(error_message)
                log_error("No PDF links", error_message, url=page_url)
                return

            # Format the date to 'YYYY-MM-DD' from 'December 05, 2023'
            date = self.format_date(date)

            # Download matching PDF files
            for pdf_url in pdf_links:
                if pdf_url.startswith("/"):
                    pdf_url = f"https://www.federalreserve.gov{pdf_url}"
                self._download_speech_pdf(pdf_url, year, year_folder, title, author, date)
        except Exception as e:
            error_message = f"Error processing speech page {page_url}: {e}"
            logger.error(error_message, exc_info=True)
            log_error("Process speech page error", error_message, url=page_url)

    def _download_speech_pdf(self, url, year, year_folder, title, author, date, retries=5, delay=5, backoff_factor=2):
        """
        Download a PDF file with a retry mechanism and improved SSL error handling.
        :param url: PDF file URL.
        :param year: Year of the speech.
        :param year_folder: Folder to save the PDF.
        :param title: Title of the speech.
        :param author: Author of the speech.
        :param date: Date of the speech.
        :param retries: Number of retry attempts.
        :param delay: Initial delay in seconds between retries.
        :param backoff_factor: Factor by which delay increases after each retry.
        """
        for attempt in range(retries):
            try:
                logger.info(f"Attempting to download PDF (Attempt {attempt+1}): {url}")
                response = SpeechParser.fetch_with_retries(url)  # Use your fetch method here
                if response is None:
                    raise requests.exceptions.RequestException(f"Failed to download PDF: {url}")

                clean_title = re.sub(r'[\\/*?:"<>|]', "", title)[:50]
                if not clean_title:
                    clean_title = os.path.basename(url).split(".pdf")[0]

                filename = f"{clean_title}_{date}.pdf"

                # Concatenate absolute path
                absolute_save_path = os.path.join(year_folder, filename)

                # Keep the relative path relative to base_folder
                save_path = os.path.relpath(absolute_save_path, self.base_folder)

                with self.lock:
                    if save_path in self.downloaded_files:
                        logger.info(f"Speech {filename} already exists. Skipping download.")
                        return
                    self.downloaded_files.add(save_path)

                if not os.path.exists(absolute_save_path):
                    with open(absolute_save_path, 'wb') as f:
                        f.write(response.content)
                    logger.info(f"Downloaded and saved speech to {save_path}")

                metadata = {
                    'url': url,
                    'year': year,
                    'title': title,
                    'author': author,
                    'date': date,
                    'file_path': save_path,
                }

                with self.lock:
                    self.speech_metadata.append(metadata)

                # If download is successful, break out of the retry loop
                return

            except requests.exceptions.SSLError as ssl_err:
                error_message = f"SSL error occurred on attempt {attempt+1} for URL '{url}': {ssl_err}"
                logger.warning(error_message)
                if attempt < retries - 1:
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                    delay *= backoff_factor  # Exponentially increase delay on each retry
                else:
                    logger.error(f"Failed to download PDF after {retries} attempts due to SSL error: {url}")
                    log_error("PDF download failed due to SSL error", error_message, url=url)

            except requests.exceptions.RequestException as e:
                error_message = f"Error downloading PDF: {e}"
                logger.error(error_message)
                if attempt < retries - 1:
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                    delay *= backoff_factor  # Exponentially increase delay on each retry
                else:
                    logger.error(f"Failed to download PDF after {retries} attempts: {url}")
                    log_error("PDF download failed", error_message, url=url)


    def save_metadata(self):
        try:
            logger.info("Saving metadata using updater...")

            # Sort metadata by 'date' before saving (assume 'date' is in 'YYYY-MM-DD' format)
            self.speech_metadata.sort(key=lambda x: x['date'])

            # Deduplicate metadata by 'title'
            seen_titles = set()
            unique_metadata = []
            for item in self.speech_metadata:
                if item['title'] not in seen_titles:
                    unique_metadata.append(item)
                    seen_titles.add(item['title'])

            # Use SpeechUpdater to update and save metadata after sorting and deduplication
            metadata_file = os.path.join(self.base_folder, 'speech_metadata.csv')
            backup_folder = os.path.join(self.base_folder, 'backup_metadata')
            updater = SpeechUpdater(metadata_file=metadata_file, backup_folder=backup_folder)
            updater.update(unique_metadata)
        except Exception as e:
            logger.error(f"Unexpected error while saving metadata: {e}", exc_info=True)

    @staticmethod
    def format_date(date_str):
        """
        Convert 'December 05, 2023' format to 'YYYY-MM-DD' format.
        """
        try:
            # Convert the date string to a datetime object
            date_obj = datetime.strptime(date_str, '%B %d, %Y')
            # Format the datetime object into 'YYYY-MM-DD' format
            return date_obj.strftime('%Y-%m-%d')
        except ValueError:
            logger.error(f"Invalid date format: {date_str}")
            return None


# Helper Functions
def create_directory_if_not_exists(directory):
    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
        except OSError as e:
            logger.error(f"Failed to create directory {directory}: {e}", exc_info=True)

def log_error(error_type, message, url=None):
    error_file = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'pdfs', 'errors.json')

    error_data = {
        'error_type': error_type,
        'message': message,
        'url': url,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    # 如果文件存在，则加载现有错误，否则创建新的
    if os.path.exists(error_file):
        try:
            with open(error_file, 'r') as f:
                errors = json.load(f)
        except json.JSONDecodeError:
            errors = []
    else:
        errors = []

    errors.append(error_data)

    # 写入错误日志文件
    with open(error_file, 'w') as f:
        json.dump(errors, f, indent=4)


if __name__ == "__main__":
    try:
        logger.info("Starting SpeechDownloader script")
        downloader = SpeechDownloader(base_folder=os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'pdfs'), start_year=config.START_YEAR)
        downloader.download_speeches_parallel()
        downloader.save_metadata()
        logger.info("SpeechDownloader script finished")
    except Exception as e:
        logger.error(f"Unexpected error in main execution: {e}", exc_info=True)
