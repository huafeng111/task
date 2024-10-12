import os
import re
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from SpeechUpdater import SpeechUpdater  # Import the updater module
import SpeechParser  # Import the parser module

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SpeechDownloader:
    """
    This class is responsible for downloading Federal Reserve speeches and saving them locally.
    It will attempt to download speeches from the specified start year to the present, retrying failed downloads.
    """

    def __init__(self, base_folder, start_year=2017):
        self.base_folder = base_folder
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

    def download_speeches_parallel(self):
        logger.info("Starting download_speeches_parallel")
        with ThreadPoolExecutor(max_workers=5) as executor:
            current_year = datetime.now().year
            for year in range(self.start_year, current_year + 1):
                year_folder = os.path.join(self.base_folder, str(year))
                create_directory_if_not_exists(year_folder)
                executor.submit(self._process_year, year, year_folder)

    def _process_year(self, year, year_folder):
        speech_page_links = SpeechParser.fetch_speech_links_for_year(year)
        for speech_page_url in speech_page_links:
            full_page_url = f"https://www.federalreserve.gov{speech_page_url}"
            self._process_speech_page(full_page_url, year, year_folder)

    def _process_speech_page(self, page_url, year, year_folder):
        pdf_links, title, author, date = SpeechParser.fetch_pdf_links_from_speech_page(page_url)
        if not pdf_links:
            return

        # Download matching PDF files
        for pdf_url in pdf_links:
            if pdf_url.startswith("/"):
                pdf_url = f"https://www.federalreserve.gov{pdf_url}"
            self._download_speech_pdf(pdf_url, year, year_folder, title, author, date)

    def _download_speech_pdf(self, url, year, year_folder, title, author, date):
        response = SpeechParser.fetch_with_retries(url)
        if response is None:
            return

        clean_title = re.sub(r'[\\/*?:"<>|]', "", title)[:50]
        if not clean_title:
            clean_title = os.path.basename(url).split(".pdf")[0]

        filename = f"{clean_title}_{year}.pdf"
        save_path = os.path.join(year_folder, filename)

        with self.lock:
            if save_path in self.downloaded_files:
                logger.info(f"Speech {filename} already exists. Skipping download.")
                return
            self.downloaded_files.add(save_path)

        if not os.path.exists(save_path):
            with open(save_path, 'wb') as f:
                f.write(response.content)

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

    def save_metadata(self):
        logger.info("Saving metadata using updater...")
        # Use SpeechUpdater to update and save metadata
        metadata_file = os.path.join(self.base_folder, 'speech_metadata.csv')
        backup_folder = os.path.join(self.base_folder, 'backup_metadata')
        updater = SpeechUpdater(metadata_file=metadata_file, backup_folder=backup_folder)
        updater.update(self.speech_metadata)

# Helper Functions
def create_directory_if_not_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

if __name__ == "__main__":
    logger.info("Starting SpeechDownloader script")
    downloader = SpeechDownloader(base_folder='./data/pdfs', start_year=2024)
    downloader.download_speeches_parallel()
    downloader.save_metadata()
    logger.info("SpeechDownloader script finished")
