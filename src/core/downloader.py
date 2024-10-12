import os
import requests
import re
import pandas as pd
import logging
import uuid
import time
from bs4 import BeautifulSoup
from datetime import datetime
from requests.exceptions import HTTPError
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SpeechDownloader:
    """
    This class is responsible for downloading Federal Reserve speeches and saving them locally.
    It will attempt to download speeches from 2017 to the present, retrying failed downloads.
    """

    def __init__(self, base_folder, start_year=2017):
        self.base_folder = base_folder
        self.start_year = start_year
        self.speech_metadata = []

        # Ensure the base folder exists
        create_directory_if_not_exists(self.base_folder)

    def download_speeches_parallel(self):
        """
        Main function to download speeches from 2017 to the present in parallel.
        """
        with ThreadPoolExecutor(max_workers=5) as executor:
            current_year = datetime.now().year
            for year in range(self.start_year, current_year + 1):
                year_folder = os.path.join(self.base_folder, str(year))
                create_directory_if_not_exists(year_folder)
                logger.info(f"Submitting download tasks for {year}...")
                executor.submit(self._fetch_speech_links_for_year, year, year_folder)

    def _fetch_speech_links_for_year(self, year, year_folder):
        """
        Fetches speech page links for a given year from the Federal Reserve website.
        """
        base_url = f"https://www.federalreserve.gov/newsevents/speech/{year}-speeches.htm"
        soup = _get_soup_from_url(base_url)

        if not soup:
            return

        # Extract .htm links that point to speech details
        speech_page_links = [
            link['href'] for link in soup.find_all('a', href=True)
            if link['href'].startswith("/newsevents/speech/") and link['href'].endswith(".htm")
        ]

        logger.info(f"Found {len(speech_page_links)} speech pages for {year}.")

        # Visit each speech page to extract the actual PDF links
        for speech_page_url in speech_page_links:
            full_page_url = f"https://www.federalreserve.gov{speech_page_url}"
            self._fetch_pdf_links_from_speech_page(full_page_url, year, year_folder)

    def _fetch_pdf_links_from_speech_page(self, page_url, year, year_folder):
        """
        Fetches PDF links from a specific speech page and downloads them.
        """
        soup = _get_soup_from_url(page_url)
        if not soup:
            return

        # Extract speech title and author for metadata
        title_tag = soup.find('h3', class_='title')
        title = title_tag.get_text(strip=True) if title_tag else "No Title"

        author_tag = soup.find('p', class_='author')
        author = author_tag.get_text(strip=True) if author_tag else "Unknown Author"

        # Extract .pdf links from the speech page
        pdf_links = [
            link['href'] for link in soup.find_all('a', href=True) if link['href'].endswith(".pdf")
        ]

        # Download each PDF
        for pdf_url in pdf_links:
            if pdf_url.startswith("/"):
                pdf_url = f"https://www.federalreserve.gov{pdf_url}"
            self._download_speech_pdf(pdf_url, year, year_folder, title, author)

    def _download_speech_pdf(self, url, year, year_folder, title, author):
        """
        Downloads a PDF speech from the given URL and saves it in the specified year folder.
        """
        response = fetch_with_retries(url)
        if response is None:
            return

        # Extract the filename from the URL
        filename = f"{uuid.uuid4()}_speech_{year}.pdf"
        save_path = os.path.join(year_folder, filename)

        if not os.path.exists(save_path):
            with open(save_path, 'wb') as f:
                f.write(response.content)
            logger.info(f"Downloaded {filename} to {save_path}")

            # Collect metadata
            metadata = {
                'url': url,
                'year': year,
                'title': title,
                'author': author,
                'file_path': save_path,
            }
            self.speech_metadata.append(metadata)

            # Log metadata for debugging
            logger.debug(f"Collected metadata: {metadata}")
        else:
            logger.info(f"Speech {filename} already exists. Skipping download.")

    def save_metadata(self):
        """
        Saves the metadata of the downloaded speeches as a CSV file.
        """
        if self.speech_metadata:
            metadata_df = pd.DataFrame(self.speech_metadata)
            metadata_file = os.path.join(self.base_folder, 'speech_metadata.csv')
            metadata_df.to_csv(metadata_file, index=False)
            logger.info(f"Metadata saved to {metadata_file}")
        else:
            logger.info("No metadata to save.")

# Helper Functions
def create_directory_if_not_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def _get_soup_from_url(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'html.parser')
    except HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")
    except Exception as err:
        logger.error(f"Error occurred: {err}")
    return None

def fetch_with_retries(url, retries=3, delay=5):
    for i in range(retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response
        except (HTTPError, requests.exceptions.ConnectionError) as e:
            logger.warning(f"Attempt {i+1} failed for {url}: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
    logger.error(f"Failed to fetch URL after {retries} retries: {url}")
    return None

if __name__ == "__main__":
    # Example of usage: Download speeches from 2017 to the present and save to data/pdfs
    downloader = SpeechDownloader(base_folder='../data/pdfs', start_year=2017)
    downloader.download_speeches_parallel()
    downloader.save_metadata()