import os
import requests
import re
import pandas as pd
import logging
import time
from bs4 import BeautifulSoup
from datetime import datetime
from requests.exceptions import HTTPError
from concurrent.futures import ThreadPoolExecutor
from SpeechUpdater import SpeechUpdater  # 导入更新模块
from threading import Lock  # 用于线程安全

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
        self.downloaded_files = set()  # 用于记录已下载的文件，防止重复下载
        self.lock = Lock()  # 线程锁，确保线程安全

        # Ensure the base folder exists
        create_directory_if_not_exists(self.base_folder)

    def download_speeches_parallel(self):
        logger.info("Starting download_speeches_parallel")
        with ThreadPoolExecutor(max_workers=5) as executor:
            current_year = datetime.now().year
            for year in range(self.start_year, current_year + 1):
                year_folder = os.path.join(self.base_folder, str(year))
                create_directory_if_not_exists(year_folder)
                executor.submit(self._fetch_speech_links_for_year, year, year_folder)

    def _fetch_speech_links_for_year(self, year, year_folder):
        base_url = f"https://www.federalreserve.gov/newsevents/speech/{year}-speeches.htm"
        soup = _get_soup_from_url(base_url)
        if not soup:
            return

        # 获取所有指向演讲详情页面的链接
        speech_page_links = [
            link['href'] for link in soup.find_all('a', href=True)
            if link['href'].startswith("/newsevents/speech/") and link['href'].endswith(".htm")
        ]

        for speech_page_url in speech_page_links:
            full_page_url = f"https://www.federalreserve.gov{speech_page_url}"
            self._fetch_pdf_links_from_speech_page(full_page_url, year, year_folder)

    def _fetch_pdf_links_from_speech_page(self, page_url, year, year_folder):
        """
        从特定的演讲页面中提取与该演讲相关的 PDF 链接。
        只会提取与页面 URL 部分匹配的 PDF 文件。
        """
        soup = _get_soup_from_url(page_url)
        if not soup:
            return

        # 提取页面的文件名（用于匹配 .pdf 文件）
        page_filename = os.path.basename(page_url).split(".htm")[0]  # 例如 bowman20231205a

        title_tag = soup.find('h3', class_='title')
        title = title_tag.find('em').get_text(strip=True) if title_tag and title_tag.find('em') else "No Title"

        author_tag = soup.find('p', class_='speaker')
        author = author_tag.get_text(strip=True) if author_tag else "Unknown Author"

        date_tag = soup.find('p', class_='article__time')
        date = date_tag.get_text(strip=True) if date_tag else "Unknown Date"

        # 搜索与当前演讲页面相关的 .pdf 链接
        pdf_links = [
            link['href'] for link in soup.find_all('a', href=True)
            if link['href'].endswith(".pdf") and page_filename in link['href']
        ]

        # 下载匹配的 PDF 文件
        for pdf_url in pdf_links:
            if pdf_url.startswith("/"):
                pdf_url = f"https://www.federalreserve.gov{pdf_url}"
            self._download_speech_pdf(pdf_url, year, year_folder, title, author, date)

    def _download_speech_pdf(self, url, year, year_folder, title, author, date):
        response = fetch_with_retries(url)
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
            self.speech_metadata.append(metadata)


    def save_metadata(self):
        logger.info("Saving metadata using updater...")
        # 使用 SpeechUpdater 进行元数据的更新和保存
        metadata_file = os.path.join(self.base_folder, 'speech_metadata.csv')
        backup_folder = os.path.join(self.base_folder, 'backup_metadata')
        updater = SpeechUpdater(metadata_file=metadata_file, backup_folder=backup_folder)
        updater.update(self.speech_metadata)

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
    return None

if __name__ == "__main__":
    logger.info("Starting SpeechDownloader script")
    downloader = SpeechDownloader(base_folder='./data/pdfs', start_year=2024)
    downloader.download_speeches_parallel()
    downloader.save_metadata()
    logger.info("SpeechDownloader script finished")
