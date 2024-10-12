import os
import requests
import re
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from requests.exceptions import HTTPError

class SpeechDownloader:
    """
    This class is responsible for downloading Federal Reserve speeches and saving them locally.
    It will attempt to download speeches from 2017 to the present, retrying failed downloads.
    """

    def __init__(self, base_folder, start_year=2017):
        self.base_folder = base_folder
        self.start_year = start_year
        self.speech_metadata = []

        # Make sure the necessary folders exist
        if not os.path.exists(os.path.join(self.base_folder, 'raw_data')):
            os.makedirs(os.path.join(self.base_folder, 'raw_data'))

    def download_speeches(self):
        """
        Main function to download speeches from 2017 to present.
        """
        current_year = datetime.now().year
        for year in range(self.start_year, current_year + 1):
            print(f"Downloading speeches for {year}...")
            try:
                self._fetch_speech_links_for_year(year)
            except Exception as e:
                print(f"Error fetching speeches for {year}: {e}")

    def _fetch_speech_links_for_year(self, year):
        """
        Fetches speech page links for a given year from the Federal Reserve website.
        """
        base_url = f"https://www.federalreserve.gov/newsevents/speech/{year}-speeches.htm"

        try:
            response = requests.get(base_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            # Extract .htm links that point to speech details
            speech_page_links = []
            for link in soup.find_all('a', href=True):
                if link['href'].startswith("/newsevents/speech/") and link['href'].endswith(".htm"):
                    speech_page_links.append(link['href'])

            # Print out speech page links for debugging
            print(f"Found {len(speech_page_links)} speech pages for {year}.")

            # Visit each speech page to extract the actual PDF links
            for speech_page_url in speech_page_links:
                full_page_url = f"https://www.federalreserve.gov{speech_page_url}"
                self._fetch_pdf_links_from_speech_page(full_page_url, year)

        except HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except Exception as err:
            print(f"Other error occurred: {err}")

    def _fetch_pdf_links_from_speech_page(self, page_url, year):
        """
        Fetches PDF links from a specific speech page and downloads them.
        """
        try:
            response = requests.get(page_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            # Extract .pdf links from the speech page
            pdf_links = []
            for link in soup.find_all('a', href=True):
                if link['href'].endswith(".pdf"):
                    pdf_links.append(link['href'])

            # Download each PDF
            for pdf_url in pdf_links:
                if pdf_url.startswith("/"):
                    pdf_url = f"https://www.federalreserve.gov{pdf_url}"
                self._download_speech_pdf(pdf_url, year)

        except HTTPError as http_err:
            print(f"HTTP error occurred while fetching PDF links from {page_url}: {http_err}")
        except Exception as err:
            print(f"Other error occurred: {err}")

    def _download_speech_pdf(self, url, year):
        """
        Downloads a PDF speech from the given URL and saves it in the specified folder.
        """
        try:
            response = requests.get(url)
            response.raise_for_status()

            # Extract date and title from the URL
            filename = url.split('/')[-1]
            speech_date_match = re.search(r'\d{8}', filename)
            speech_date = speech_date_match.group() if speech_date_match else "unknown_date"
            save_path = os.path.join(self.base_folder, f'raw_data/speech_{speech_date}.pdf')

            if not os.path.exists(save_path):
                with open(save_path, 'wb') as f:
                    f.write(response.content)
                print(f"Downloaded {filename} to {save_path}")

                # Collect metadata
                metadata = {
                    'url': url,
                    'year': year,
                    'date': speech_date,
                    'file_path': save_path,
                }
                self.speech_metadata.append(metadata)

                # Print metadata for debugging
                print(f"Collected metadata: {metadata}")
            else:
                print(f"Speech {filename} already exists. Skipping download.")

        except Exception as e:
            print(f"Error downloading speech {url}: {e}")

    def save_metadata(self):
        """
        Saves the metadata of the downloaded speeches as a CSV file.
        """
        if self.speech_metadata:
            metadata_df = pd.DataFrame(self.speech_metadata)
            metadata_file = os.path.join(self.base_folder, 'speech_metadata.csv')
            metadata_df.to_csv(metadata_file, index=False)
            print(f"Metadata saved to {metadata_file}")
        else:
            print("No metadata to save.")

if __name__ == "__main__":
    # Example of usage
    downloader = SpeechDownloader(base_folder='./data/Fed', start_year=2017)
    downloader.download_speeches()
    downloader.save_metadata()
