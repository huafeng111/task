# -*- coding: utf-8 -*-
import logging
import re
import sys
import json
import os
from abc import ABC, abstractmethod
from firecrawl import FirecrawlApp
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')

class BaseCrawler(ABC):
    def __init__(self, url, firecrawl_api_key=None, company_name=None):
        self.url = url
        self.data = None
        self.company_name = company_name
        self.logger = logging.getLogger(self.__class__.__name__)

        if firecrawl_api_key:
            self.firecrawl_app = FirecrawlApp(api_key=firecrawl_api_key)
        else:
            self.firecrawl_app = None

    def fetch_data(self):
        try:
            if self.firecrawl_app:
                self.data = self.call_firecrawler(self.url)
                self.logger.info(f"Successfully fetched data from {self.url}")
            else:
                self.logger.error("FirecrawlApp not initialized, cannot fetch data.")
                raise ValueError("FirecrawlApp is not initialized")

        except Exception as e:
            self.logger.error(f"Error fetching data from {self.url}: {e}")
            raise

    def call_firecrawler(self, url):
        try:
            scrape_result = self.firecrawl_app.scrape_url(
                url,
                params={
                    'formats': ['markdown', 'html']
                }
            )
            self.logger.info(f"Firecrawl successfully scraped: {scrape_result}")
            return scrape_result
        except Exception as e:
            self.logger.error(f"Failed to call Firecrawl: {e}")
            raise

    def save_data(self, data, file_suffix):
        """
        Save the scraped data or extracted URLs to a JSON file.
        The filename is generated based on the company name and current date,
        with the provided suffix used to distinguish different data.
        """
        if data:
            try:
                current_date = datetime.now().strftime('%Y-%m-%d')

                if not self.company_name:
                    self.company_name = "unknown_company"

                file_name = f"{self.company_name}_{file_suffix}_{current_date}.json"
                directory = os.path.join('data', 'metaData')
                if not os.path.exists(directory):
                    os.makedirs(directory)

                file_path = os.path.join(directory, file_name)

                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)

                self.logger.info(f"Data successfully saved to {file_path}")
                print(f"Data successfully saved to {file_path}")

            except Exception as e:
                self.logger.error(f"Error saving data: {e}")
                raise
        else:
            self.logger.warning("No data to save.")

    def process_data(self):
        """
        Process the scraped data, extract all URLs from the page, and save them.
        """
        if self.data:
            # Save the original scraped data
            self.save_data(self.data, "metaData")

            # Extract URLs
            if isinstance(self.data, dict):
                content_markdown = self.data.get('markdown', "")
                content_html = self.data.get('html', "")

                # Extract URLs from markdown and html content
                urls_markdown = self.extract_urls(content_markdown)
                urls_html = self.extract_urls(content_html)

                # Merge both sets of URLs and remove duplicates
                all_urls = list(set(urls_markdown + urls_html))
            else:
                all_urls = self.extract_urls(str(self.data))

            # Save the extracted URLs to a separate file
            self.save_data({"urls": all_urls}, "urls")
            self.logger.info(f"Extracted {len(all_urls)} URLs")

        else:
            self.logger.warning("No data to process.")

    def extract_urls(self, data):
        """
        Use a regular expression to extract all URLs from the scraped text,
        and remove any trailing punctuation.
        """
        # First, remove newline characters to avoid splitting URLs
        cleaned_data = data.replace('\n', ' ').replace('\r', ' ')

        # Improved regular expression to capture more URLs
        url_pattern = re.compile(r'https?://[^\s"\'<>]+')
        urls = re.findall(url_pattern, cleaned_data)

        # Define the trailing punctuation that needs to be removed
        trailing_punctuations = '.,);\'"!?'

        # Clean each URL by removing trailing punctuation
        cleaned_urls = [url.rstrip(trailing_punctuations) for url in urls]

        # Filter out empty strings (if any)
        cleaned_urls = [url for url in cleaned_urls if url]

        return cleaned_urls
