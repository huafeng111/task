import os
import requests
from bs4 import BeautifulSoup
import logging
import re
import time
from requests.exceptions import HTTPError

logger = logging.getLogger(__name__)

def get_soup_from_url(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'html.parser')
    except HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")
    except Exception as err:
        logger.error(f"Error occurred: {err}")
    return None

def fetch_with_retries(url, retries=3, delay=5, backoff_factor=2):
    for i in range(retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response
        except (HTTPError, requests.exceptions.ConnectionError) as e:
            logger.warning(f"Attempt {i+1} failed for {url}: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
            delay *= backoff_factor  # 增加退避因子


def fetch_speech_links_for_year(year):
    base_url = f"https://www.federalreserve.gov/newsevents/speech/{year}-speeches.htm"
    soup = get_soup_from_url(base_url)
    if not soup:
        return []

    # Get all links pointing to speech detail pages
    speech_page_links = [
        link['href'] for link in soup.find_all('a', href=True)
        if link['href'].startswith("/newsevents/speech/") and link['href'].endswith(".htm")
    ]
    return speech_page_links

def fetch_pdf_links_from_speech_page(page_url):
    soup = get_soup_from_url(page_url)
    if not soup:
        return [], "No Title", "Unknown Author", "Unknown Date"

    # Extract the page filename (used to match .pdf files)
    page_filename = os.path.basename(page_url).split(".htm")[0]  # e.g., bowman20231205a

    title_tag = soup.find('h3', class_='title')
    title = title_tag.find('em').get_text(strip=True) if title_tag and title_tag.find('em') else "No Title"

    author_tag = soup.find('p', class_='speaker')
    author = author_tag.get_text(strip=True) if author_tag else "Unknown Author"

    date_tag = soup.find('p', class_='article__time')
    date = date_tag.get_text(strip=True) if date_tag else "Unknown Date"

    # Search for .pdf links related to the current speech page
    pdf_links = [
        link['href'] for link in soup.find_all('a', href=True)
        if link['href'].endswith(".pdf") and page_filename in link['href']
    ]

    return pdf_links, title, author, date
