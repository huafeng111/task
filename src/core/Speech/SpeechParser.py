import os
import requests
from bs4 import BeautifulSoup
import logging
import re
import time
from requests.exceptions import HTTPError, ConnectionError, Timeout

logger = logging.getLogger(__name__)

# Set up logging format for better readability
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_soup_from_url(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'html.parser')
    except HTTPError as http_err:
        logger.error(f"HTTP error occurred while fetching URL '{url}': {http_err}")
    except ConnectionError as conn_err:
        logger.error(f"Connection error occurred while fetching URL '{url}': {conn_err}")
    except Timeout as timeout_err:
        logger.error(f"Timeout occurred while fetching URL '{url}': {timeout_err}")
    except Exception as err:
        logger.error(f"An unexpected error occurred while fetching URL '{url}': {err}")
    return None

def fetch_with_retries(url, retries=3, delay=5, backoff_factor=2):
    for i in range(retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response
        except HTTPError as http_err:
            logger.warning(f"HTTP error occurred on attempt {i + 1} for URL '{url}': {http_err}. Retrying in {delay} seconds...")
        except ConnectionError as conn_err:
            logger.warning(f"Connection error occurred on attempt {i + 1} for URL '{url}': {conn_err}. Retrying in {delay} seconds...")
        except Timeout as timeout_err:
            logger.warning(f"Timeout occurred on attempt {i + 1} for URL '{url}': {timeout_err}. Retrying in {delay} seconds...")
        except Exception as err:
            logger.warning(f"An unexpected error occurred on attempt {i + 1} for URL '{url}': {err}. Retrying in {delay} seconds...")
        time.sleep(delay)
        delay *= backoff_factor  # Increase the delay for each retry

    logger.error(f"Failed to fetch URL '{url}' after {retries} attempts.")
    return None

def fetch_speech_links_for_year(year):
    base_url = f"https://www.federalreserve.gov/newsevents/speech/{year}-speeches.htm"
    soup = get_soup_from_url(base_url)
    if not soup:
        logger.error(f"Unable to fetch or parse the base URL for year {year}. Returning empty list.")
        return []

    # Get all links pointing to speech detail pages
    speech_page_links = [
        link['href'] for link in soup.find_all('a', href=True)
        if link['href'].startswith("/newsevents/speech/") and link['href'].endswith(".htm")
    ]

    if not speech_page_links:
        logger.warning(f"No speech links found for the year {year}.")

    return speech_page_links

def fetch_pdf_links_from_speech_page(page_url):
    if not page_url.startswith("https://www.federalreserve.gov"):
        page_url = f"https://www.federalreserve.gov{page_url}"

    soup = get_soup_from_url(page_url)
    if not soup:
        logger.error(f"Unable to fetch or parse the speech page URL: {page_url}. Returning default values.")
        return [], "No Title", "Unknown Author", "Unknown Date"

    # Extract the page filename (used to match .pdf files)
    page_filename = os.path.basename(page_url).split(".htm")[0]  # e.g., bowman20231205a

    title_tag = soup.find('h3', class_='title')
    if title_tag:
        title = title_tag.find('em').get_text(strip=True) if title_tag.find('em') else "No Title"
    else:
        logger.warning(f"Title not found for page: {page_url}")
        title = "No Title"

    author_tag = soup.find('p', class_='speaker')
    author = author_tag.get_text(strip=True) if author_tag else "Unknown Author"
    if author == "Unknown Author":
        logger.warning(f"Author not found for page: {page_url}")

    date_tag = soup.find('p', class_='article__time')
    date = date_tag.get_text(strip=True) if date_tag else "Unknown Date"
    if date == "Unknown Date":
        logger.warning(f"Date not found for page: {page_url}")

    # Search for .pdf links related to the current speech page
    pdf_links = [
        link['href'] for link in soup.find_all('a', href=True)
        if link['href'].endswith(".pdf") and page_filename in link['href']
    ]

    if not pdf_links:
        logger.warning(f"No PDF links found for speech page: {page_url}")

    # Make sure the PDF links are complete URLs
    pdf_links = [
        link if link.startswith("https://") else f"https://www.federalreserve.gov{link}"
        for link in pdf_links
    ]

    return pdf_links, title, author, date

