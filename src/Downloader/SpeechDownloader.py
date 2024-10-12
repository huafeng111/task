import os
import re
import json
import requests
import logging
import pymongo
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
from PyPDF2 import PdfReader
from pymongo import MongoClient
from bson import ObjectId

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MongoDB Setup
client = MongoClient('mongodb://localhost:27017/')
db = client['USA_FED']
collection = db['speeches']

# Constants
BASE_DIR = 'data/Fed'
DATE_FORMAT = '%Y%m%d'
START_YEAR = 2017
FED_BASE_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
REGIONAL_SITES = {
    'newyork': 'https://www.newyorkfed.org/press#speeches',
    'richmond': 'https://www.richmondfed.org/press_room/speeches',
    'sanfrancisco': 'https://www.frbsf.org/news-and-media/speeches/mary-c-daly/',
    'cleveland': 'https://www.clevelandfed.org/collections/speeches/',
    'atlanta': 'https://www.atlantafed.org/news/speeches',
    'dallas': 'https://www.dallasfed.org/news/speeches/logan#yr2024'
}

# Helper Functions
def fetch_url_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        logging.error(f"Failed to fetch {url}: {e}")
        return None

def parse_pdf_metadata(filename):
    try:
        pdf_reader = PdfReader(filename)
        doc_info = pdf_reader.metadata
        title = doc_info.get('/Title', 'No Title Found')
        return title
    except Exception as e:
        logging.error(f"Failed to parse PDF {filename}: {e}")
        return 'No Title Found'

def download_pdf(url, path):
    content = fetch_url_content(url)
    if content:
        with open(path, 'wb') as f:
            f.write(content)
        logging.info(f"Downloaded {url} to {path}")
        return True
    return False

def store_in_mongodb(metadata, speech_text):
    try:
        metadata['creation_date'] = datetime.utcnow()
        metadata['speech_text'] = speech_text
        collection.insert_one(metadata)
        logging.info(f"Inserted document for {metadata['speaker']} on {metadata['date']}")
    except pymongo.errors.PyMongoError as e:
        logging.error(f"Failed to insert into MongoDB: {e}")

def extract_fed_speeches():
    logging.info(f"Fetching speeches from {FED_BASE_URL}")
    html_content = fetch_url_content(FED_BASE_URL)
    soup = BeautifulSoup(html_content, 'html.parser')

    speeches = []
    for link in soup.find_all('a', href=True):
        if link.get('href').endswith(".pdf"):
            url = "https://www.federalreserve.gov" + link['href']
            filename = link['href'].split('/')[-1].replace('.pdf', '')
            match = re.search(r'\d{8}', filename)
            if match:
                date_str = match.group()
                date = datetime.strptime(date_str, DATE_FORMAT)
                speeches.append({
                    'url': url,
                    'date': date_str,
                    'timestamp': int(date.timestamp()),
                    'title': link.text,
                    'speaker': 'Federal Reserve Board',
                    'speaker_type': 'federal'
                })
    return speeches

def download_speeches_from_sources(speeches):
    for speech in speeches:
        file_path = os.path.join(BASE_DIR, f"speeches/{speech['date']}.pdf")
        if not os.path.exists(file_path):
            if download_pdf(speech['url'], file_path):
                title = parse_pdf_metadata(file_path)
                speech['title'] = title
                speech_text = extract_text_from_pdf(file_path)
                store_in_mongodb(speech, speech_text)

def extract_text_from_pdf(file_path):
    try:
        with open(file_path, 'rb') as f:
            reader = PdfReader(f)
            text = ''
            for page in reader.pages:
                text += page.extract_text() + '\n'
            return text
    except Exception as e:
        logging.error(f"Failed to extract text from {file_path}: {e}")
        return ''

def update_speeches():
    # Fetch Federal Reserve speeches
    fed_speeches = extract_fed_speeches()

    # Fetch speeches from regional Feds
    for region, url in REGIONAL_SITES.items():
        logging.info(f"Fetching speeches from {region} site: {url}")
        regional_speeches = extract_regional_speeches(url, region)
        fed_speeches.extend(regional_speeches)

    # Download and store speeches in MongoDB
    download_speeches_from_sources(fed_speeches)
    logging.info("Speech update completed.")

def extract_regional_speeches(url, region):
    content = fetch_url_content(url)
    soup = BeautifulSoup(content, 'html.parser')

    speeches = []
    for link in soup.find_all('a', href=True):
        if '2024' in link['href']:  # Adjust this to handle other years too
            speech_url = f"https://{region}.fed.org{link['href']}"
            date_str = speech_url.split('/')[-1][:8]
            date = datetime.strptime(date_str, DATE_FORMAT)
            speeches.append({
                'url': speech_url,
                'date': date_str,
                'timestamp': int(date.timestamp()),
                'title': link.text,
                'speaker': f"{region.capitalize()} Fed Speaker",
                'speaker_type': 'regional'
            })
    return speeches

if __name__ == "__main__":
    update_speeches()
