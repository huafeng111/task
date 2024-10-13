import os
import requests
from bs4 import BeautifulSoup
import logging
import re
import time
from requests.exceptions import HTTPError

logger = logging.getLogger(__name__)

# 获取网页的 BeautifulSoup 对象
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

# 实现带重试机制的请求方法
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
    return None

# 解析纽约联储演讲页面，获取所有演讲链接
def fetch_speech_links_nyfed():
    base_url = "https://www.newyorkfed.org/press#speeches"
    soup = get_soup_from_url(base_url)
    if not soup:
        return []

    # 查找所有演讲链接，演讲链接包含 "/newsevents/speeches/" 关键词
    speech_page_links = [
        link['href'] for link in soup.find_all('a', href=True)
        if "/newsevents/speeches/" in link['href']
    ]
    return speech_page_links

# 解析演讲页面的详细信息，比如PDF文件、标题、作者、日期等
def fetch_speech_details_nyfed(page_url):
    soup = get_soup_from_url(page_url)
    if not soup:
        return [], "No Title", "Unknown Speaker", "Unknown Date"

    # 提取演讲标题
    title_tag = soup.find('h1', class_='title')
    title = title_tag.get_text(strip=True) if title_tag else "No Title"

    # 提取演讲者信息
    speaker_tag = soup.find('div', class_='speaker')
    speaker = speaker_tag.get_text(strip=True) if speaker_tag else "Unknown Speaker"

    # 提取演讲日期
    date_tag = soup.find('p', class_='time')
    date = date_tag.get_text(strip=True) if date_tag else "Unknown Date"

    # 查找页面中的 PDF 链接
    pdf_links = [
        link['href'] for link in soup.find_all('a', href=True)
        if link['href'].endswith(".pdf")
    ]

    return pdf_links, title, speaker, date

# 主函数，用于解析纽约联储演讲页面
def parse_nyfed_speeches():
    # 获取所有演讲链接
    speech_links = fetch_speech_links_nyfed()

    # 用于存储解析的演讲数据
    all_speech_data = []

    # 遍历所有演讲链接，解析详细信息
    for relative_link in speech_links:
        full_url = f"https://www.newyorkfed.org{relative_link}"
        pdf_links, title, speaker, date = fetch_speech_details_nyfed(full_url)
        speech_data = {
            "url": full_url,
            "title": title,
            "speaker": speaker,
            "date": date,
            "pdf_links": pdf_links
        }
        all_speech_data.append(speech_data)
        print(f"Fetched data for {title}: {full_url}")

    return all_speech_data

if __name__ == "__main__":
    # 调用主函数
    nyfed_speeches = parse_nyfed_speeches()

    # 打印获取的演讲信息
    for speech in nyfed_speeches:
        print(f"Title: {speech['title']}, Speaker: {speech['speaker']}, Date: {speech['date']}, URL: {speech['url']}")
        if speech['pdf_links']:
            print(f"PDF Links: {', '.join(speech['pdf_links'])}")
