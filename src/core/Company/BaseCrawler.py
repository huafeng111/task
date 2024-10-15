import logging
import re
import sys
import json
import os
import hashlib
from abc import ABC, abstractmethod
from firecrawl import FirecrawlApp
from datetime import datetime
from urllib.parse import urlparse
from tenacity import retry, wait_fixed, stop_after_attempt
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

sys.stdout.reconfigure(encoding='utf-8')

class BaseCrawler(ABC):
    def __init__(self, url, firecrawl_api_key=None, company_name=None):
        self.url = url
        self.data = None
        self.company_name = company_name if company_name else "unknown_company"
        self.logger = self.setup_logger()

        if firecrawl_api_key:
            self.firecrawl_app = FirecrawlApp(api_key=firecrawl_api_key)
        else:
            self.firecrawl_app = None

        # URL 提取正则表达式
        self.url_pattern = re.compile(
            r"https?://"                     # 匹配 http 或 https
            r"(?:[a-zA-Z0-9-]+\.)*"           # 匹配域名中的子域名部分 (可选)
            r"[a-zA-Z0-9-]+"                  # 匹配主域名
            r"(?:\.[a-zA-Z]{2,})"             # 匹配顶级域名（如 .com, .org）
            r"(?::\d{1,5})?"                  # 匹配端口号（可选）
            r"(?:/[^?#\s]*)?"                 # 匹配路径（可选）
            r"(?:\?[^#\s]*)?"                 # 匹配查询参数（可选）
            r"(?:#[^\s]*)?"                   # 匹配片段标识符（可选）
        )

        # URL 文件路径
        self.url_file_path = os.path.join(os.getenv('DATA_DIR', 'data'), 'metaData', f"{self.company_name}_urls.json")

    def setup_logger(self):
        logger = logging.getLogger(self.__class__.__name__)
        if not logger.hasHandlers():
            handler = logging.StreamHandler(sys.stdout)
            file_handler = logging.FileHandler(f'logs/{self.company_name}_crawler.log')
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            file_handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.addHandler(file_handler)
            logger.setLevel(logging.DEBUG)  # 可配置的日志级别
        return logger

    def fetch_data(self):
        if not self.firecrawl_app:
            self.logger.error("FirecrawlApp not initialized, cannot fetch data.")
            raise ValueError("FirecrawlApp is not initialized")

        try:
            self.data = self.call_firecrawler(self.url)
            self.logger.info(f"Successfully fetched data from {self.url}")
        except Exception as e:
            self.logger.error(f"Error fetching data from {self.url}: {e}")
            raise

    @retry(wait=wait_fixed(5), stop=stop_after_attempt(3))  # 重试机制
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
        if not data:
            self.logger.warning("No data to save.")
            return

        try:
            current_date = datetime.now().strftime('%Y-%m-%d')
            url_hash = hashlib.md5(self.url.encode()).hexdigest()  # 使用URL的MD5哈希作为文件名的一部分
            file_name = f"{self.company_name}_{file_suffix}_{url_hash}_{current_date}.json"
            directory = os.path.join(os.getenv('DATA_DIR', 'data'), 'metaData')
            os.makedirs(directory, exist_ok=True)

            file_path = os.path.join(directory, file_name)

            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

            self.logger.info(f"Data successfully saved to {file_path}")
            print(f"Data successfully saved to {file_path}")

            # 如果是 URL 数据，则还要保存到增量文件中
            if file_suffix == "urls":
                self.url_file_path = file_path  # 保存文件路径
        except Exception as e:
            self.logger.error(f"Error saving data: {e}")
            raise

    def process_data(self):
        if not self.data:
            self.logger.warning("No data to process.")
            return

        # 保存原始抓取的数据
        self.save_data(self.data, "metaData")

        # 合并 HTML 和 Markdown 内容进行处理
        content = (self.data.get('markdown', "") + " " + self.data.get('html', "")).strip()
        all_urls = self.extract_urls(content)

        # 加载现有的URL
        existing_urls = self.load_existing_urls()

        # 去重
        new_urls = set(all_urls) - set(existing_urls)

        if new_urls:
            # 保存提取的 URL 到单独的文件，合并新旧URL
            self.save_data({"urls": list(existing_urls | new_urls)}, "urls")
            self.logger.info(f"Extracted {len(new_urls)} new URLs")
        else:
            self.logger.info("No new URLs extracted.")

    def extract_urls(self, data):
        cleaned_data = data.replace('\n', ' ').replace('\r', ' ')
        urls = re.findall(self.url_pattern, cleaned_data)

        # 清理 URL 并验证其合法性
        trailing_punctuations = '.,);\'"!?'
        cleaned_urls = [url.rstrip(trailing_punctuations) for url in urls]
        valid_urls = [url for url in cleaned_urls if urlparse(url).scheme in ['http', 'https']]

        # 使用 set 去重
        return list(set(valid_urls))

    def load_existing_urls(self):
        """加载已经存在的 URL 文件，如果存在则返回 URL 列表，不存在则返回空列表"""
        if os.path.exists(self.url_file_path):
            try:
                with open(self.url_file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return set(data.get('urls', []))
            except Exception as e:
                self.logger.error(f"Error loading existing URLs: {e}")
        return set()

    def fetch_data_concurrently(self, urls):
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(self.call_firecrawler, urls))
        return results
