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
                self.logger.info(f"成功从 {self.url} 获取数据")
            else:
                self.logger.error("FirecrawlApp 未初始化，无法抓取数据。")
                raise ValueError("FirecrawlApp is not initialized")

        except Exception as e:
            self.logger.error(f"从 {self.url} 获取数据时出错: {e}")
            raise

    def call_firecrawler(self, url):
        try:
            scrape_result = self.firecrawl_app.scrape_url(
                url,
                params={
                    'formats': ['markdown', 'html']
                }
            )
            self.logger.info(f"Firecrawl 抓取成功: {scrape_result}")
            return scrape_result
        except Exception as e:
            self.logger.error(f"调用 Firecrawl 失败: {e}")
            raise

    def save_data(self, data, file_suffix):
        """
        保存抓取到的数据或提取的 URL 到一个 JSON 文件。
        文件名根据公司名和当前日期生成，并使用提供的后缀区分不同数据。
        """
        if data:
            try:
                current_date = datetime.now().strftime('%Y-%m-%d')

                if not self.company_name:
                    self.company_name = "unknown_company"

                file_name = f"{self.company_name}_{file_suffix}_{current_date}.json"
                directory = os.path.join('data')
                if not os.path.exists(directory):
                    os.makedirs(directory)

                file_path = os.path.join(directory, file_name)

                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)

                self.logger.info(f"数据成功保存到 {file_path}")
                print(f"Data successfully saved to {file_path}")

            except Exception as e:
                self.logger.error(f"保存数据时出错: {e}")
                raise
        else:
            self.logger.warning("没有可保存的数据")

    def process_data(self):
        """
        处理抓取到的数据，提取页面中的所有 URL 并存储。
        """
        if self.data:
            # 保存原始抓取数据
            self.save_data(self.data, "metaData")

            # 提取 URL
            if isinstance(self.data, dict):
                content_markdown = self.data.get('markdown', "")
                content_html = self.data.get('html', "")

                # 提取 markdown 和 html 中的 URLs
                urls_markdown = self.extract_urls(content_markdown)
                urls_html = self.extract_urls(content_html)

                # 合并两部分提取的 URL，并去重
                all_urls = list(set(urls_markdown + urls_html))
            else:
                all_urls = self.extract_urls(str(self.data))

            # 将提取的 URL 另存为 URL 文件
            self.save_data({"urls": all_urls}, "urls")
            self.logger.info(f"提取到 {len(all_urls)} 个 URL")

        else:
            self.logger.warning("没有数据可供处理。")

    def extract_urls(self, data):
        """
        使用正则表达式从抓取到的文本中提取所有的 URL，并移除尾随的标点符号。
        """
        # 首先移除掉换行符，避免分隔URL
        cleaned_data = data.replace('\n', ' ').replace('\r', ' ')

        # 改进后的正则表达式来捕获更多的URL
        url_pattern = re.compile(r'https?://[^\s"\'<>]+')
        urls = re.findall(url_pattern, cleaned_data)

        # 定义需要移除的尾随标点符号
        trailing_punctuations = '.,);\'"!?'

        # 清理每个 URL，移除尾随的标点符号
        cleaned_urls = [url.rstrip(trailing_punctuations) for url in urls]

        # 过滤掉空字符串（如果有）
        cleaned_urls = [url for url in cleaned_urls if url]

        return cleaned_urls
