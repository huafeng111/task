# -*- coding: utf-8 -*-

from src.core.Company.BaseCrawler import BaseCrawler

class TeslaCrawler(BaseCrawler):
    def __init__(self, url, firecrawl_api_key):
        # 调用父类的构造函数，初始化 url、firecrawl_api_key 和 company_name
        super().__init__(url, firecrawl_api_key, company_name="Tesla")

if __name__ == "__main__":
    url = 'https://ir.tesla.com/'
    api_key = 'fc-ac2ce7c810a747cfb2355d7ce2391672'  # 使用有效的 API key

    # 创建 TeslaCrawler 实例
    tesla_crawler = TeslaCrawler(url, api_key)

    # 抓取数据
    tesla_crawler.fetch_data()

    # 处理数据
    tesla_crawler.process_data()
