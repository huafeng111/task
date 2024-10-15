# -*- coding: utf-8 -*-
from src.core.Company.BaseCrawler import BaseCrawler

class NvidiaCrawler(BaseCrawler):
    def __init__(self, url, firecrawl_api_key):
        # 调用父类的构造函数，初始化 url 和 firecrawl_api_key
        super().__init__(url, firecrawl_api_key, company_name="Nvidia")

if __name__ == "__main__":
    url = 'https://investor.nvidia.com/home/default.aspx'
    api_key = 'fc-ac2ce7c810a747cfb2355d7ce2391672'

    # 创建 NvidiaCrawler 实例
    nvidia_crawler = NvidiaCrawler(url, api_key)

    # 抓取数据
    nvidia_crawler.fetch_data()

    # 处理数据（提取 URL 和保存原始数据）
    nvidia_crawler.process_data()
