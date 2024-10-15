# -*- coding: utf-8 -*-

from src.core.Company.BaseCrawler import BaseCrawler

class LillyCrawler(BaseCrawler):
    def __init__(self, url, firecrawl_api_key):
        # 调用父类的构造函数，初始化 url 和 firecrawl_api_key
        super().__init__(url, firecrawl_api_key, company_name="Lilly")  # 设置公司名为 "Lilly"

    # 不需要重写 process_data，因为父类已经实现了提取 URL 的逻辑

if __name__ == "__main__":
    url = 'https://investor.lilly.com/'
    api_key = 'fc-ac2ce7c810a747cfb2355d7ce2391672'  # 使用有效的 API key

    # 创建 LillyCrawler 实例
    lilly_crawler = LillyCrawler(url, api_key)

    # 抓取数据
    lilly_crawler.fetch_data()

    # 处理数据（提取 URL）
    lilly_crawler.process_data()


