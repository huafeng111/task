# -*- coding: utf-8 -*-
import json
import os
from datetime import datetime
from src.core.Company.Dector.FileTypeDector import detect_file_type
from src.core.Company.Processor.FileProcessor import FileProcessor


class CrawlerController:
    def __init__(self, company_list_file):
        self.company_list_file = company_list_file
        self.company_urls = self.load_company_urls()

    def load_company_urls(self):
        """
        加载目标公司列表及其主页的 URL。
        从 CompanyList/CompanyPage.json 中加载公司名称和目标主页。
        """
        with open(self.company_list_file, 'r', encoding='utf-8') as f:
            company_data = json.load(f)
        return company_data

    def load_urls_for_company(self, company_name):
        """
        根据公司名称加载该公司存储的 URL 文件。
        路径格式为：CompanyList/{company_name}/data/metaData/{company_name}_urls_{current_date}.json
        """
        current_date = datetime.now().strftime('%Y-%m-%d')
        # 修改文件路径为正确的存储位置
        file_path = f"../CompanyList/{company_name}/data/metaData/{company_name}_urls_{current_date}.json"

        if not os.path.exists(file_path):
            print(f"URL 文件未找到: {file_path}")
            return []

        # 加载公司对应的 URL 列表
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data.get('urls', [])

    def start_crawling(self):
        """
        对每个公司，加载 URL 文件并检测每个 URL 的文件类型，然后调用处理工作流。
        """
        for company_name, homepage_url in self.company_urls.items():
            print(f"开始处理公司: {company_name}")

            # 加载公司对应的 URL 列表
            urls = self.load_urls_for_company(company_name)

            if not urls:
                print(f"未找到 {company_name} 的 URL，跳过。")
                continue

            # 处理每个 URL
            for url in urls:
                # 检测文件类型
                file_type = detect_file_type(url)
                print(f"Processing URL: {url}, detected as {file_type}")

                # 根据文件类型调用相应的工作流处理
                process_workflow(company_name, url, file_type)


def process_workflow(company_name, url, file_type):
    """
    根据文件类型处理 URL 的工作流函数。
    """
    file_processor = FileProcessor(company_name, url, file_type)
    file_processor.process()


if __name__ == "__main__":
    # 将目标公司 JSON 文件路径更改为正确的位置
    company_list_file = '../CompanyList/CompanyPage.json'
    crawler = CrawlerController(company_list_file)
    crawler.start_crawling()
