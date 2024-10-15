# -*- coding: utf-8 -*-

import os
import requests
import html2text  # 引入 html2text 库

class FileProcessor:
    def __init__(self, company_name, url, file_type):
        self.company_name = company_name
        self.url = url
        self.file_type = file_type
        self.base_dir = f"CompanyList/{self.company_name}"

    def process(self):
        """
        根据文件类型调用不同的处理逻辑。
        """
        if self.file_type == 'pdf':
            self.process_pdf()
        elif self.file_type == 'ppt':
            self.process_ppt()
        elif self.file_type == 'plaintext':
            self.process_plaintext()
        elif self.file_type in ['png', 'jpeg', 'jpg', 'svg']:
            self.process_image()
        else:
            print(f"Unsupported file type: {self.file_type}")

    def download_file(self, download_dir, file_extension):
        """
        下载文件并保存到指定目录，避免重复下载。
        """
        os.makedirs(download_dir, exist_ok=True)
        file_name = self.url.split('/')[-1]
        file_path = os.path.join(download_dir, file_name)

        # 如果文件已经存在，避免重复下载
        if os.path.exists(file_path):
            print(f"File already exists, skipping download: {file_path}")
            return

        # 下载文件并保存
        try:
            response = requests.get(self.url)
            with open(file_path, 'wb') as file:
                file.write(response.content)
            print(f"Downloaded {file_extension.upper()} file to: {file_path}")
        except Exception as e:
            print(f"Failed to download {file_extension.upper()} from {self.url}. Error: {e}")

    def process_pdf(self):
        """
        处理 PDF 文件：下载并保存到公司 PDF 目录。
        """
        download_dir = os.path.join(self.base_dir, 'pdf')
        self.download_file(download_dir, 'pdf')

    def process_ppt(self):
        """
        处理 PPT 文件：下载并保存到公司 PPT 目录。
        """
        download_dir = os.path.join(self.base_dir, 'ppt')
        self.download_file(download_dir, 'ppt')

    def process_image(self):
        """
        处理图像文件：下载并保存到公司图像目录。
        """
        download_dir = os.path.join(self.base_dir, 'image')
        self.download_file(download_dir, self.file_type)

    def process_plaintext(self):
        """
        处理纯文本文件：将 HTML 转换为文本并保存到公司 HTML 目录。
        """
        download_dir = os.path.join(self.base_dir, 'html')
        os.makedirs(download_dir, exist_ok=True)
        file_name = f"{self.url.split('/')[-1].split('.')[0]}.txt"
        file_path = os.path.join(download_dir, file_name)

        # 如果文本文件已经存在，避免重复处理
        if os.path.exists(file_path):
            print(f"Text file already exists, skipping: {file_path}")
            return

        try:
            # 下载 HTML 并转换为文本
            response = requests.get(self.url)
            html_content = response.text

            # 使用 html2text 将 HTML 转换为纯文本
            h = html2text.HTML2Text()
            h.ignore_links = True  # 忽略超链接
            text = h.handle(html_content)

            # 保存转换后的文本
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(text)
            print(f"Converted and saved HTML to text at: {file_path}")
        except Exception as e:
            print(f"Failed to process plaintext from {self.url}. Error: {e}")
