# -*- coding: utf-8 -*-
import json
import os
import requests
import html2text  # 引入 html2text 库
import time
import random

from PyPDF2 import PdfReader


class FileProcessor:
    def __init__(self, company_name, url, file_type):
        self.company_name = company_name
        self.url = url
        self.file_type = file_type
        self.base_dir = f"../CompanyList/{self.company_name}/data"
        self.session = requests.Session()  # 使用会话对象
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive"
        }
        self.session.headers.update(self.headers)  # 在会话中更新头信息

        # 使用 SmartProxy 代理
        self.proxies = {
            "http": "http://speyinarxb:81ZxuK_Rgj4Fc2tidi@gate.smartproxy.com:7000",
            "https": "http://speyinarxb:81ZxuK_Rgj4Fc2tidi@gate.smartproxy.com:7000"
        }

    def rotate_ip(self):
        """模拟IP轮换，可以定制轮换策略"""
        time.sleep(random.uniform(1, 3))
        self.session.proxies.update(self.proxies)

    def log_error(self, error_message):
        """记录爬取失败的 URL 及相关错误信息到 error.json 文件。"""
        error_dir = os.path.join(self.base_dir, 'error')
        os.makedirs(error_dir, exist_ok=True)
        error_file_path = os.path.join(error_dir, 'error.json')

        error_data = {
            "url": self.url,
            "file_type": self.file_type,
            "error": error_message
        }

        if os.path.exists(error_file_path):
            with open(error_file_path, 'r', encoding='utf-8') as error_file:
                try:
                    errors = json.load(error_file)
                except json.JSONDecodeError:
                    errors = []
        else:
            errors = []

        errors.append(error_data)

        with open(error_file_path, 'w', encoding='utf-8') as error_file:
            json.dump(errors, error_file, ensure_ascii=False, indent=4)

        print(f"Error logged to {error_file_path}")

    def process(self):
        """根据文件类型调用不同的处理逻辑。"""
        if self.file_type == 'pdf':
            self.process_pdf()
        elif self.file_type == 'ppt':
            self.process_ppt()
        elif self.file_type == 'plaintext':
            self.process_plaintext()
        elif self.file_type == "image":
            self.process_image()
        elif self.file_type == 'html':
            self.process_html()
        else:
            print(f"Unsupported file type: {self.file_type}")

    def download_file(self, download_dir, file_extension):
        """下载文件并保存到指定目录，避免重复下载。"""
        os.makedirs(download_dir, exist_ok=True)
        file_name = self.url.split('/')[-1]
        file_path = os.path.join(download_dir, file_name)

        if os.path.exists(file_path):
            print(f"File already exists, skipping download: {file_path}")
            return file_path

        self.rotate_ip()

        try:
            response = self.session.get(self.url)
            response.raise_for_status()

            with open(file_path, 'wb') as file:
                file.write(response.content)

            print(f"Downloaded and saved {file_extension.upper()} to: {file_path}")
            return file_path

        except Exception as e:
            error_message = f"Failed to download {file_extension.upper()} from {self.url}. Error: {e}"
            print(error_message)
            self.log_error(error_message)
            return None

    def process_pdf(self):
        """处理 PDF 文件：下载并保存到公司 PDF 目录，并提取内容。"""
        download_dir = os.path.join(self.base_dir, 'pdf')
        pdf_path = self.download_file(download_dir, 'pdf')

        if pdf_path:
            try:
                reader = PdfReader(pdf_path)
                text = ""
                for page in reader.pages:
                    text += page.extract_text()  # 提取 PDF 内容

                print(f"Extracted PDF text preview (first 500 chars): {text[:500]}")
                # 你可以选择将提取的文本保存到某个 JSON 文件或其他地方

            except Exception as e:
                error_message = f"Failed to extract text from PDF {pdf_path}. Error: {e}"
                print(error_message)
                self.log_error(error_message)

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
        os.makedirs(download_dir, exist_ok=True)
        file_name = self.url.split('/')[-1]  # 获取文件名
        file_path = os.path.join(download_dir, file_name)

        # 如果文件已经存在，避免重复下载
        if os.path.exists(file_path):
            print(f"Image file already exists, skipping download: {file_path}")
            return

        # IP轮换
        self.rotate_ip()

        try:
            # 下载图像文件
            response = self.session.get(self.url)  # 使用 session 发送请求
            response.raise_for_status()  # 检查响应状态

            # 将图像保存到文件中
            with open(file_path, 'wb') as file:
                file.write(response.content)

            print(f"Downloaded and saved image to: {file_path}")

        except Exception as e:
            error_message = f"Failed to download image from {self.url}. Error: {e}"
            print(error_message)
            self.log_error(error_message)

    def process_html(self):
        """
        处理 HTML 文件：将 HTML 转换为纯文本，并保存到公司 HTML 数据 JSON 文件中。
        如果 'html_data.json' 文件已存在，会加载并检查是否存在重复 URL。
        如果没有重复，则添加新的 URL 和转换的文本数据。
        """
        download_dir = os.path.join(self.base_dir, 'html')
        os.makedirs(download_dir, exist_ok=True)
        json_file_path = os.path.join(download_dir, 'html_data.json')

        # 初始化用于保存 URL 和文本数据的字典
        data = {
            "entries": []
        }

        # 如果 json 文件已经存在，读取现有数据
        if os.path.exists(json_file_path):
            with open(json_file_path, 'r', encoding='utf-8') as json_file:
                try:
                    data = json.load(json_file)  # 加载已有数据
                except json.JSONDecodeError:
                    print(f"Error decoding JSON from {json_file_path}. Starting with an empty structure.")

        # 检查是否已经存在相同的 URL
        existing_urls = {entry['url'] for entry in data['entries']}
        if self.url in existing_urls:
            print(f"URL already exists in html_data.json, skipping: {self.url}")
            return

        # IP轮换
        self.rotate_ip()

        try:
            # 下载 HTML 内容并设置正确的编码
            response = self.session.get(self.url)  # 使用 session 发送请求
            response.encoding = response.apparent_encoding  # 使用 requests 的编码检测功能
            response.raise_for_status()  # 检查响应状态
            html_content = response.text

            # 使用 html2text 将 HTML 转换为纯文本
            h = html2text.HTML2Text()
            h.ignore_links = True  # 忽略超链接
            text = h.handle(html_content)



            # 将新的 URL 和转换的文本数据添加到现有的数据中
            new_entry = {
                "url": self.url,
                "text": text
            }
            data['entries'].append(new_entry)

            # 保存更新后的数据回到 json 文件
            with open(json_file_path, 'w', encoding='utf-8') as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=4)

            print(f"Converted HTML to text and added to JSON at: {json_file_path}")

        except Exception as e:
            error_message = f"Failed to process HTML from {self.url}. Error: {e}"
            print(error_message)
            self.log_error(error_message)

    def process_plaintext(self):
        """
        处理纯文本文件：将 HTML 转换为文本并保存到公司 HTML 数据 JSON 文件中。
        如果 'html_data.json' 文件已存在，会加载并检查是否存在重复 URL。
        如果没有重复，则添加新的 URL 和转换的文本数据。
        """
        download_dir = os.path.join(self.base_dir, 'html')
        os.makedirs(download_dir, exist_ok=True)
        json_file_path = os.path.join(download_dir, 'html_data.json')

        # 初始化用于保存 URL 和文本数据的字典
        data = {
            "entries": []
        }

        # 如果 json 文件已经存在，读取现有数据
        if os.path.exists(json_file_path):
            with open(json_file_path, 'r', encoding='utf-8') as json_file:
                try:
                    data = json.load(json_file)  # 加载已有数据
                except json.JSONDecodeError:
                    print(f"Error decoding JSON from {json_file_path}. Starting with an empty structure.")

        # 检查是否已经存在相同的 URL
        existing_urls = {entry['url'] for entry in data['entries']}
        if self.url in existing_urls:
            print(f"URL already exists in html_data.json, skipping: {self.url}")
            return

        # IP轮换
        self.rotate_ip()

        try:
            # 下载 HTML 并转换为纯文本
            response = self.session.get(self.url)  # 使用 session 发送请求
            response.raise_for_status()  # 检查响应状态
            html_content = response.text

            # 使用 html2text 将 HTML 转换为纯文本
            h = html2text.HTML2Text()
            h.ignore_links = True  # 忽略超链接
            text = h.handle(html_content)

            # 将新的 URL 和转换的文本数据添加到现有的数据中
            new_entry = {
                "url": self.url,
                "text": text
            }
            data['entries'].append(new_entry)

            # 保存更新后的数据回到 json 文件
            with open(json_file_path, 'w', encoding='utf-8') as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=4)

            print(f"Converted HTML to text and added to JSON at: {json_file_path}")

        except Exception as e:
            error_message = f"Failed to process plaintext from {self.url}. Error: {e}"
            print(error_message)
            self.log_error(error_message)
