# -*- coding: utf-8 -*-

import os
import pandas as pd
import PyPDF2
from pymongo import MongoClient

class PDFHandler:
    def __init__(self, csv_relative_path, mongo_uri, db_name, collection_name):
        # 获取当前脚本所在目录并构建相对路径
        script_dir = os.path.dirname(os.path.abspath(__file__))  # 获取当前脚本的绝对路径
        self.csv_file = os.path.abspath(os.path.join(script_dir, csv_relative_path))  # 通过相对路径找到 CSV 文件
        self.mongo_client = MongoClient(mongo_uri)
        self.db = self.mongo_client[db_name]
        self.collection = self.db[collection_name]

    def load_csv(self):
        # 读取 CSV 文件
        try:
            data = pd.read_csv(self.csv_file)
            print("CSV 文件加载成功")
            return data
        except Exception as e:
            print(f"加载 CSV 文件失败: {e}")
            return None

    def extract_pdf_text(self, file_path):
        # 从 PDF 文件中提取文本
        try:
            with open(file_path, 'rb') as file:
                reader = PyPDF2.PdfReader(file)
                text = ""
                for page in range(len(reader.pages)):
                    text += reader.pages[page].extract_text()
            return text
        except Exception as e:
            print(f"无法读取 PDF 文件 {file_path}: {e}")
            return None

    def process_and_store(self):
        # 加载 CSV 并处理每一行
        data = self.load_csv()
        if data is None:
            return

        for index, row in data.iterrows():
            url = row['url']
            year = row['year']
            title = row['title']
            author = row['author']
            date = row['date']
            file_path = row['file_path']

            # 构建相对路径的 PDF 文件路径
            pdf_relative_path = os.path.abspath(os.path.join(os.path.dirname(self.csv_file), file_path))

            # 提取 PDF 文本内容
            if os.path.exists(pdf_relative_path):
                text = self.extract_pdf_text(pdf_relative_path)
                if text:
                    document = {
                        "url": url,
                        "year": year,
                        "title": title,
                        "author": author,
                        "date": date,
                        "text": text
                    }

                    # 将数据存储到 MongoDB
                    try:
                        self.collection.insert_one(document)
                        print(f"成功保存文档: {title}")
                    except Exception as e:
                        print(f"无法保存文档 {title}: {e}")
            else:
                print(f"PDF 文件未找到: {pdf_relative_path}")

if __name__ == "__main__":
    # 相对路径设置为相对于 PdfToText.py 文件的 CSV 文件位置
    csv_relative_path = "../../../data/pdfs/speech_metadata.csv"  # 根据新的目录结构设置相对路径
    mongo_uri = "mongodb://localhost:27017/"
    db_name = "your_database"
    collection_name = "your_collection"

    handler = PDFHandler(csv_relative_path, mongo_uri, db_name, collection_name)
    handler.process_and_store()
