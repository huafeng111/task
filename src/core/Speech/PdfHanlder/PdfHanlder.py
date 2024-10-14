# -*- coding: utf-8 -*-

import os
import fitz  # PyMuPDF
import json
import pandas as pd
import aiofiles  # For async file operations
import asyncio
import logging
from aiofiles import os as aio_os  # async os operations

# 配置日志
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class PDFHandler:
    def __init__(self, csv_relative_path):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.csv_file = os.path.abspath(os.path.join(script_dir, csv_relative_path))
        self.base_dir = os.path.dirname(self.csv_file)

        logging.info(f"CSV file path: {self.csv_file}")
        logging.info(f"Base directory: {self.base_dir}")
        logging.info(f"Current working directory: {os.getcwd()}")

        if not os.path.exists(self.csv_file):
            logging.error(f"File not found: {self.csv_file}")
            raise FileNotFoundError(f"CSV file not found at {self.csv_file}")

        try:
            self.csv_data = pd.read_csv(self.csv_file)
            logging.info("CSV data loaded successfully")
        except Exception as e:
            logging.error(f"Error reading CSV file: {e}")
            raise

    async def extract_pdf_metadata(self, file_path):
        original_cwd = os.getcwd()
        try:
            os.chdir(self.base_dir)
            doc = fitz.open(file_path)
            metadata = doc.metadata
            pages = [doc.load_page(i).get_text("text") for i in range(doc.page_count)]

            logging.info(f"PDF metadata and text extracted for {file_path}")
            return {"metadata": metadata, "pages": pages}
        except FileNotFoundError:
            logging.error(f"PDF file not found: {file_path}")
            return None
        except Exception as e:
            logging.error(f"Could not read PDF file {file_path}: {e}")
            return None
        finally:
            os.chdir(original_cwd)

    async def load_existing_metadata(self, output_file):
        if await aio_os.path.exists(output_file):
            try:
                async with aiofiles.open(output_file, 'r', encoding='utf-8') as f:
                    all_metadata = json.loads(await f.read())
                logging.info(f"Existing metadata loaded from {output_file}")
                return all_metadata
            except Exception as e:
                logging.error(f"Error loading existing metadata: {e}")
                return []
        else:
            logging.warning(f"No existing metadata found at {output_file}")
            return []

    async def save_all_metadata(self, all_metadata, output_file):
        try:
            async with aiofiles.open(output_file, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(all_metadata, ensure_ascii=False, indent=4))
            logging.info(f"All metadata and text saved to {output_file}")
        except Exception as e:
            logging.error(f"Failed to save all metadata: {e}")
            raise

    async def process_all_pdfs(self, output_file):
        all_metadata = await self.load_existing_metadata(output_file)
        existing_pdf_paths = {entry.get('csv_metadata', {}).get('file_path', '') for entry in all_metadata}

        for index, row in self.csv_data.iterrows():
            relative_pdf_path = row['file_path']

            if relative_pdf_path in existing_pdf_paths:
                logging.info(f"Skipping already processed PDF: {relative_pdf_path}")
                continue

            pdf_data = await self.extract_pdf_metadata(relative_pdf_path)

            if pdf_data:
                csv_metadata = row.to_dict()
                title = csv_metadata.pop('title')

                combined_metadata = {
                    "title": title,
                    "pdf_metadata": pdf_data['metadata'],
                    "pages": pdf_data['pages'],
                    "csv_metadata": csv_metadata
                }

                all_metadata.append(combined_metadata)
            else:
                logging.error(f"Failed to extract data from PDF: {relative_pdf_path}")

        await self.save_all_metadata(all_metadata, output_file)

    async def validate_pdfs_in_json(self, json_file_path):
        csv_pdf_paths = self.csv_data['file_path'].tolist()

        if not await aio_os.path.exists(json_file_path):
            logging.error(f"JSON file not found: {json_file_path}")
            return

        try:
            async with aiofiles.open(json_file_path, 'r', encoding='utf-8') as f:
                json_data = json.loads(await f.read())
        except Exception as e:
            logging.error(f"Error reading JSON file: {e}")
            return

        json_pdf_paths = [entry['csv_metadata']['file_path'] for entry in json_data]
        missing_pdfs = [pdf for pdf in csv_pdf_paths if pdf not in json_pdf_paths]

        if not missing_pdfs:
            logging.info("Validation successful: All PDFs in the CSV are present in the JSON.")
        else:
            logging.warning(f"Validation failed: The following PDFs are missing in the JSON:\n{missing_pdfs}")

if __name__ == "__main__":
    # 获取当前脚本文件的路径
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # 定义相对路径，并将其转换为绝对路径
    csv_relative_path = "../../../data/pdfs/speech_metadata.csv"
    output_metadata_file = "./UploadDb/all_metadata_and_text.json"

    # 将相对路径转换为基于脚本的绝对路径
    csv_absolute_path = os.path.abspath(os.path.join(script_dir, csv_relative_path))
    output_metadata_absolute_path = os.path.abspath(os.path.join(script_dir, output_metadata_file))


    # 创建 PDFHandler 实例
    handler = PDFHandler(csv_absolute_path)

    # 运行异步函数
    asyncio.run(handler.process_all_pdfs(output_metadata_absolute_path))
    asyncio.run(handler.validate_pdfs_in_json(output_metadata_absolute_path))
