import sys
import os
import asyncio
from fastapi import FastAPI
import uvicorn
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import json
import logging

# 动态添加项目根目录到 sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
sys.path.append(project_root)

# 正常导入模块
from core.Speech.SpeechDownloader import SpeechDownloader
from core.Speech.PdfHandler.PdfHandler import PDFHandler
from core.Speech.PdfHandler.UploadDb.MongoDbManager import AsyncMongoDBManager, upload_json_to_mongodb

app = FastAPI()

# 日志配置
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# APScheduler 初始化
scheduler = AsyncIOScheduler()

# MongoDB Manager 初始化
mongo_db_manager = None

# 定时任务：串联三个步骤
async def scheduled_job():
    try:
        logger.info("定时任务开始：下载演讲、处理 PDF 并上传到 MongoDB")

        # Step 1: 下载演讲 PDF
        downloader = SpeechDownloader()
        downloader.download_speeches_parallel()
        downloader.save_metadata()

        # Step 2: 处理 PDF 并生成 JSON 元数据文件
        pdf_handler = PDFHandler(csv_relative_path="../../../data/pdfs/speech_metadata.csv")
        output_metadata_file = "./UploadDb/all_metadata_and_text.json"
        pdf_handler.process_all_pdfs(output_metadata_file)

        # Step 3: 上传 JSON 数据到 MongoDB
        await upload_json_to_mongodb(output_metadata_file, mongo_db_manager)

        logger.info("定时任务完成")

    except Exception as e:
        logger.error(f"定时任务执行过程中出错: {e}")

# FastAPI 应用启动事件
@app.on_event("startup")
async def startup_event():
    global mongo_db_manager

    # 初始化 MongoDB 管理器
    mongo_db_manager = AsyncMongoDBManager()

    # 创建 MongoDB 索引
    await mongo_db_manager.create_unique_index()

    # 定时任务调度配置
    scheduler.add_job(
        scheduled_job,
        CronTrigger(hour="*", minute="0"),  # 每小时执行一次，可根据需求修改为 cron 表达式
        id="speech_job",
        replace_existing=True
    )

    # 启动定时任务调度器
    scheduler.start()
    logger.info("APScheduler 启动完成，定时任务已添加")

@app.on_event("shutdown")
async def shutdown_event():
    # 清理资源
    if mongo_db_manager:
        await mongo_db_manager.close_connection()

# 运行 FastAPI 应用
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000, reload=True)
