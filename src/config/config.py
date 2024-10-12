import os

MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "USA_FED"
COLLECTION_NAME = "speeches"

# Data storage directories
DATA_DIR = os.path.join(os.getcwd(), 'data')
PDF_DIR = os.path.join(DATA_DIR, 'pdfs')

# URL patterns for Federal Reserve sites
FEDERAL_RESERVE_URLS = {
    "FED": "https://www.federalreserve.gov/newsevents/speech/2021-speeches.htm",  # 使用2021年的演讲页面测试
    # "NY_FED": "https://www.newyorkfed.org/newsevents/speeches",
    # "DALLAS_FED": "https://www.dallasfed.org/news/speeches",
    # More URLs can be added as needed
}
