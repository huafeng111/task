import os

# Logging configuration
LOG_FILE = 'speech_downloader.log'
LOG_MAX_BYTES = 5 * 1024 * 1024  # 5 MB per log file
LOG_BACKUP_COUNT = 3  # Keep 3 backups

# Database configuration
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "USA_FED"
COLLECTION_NAME = "speeches"



BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# BASE_DIR will now point to the core directory, 3 levels up from the current file's location

DATA_DIR = os.path.join(BASE_DIR, 'data')  # Point to \task\src\core\Speech\data
PDF_DIR = os.path.join(DATA_DIR, 'pdfs')  # Create the 'pdfs' directory inside the data folder


# Download configuration
START_YEAR = 2023
MAX_WORKERS = 5
