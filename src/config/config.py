import os
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()  # This will read the .env file and load the variables into the environment

# Function to get environment variable or return a default value
def get_env_variable(var_name, default_value=None):
    return os.environ.get(var_name, default_value)

# Logging configuration
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'speech_downloader.log')
LOG_MAX_BYTES = 5 * 1024 * 1024  # 5 MB per log file
LOG_BACKUP_COUNT = 3  # Keep 3 backups

# Database configuration
MONGO_URI = get_env_variable("MONGO_URI", "mongodb://localhost:27017/")
DATABASE_NAME = get_env_variable("DATABASE_NAME", "USA_FED")
COLLECTION_NAME = get_env_variable("COLLECTION_NAME", "speeches")

# Base directory configuration
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# BASE_DIR will now point to the core directory, 3 levels up from the current file's location

# Data directories
DATA_DIR = os.path.join(BASE_DIR, 'data')  # Point to \task\src\core\Speech\data
PDF_DIR = os.path.join(DATA_DIR, 'pdfs')  # Create the 'pdfs' directory inside the data folder

# Download configuration
START_YEAR = 2017
MAX_WORKERS = 5
