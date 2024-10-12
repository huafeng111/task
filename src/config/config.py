import os

MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "USA_FED"
COLLECTION_NAME = "speeches"

# Data storage directories
DATA_DIR = os.path.join(os.getcwd(), 'data')
PDF_DIR = os.path.join(DATA_DIR, 'pdfs')


