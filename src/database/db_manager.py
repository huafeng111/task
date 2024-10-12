import pymongo
from config.config import MONGO_URI, DATABASE_NAME, COLLECTION_NAME
from utils.logger import get_logger

logger = get_logger(__name__)

class MongoDBManager:
    def __init__(self):
        self.client = pymongo.MongoClient(MONGO_URI)
        self.db = self.client[DATABASE_NAME]
        self.collection = self.db[COLLECTION_NAME]

    def insert_speech(self, speech_data):
        try:
            self.collection.insert_one(speech_data)
            logger.info(f"Inserted speech into MongoDB: {speech_data['title']}")
        except Exception as e:
            logger.error(f"Error inserting speech: {str(e)}")

    def get_speeches(self, query):
        return self.collection.find(query)
