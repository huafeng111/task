import os
import pymongo
from src.core.Speech.SpeechDownloader import dynamic_import

# Dynamically import config.py
config_module_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "config", "config.py")
config = dynamic_import("config", config_module_path)

class MongoDBManager:
    def __init__(self):
        try:
            # Access MongoDB URI, database name, and collection name from dynamically imported config
            mongo_uri = getattr(config, "MONGO_URI")
            database_name = getattr(config, "DATABASE_NAME")
            collection_name = getattr(config, "COLLECTION_NAME")

            # Initialize the MongoDB client using the MONGO_URI from config
            self.client = pymongo.MongoClient(mongo_uri)
            self.db = self.client[database_name]  # Access the database
            self.collection = self.db[collection_name]  # Access the collection
            print(f"Connected to MongoDB database: {database_name}, collection: {collection_name}")
        except Exception as e:
            print(f"Failed to connect to MongoDB: {str(e)}")

    def insert_speech(self, speech_data):
        try:
            # Insert a document (speech) into the MongoDB collection
            self.collection.insert_one(speech_data)
            print(f"Inserted speech into MongoDB: {speech_data['title']}")
        except Exception as e:
            print(f"Error inserting speech: {str(e)}")

    def get_speeches(self, query):
        try:
            # Query the MongoDB collection
            return list(self.collection.find(query))
        except Exception as e:
            print(f"Error retrieving speeches: {str(e)}")
            return []

    def close_connection(self):
        # Close the MongoDB connection when done
        self.client.close()
        print("MongoDB connection closed.")

# Example usage
if __name__ == "__main__":
    db_manager = MongoDBManager()

    # Insert a test document
    test_speech = {
        "title": "Test Speech",
        "author": "Test Author",
        "year": 2024,
        "content": "This is a test speech content"
    }
    db_manager.insert_speech(test_speech)

    # Retrieve speeches (e.g., all speeches)
    speeches = db_manager.get_speeches({})
    print(f"Retrieved {len(speeches)} speeches.")

    # Close the connection
    db_manager.close_connection()
