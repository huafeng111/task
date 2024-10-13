import os
import json
import pymongo
import importlib

def dynamic_import(module_name, module_path):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
# Dynamically import config.py for MongoDB connection settings
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
            # Insert a single speech document into the MongoDB collection
            self.collection.insert_one(speech_data)
            print(f"Inserted speech into MongoDB: {speech_data.get('csv_metadata', {}).get('title', 'Unknown Title')}")
        except Exception as e:
            print(f"Error inserting speech: {str(e)}")

    def insert_many_speeches(self, speeches_data):
        try:
            # Insert multiple documents (speeches) into the MongoDB collection
            if speeches_data:
                self.collection.insert_many(speeches_data)
                print(f"Inserted {len(speeches_data)} speeches into MongoDB.")
            else:
                print("No speeches to insert.")
        except Exception as e:
            print(f"Error inserting speeches: {str(e)}")

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

# Upload JSON data to MongoDB
def upload_json_to_mongodb(json_file_path, db_manager):
    try:
        # Load JSON data from the file
        if os.path.exists(json_file_path):
            with open(json_file_path, 'r', encoding='utf-8') as f:
                speeches_data = json.load(f)
                print(f"Loaded {len(speeches_data)} speeches from {json_file_path}")

                # Insert all speeches into MongoDB
                db_manager.insert_many_speeches(speeches_data)
        else:
            print(f"JSON file not found: {json_file_path}")
    except Exception as e:
        print(f"Error uploading JSON data to MongoDB: {str(e)}")

if __name__ == "__main__":
    # Path to the JSON file containing speech metadata and text
    json_file_path = "all_metadata_and_text.json"  # Adjust this path as necessary

    # Initialize MongoDBManager
    db_manager = MongoDBManager()

    # Upload the JSON data to MongoDB
    upload_json_to_mongodb(json_file_path, db_manager)

    # Close the MongoDB connection
    db_manager.close_connection()
