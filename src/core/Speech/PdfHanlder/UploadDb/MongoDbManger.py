import os
import json
import motor.motor_asyncio  # Use motor to replace pymongo for asynchronous operations
import importlib.util
import asyncio
from pymongo.errors import BulkWriteError  # 用于捕获批量写入错误

def dynamic_import(module_name, module_path):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

# Dynamically import config.py, used for MongoDB connection settings
current_script_dir = os.path.dirname(os.path.abspath(__file__))
config_module_path = os.path.abspath(
    os.path.join(current_script_dir, '..', '..', '..', '..', 'config', 'config.py')
)

config = dynamic_import("config", config_module_path)

class AsyncMongoDBManager:
    def __init__(self):
        try:
            # Retrieve MongoDB URI, database name, and collection name from the dynamically imported config
            mongo_uri = getattr(config, "MONGO_URI")
            database_name = getattr(config, "DATABASE_NAME")
            collection_name = getattr(config, "COLLECTION_NAME")

            # Print URI for debugging purposes
            print(f"MongoDB URI: {mongo_uri}", database_name, collection_name)

            # Initialize MongoDB client using motor's AsyncIOMotorClient
            self.client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri)
            self.db = self.client[database_name]  # Access the database
            self.collection = self.db[collection_name]  # Access the collection
            print(f"Connected to MongoDB database: {database_name}, collection: {collection_name}")

        except Exception as e:
            print(f"Failed to connect to MongoDB: {str(e)}")
            self.client = None

    async def create_unique_index(self):
        try:
            # 创建 title 字段的唯一索引
            await self.collection.create_index([("title", 1)], unique=True)
            print("Created unique index on 'title' field.")
        except Exception as e:
            print(f"Failed to create unique index on 'title': {str(e)}")

    async def insert_speech(self, speech_data):
        if not self.client:
            print("MongoDB client not initialized, cannot insert data.")
            return
        try:
            await self.collection.insert_one(speech_data)
            print(f"Inserted speech into MongoDB: {speech_data.get('csv_metadata', {}).get('title', 'Unknown Title')}")
        except Exception as e:
            print(f"Error inserting speech: {str(e)}")

    async def insert_many_speeches(self, speeches_data):
        if not self.client:
            print("MongoDB client not initialized, cannot insert data.")
            return
        try:
            if speeches_data:
                # 使用 ordered=False 选项，以便即使某些文档失败，其他文档也会继续插入
                result = await self.collection.insert_many(speeches_data, ordered=False)
                print(f"Inserted {len(result.inserted_ids)} speeches into MongoDB.")
            else:
                print("No speeches to insert.")
        except BulkWriteError as bwe:
            # 捕获批量写入错误，处理违反唯一约束的文档
            write_errors = bwe.details.get('writeErrors', [])
            # 获取成功插入的文档数量
            inserted_count = bwe.details.get('nInserted', 0)
            print(f"Inserted {inserted_count} speeches before encountering an error.")

            # 输出错误信息，或根据需要做进一步处理
            for error in write_errors:
                if error.get('code') == 11000:  # 11000 是唯一索引冲突的错误码
                    print(f"Duplicate entry found for document: {error.get('op')}")
                else:
                    print(f"Error inserting document: {error}")
        except Exception as e:
            print(f"Error inserting speeches: {str(e)}")

    async def get_speeches(self, query):
        if not self.client:
            print("MongoDB client not initialized, cannot retrieve data.")
            return []
        try:
            return await self.collection.find(query).to_list(length=100)
        except Exception as e:
            print(f"Error retrieving speeches: {str(e)}")
            return []

    async def close_connection(self):
        # No longer necessary to manually close the connection, motor will handle it automatically
        if self.client:
            print("MongoDB connection is managed, no need to close manually.")

# Upload JSON data to MongoDB
async def upload_json_to_mongodb(json_file_path, db_manager):
    try:
        # Load JSON data from the file using UTF-8 encoding
        if os.path.exists(json_file_path):
            with open(json_file_path, 'r', encoding='utf-8') as f:
                speeches_data = json.load(f)
                print(f"Loaded {len(speeches_data)} speeches from {json_file_path}")

                # Insert all speeches into MongoDB
                await db_manager.insert_many_speeches(speeches_data)
        else:
            print(f"JSON file not found: {json_file_path}")
    except Exception as e:
        print(f"Error uploading JSON data to MongoDB: {str(e)}")


# 将所有异步操作放在一个函数中，避免多次调用 asyncio.run()
async def main():
    # Path to the JSON file containing speech metadata and text, use absolute path to ensure the file exists
    json_file_path = r"all_metadata_and_text.json"

    # Initialize MongoDBManager
    db_manager = AsyncMongoDBManager()

    # Create unique index for 'title' field
    await db_manager.create_unique_index()

    # Upload JSON data to MongoDB
    await upload_json_to_mongodb(json_file_path, db_manager)

    # Close connection
    await db_manager.close_connection()

if __name__ == "__main__":
    # 使用 asyncio.run() 来运行所有异步操作
    asyncio.run(main())
