# -*- coding: utf-8 -*-

import os
import json
import pymongo
import importlib.util  # 确保导入 importlib.util

def dynamic_import(module_name, module_path):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

# 动态导入 config.py，用于 MongoDB 连接设置
current_script_dir = os.path.dirname(os.path.abspath(__file__))
print(current_script_dir)
# 调整路径，确保正确指向 config.py 文件的实际位置
config_module_path = os.path.abspath(
    os.path.join(current_script_dir, '..', '..', '..', '..','config', 'config.py')
)
print(f"配置模块路径: {config_module_path}")

config = dynamic_import("config", config_module_path)

class MongoDBManager:
    def __init__(self):
        try:
            # 从动态导入的 config 中获取 MongoDB URI、数据库名称和集合名称
            mongo_uri = getattr(config, "MONGO_URI")
            database_name = getattr(config, "DATABASE_NAME")
            collection_name = getattr(config, "COLLECTION_NAME")

            # 使用 MONGO_URI 初始化 MongoDB 客户端
            self.client = pymongo.MongoClient(mongo_uri)
            self.db = self.client[database_name]  # 访问数据库
            self.collection = self.db[collection_name]  # 访问集合
            print(f"已连接到 MongoDB 数据库: {database_name}, 集合: {collection_name}")
        except Exception as e:
            print(f"连接 MongoDB 失败: {str(e)}")

    def insert_speech(self, speech_data):
        try:
            # 将单个演讲文档插入 MongoDB 集合
            self.collection.insert_one(speech_data)
            print(f"已插入演讲到 MongoDB: {speech_data.get('csv_metadata', {}).get('title', '未知标题')}")
        except Exception as e:
            print(f"插入演讲时出错: {str(e)}")

    def insert_many_speeches(self, speeches_data):
        try:
            # 将多个演讲文档插入 MongoDB 集合
            if speeches_data:
                self.collection.insert_many(speeches_data)
                print(f"已插入 {len(speeches_data)} 篇演讲到 MongoDB。")
            else:
                print("没有演讲可插入。")
        except Exception as e:
            print(f"插入演讲时出错: {str(e)}")

    def get_speeches(self, query):
        try:
            # 查询 MongoDB 集合
            return list(self.collection.find(query))
        except Exception as e:
            print(f"检索演讲时出错: {str(e)}")
            return []

    def close_connection(self):
        # 完成后关闭 MongoDB 连接
        self.client.close()
        print("已关闭 MongoDB 连接。")

# 将 JSON 数据上传到 MongoDB
def upload_json_to_mongodb(json_file_path, db_manager):
    try:
        # 从文件中加载 JSON 数据
        if os.path.exists(json_file_path):
            with open(json_file_path, 'r', encoding='utf-8') as f:
                speeches_data = json.load(f)
                print(f"从 {json_file_path} 加载了 {len(speeches_data)} 篇演讲")

                # 将所有演讲插入 MongoDB
                db_manager.insert_many_speeches(speeches_data)
        else:
            print(f"未找到 JSON 文件: {json_file_path}")
    except Exception as e:
        print(f"上传 JSON 数据到 MongoDB 时出错: {str(e)}")

if __name__ == "__main__":
    # 包含演讲元数据和文本的 JSON 文件路径
    json_file_path = "all_metadata_and_text.json"  # 根据需要调整此路径

    # 初始化 MongoDBManager
    db_manager = MongoDBManager()

    # 将 JSON 数据上传到 MongoDB
    upload_json_to_mongodb(json_file_path, db_manager)

    # 关闭 MongoDB 连接
    db_manager.close_connection()
