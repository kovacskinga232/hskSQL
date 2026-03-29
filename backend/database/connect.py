from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

client = MongoClient(os.getenv("MONGODB_URI"))

def get_client():
    return client


def get_collection(db_name: str, table_name: str):
    db = client[db_name]
    return db[table_name]