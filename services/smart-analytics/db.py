# Folder: services/smart-analytics/db.py
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/smartcity")
client = MongoClient(MONGO_URI)
db = client.get_default_database() if client else client["smartcity"]

# Collections
raw_collection = db["telemetry_raw"]
analytics_collection = db["telemetry_analytics"]

# Helper to insert raw event
def insert_raw(event: dict):
    # ensure timestamp field is stored as datetime if present (later can normalize)
    raw_collection.insert_one(event)
