from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['vehicle_rental']

users_collection = db["users"]
vehicles_collection = db["vehicles"]
rentals_collection = db["rentals"]
branches_collection = db["branches"]

