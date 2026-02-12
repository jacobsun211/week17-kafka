from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"
mongo = MongoClient(MONGO_URI)
collection = mongo["week17"]["suspicious"]