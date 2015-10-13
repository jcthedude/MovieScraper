from pymongo import MongoClient

tvdb_api_key = "58AE0E6345017543"

client = MongoClient("mongodb://admin:Campana1@45.55.23.232:27017")
db = client.tv
collection_show = db.show
collection_show_list = db.show_list
