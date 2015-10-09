import urllib3
from pymongo import MongoClient
from xml.dom import minidom
from xml.parsers.expat import ExpatError
from datetime import datetime

api_key = "58AE0E6345017543"

client = MongoClient("mongodb://admin:Campana1@107.170.248.43:27017")
db = client.tv
collection_show = db.show


def db_select_show():
    print("Fetching all shows with no TVDB ID...")
    id_list = collection_show.find({"tvdb_fetched": {"$exists": False}}, {'id': 1, 'order': 1, '_id': 0}).sort([("order", 1)])

    return id_list


def get_tvdb_id(order, show_id):
    url = "http://thetvdb.com/api/GetSeriesByRemoteID.php?imdbid=" + str(show_id)

    http = urllib3.PoolManager()
    r = http.request('GET', url)

    print(order, url)

    if r.status != 404:
        try:
            xml = minidom.parseString(r.data)

            for node in xml.getElementsByTagName('Series'):
                try:
                    tvdb_id = node.getElementsByTagName('seriesid')[0].firstChild.data
                except AttributeError:
                    tvdb_id = None
                return tvdb_id
        except ExpatError as e:
            print("ERROR WITH API PAGE: ", e)
            return None
            pass
    else:
        print("404 error.")
        return None


def main():
    start_time = datetime.now()
    ids = db_select_show()
    count = 1

    for id in ids:
        show_id = id['id']
        order = id['order']
        tvdb_id = get_tvdb_id(order, show_id)

        if tvdb_id is not None:
            collection_show.update({"id": show_id}, {"$unset": {"tvdb_fetched": 1, "tvdb_id": 1}}, False, False)
            collection_show.update_one({"id": show_id}, {"$set": {"tvdb_fetched": True, "tvdb_id": tvdb_id}})
        else:
            collection_show.update({"id": show_id}, {"$unset": {"tvdb_fetched": 1, "tvdb_id": 1}}, False, False)
            collection_show.update_one({"id": show_id}, {"$set": {"tvdb_fetched": True, "tvdb_id": "N/A"}})
            print("No TVDB match found.")

        count += 1

    print("Process complete. ", count - 1, "shows processed.")
    end_time = datetime.now()
    duration = end_time - start_time
    print("Start time: ", str(start_time))
    print("End time: ", str(end_time))
    print("Total duration (minutes): ", str(duration.seconds / 60))


main()
