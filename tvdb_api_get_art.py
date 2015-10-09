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
    print("Fetching all shows with no TVDB art...")
    id_list = collection_show.find({"tvdb_id": {"$ne": "N/A"}, "tvdb_art_fetched": {"$exists": False}}, {'id': 1, 'tvdb_id': 1, 'order': 1, '_id': 0}).sort([("order", 1)])

    return id_list


def get_art(order, tvdb_id):
    url = "http://thetvdb.com/api/" + api_key + "/series/" + str(tvdb_id) + "/all"

    http = urllib3.PoolManager()
    r = http.request('GET', url)

    print(order, url)

    if r.status != 404:
        try:
            xml = minidom.parseString(r.data)

            for node in xml.getElementsByTagName('Series'):
                try:
                    banner = node.getElementsByTagName('banner')[0].firstChild.data
                except AttributeError:
                    banner = None
                try:
                    fanart = node.getElementsByTagName('fanart')[0].firstChild.data
                except AttributeError:
                    fanart = None
                try:
                    poster = node.getElementsByTagName('poster')[0].firstChild.data
                except AttributeError:
                    poster = None
                return banner, fanart, poster
        except ExpatError as e:
            print("ERROR WITH API PAGE: ", e)
            return None, None, None
            pass
    else:
        print("404 error.")
        return None, None, None


def main_tvdb_art():
    start_time = datetime.now()
    ids = db_select_show()
    count = 1

    for id in ids:
        show_id = id['id']
        tvdb_id = id['tvdb_id']
        order = id['order']
        banner, fanart, poster = get_art(order, tvdb_id)

        if banner is not None:
            banner = "http://thetvdb.com/banners/" + banner
            collection_show.update({"id": show_id}, {"$unset": {"banner": 1}}, False, False)
            collection_show.update_one({"id": show_id}, {"$set": {"banner": banner}})
        else:
            print("No banner found.")
        if fanart is not None:
            fanart = "http://thetvdb.com/banners/" + fanart
            collection_show.update({"id": show_id}, {"$unset": {"fanart": 1}}, False, False)
            collection_show.update_one({"id": show_id}, {"$set": {"fanart": fanart}})
        else:
            print("No fanart found.")
        if poster is not None:
            poster = "http://thetvdb.com/banners/" + poster
            collection_show.update({"id": show_id}, {"$unset": {"poster": 1}}, False, False)
            collection_show.update_one({"id": show_id}, {"$set": {"poster": poster}})
        else:
            print("No poster found.")

        collection_show.update({"id": show_id}, {"$unset": {"tvdb_art_fetched": 1}}, False, False)
        collection_show.update_one({"id": show_id}, {"$set": {"tvdb_art_fetched": True}})

        count += 1

    print("Process complete. ", count - 1, "shows processed.")
    end_time = datetime.now()
    duration = end_time - start_time
    print("Start time: ", str(start_time))
    print("End time: ", str(end_time))
    print("Total duration (minutes): ", str(duration.seconds / 60))


main_tvdb_art()
