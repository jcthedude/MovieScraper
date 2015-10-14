from globals import *
import urllib3
from xml.dom import minidom
from xml.parsers.expat import ExpatError
from datetime import datetime


def db_select_show():
    print("Fetching all shows with no TVDB art...")
    id_list = collection_show.find({"tvdb_id": {"$ne": "N/A"}, "tvdb_detail_fetched": {"$exists": False}}, {'id': 1, 'tvdb_id': 1, 'order': 1, '_id': 0}).sort([("order", 1)])

    return id_list


def get_show_details(order, tvdb_id):
    url = "http://thetvdb.com/api/" + tvdb_api_key + "/series/" + str(tvdb_id) + "/all"

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
                try:
                    air_day = node.getElementsByTagName('Airs_DayOfWeek')[0].firstChild.data
                except AttributeError:
                    air_day = None
                try:
                    air_time = node.getElementsByTagName('Airs_Time')[0].firstChild.data
                except AttributeError:
                    air_time = None
                try:
                    network = node.getElementsByTagName('Network')[0].firstChild.data
                except AttributeError:
                    network = None
                return banner, fanart, poster, air_day, air_time, network
        except ExpatError as e:
            print("ERROR WITH API PAGE: ", e)
            return None, None, None
            pass
    else:
        print("404 error.")
        return None, None, None


def main():
    start_time = datetime.now()
    ids = db_select_show()
    count = 1

    for id in ids:
        show_id = id['id']
        tvdb_id = id['tvdb_id']
        order = id['order']
        timestamp = datetime.now()
        banner, fanart, poster, air_day, air_time, network = get_show_details(order, tvdb_id)

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
        if air_day is not None:
            collection_show.update({"id": show_id}, {"$unset": {"air_day": 1}}, False, False)
            collection_show.update_one({"id": show_id}, {"$set": {"air_day": air_day}})
        else:
            print("No air day found.")
        if air_time is not None:
            collection_show.update({"id": show_id}, {"$unset": {"air_time": 1}}, False, False)
            collection_show.update_one({"id": show_id}, {"$set": {"air_time": air_time}})
        else:
            print("No air time found.")
        if network is not None:
            collection_show.update({"id": show_id}, {"$unset": {"network": 1}}, False, False)
            collection_show.update_one({"id": show_id}, {"$set": {"network": network}})
        else:
            print("No network found.")

        collection_show.update({"id": show_id}, {"$unset": {"tvdb_detail_fetched": 1, "tvdb_detail_timestamp": 1}}, False, False)
        collection_show.update_one({"id": show_id}, {"$set": {"tvdb_detail_fetched": True, "tvdb_detail_timestamp": timestamp}})

        count += 1

    print("Process complete. ", count - 1, "shows processed.")
    end_time = datetime.now()
    duration = end_time - start_time
    print("Start time: ", str(start_time))
    print("End time: ", str(end_time))
    print("Total duration (minutes): ", str(duration.seconds / 60))


main()
