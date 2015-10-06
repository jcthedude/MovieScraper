import urllib3
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://admin:Campana1@107.170.248.43:27017")
db = client.tv
collection_show_list = db.show_list
collection_show = db.show


def db_select_imdb_series_list():
    print("Fetching  all series...")
    id_list = collection_show_list.find({"id": "tt0773262"}, {'id': 1, 'order': 1, '_id': 0}).sort([("order", 1)])

    return id_list


def imdb_fetch_episode_details():
    start_time = datetime.now()
    ids = db_select_imdb_series_list()
    count = 1

    for id in ids:
        show_id = id['id']
        order = id['order']
        timestamp = datetime.utcnow()

        valid_url = True
        url = "http://www.imdb.com/title/" + show_id
        http = urllib3.PoolManager()

        try:
            r = http.request('GET', url)
        except:
            valid_url = False
            print("Problem with URL data returned.")
            pass

        print(order, url)

        if valid_url:
            soup = BeautifulSoup(r.data, 'html.parser')
            try:
                soup_season = soup.find_all("div", {"class": "seasons-and-year-nav"})[0].find_all('a')
            except IndexError:
                soup_season = None
                pass

            if soup_season is not None:
                season_list = []
                for season in soup_season:
                    season_id = season.get_text().strip()
                    if season_id.isdigit() and int(season_id) < 100:
                        season_dict = ({"id": season_id})
                        season_list.append(season_dict)
                collection_show.update_one({"id": show_id}, {"$set": {"season": season_list, "timestamp": timestamp}})
            else:
                print("No season found")

            count += 1

        else:
            count += 1

    print("Process complete. ", count-1, "series processed.")
    end_time = datetime.now()
    duration = end_time - start_time
    print("Start time: ", str(start_time))
    print("End time: ", str(end_time))
    print("Total duration (minutes): ", str(duration.seconds / 60))


imdb_fetch_episode_details()


