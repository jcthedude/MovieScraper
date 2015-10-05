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
    id_list = collection_show_list.find({"id": "tt0773262"}, {'id': 1, 'name': 1, 'order': 1, '_id': 0}).sort([("order", 1)])

    return id_list


def imdb_fetch_episode_details():
    start_time = datetime.now()
    ids = db_select_imdb_series_list()
    count = 1

    for id in ids:
        show_id = id['id']
        name = id['name']
        order = id['order']
        timestamp = datetime.utcnow()

        valid_url = True
        url = "http://www.imdb.com/title/" + show_id + "/epcast?"
        http = urllib3.PoolManager()

        try:
            r = http.request('GET', url)
        except:
            valid_url = False
            print("Problem with URL data returned.")
            pass

        print(order, url)

        if valid_url:
            show = {}
            show.update({"id": show_id, "timestamp": timestamp})

            soup = BeautifulSoup(r.data, 'html.parser')
            try:
                soup_season = soup.find_all('h3')
            except IndexError:
                soup_season = None
                pass
            try:
                soup_episode = soup.find_all('h4')
            except IndexError:
                soup_season = None
                pass

            if soup_season is not None:
                for season in soup_season:
                    season_name = season.get_text().strip()
                    if "Season" in season_name:
                        print(season_name)
            else:
                print("No season found")

            if soup_episode is not None:
                for episode in soup_episode:
                    episode_full_id = episode.get_text().strip()
                    episode_id = ''.join(filter(lambda x: x.isdigit(), episode_full_id))
                    episode_name = episode.find_all('a')[0].get_text().strip()
                    print(episode_full_id, episode_name)
            else:
                print("No episode found")

            # collection_show.insert(show)
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


