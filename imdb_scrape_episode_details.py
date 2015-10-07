import urllib3
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import datetime

# setup mongodb collections
client = MongoClient("mongodb://admin:Campana1@107.170.248.43:27017")
db = client.tv
collection_show = db.show


def db_select_imdb_show_list():
    # fetch the list of all shows to get details for
    print("Fetching  all series...")
    id_list = collection_show.find({"order": {"$lt": 2}}, {'id': 1, 'name': 1, 'order': 1, '_id': 0}).sort([("order", 1)])

    return id_list


def db_select_imdb_season_list(show_id):
    # fetch the list of all shows to get details for
    print("Fetching  all seasons...")
    id_list = collection_show.find({"id": show_id}, {'season.id': 1, '_id': 0}).sort([("season.id", 1)])

    return id_list


def imdb_fetch_episode_details():
    # declare variables
    start_time = datetime.now()
    count = 1

    # get show list
    ids = db_select_imdb_show_list()

    # process each show returned from the show list
    for id in ids:
        # declare variables
        show_id = id['id']
        order = id['order']
        timestamp = datetime.utcnow()
        seasons = db_select_imdb_season_list(show_id)

        for season in seasons:
            season_id = season.get("season")[0]['id']
            valid_url = True
            url = "http://www.imdb.com/title/" + show_id + "/episodes?season=" + season_id
            http = urllib3.PoolManager()

            # make sure http request is valid
            try:
                r = http.request('GET', url)
            except:
                valid_url = False
                print("Problem with URL data returned.")
                pass

            # print details for console tracking
            print(order, url)

            # proceed with the process is there's a valid http response
            if valid_url:
                # soup the data returned from the http request
                soup = BeautifulSoup(r.data, 'html.parser')

                # setup soups
                try:
                    soup_episode = soup.find_all("div", {"itemprop": "episodes"})
                except IndexError:
                    soup_episode = None
                    pass

                # parse soups and input data into show dict
                if soup_episode is not None:
                    for episode in soup_episode:
                        episode_id = episode.find_all('meta')[0]['content'].strip()
                        print(episode_id)
                    #collection_show.update_one({"id": show_id, "season.id": season}, {"$set": {"season": season_list, "timestamp": timestamp}})
                else:
                    print("No season found")

                count += 1

            else:
                count += 1

    # print process results
    print("Process complete. ", count-1, "series processed.")
    end_time = datetime.now()
    duration = end_time - start_time
    print("Start time: ", str(start_time))
    print("End time: ", str(end_time))
    print("Total duration (minutes): ", str(duration.seconds / 60))


imdb_fetch_episode_details()

