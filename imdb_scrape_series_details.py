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
    id_list = collection_show_list.find({}, {'id': 1, 'name': 1, 'order': 1, '_id': 0}).sort([("order", 1)])

    return id_list


# def db_insert_imdb_series_season_list(cursor, id, seasonNumber, url):
#     cursor.execute("""INSERT INTO imdb_series_season_list SELECT %s, %s, %s""", (id, seasonNumber, url))


def imdb_fetch_series_season_list():
    start_time = datetime.now()
    ids = db_select_imdb_series_list()
    count = 1

    for id in ids:
        show_id = id['id']
        name = id['name']
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

        print(count, url)

        if valid_url:
            show = {}
            show.update({"id": show_id, "name": name, "order": order, "timestamp": timestamp})

            soup = BeautifulSoup(r.data, 'html.parser')
            soup_image = soup.find_all("div", {"class": "image"})[0].find_all('a')[0].find_all('img')[0]['src']
            soup_description = soup.find_all("p", {"itemprop": "description"})[0]
            soup_content_rating = soup.find_all("span", {"itemprop": "contentRating"})[0]
            soup_runtime = soup.find_all("time", {"itemprop": "duration"})[0]
            soup_creator = soup.find_all("div", {"itemprop": "creator"})[0].find_all('a')
            soup_rating = soup.find_all("span", {"itemprop": "ratingValue"})[0]
            soup_rating_count = soup.find_all("span", {"itemprop": "ratingCount"})[0]
            soup_genre = soup.find_all("div", {"itemprop": "genre"})[0].find_all('a')
            soup_rundate = soup.find_all("span", {"class": "nobr"})[0]
            soup_recommended = soup.find_all("div", {"class": "rec_slide"})[0].find_all('a')
            soup_cast = soup.find_all("table", {"class": "cast_list"})[0]

            if len(soup_image) != 0:
                image = soup_image
                show.update({'image': image})
            else:
                print("No image found")

            if len(soup_description) != 0:
                description = soup_description.get_text().strip()
                show.update({'description': description})
            else:
                print("No description found")

            if len(soup_content_rating) != 0:
                content_rating = soup_content_rating.get_text().strip()
                show.update({'content_rating': content_rating})
            else:
                print("No content rating found")

            if len(soup_runtime) != 0:
                runtime = soup_runtime.get_text().strip()
                show.update({'runtime': runtime})
            else:
                print("No runtime found")

            if len(soup_creator) != 0:
                soup_count = 1
                creator_list = []
                for creator in soup_creator:
                    creator_name = creator.get_text().strip()
                    creator_id = creator['href'][6:-15]
                    creator_dict = ({"id": creator_id, "name": creator_name, "order": soup_count})
                    creator_list.append(creator_dict)
                    soup_count += 1
                show.update({"creator": creator_list})
            else:
                print("No creator found")

            if len(soup_rating) != 0:
                rating = soup_rating.get_text().strip()
                show.update({'rating': rating})
            else:
                print("No rating found")

            if len(soup_rating_count) != 0:
                rating_count = soup_rating_count.get_text().strip()
                show.update({'rating_count': rating_count})
            else:
                print("No rating count found")

            if len(soup_genre) != 0:
                soup_count = 1
                genre_list = []
                for genre in soup_genre:
                    genre = genre.get_text().strip()
                    genre_list.append({"name": genre, "order": soup_count})
                    soup_count += 1
                show.update({"genre": genre_list})
            else:
                print("No actor found")

            if len(soup_rundate) != 0:
                rundate = soup_rundate.get_text().strip()
                show.update({'rundate': rundate})
                if " " in rundate:
                    status = "Continuing"
                else:
                    status = "Ended"
                show.update({'rundate': rundate, 'status': status})
            else:
                print("No run date found")

            if len(soup_recommended) != 0:
                soup_count = 1
                recommendation_list = []
                for rec in soup_recommended:
                    recommend_name = rec.find_all('img')[0]['alt']
                    recommend_id = rec['href'][7:-17]
                    try:
                        recommend_image = rec.find_all('img')[0]['loadlate']
                    except:
                        recommend_image = rec.find_all('img')[0]['src']
                    recommendation_dict = ({"id": recommend_id, "name": recommend_name, "order": soup_count
                                            , "image": recommend_image})
                    recommendation_list.append(recommendation_dict)
                    soup_count += 1
                show.update({"recommendation": recommendation_list})
            else:
                print("No recommendations found")

            if len(soup_cast) != 0:
                soup_count = 1
                actor = []
                for cast in soup_cast.find_all("td", {"class": "primary_photo"}):
                    actor_name = cast.find_all('img')[0]['title']
                    actor_id = cast.find_all('a')[0]['href'][6:-15]
                    try:
                        actor_image = cast.find_all('img')[0]['loadlate']
                    except:
                        actor_image = cast.find_all('img')[0]['src']
                    actor_dict = ({"id": actor_id, "name": actor_name, "order": soup_count, "image": actor_image})
                    actor.append(actor_dict)
                    soup_count += 1

                soup_count = 1
                character = []
                for cast in soup_cast.find_all("td", {"class": "character"}):
                    character_name = cast.find_all('div')[0].get_text().split('(', 1)[0].split('/', 1)[0].strip()
                    character.append(character_name)
                    soup_count += 1

                i = 0
                actor_details_list = []
                while i < soup_count - 1:
                    actor_details = ({"id": actor[i]["id"], "name": actor[i]["name"], "order": actor[i]["order"]
                                     , "image": actor[i]["image"], "character": character[i]})
                    actor_details_list.append(actor_details)
                    i += 1
                show.update({"cast": actor_details_list})
                print(actor_details_list)
            else:
                print("No cast found")

            print(show)
            collection_show.insert(show)

        else:
            count += 1

    print("Process complete. ", count-1, "series processed.")
    end_time = datetime.now()
    duration = end_time - start_time
    print("Start time: ", str(start_time))
    print("End time: ", str(end_time))
    print("Total duration (minutes): ", str(duration.seconds / 60))


imdb_fetch_series_season_list()


