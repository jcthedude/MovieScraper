from datetime import datetime

import urllib3
from bs4 import BeautifulSoup

from globals import *


def db_select_imdb_show_list():
    # fetch the list of all shows to get details for
    print("Fetching all shows...")
    id_list = collection_show_list.find({"order": {"$gt": 0}}, {'id': 1, 'name': 1, 'order': 1, '_id': 0}).sort([("order", 1)])

    return id_list


def imdb_fetch_show_details():
    # declare variables
    start_time = datetime.now()
    count = 1

    # get show list
    ids = db_select_imdb_show_list()

    # process each show returned from the show list
    for id in ids:
        # declare variables
        show_id = id['id']
        name = id['name']
        order = id['order']
        timestamp = datetime.now()
        valid_url = True
        url = "http://www.imdb.com/title/" + show_id
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
            show = {}
            show.update({"id": show_id, "name": name, "order": order, "timestamp": timestamp})

            # soup the data returned from the http request
            soup = BeautifulSoup(r.data, 'html.parser')

            # setup soups
            try:
                soup_image = soup.find_all("div", {"class": "image"})[0].find_all('a')[0].find_all('img')[0]['src']
            except IndexError:
                soup_image = None
                pass
            try:
                soup_description = soup.find_all("p", {"itemprop": "description"})[0]
            except IndexError:
                soup_description = None
                pass
            try:
                soup_content_rating = soup.find_all("span", {"itemprop": "contentRating"})[0]
            except IndexError:
                soup_content_rating = None
                pass
            try:
                soup_runtime = soup.find_all("time", {"itemprop": "duration"})[0]
            except IndexError:
                soup_runtime = None
                pass
            try:
                soup_creator = soup.find_all("div", {"itemprop": "creator"})[0].find_all('a')
            except IndexError:
                soup_creator = None
                pass
            try:
                soup_rating = soup.find_all("span", {"itemprop": "ratingValue"})[0]
            except IndexError:
                soup_rating = None
                pass
            try:
                soup_rating_count = soup.find_all("span", {"itemprop": "ratingCount"})[0]
            except IndexError:
                soup_rating_count = None
                pass
            try:
                soup_genre = soup.find_all("div", {"itemprop": "genre"})[0].find_all('a')
            except IndexError:
                soup_genre = None
                pass
            try:
                soup_rundate = soup.find_all("span", {"class": "nobr"})[0]
            except IndexError:
                soup_rundate = None
                pass
            try:
                soup_recommended = soup.find_all("div", {"class": "rec_slide"})[0].find_all('a')
            except IndexError:
                soup_recommended = None
                pass
            try:
                soup_cast = soup.find_all("table", {"class": "cast_list"})[0]
            except IndexError:
                soup_cast = None
                pass

            # parse soups and input data into show dict
            if soup_image is not None:
                image = soup_image
                show.update({'image': image})
            else:
                print("No image found")

            if soup_description is not None:
                description = soup_description.get_text().strip()
                show.update({'description': description})
            else:
                print("No description found")

            if soup_content_rating is not None:
                content_rating = soup_content_rating.get_text().strip()
                show.update({'content_rating': content_rating})
            else:
                print("No content rating found")

            if soup_runtime is not None:
                runtime = soup_runtime.get_text().strip()
                show.update({'runtime': runtime})
            else:
                print("No runtime found")

            if soup_creator is not None:
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

            if soup_rating is not None:
                rating = soup_rating.get_text().strip()
                show.update({'rating': rating})
            else:
                print("No rating found")

            if soup_rating_count is not None:
                rating_count = soup_rating_count.get_text().strip()
                show.update({'rating_count': rating_count})
            else:
                print("No rating count found")

            if soup_genre is not None:
                soup_count = 1
                genre_list = []
                for genre in soup_genre:
                    genre = genre.get_text().strip()
                    genre_list.append({"name": genre, "order": soup_count})
                    soup_count += 1
                show.update({"genre": genre_list})
            else:
                print("No genre found")

            if soup_rundate is not None:
                rundate = soup_rundate.get_text().strip()
                show.update({'rundate': rundate})
                if " " in rundate:
                    status = "Continuing"
                else:
                    status = "Ended"
                show.update({'rundate': rundate, 'status': status})
            else:
                print("No run date found")

            if soup_recommended is not None:
                soup_count = 1
                recommendation_list = []
                for rec in soup_recommended:
                    try:
                        recommend_name = rec.find_all('img')[0]['alt']
                        recommend_id = rec['href'][7:-17]
                        try:
                            recommend_image = rec.find_all('img')[0]['loadlate']
                        except:
                            recommend_image = rec.find_all('img')[0]['src']
                        recommendation_dict = ({"id": recommend_id, "name": recommend_name, "order": soup_count, "image": recommend_image})
                        recommendation_list.append(recommendation_dict)
                        soup_count += 1
                    except IndexError:
                        print("Skipping recommendation...no image available.")
                        pass
                show.update({"recommendation": recommendation_list})
            else:
                print("No recommendations found")

            if soup_cast is not None:
                soup_count = 1
                actor = []
                for cast in soup_cast.find_all("td", {"class": "primary_photo"}):
                    actor_name = cast.find_all('img')[0]['title']
                    actor_id = cast.find_all('a')[0]['href'][6:-15]
                    try:
                        try:
                            actor_image = cast.find_all('img')[0]['loadlate']
                        except KeyError:
                            actor_image = cast.find_all('img')[0]['src']
                    except KeyError:
                        actor_image = "http://ia.media-imdb.com/images/G/01/imdb/images/nopicture/32x44/name-2138558783._CB379389446_.png"
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
                    actor_details = ({"id": actor[i]["id"], "name": actor[i]["name"], "order": actor[i]["order"], "image": actor[i]["image"], "character": character[i]})
                    actor_details_list.append(actor_details)
                    i += 1
                show.update({"cast": actor_details_list})
            else:
                print("No cast found")

            # insert show dict into show db collection
            collection_show.delete_one({"id": show_id})
            collection_show.insert(show)
            count += 1

        else:
            count += 1

    # print process results
    print("Process complete. ", count - 1, "shows processed.")
    end_time = datetime.now()
    duration = end_time - start_time
    print("Start time: ", str(start_time))
    print("End time: ", str(end_time))
    print("Total duration (minutes): ", str(duration.seconds / 60))


imdb_fetch_show_details()


