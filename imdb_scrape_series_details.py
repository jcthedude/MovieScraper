import urllib3
from bs4 import BeautifulSoup
import mysql.connector as sql

db_config = {
  'user': 'slampana',
  'password': 'Campana1',
  'host': '107.170.244.175',
  'database': 'tvdb',
  'raise_on_warnings': True,
}


def db_select_imdb_series_list():
    try:
        print("Fetching  all series...")
        connection = sql.connect(**db_config)
        cursor = connection.cursor()
        cursor.execute("""SELECT DISTINCT id FROM imdb_series_list WHERE id = 'tt3597854' ORDER BY row""")
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        return rows
    except sql.Error as e:
        print("ERROR WITH SQL CONNECTION: ", e)


# def db_insert_imdb_series_season_list(cursor, id, seasonNumber, url):
#     cursor.execute("""INSERT INTO imdb_series_season_list SELECT %s, %s, %s""", (id, seasonNumber, url))


def imdb_fetch_series_season_list():
    rows = db_select_imdb_series_list()
    count = 1

    connection = sql.connect(**db_config)
    cursor = connection.cursor()

    for row in rows:
        series_id = row[0]
        valid_url = True
        url = "http://www.imdb.com/title/" + series_id
        http = urllib3.PoolManager()

        try:
            r = http.request('GET', url)
        except:
            valid_url = False
            print("Problem with URL data returned.")
            pass

        print(count, url)

        if valid_url:
            soup = BeautifulSoup(r.data, 'html.parser')
            soup_description = soup.findAll("div", {"class": "inline canwrap"})[0]
            soup_content_rating = soup.findAll("span", {"itemprop": "contentRating"})[0]
            soup_runtime = soup.findAll("time", {"itemprop": "duration"})[0]
            soup_creator = soup.findAll("div", {"itemprop": "creator"})[0].findAll('a')
            soup_actor = soup.findAll("div", {"itemprop": "actors"})[0].findAll('a')
            soup_rating = soup.findAll("span", {"itemprop": "ratingValue"})[0]
            soup_rating_count = soup.findAll("span", {"itemprop": "ratingCount"})[0]

            if len(soup_description) != 0:
                description = soup_description.get_text().strip()
                print(description)
            else:
                print("No description found")

            if len(soup_content_rating) != 0:
                content_rating = soup_content_rating.get_text().strip()
                print(content_rating)
            else:
                print("No content rating found")

            if len(soup_runtime) != 0:
                runtime = soup_runtime.get_text().strip()
                print(runtime)
            else:
                print("No runtime found")

            if len(soup_creator) != 0:
                for creator in soup_creator:
                    creator_name = creator.get_text().strip()
                    creator_id = creator['href'][6:-15]
                    print(creator_name, creator_id)
            else:
                print("No creator found")

            if len(soup_actor) != 0:
                for actor in soup_actor:
                    actor_name = actor.get_text().strip()
                    actor_id = actor['href'][6:-15]
                    if "See full cast" not in actor_name:
                        print(actor_name, actor_id)
            else:
                print("No actor found")

            if len(soup_rating) != 0:
                rating = soup_rating.get_text().strip()
                print(rating)
            else:
                print("No rating found")

            if len(soup_rating_count) != 0:
                rating_count = soup_rating_count.get_text().strip()
                print(rating_count)
            else:
                print("No rating count found")
        else:
            count += 1

    cursor.close()
    connection.close()
    print("Process complete. ", count-1, "series processed.")


imdb_fetch_series_season_list()


