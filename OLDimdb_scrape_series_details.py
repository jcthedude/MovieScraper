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
            soup_image = soup.find_all("div", {"class": "image"})[0].find_all('a')[0].find_all('img')[0]['src']
            soup_description = soup.find_all("div", {"class": "inline canwrap"})[0]
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
                print(image)
            else:
                print("No image found")

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
                soup_count = 1
                for creator in soup_creator:
                    creator_name = creator.get_text().strip()
                    creator_id = creator['href'][6:-15]
                    print(soup_count, creator_name, creator_id)
                    soup_count += 1
            else:
                print("No creator found")

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

            if len(soup_genre) != 0:
                soup_count = 1
                for genre in soup_genre:
                    genre = genre.get_text().strip()
                    print(soup_count, genre)
                    soup_count += 1
            else:
                print("No actor found")

            if len(soup_rundate) != 0:
                rundate = soup_rundate.get_text().strip()
                if " " in rundate:
                    status = "Continuing"
                else:
                    status = "Ended"
                print(rundate, status)
            else:
                print("No run date found")

            if len(soup_recommended) != 0:
                soup_count = 1
                for rec in soup_recommended:
                    recommend_name = rec.find_all('img')[0]['alt']
                    recommend_id = rec['href'][7:-17]
                    recommend_image = rec.find_all('img')[0]['src']
                    print(soup_count, recommend_name, recommend_id, recommend_image)
                    soup_count += 1
            else:
                print("No recommendations found")

            if len(soup_cast) != 0:
                soup_count = 1
                for cast in soup_cast.find_all("td", {"class": "primary_photo"}):
                    actor_name = cast.find_all('img')[0]['title']
                    actor_id = cast.find_all('a')[0]['href'][6:-15]
                    actor_image = cast.find_all('img')[0]['loadlate']
                    print(soup_count, actor_name, actor_id, actor_image)
                    soup_count += 1

                soup_count = 1
                for cast in soup_cast.find_all("td", {"class": "character"}):
                    character = cast.find_all('div')[0].get_text().split('(', 1)[0].split('/', 1)[0].strip()
                    print(soup_count, character)
                    soup_count += 1
            else:
                print("No cast found")

        else:
            count += 1

    cursor.close()
    connection.close()
    print("Process complete. ", count-1, "series processed.")


imdb_fetch_series_season_list()


