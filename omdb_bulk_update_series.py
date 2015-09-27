import urllib3
import mysql.connector as sql
from xml.dom import minidom
from xml.parsers.expat import ExpatError
from datetime import datetime

api_key = ""
db_config = {
  'user': 'slampana',
  'password': 'Campana1',
  'host': '107.170.244.175',
  'database': 'tvdb',
  'raise_on_warnings': True,
}


def db_insert(imdb_title, imdb_id, imdb_rating, imdb_votes, imdb_runtime, imdb_year, imdb_genre, imdb_plot, imdb_country, imdb_awards, imdb_poster, imdb_type, omdb_error):
    print("Starting on: ", imdb_id, "-", imdb_title)
    if imdb_votes == "N/A":
        imdb_votes = 0
    elif imdb_votes is None:
        imdb_votes = None
    else:
        imdb_votes = int(str.replace(imdb_votes, ",", ""))
    if imdb_rating == "N/A":
        imdb_rating = 0

    connection = sql.connect(**db_config)
    cursor = connection.cursor()
    cursor.execute("""INSERT INTO series_staging_omdb SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, %s, CURRENT_TIMESTAMP""", (imdb_id, imdb_rating, imdb_votes, imdb_runtime, imdb_year, imdb_genre, imdb_plot, imdb_country, imdb_awards, imdb_poster, imdb_type, omdb_error))
    connection.commit()
    print("Finished with: ", imdb_id, "-", imdb_title)
    cursor.close()
    connection.close()


def db_update():
    start_time = datetime.now()
    print("Starting update...")
    connection = sql.connect(**db_config)
    cursor = connection.cursor()
    cursor.execute("""CALL update_series_omdb()""")
    connection.commit()
    cursor.close()
    connection.close()
    print("UPDATE COMPLETE!!!: ")
    end_time = datetime.now()
    duration = end_time - start_time
    print("Update duration (minutes): ", str(duration.seconds / 60))


def get_omdb_data(imdb_id):
    url = "http://www.omdbapi.com/?i=" + imdb_id + "&plot=full&r=xml"
    print("Url: ", url)
    http = urllib3.PoolManager()
    r = http.request('GET', url)

    if r.status != 404:
        try:
            xml = minidom.parseString(r.data)
            if xml.getElementsByTagName("root")[0].getAttribute("response") == "True":
                try:
                    imdb_title = xml.getElementsByTagName("movie")[0].getAttribute("title")
                    imdb_rating = xml.getElementsByTagName("movie")[0].getAttribute("imdbRating")
                    imdb_votes = xml.getElementsByTagName("movie")[0].getAttribute("imdbVotes")
                    imdb_runtime = xml.getElementsByTagName("movie")[0].getAttribute("runtime")
                    imdb_year = xml.getElementsByTagName("movie")[0].getAttribute("year")
                    imdb_genre = xml.getElementsByTagName("movie")[0].getAttribute("genre")
                    imdb_plot = xml.getElementsByTagName("movie")[0].getAttribute("plot")
                    imdb_country = xml.getElementsByTagName("movie")[0].getAttribute("country")
                    imdb_awards = xml.getElementsByTagName("movie")[0].getAttribute("awards")
                    imdb_poster = xml.getElementsByTagName("movie")[0].getAttribute("poster")
                    imdb_type = xml.getElementsByTagName("movie")[0].getAttribute("type")

                    return imdb_title, imdb_rating, imdb_votes, imdb_runtime, imdb_year, imdb_genre, imdb_plot, imdb_country, imdb_awards, imdb_poster, imdb_type, None
                except IndexError as e:
                        print(imdb_id, "- INDEX ERROR: ", e)
                        return None, None, None, None, None, None, None, None, None, None, None, str(e)
                        pass
            else:
                e = xml.getElementsByTagName("error")[0].firstChild.data
                print(e)
                return None, None, None, None, None, None, None, None, None, None, None, str(e)
        except ExpatError as e:
            pass
            print(imdb_id, "- EXPAT ERROR: ", e)
            return None, None, None, None, None, None, None, None, None, None, None, str(e)


def main_bulk_update():
    try:
        start_time = datetime.now()
        connection = sql.connect(**db_config)
        cursor = connection.cursor()

        # Add filters in this SELECT statement to determine the rows to be updated
        cursor.execute("""SELECT DISTINCT imdbId FROM series WHERE imdbId IS NOT NULL AND omdbFetched = 0""")
        rows = cursor.fetchall()
        for row in rows:
            imdb_id = row[0]

            imdb_title, imdb_rating, imdb_votes, imdb_runtime, imdb_year, imdb_genre, imdb_plot, imdb_country, imdb_awards, imdb_poster, imdb_type, omdb_error = get_omdb_data(imdb_id)
            db_insert(imdb_title, imdb_id, imdb_rating, imdb_votes, imdb_runtime, imdb_year, imdb_genre, imdb_plot, imdb_country, imdb_awards, imdb_poster, imdb_type, omdb_error)

        print("INSERT COMPLETE!!!")
        db_update()
        cursor.close()
        connection.close()
        end_time = datetime.now()
        duration = end_time - start_time
        print("Start time: ", str(start_time))
        print("End time: ", str(end_time))
        print("Total duration (minutes): ", str(duration.seconds / 60))
    except sql.Error as e:
        print("ERROR WITH SQL CONNECTION: ", e)
    except ExpatError as e:
            print("ERROR WITH API PAGE: ", e)


main_bulk_update()
