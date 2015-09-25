import urllib3
import mysql.connector as sql
from xml.dom import minidom
from xml.parsers.expat import ExpatError

api_key = ""
db_config = {
  'user': 'slampana',
  'password': 'Campana1',
  'host': '212.47.227.68',
  'database': 'tvdb',
  'raise_on_warnings': True,
}


def db_update(imdb_title, imdb_id, imdb_rating, imdb_votes, imdb_runtime, imdb_year, imdb_genre, imdb_plot, imdb_country, imdb_awards, imd_poster, imdb_type):
    print("Updating: ", imdb_id, "-", imdb_title)

    imdb_votes = int(str.replace(imdb_votes, ",", ""))

    connection = sql.connect(**db_config)
    cursor = connection.cursor()

    cursor.execute("""UPDATE series SET imdbRating = %s, imdbVotes = %s, imdbRuntime = %s, imdbYear = %s
      , imdbGenre = %s, imdbPlot = %s, imdbCountry = %s, imdbAwards = %s, imdPoster = %s, imdbType = %s
      , omdbFetched = 1, lastUpdated = CURRENT_TIMESTAMP WHERE imdbId = %s""", (imdb_rating, imdb_votes, imdb_runtime, imdb_year, imdb_genre, imdb_plot, imdb_country, imdb_awards, imd_poster, imdb_type, imdb_id))

    connection.commit()
    print("Done updating: ", imdb_id, "-", imdb_title)
    cursor.close()


def get_omdb_data(imdb_id):
    url = "http://www.omdbapi.com/?i=" + imdb_id + "&plot=full&r=xml"

    http = urllib3.PoolManager()
    r = http.request('GET', url)

    if r.status != 404:
        try:
            xml = minidom.parseString(r.data)

            imdb_title = xml.getElementsByTagName("movie")[0].getAttribute("title")
            imdb_rating = xml.getElementsByTagName("movie")[0].getAttribute("imdbRating")
            imdb_votes = xml.getElementsByTagName("movie")[0].getAttribute("imdbVotes")
            imdb_runtime = xml.getElementsByTagName("movie")[0].getAttribute("runtime")
            imdb_year = xml.getElementsByTagName("movie")[0].getAttribute("year")
            imdb_genre = xml.getElementsByTagName("movie")[0].getAttribute("genre")
            imdb_plot = xml.getElementsByTagName("movie")[0].getAttribute("plot")
            imdb_country = xml.getElementsByTagName("movie")[0].getAttribute("country")
            imdb_awards = xml.getElementsByTagName("movie")[0].getAttribute("awards")
            imd_poster = xml.getElementsByTagName("movie")[0].getAttribute("poster")
            imdb_type = xml.getElementsByTagName("movie")[0].getAttribute("type")

            return imdb_title, imdb_rating, imdb_votes, imdb_runtime, imdb_year, imdb_genre, imdb_plot, imdb_country, imdb_awards, imd_poster, imdb_type
        except ExpatError as e:
            print("ERROR WITH API PAGE: ", e)
            return None, None, None, None, None, None, None, None, None, None
            pass


def main_bulk_update():
    try:
        connection = sql.connect(**db_config)
        cursor = connection.cursor()

        # Add filters in this SELECT statement to determine the rows to be updated
        cursor.execute("""SELECT imdbId FROM series WHERE imdbId IS NOT NULL AND omdbFetched = 0""")

        rows = cursor.fetchall()

        for row in rows:
            imdb_id = row[0]

            imdb_title, imdb_rating, imdb_votes, imdb_runtime, imdb_year, imdb_genre, imdb_plot, imdb_country, imdb_awards, imd_poster, imdb_type = get_omdb_data(imdb_id)
            db_update(imdb_title, imdb_id, imdb_rating, imdb_votes, imdb_runtime, imdb_year, imdb_genre, imdb_plot, imdb_country, imdb_awards, imd_poster, imdb_type)

        print("ALL UPDATES COMPLETE!!!")
        cursor.close()
    except sql.Error as e:
        print("ERROR WITH SQL CONNECTION: ", e)
    except ExpatError as e:
            print("ERROR WITH API PAGE: ", e)
    finally:
        if connection:
            connection.close()


main_bulk_update()
