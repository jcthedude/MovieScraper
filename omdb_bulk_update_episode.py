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


def db_insert(imdb_title, imdb_series_id, imdb_id, imdb_rating, imdb_votes, imdb_runtime, imdb_year, imdb_genre, imdb_plot, imdb_country, imdb_awards, imd_poster, imdb_type):
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
    cursor.execute("""INSERT INTO episode_staging_omdb SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, CURRENT_TIMESTAMP""", (imdb_id, imdb_series_id, imdb_rating, imdb_votes, imdb_runtime, imdb_year, imdb_genre, imdb_plot, imdb_country, imdb_awards, imd_poster, imdb_type))

    connection.commit()
    print("Finished with: ", imdb_id, "-", imdb_title)
    cursor.close()
    connection.close()


def db_update():
    print("Starting update...")
    connection1 = sql.connect(**db_config)
    cursor1 = connection1.cursor()
    cursor1.execute("""UPDATE episode_staging_omdb AS a
        INNER JOIN episode AS b ON b.imdbId = a.imdbId
        SET b.imdbSeriesId = a.imdbSeriesId
        ,b.imdbRating = a.imdbRating
        ,b.imdbVotes = a.imdbVotes
        ,b.imdbRuntime = a.imdbRuntime
        ,b.imdbYear = a.imdbYear
        ,b.imdbGenre = a.imdbGenre
        ,b.imdbPlot = a.imdbPlot
        ,b.imdbCountry = a.imdbCountry
        ,b.imdbAwards = a.imdbAwards
        ,b.imdbPoster = a.imdbPoster
        ,b.imdbType = a.imdbType
        ,b.omdbFetched = a.omdbFetched
        ,b.lastUpdated = a.lastUpdated""")
    connection1.commit()
    cursor1.close()
    connection1.close()
    print("UPDATE COMPLETE!!!: ")

    print("Starting delete...")
    connection2 = sql.connect(**db_config)
    cursor2 = connection2.cursor()
    cursor2.execute("""TRUNCATE TABLE episode_staging_omdb""")
    connection2.commit()
    cursor1.close()
    connection2.close()
    print("DELETE COMPLETE!!!: ")

def get_omdb_data(imdb_id):
    url = "http://www.omdbapi.com/?i=" + imdb_id + "&plot=full&r=xml"
    http = urllib3.PoolManager()
    r = http.request('GET', url)

    if r.status != 404:
        try:
            xml = minidom.parseString(r.data)
            try:
                imdb_title = xml.getElementsByTagName("movie")[0].getAttribute("title")
                imdb_series_id = xml.getElementsByTagName("movie")[0].getAttribute("seriesID")
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

                return imdb_title, imdb_series_id, imdb_rating, imdb_votes, imdb_runtime, imdb_year, imdb_genre, imdb_plot, imdb_country, imdb_awards, imd_poster, imdb_type
            except IndexError as e:
                print(imdb_id, "- INDEX ERROR: ", e)
                return None, None, None, None, None, None, None, None, None, None, None
                pass
        except ExpatError as e:
            print(imdb_id, "- EXPAT ERROR: ", e)
            return None, None, None, None, None, None, None, None, None, None, None
            pass


def main_bulk_update():
    try:
        connection = sql.connect(**db_config)
        cursor = connection.cursor()
        # Add filters in this SELECT statement to determine the rows to be updated
        cursor.execute("""SELECT DISTINCT imdbId FROM episode WHERE imdbId IS NOT NULL AND omdbFetched = 0""")

        rows = cursor.fetchall()
        for row in rows:
            imdb_id = row[0]

            imdb_title, imdb_series_id, imdb_rating, imdb_votes, imdb_runtime, imdb_year, imdb_genre, imdb_plot, imdb_country, imdb_awards, imd_poster, imdb_type = get_omdb_data(imdb_id)
            db_insert(imdb_title, imdb_series_id, imdb_id, imdb_rating, imdb_votes, imdb_runtime, imdb_year, imdb_genre, imdb_plot, imdb_country, imdb_awards, imd_poster, imdb_type)

        print("INSERT COMPLETE!!!")
        db_update()
        cursor.close()
        connection.close()
    except sql.Error as e:
        print("ERROR WITH SQL CONNECTION: ", e)
    except ExpatError as e:
            print("ERROR WITH API PAGE: ", e)


main_bulk_update()
