import sys
import urllib3
import mysql.connector as sql
from mysql.connector import errorcode
from xml.dom import minidom

api_key = "58AE0E6345017543"
db_config = {
  'user': 'slampana',
  'password': 'Campana1',
  'host': '212.47.227.68',
  'database': 'tvdb',
  'raise_on_warnings': True,
}


def db_select(connection, series_id):
    cursor = connection.cursor()
    cursor.execute("""SELECT * FROM series WHERE id = %s""", (series_id,))

    row = cursor.fetchone()

    while row is not None:
            print(row[1], "-", row[4], "-", row[8])
            row = cursor.fetchone()

    cursor.close()


def db_update(series_id, name, banner, fanart, poster):
    print("Updating: ", series_id, "-", name)
    print(banner, fanart, poster)

    connection = sql.connect(**db_config)
    cursor = connection.cursor()

    cursor.execute("""UPDATE series SET banner = %s, fanart = %s, poster = %s, lastUpdated = CURRENT_TIMESTAMP WHERE id = %s""", (banner, fanart, poster, series_id))

    connection.commit()
    print("Done updating: ", series_id, "-", name)
    cursor.close()


def search_series(search_text):
    formatted_search_text = str.replace(search_text, " ", "%20")
    url = "http://thetvdb.com/api/GetSeries.php?seriesname=" + formatted_search_text

    http = urllib3.PoolManager()
    r = http.request('GET', url)
    xml = minidom.parseString(r.data)

    for node in xml.getElementsByTagName('Series'):
        print(node.getElementsByTagName('seriesid')[0].firstChild.data, "-", sep='', end='')
        print(node.getElementsByTagName('SeriesName')[0].firstChild.data, "-", sep='', end='')
        print(node.getElementsByTagName('FirstAired')[0].firstChild.data, sep='')


def get_series_details(series_id):
    url = "http://thetvdb.com/api/" + api_key + "/series/" + str(series_id) + "/all"

    http = urllib3.PoolManager()
    r = http.request('GET', url)
    xml = minidom.parseString(r.data)

    for node in xml.getElementsByTagName('Series'):
        print(node.getElementsByTagName('id')[0].firstChild.data, "-", sep='', end='')
        print(node.getElementsByTagName('SeriesName')[0].firstChild.data, "-", sep='', end='')
        print(node.getElementsByTagName('FirstAired')[0].firstChild.data, sep='')
        print(node.getElementsByTagName('Genre')[0].firstChild.data.split("|")[1:-1])
        print(node.getElementsByTagName('Status')[0].firstChild.data, sep='')


def get_episode_details(series_id):
    url = "http://thetvdb.com/api/" + api_key + "/series/" + str(series_id) + "/all"

    http = urllib3.PoolManager()
    r = http.request('GET', url)
    xml = minidom.parseString(r.data)

    for node in xml.getElementsByTagName('Episode'):
        season = int(node.getElementsByTagName('SeasonNumber')[0].firstChild.data)
        episode = int(node.getElementsByTagName('EpisodeNumber')[0].firstChild.data)
        if season > 0:
            print(node.getElementsByTagName('id')[0].firstChild.data, "-", sep='', end='')
            print("S", "{:0>2d}".format(season), "E", "{:0>2d}".format(episode), "-", sep='', end='')
            print(node.getElementsByTagName('EpisodeName')[0].firstChild.data, sep='')


def get_art(series_id):
    url = "http://thetvdb.com/api/" + api_key + "/series/" + str(series_id) + "/all"

    http = urllib3.PoolManager()
    r = http.request('GET', url)
    try:
        xml = minidom.parseString(r.data)

        for node in xml.getElementsByTagName('Series'):
            try:
                name = node.getElementsByTagName('SeriesName')[0].firstChild.data
            except AttributeError:
                name = None
            try:
                banner = node.getElementsByTagName('banner')[0].firstChild.data
            except AttributeError:
                banner = None
            try:
                fanart = node.getElementsByTagName('fanart')[0].firstChild.data
            except AttributeError:
                fanart = None
            try:
                poster = node.getElementsByTagName('poster')[0].firstChild.data
            except AttributeError:
                poster = None

            return name, banner, fanart, poster
    except:
        print("Error with API page")
        return None, None, None, None
        pass


def main_select_update():
    try:
        search_text = input("Series Name: ")
        search_series(search_text)

        series_id = input("Series ID: ")
        get_series_details(series_id)
        get_episode_details(series_id)
        get_art(series_id)
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise

    try:
        connection = sql.connect(**db_config)

        db_select(connection, str(series_id))

        name, banner, fanart, poster = get_art(series_id)
        db_update(str(series_id), name, banner, fanart, poster)
    except sql.Error as e:
        if e.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif e.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(e)
    finally:
        if connection:
            connection.close()


def main_bulk_update():
    try:
        connection = sql.connect(**db_config)
        cursor = connection.cursor()
        cursor.execute("""SELECT id FROM series WHERE banner IS NULL AND id > 79676""")

        rows = cursor.fetchall()

        for row in rows:
            series_id = row[0]

            name, banner, fanart, poster = get_art(series_id)
            db_update(str(series_id), name, banner, fanart, poster)

        print("Updates complete!")
        cursor.close()
    except sql.Error as e:
        if e.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif e.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(e)
    finally:
        if connection:
            connection.close()


main_bulk_update()
