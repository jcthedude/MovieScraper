import urllib3
import mysql.connector as sql
from xml.dom import minidom
from xml.parsers.expat import ExpatError

api_key = "58AE0E6345017543"
db_config = {
  'user': 'slampana',
  'password': 'Campana1',
  'host': '212.47.227.68',
  'database': 'tvdb',
  'raise_on_warnings': True,
}


def db_update(series_id, name, banner, fanart, poster):
    print("Updating: ", series_id, "-", name)
    print(banner, fanart, poster)

    connection = sql.connect(**db_config)
    cursor = connection.cursor()

    cursor.execute("""UPDATE series SET banner = %s, fanart = %s, poster = %s, tvdbFetched = 1
      , lastUpdated = CURRENT_TIMESTAMP WHERE id = %s""", (banner, fanart, poster, series_id))

    connection.commit()
    print("Done updating: ", series_id, "-", name)
    cursor.close()


def get_art(series_id):
    url = "http://thetvdb.com/api/" + api_key + "/series/" + str(series_id) + "/all"

    http = urllib3.PoolManager()
    r = http.request('GET', url)

    if r.status != 404:
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
        except ExpatError as e:
            print("ERROR WITH API PAGE: ", e)
            return None, None, None, None
            pass
    else:
        return None, None, None, None


def main_bulk_art_update():
    try:
        connection = sql.connect(**db_config)
        cursor = connection.cursor()

        # Add filters in this SELECT statement to determine the rows to be updated
        cursor.execute("""SELECT id FROM series""")

        rows = cursor.fetchall()

        for row in rows:
            series_id = row[0]

            name, banner, fanart, poster = get_art(series_id)
            db_update(str(series_id), name, banner, fanart, poster)

        print("ALL UPDATES COMPLETE!!!")
        cursor.close()
    except sql.Error as e:
        print("ERROR WITH SQL CONNECTION: ", e)
    except ExpatError as e:
            print("ERROR WITH API PAGE: ", e)
    finally:
        if connection:
            connection.close()


main_bulk_art_update()
