import urllib3
import mysql.connector as sql
from xml.dom import minidom
from xml.parsers.expat import ExpatError
from datetime import datetime

api_key = "58AE0E6345017543"
db_config = {
  'user': 'slampana',
  'password': 'Campana1',
  'host': '107.170.244.175',
  'database': 'tvdb',
  'raise_on_warnings': True,
}


def db_select_timestamp():
    try:
        connection = sql.connect(**db_config)
        cursor = connection.cursor()
        cursor.execute("""SELECT MAX(lastUpdated) FROM tvdb_timestamp""")
        row = cursor.fetchone()
        while row is not None:
            last_updated = row[0]
            return last_updated
        cursor.close()
        connection.close()
    except sql.Error as e:
        print("ERROR WITH SQL CONNECTION: ", e)
    except ExpatError as e:
            print("ERROR WITH API PAGE: ", e)


def db_insert_time(timestamp):
    connection = sql.connect(**db_config)
    cursor = connection.cursor()
    cursor.execute("""INSERT INTO tvdb_timestamp SELECT 0, %s""", (timestamp, ))
    connection.commit()
    cursor.close()
    connection.close()


def db_insert_series(series_id):
    connection = sql.connect(**db_config)
    cursor = connection.cursor()
    cursor.execute("""INSERT INTO series_new SELECT %s""", (series_id, ))
    connection.commit()
    cursor.close()
    connection.close()


def db_insert_episode(episode_id):
    connection = sql.connect(**db_config)
    cursor = connection.cursor()
    cursor.execute("""INSERT INTO episode_new SELECT %s""", (episode_id, ))
    connection.commit()
    cursor.close()
    connection.close()


def db_update_series(series_values):
    print("Updating: ", series_values['id'])
    if series_values['id'] is None or series_values['name'] is None:
        print("Skipping...no series id or name.")
    else:
        connection = sql.connect(**db_config)
        cursor = connection.cursor()
        cursor.execute("""INSERT INTO series_staging_tvdb SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            , 1, CURRENT_TIMESTAMP""", (series_values['id'], series_values['name']
            , series_values['overview'], series_values['firstAired'], series_values['genre'], series_values['network']
            , series_values['airsDayOfWeek'], series_values['airsTime'], series_values['runtime'], series_values['rating']
            , series_values['status'], series_values['banner'], series_values['fanart'], series_values['poster']
            , series_values['zap2itId'], series_values['imdbId']))
        connection.commit()
        print("Done updating: ", series_values['id'])
        cursor.close()
        connection.close()


def db_update_episode(episode_values):
    print("Updating: ", episode_values['id'])
    if episode_values['id'] is None or episode_values['name'] is None or episode_values['seriesId'] is None\
        or episode_values['seasonId'] is None or episode_values['seasonNumber'] is None\
        or episode_values['episodeNumber'] is None:
        print("Skipping...no episode id, name, seriesId, seasonId, seasonNumber or episodeNumber.")
    else:
        connection = sql.connect(**db_config)
        cursor = connection.cursor()
        cursor.execute("""INSERT INTO episode_staging_tvdb SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            , 1, CURRENT_TIMESTAMP""", (episode_values['id'], episode_values['seriesId']
            , episode_values['seasonId'], episode_values['seasonNumber'], episode_values['episodeNumber']
            , episode_values['name'], episode_values['overview'], episode_values['firstAired'], episode_values['guest']
            , episode_values['director'], episode_values['writer'], episode_values['image'], episode_values['imdbId']))
        connection.commit()
        print("Done updating: ", episode_values['id'])
        cursor.close()
        connection.close()


def get_new_ids(last_updated):
    url = "http://thetvdb.com/api/Updates.php?type=all&time=" + str(last_updated)
    http = urllib3.PoolManager()
    r = http.request('GET', url)

    if r.status != 404:
        try:
            xml = minidom.parseString(r.data)

            print("Starting timestamp update...")
            for node in xml.getElementsByTagName('Time'):
                db_insert_time(node.firstChild.data)
            print("Finished with timestamp update...")

            print("Starting series update...")
            for node in xml.getElementsByTagName('Series'):
                db_insert_series(node.firstChild.data)
            print("Finished with series update...")

            print("Starting episode update...")
            for node in xml.getElementsByTagName('Episode'):
                db_insert_episode(node.firstChild.data)
            print("Finished with episode update...")

        except ExpatError as e:
            print("ERROR WITH API PAGE: ", e)
            pass
    else:
        print("404 ERROR")


def get_series_details(series_id):
    series_values = {}
    url = "http://thetvdb.com/api/" + api_key + "/series/" + str(series_id) + "/all"
    http = urllib3.PoolManager()
    r = http.request('GET', url)

    if r.status != 404:
        try:
            xml = minidom.parseString(r.data)

            for node in xml.getElementsByTagName('Series'):
                try:
                    series_values['id'] = node.getElementsByTagName('id')[0].firstChild.data
                except AttributeError:
                    series_values['id'] = None
                try:
                    series_values['name'] = node.getElementsByTagName('SeriesName')[0].firstChild.data
                except AttributeError:
                    series_values['name'] = None
                try:
                    series_values['overview'] = node.getElementsByTagName('Overview')[0].firstChild.data
                except AttributeError:
                    series_values['overview'] = None
                try:
                    series_values['firstAired'] = node.getElementsByTagName('FirstAired')[0].firstChild.data
                except AttributeError:
                    series_values['firstAired'] = None
                try:
                    series_values['genre'] = node.getElementsByTagName('Genre')[0].firstChild.data
                except AttributeError:
                    series_values['genre'] = None
                try:
                    series_values['network'] = node.getElementsByTagName('Network')[0].firstChild.data
                except AttributeError:
                    series_values['network'] = None
                try:
                    series_values['airsDayOfWeek'] = node.getElementsByTagName('Airs_DayOfWeek')[0].firstChild.data
                except AttributeError:
                    series_values['airsDayOfWeek'] = None
                try:
                    series_values['airsTime'] = node.getElementsByTagName('Airs_Time')[0].firstChild.data
                except AttributeError:
                    series_values['airsTime'] = None
                try:
                    series_values['runtime'] = node.getElementsByTagName('Runtime')[0].firstChild.data
                except AttributeError:
                    series_values['runtime'] = None
                try:
                    series_values['rating'] = node.getElementsByTagName('ContentRating')[0].firstChild.data
                except AttributeError:
                    series_values['rating'] = None
                try:
                    series_values['status'] = node.getElementsByTagName('Status')[0].firstChild.data
                except AttributeError:
                    series_values['status'] = None
                try:
                    series_values['banner'] = node.getElementsByTagName('banner')[0].firstChild.data
                except AttributeError:
                    series_values['banner'] = None
                try:
                    series_values['fanart'] = node.getElementsByTagName('fanart')[0].firstChild.data
                except AttributeError:
                    series_values['fanart'] = None
                try:
                    series_values['poster'] = node.getElementsByTagName('poster')[0].firstChild.data
                except AttributeError:
                    series_values['poster'] = None
                try:
                    series_values['zap2itId'] = node.getElementsByTagName('zap2it_id')[0].firstChild.data
                except AttributeError:
                    series_values['zap2itId'] = None
                try:
                    series_values['imdbId'] = node.getElementsByTagName('IMDB_ID')[0].firstChild.data
                except AttributeError:
                    series_values['imdbId'] = None

                return series_values
        except ExpatError as e:
            print("ERROR WITH API PAGE: ", e)


def get_episode_details(episode_id):
    episode_values = {}
    url = "http://thetvdb.com/api/" + api_key + "/episodes/" + str(episode_id)
    http = urllib3.PoolManager()
    r = http.request('GET', url)

    if r.status != 404:
        try:
            xml = minidom.parseString(r.data)

            for node in xml.getElementsByTagName('Episode'):
                try:
                    episode_values['id'] = node.getElementsByTagName('id')[0].firstChild.data
                except AttributeError:
                    episode_values['id'] = None
                try:
                    episode_values['seriesId'] = node.getElementsByTagName('seriesid')[0].firstChild.data
                except AttributeError:
                    episode_values['seriesId'] = None
                try:
                    episode_values['seasonId'] = node.getElementsByTagName('seasonid')[0].firstChild.data
                except AttributeError:
                    episode_values['seasonId'] = None
                try:
                    episode_values['seasonNumber'] = node.getElementsByTagName('SeasonNumber')[0].firstChild.data
                except AttributeError:
                    episode_values['seasonNumber'] = None
                try:
                    episode_values['episodeNumber'] = node.getElementsByTagName('EpisodeNumber')[0].firstChild.data
                except AttributeError:
                    episode_values['episodeNumber'] = None
                try:
                    episode_values['name'] = node.getElementsByTagName('EpisodeName')[0].firstChild.data
                except AttributeError:
                    episode_values['name'] = None
                try:
                    episode_values['overview'] = node.getElementsByTagName('Overview')[0].firstChild.data
                except AttributeError:
                    episode_values['overview'] = None
                try:
                    episode_values['firstAired'] = node.getElementsByTagName('FirstAired')[0].firstChild.data
                except AttributeError:
                    episode_values['firstAired'] = None
                try:
                    episode_values['guest'] = node.getElementsByTagName('GuestStars')[0].firstChild.data
                except AttributeError:
                    episode_values['guest'] = None
                try:
                    episode_values['director'] = node.getElementsByTagName('Director')[0].firstChild.data
                except AttributeError:
                    episode_values['director'] = None
                try:
                    episode_values['writer'] = node.getElementsByTagName('Writer')[0].firstChild.data
                except AttributeError:
                    episode_values['writer'] = None
                try:
                    episode_values['image'] = node.getElementsByTagName('filename')[0].firstChild.data
                except AttributeError:
                    episode_values['image'] = None
                try:
                    episode_values['imdbId'] = node.getElementsByTagName('IMDB_ID')[0].firstChild.data
                except AttributeError:
                    episode_values['imdbId'] = None

                return episode_values
        except ExpatError as e:
            print("ERROR WITH API PAGE: ", e)


def series_update():
    try:
        print("Updating series...")
        connection = sql.connect(**db_config)
        cursor = connection.cursor()
        cursor.execute("""SELECT DISTINCT id FROM series_new""")
        rows = cursor.fetchall()

        for row in rows:
            series_id = row[0]
            db_update_series(get_series_details(series_id))

        cursor.close()
        connection.close()
    except sql.Error as e:
        print("ERROR WITH SQL CONNECTION: ", e)
    except ExpatError as e:
            print("ERROR WITH API PAGE: ", e)


def episode_update():
    try:
        print("Updating episodes...")
        connection = sql.connect(**db_config)
        cursor = connection.cursor()
        cursor.execute("""SELECT DISTINCT id FROM episode_new""")
        rows = cursor.fetchall()

        for row in rows:
            episode_id = row[0]
            db_update_episode(get_episode_details(episode_id))

        cursor.close()
        connection.close()
    except sql.Error as e:
        print("ERROR WITH SQL CONNECTION: ", e)
    except ExpatError as e:
            print("ERROR WITH API PAGE: ", e)


def main_get_new():
    start_time = datetime.now()
    get_new_ids(db_select_timestamp())
    print("Done getting new series and episodes...")
    series_update()
    print("Done inserting new series...")
    episode_update()
    print("Done inserting new episodes...")
    end_time = datetime.now()
    duration = end_time - start_time
    print("Start time: ", str(start_time))
    print("End time: ", str(end_time))
    print("Total duration (minutes): ", str(duration.seconds / 60))


main_get_new()
