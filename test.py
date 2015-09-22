import sys
import urllib3
from xml.dom import minidom

api_key = "58AE0E6345017543"
#series_id = 79349


def search_series(search_text):
    formatted_search_text = str.replace(search_text, " ", "%20")
    url = "http://thetvdb.com/api/GetSeries.php?seriesname=" + formatted_search_text

    http = urllib3.PoolManager()
    r = http.request('GET', url)
    xml = minidom.parse(r)

    for node in xml.getElementsByTagName('Series'):
        print(node.getElementsByTagName('seriesid')[0].firstChild.data, "-", sep='', end='')
        print(node.getElementsByTagName('SeriesName')[0].firstChild.data, "-", sep='', end='')
        print(node.getElementsByTagName('FirstAired')[0].firstChild.data, sep='')


def get_episode_details(series_id):
    url = "http://thetvdb.com/api/" + api_key + "/series/" + str(series_id) + "/all"

    http = urllib3.PoolManager()
    r = http.request('GET', url)
    xml = minidom.parse(r)

    for node in xml.getElementsByTagName('Episode'):
        season = int(node.getElementsByTagName('SeasonNumber')[0].firstChild.data)
        episode = int(node.getElementsByTagName('EpisodeNumber')[0].firstChild.data)
        if season > 0:
            print(node.getElementsByTagName('id')[0].firstChild.data, "-", sep='', end='')
            print("S", "{:0>2d}".format(season), "E", "{:0>2d}".format(episode), "-", sep='', end='')
            print(node.getElementsByTagName('EpisodeName')[0].firstChild.data, sep='')


def main():
    try:
        search_text = input("Series Name: ")
        search_series(search_text)

        series_id = input("Series ID: ")
        get_episode_details(series_id)
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise


main()
