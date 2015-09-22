import urllib3
from xml.dom import minidom

http = urllib3.PoolManager()
r = http.request('GET', 'http://thetvdb.com/api/58AE0E6345017543/series/79349/all')
xmldoc = minidom.parse(r)

for node in xmldoc.getElementsByTagName('Episode'):
    season = int(node.getElementsByTagName('SeasonNumber')[0].firstChild.data)
    episode = int(node.getElementsByTagName('EpisodeNumber')[0].firstChild.data)
    if season > 0:
        print(node.getElementsByTagName('id')[0].firstChild.data, "-", sep='', end='')
        print("S", season, "E", episode, "-", sep='', end='')
        print(node.getElementsByTagName('EpisodeName')[0].firstChild.data, sep='')
