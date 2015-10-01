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


def db_insert_imdb_series_season_list(cursor, id, seasonNumber):
    cursor.execute("""INSERT INTO imdb_series_season_list SELECT %s, %s, %s""", (count, id, name))
    print("Insert complete: ", id, seasonNumber)


def imdb_fetch_series_season_list():
    url = "http://www.imdb.com/title/tt1954347/"
    http = urllib3.PoolManager()
    r = http.request('GET', url)
    print(r.status)
    print(url)

    soup = BeautifulSoup(r.data, 'html.parser')
    rows = soup.find_all("div", class_="seasons-and-year-nav")
    for row in rows:
        links = row.findAll('a')
        for link in links:
            if "season" in str(link):
                print(link.string)

    # if not rows:
    #     print("No rows returned.")
    #     break
    # for row in rows:
    #     row_content = row.contents[0]
    #     # season_number = row_content.get('href')
    #     print(row['href'])
    #     id = "tt1954347"
    #     # db_insert_imdb_series_season_list(cursor, str(id), str(season_number))
    #
    # break


imdb_fetch_series_season_list()


