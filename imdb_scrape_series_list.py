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


def db_insert_imdb_series_list(cursor, count, id, name):
    cursor.execute("""INSERT INTO imdb_series_list SELECT %s, %s, %s""", (count, id, name))
    print("Insert complete: ", id, name)


def imdb_fetch_series_list():
    record_count = 100001
    status = 200
    count = 100001

    connection = sql.connect(**db_config)
    cursor = connection.cursor()

    while status == 200:
        url = "http://www.imdb.com/search/title?count=250&sort=num_votes&start=" + str(record_count) +\
              "&title_type=tv_series&view=simple"
        http = urllib3.PoolManager()
        r = http.request('GET', url)
        print(url)
        status = r.status

        soup = BeautifulSoup(r.data, 'html.parser')
        rows = soup.find_all("td", class_="title")
        print(rows)

        for row in rows:
            print(count)
            row_content = row.contents[0]
            name = row_content.contents[0]
            link = row_content.get('href')
            id = link[7:-1]
            # db_insert_imdb_series_list(cursor, count, str(id), str(name))
            count += 1

        connection.commit()
        record_count += 250

    connection.commit()
    cursor.close()
    connection.close()
    print(r.status)


imdb_fetch_series_list()

