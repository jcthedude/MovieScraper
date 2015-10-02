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


def db_select_imdb_series_list():
    try:
        print("Fetching  all series...")
        connection = sql.connect(**db_config)
        cursor = connection.cursor()
        cursor.execute("""SELECT DISTINCT id FROM imdb_series_list WHERE row >= 4476 ORDER BY row""")
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        return rows
    except sql.Error as e:
        print("ERROR WITH SQL CONNECTION: ", e)


def db_insert_imdb_series_season_list(cursor, id, seasonNumber, url):
    cursor.execute("""INSERT INTO imdb_series_season_list SELECT %s, %s, %s""", (id, seasonNumber, url))


def imdb_fetch_series_season_list():
    rows = db_select_imdb_series_list()
    count = 1

    connection = sql.connect(**db_config)
    cursor = connection.cursor()

    for row in rows:
        series_id = row[0]
        valid_url = True
        url = "http://www.imdb.com/title/" + series_id
        http = urllib3.PoolManager()

        try:
            r = http.request('GET', url)
        except:
            valid_url = False
            print("Problem with URL data returned.")
            pass

        print(count, url)

        if valid_url:
            soup = BeautifulSoup(r.data, 'html.parser')
            rows = soup.find_all("div", class_="seasons-and-year-nav")

            if len(rows) != 0:
                for row in rows:
                    links = row.findAll('a')
                    for link in links:
                        if "season" in str(link):
                            season_number = str(link.string)
                            url = str(link['href'])
                            print(season_number, url)
                            if season_number.isdigit():
                                db_insert_imdb_series_season_list(cursor, str(series_id), season_number, url)

                connection.commit()
                print("Insert complete: ", series_id)
                count += 1
            else:
                print("No seasons found for: ", series_id)
                count += 1
        else:
            count += 1

    cursor.close()
    connection.close()
    print("Process complete. ", count-1, "series processed.")


imdb_fetch_series_season_list()


