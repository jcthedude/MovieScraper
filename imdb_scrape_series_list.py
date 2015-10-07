import urllib3
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://admin:Campana1@107.170.248.43:27017")
db = client.tv
collection = db.show_list


def imdb_fetch_series_list():
    start_time = datetime.now()
    record_count = 1
    status = 200
    order = 1

    while status == 200:
        url = "http://www.imdb.com/search/title?languages=en|1&count=250&title_type=tv_series&sort=moviemeter,asc" \
              "&view=simple&start=" + str(record_count)
        http = urllib3.PoolManager()
        r = http.request('GET', url)
        show_list = []
        print(url)
        status = r.status

        soup = BeautifulSoup(r.data, 'html.parser')
        rows = soup.find_all("td", class_="title")

        if not rows:
            print("No rows returned.")
            break
        for row in rows:
            print(order)
            row_content = row.contents[0]
            name = row_content.contents[0]
            link = row_content.get('href')
            id = link[7:-1]

            show = {"id": id,
                "name": name,
                "order": order,
                "timestamp": datetime.now()}
            print("Adding to list: ", order, id, name)
            show_list.append(show)

            order += 1

        collection.insert_many(show_list)
        print("Bulk insert complete.")
        record_count += 250

    print("Completed.  Fetched", order, "shows.")
    end_time = datetime.now()
    duration = end_time - start_time
    print("Start time: ", str(start_time))
    print("End time: ", str(end_time))
    print("Total duration (minutes): ", str(duration.seconds / 60))


imdb_fetch_series_list()


