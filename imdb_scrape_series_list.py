import urllib3
from bs4 import BeautifulSoup


def main():
    record_count = 1
    status = 200

    while record_count < 1000 and status == 200:
        url = "http://www.imdb.com/search/title?count=250&sort=num_votes&start=" + str(record_count) +\
              "&title_type=tv_series&view=simple"
        http = urllib3.PoolManager()
        r = http.request('GET', url)

        status = r.status

        soup = BeautifulSoup(r.data, 'html.parser')
        # id = soup.find("td", {"class": "title"}).h6.contents
        rows = soup.find_all("td", class_="title")
        for row in rows:
            # print(row)
            row_content = row.contents[0]
            title = row_content.contents[0]
            link = row_content.get('href')
            print(title, link)

        record_count += 250

    print("Record count: ", record_count)


main()


