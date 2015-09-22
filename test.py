#!/usr/bin/python

import urllib3

http = urllib3.PoolManager()
r = http.request('GET', 'http://thetvdb.com/api/58AE0E6345017543/series/79349/all')
print(r.status)
print(r.headers)
print(r.data)
