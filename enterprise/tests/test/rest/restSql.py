import requests
import json
import sys
import argparse
import time

parser = argparse.ArgumentParser()
parser.add_argument("-t","--token", help="the token")
parser.add_argument("-s","--sql", help="the sql")
parser.add_argument("-d","--db", help="the db")
args = parser.parse_args()

headers = {'Authorization':'Bearer '+args.token}
payload = {'sql' : args.sql}
r = requests.post("http://10.0.2.15:8080/sql/"+args.db,headers=headers,data=json.dumps(payload),allow_redirects=False)

"""
add cookie
time.sleep(1)
"""
print(r.cookies)
r_again = requests.post(r.url,headers=headers,cookies=r.cookies,data=json.dumps(payload))

print r_again
print r_again.text
assert r_again.status_code == 200
r_again.raise_for_status()
