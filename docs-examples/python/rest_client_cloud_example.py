import os
from taosrest import RestClient

url = os.environ["TDENGINE_CLOUD_URL"]
token = os.environ["TDENGINE_CLOUD_TOKEN"]

client = RestClient(url, token)
res: dict = client.sql("SELECT ts, current FROM power.meters LIMIT 1")
print(res)

# output:
# {'status': 'succ', 'head': ['ts', 'current'], 'column_meta': [['ts', 9, 8], ['current', 6, 4]], 'data': [[datetime.datetime(2018, 10, 3, 14, 38, 5, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800), '+08:00')), 10.3]], 'rows': 1}
