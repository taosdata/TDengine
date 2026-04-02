import datetime
import json
import requests
import taos

HISTORICAL_API = "https://production.api.coindesk.com/v2/tb/price/values/"

id = "BTC"
end_date = datetime.datetime.utcnow()
end_date_str = end_date.strftime("%FT%H:%M")

start_date = end_date - datetime.timedelta(hours=24)
start_date_str = start_date.strftime("%FT%H:%M")
url = "https://production.api.coindesk.com/v2/tb/price/values/%s?start_date=%s&end_date=%s&ohlc=false" % (
    id,
    start_date_str,
    end_date_str,
)

response = requests.get(url).json()
# response = json.load(open("examples/prices.json"))

entries = response["data"]["entries"]

taos = taos.connect(database="bpi")

for entry in entries:
    taos.execute("insert into usd values(%d, %d)" % (entry[0], entry[1]))
