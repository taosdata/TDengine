import json
import taos
import requests

j = requests.get("http://api.coindesk.com/v1/bpi/currentprice.json").json()

# json to sql
ts = j["time"]["updatedISO"]
sql = "insert into"
for id in j["bpi"]:
    bpi = j["bpi"][id]
    sql += ' %s using price tags("%s","%s","%s") values("%s", %lf) ' % (
        id,
        bpi["code"],
        bpi["symbol"],
        bpi["description"],
        ts,
        bpi["rate_float"],
    )

# sql to TDengine
taos = taos.connect()
taos.execute("create database if not exists bpi")
taos.execute("use bpi")
taos.execute(
    "create stable if not exists price (ts timestamp, rate double)"
    + " tags (code binary(10), symbol binary(10), description binary(100))"
)
taos.execute(sql)
