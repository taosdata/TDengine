import taosrest

client = None
url="http://localhost:6041"
try:
    client = taosrest.RestClient(url=url,
                   user="root",
                   password="taosdata",
                   timeout=30)

    result = client.sql(f"SELECT ts, current, location FROM power.meters limit 100")
    if result["data"]:
        for row in result["data"]:
            print(f"ts: {row[0]}, current: {row[1]}, location:  {row[2]}")

except Exception as err:
    print(f"Failed to query data from power.meters, url:{url} ; err:{err}")
