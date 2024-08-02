import taosrest

client = None

try:
    client = taosrest.RestClient(url="http://localhost:6041",
                   user="root",
                   password="taosdata",
                   timeout=30)

    result = client.sql(f"SELECT ts, current, location FROM power.meters limit 100")
    print(result)

except Exception as err:
    print(err)
