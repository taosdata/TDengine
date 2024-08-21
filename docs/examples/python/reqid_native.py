import taos

conn = None
reqId = 3
host="localhost"
port=6030
try:
    conn = taos.connect(host=host,
                        port=port,
                        user="root",
                        password="taosdata")

    result = conn.query("SELECT ts, current, location FROM power.meters limit 100", reqId)
    # Get data from result as list of tuple
    data = result.fetch_all()
    for row in data:
         print(f"ts: {row[0]}, current: {row[1]}, location:  {row[2]}")

except Exception as err:
    print(f"Failed to execute sql with reqId:{reqId}, ErrMessage:{err}")
    
finally:
    if conn:
        conn.close()
