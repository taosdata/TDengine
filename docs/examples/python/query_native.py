import taos
host="localhost"
port=6030
conn = None
try:
    conn = taos.connect(host=host,
                        port=port,
                        user="root",
                        password="taosdata")
    sql = "SELECT ts, current, location FROM power.meters limit 100"
    result = conn.query(sql)
    # Get data from result as list of tuple
    data = result.fetch_all()
    for row in data:
        print(f"ts: {row[0]}, current: {row[1]}, location:  {row[2]}")

except Exception as err:
    print(f"Failed to query data from power.meters, sql: {sql}, ErrMessage:{err}")
finally:
    if conn:
        conn.close()
