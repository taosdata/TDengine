import taosws

conn = None
host="localhost"
port=6041
try:
    conn = taosws.connect(user="root",
                          password="taosdata",
                          host=host,
                          port=port)

    result = conn.query("SELECT ts, current, location FROM power.meters limit 100")
    for row in result:
        print(f"ts: {row[0]}, current: {row[1]}, location:  {row[2]}")

except Exception as err:
    print(f"Failed to query data from power.meters, db addr:{host}:{port} ; ErrMessage:{err}")
finally:
    if conn:
        conn.close()
