import taosws

conn = None
reqId = 3
host="localhost"
port=6041
try:
    conn = taosws.connect(
        user="root",
        password="taosdata",
        host=host,
        port=port,
    )

    result = conn.query_with_req_id("SELECT ts, current, location FROM power.meters limit 100", req_id=3)
    # Get data from result as list of tuple
    for row in result:
        print(f"ts: {row[0]}, current: {row[1]}, location:  {row[2]}")

except Exception as err:
    print(f"Failed to execute sql with reqId:{reqId}, ErrMessage:{err}")
    raise err
finally:
    if conn:
        conn.close()
