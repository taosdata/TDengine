import taosws

conn = None

try:
    conn = taosws.connect(
        user="root",
        password="taosdata",
        host="localhost",
        port=6041,
    )

    result = conn.query_with_req_id("SELECT ts, current, location FROM power.meters limit 100", req_id=1)

    for row in result:
        print(row)

except Exception as err:
    print(f"Failed to execute sql with reqId, err:{err}")
finally:
    if conn:
        conn.close()