import taosws

conn = None

try:
    conn = taosws.connect(user="root",
                          password="taosdata",
                          host="localhost",
                          port=6041)

    result = conn.query("SELECT ts, current, location FROM power.meters limit 100")
    num_of_fields = result.field_count
    print(num_of_fields)

    for row in result:
        print(row)

except Exception as err:
    print(f"Failed to query data from power.meters, err:{err}")
finally:
    if conn:
        conn.close()
