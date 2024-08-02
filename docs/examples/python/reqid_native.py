import taos

conn = None
try:
    conn = taos.connect(host="localhost",
                        port=6030,
                        user="root",
                        password="taosdata")

    sql = """
        INSERT INTO 
        power.d1001 USING power.meters (groupid, location) TAGS(2, 'California.SanFrancisco')
            VALUES (NOW + 1a, 10.30000, 219, 0.31000) 
            (NOW + 2a, 12.60000, 218, 0.33000) (NOW + 3a, 12.30000, 221, 0.31000)
        power.d1002 USING power.meters (groupid, location) TAGS(3, 'California.SanFrancisco') 
            VALUES (NOW + 1a, 10.30000, 218, 0.25000)
        """
    inserted = conn.execute(sql, 1)
    print("inserted into {affectedRows} rows to power.meters successfully.")

    result = conn.query("SELECT ts, current, location FROM power.meters limit 100", 2)
    print(result)
    # Get fields from result
    fields = result.fields
    for field in fields:
        print(field)

    # Get data from result as list of tuple
    data = result.fetch_all()
    for row in data:
        print(row)

except Exception as err:
    print(err)
finally:
    if conn:
        conn.close()