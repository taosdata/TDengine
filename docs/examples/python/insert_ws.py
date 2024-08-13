import taosws

conn = None
host="localhost"
port=6041
try:
    conn = taosws.connect(user="root",
                          password="taosdata",
                          host=host,
                          port=port)

    sql = """
        INSERT INTO 
        power.d1001 USING power.meters (groupid, location) TAGS(2, 'California.SanFrancisco')
            VALUES (NOW + 1a, 10.30000, 219, 0.31000) 
            (NOW + 2a, 12.60000, 218, 0.33000) (NOW + 3a, 12.30000, 221, 0.31000)
        power.d1002 USING power.meters (groupid, location) TAGS(3, 'California.SanFrancisco') 
            VALUES (NOW + 1a, 10.30000, 218, 0.25000)
        """
    affectedRows = conn.execute(sql)
    print(f"Successfully inserted {affectedRows} rows to power.meters.")

except Exception as err:
    print(f"Failed to insert data to power.meters, db addr:{host}:{port} ; ErrMessage:{err}")
finally:
    if conn:
        conn.close()
