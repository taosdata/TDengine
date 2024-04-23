import taos

conn = taos.connect()
cursor = conn.cursor()

cursor.execute("DROP DATABASE IF EXISTS test", req_id=1)
cursor.execute("CREATE DATABASE test keep 36500", req_id=2)
cursor.execute("USE test", req_id=3)
cursor.execute("CREATE STABLE weather(ts TIMESTAMP, temperature FLOAT) TAGS (location INT)", req_id=4)

for i in range(1000):
    location = str(i % 10)
    tb = "t" + location
    cursor.execute(f"INSERT INTO {tb} USING weather TAGS({location}) VALUES (now+{i}a, 23.5) (now+{i + 1}a, 23.5)", req_id=5+i)

cursor.execute("SELECT count(*) FROM weather", req_id=1005)
data = cursor.fetchall()
print("count:", data[0][0])
cursor.execute("SELECT tbname, * FROM weather LIMIT 2", req_id=1006)
col_names = [meta[0] for meta in cursor.description]
print(col_names)
rows = cursor.fetchall()
print(rows)

cursor.close()
conn.close()

# output:
# count: 2000
# ['tbname', 'ts', 'temperature', 'location']
# row_count: -1
# [('t0', datetime.datetime(2022, 4, 27, 14, 54, 24, 392000), 23.5, 0), ('t0', datetime.datetime(2022, 4, 27, 14, 54, 24, 393000), 23.5, 0)]
