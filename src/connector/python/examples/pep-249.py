import taos

conn = taos.connect()
cursor = conn.cursor()

cursor.execute("show databases")
results = cursor.fetchall()
for row in results:
    print(row)
