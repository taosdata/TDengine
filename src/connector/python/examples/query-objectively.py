import taos

conn = taos.connect()
conn.exec("create database if not exists pytest")

result = conn.query("show databases")
num_of_fields = result.field_count
for field in result.fields:
    print(field)
for row in result:
    print(row)
result.close()
conn.exec("drop database pytest")
conn.close()