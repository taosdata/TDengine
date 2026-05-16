import taosws

conn = taosws.connect("ws://localhost:6041")

conn.execute("create database if not exists connwspy")
conn.execute("use connwspy")
conn.execute("create table if not exists stb (ts timestamp, c1 int) tags (t1 int)")
conn.execute("create table if not exists tb1 using stb tags (1)")
conn.execute("insert into tb1 values (now, 1)")
conn.execute("insert into tb1 values (now, 2)")
conn.execute("insert into tb1 values (now, 3)")

result = conn.query("show databases")
num_of_fields = result.field_count
print(num_of_fields)

for field in result.fields:
    print(field)
for row in result:
    print(row)

conn.execute("drop database if exists connwspy")
