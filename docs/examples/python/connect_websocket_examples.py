# ANCHOR: connect
import taosws

conn = taosws.connect("taosws://root:taosdata@localhost:6041")
# ANCHOR_END: connect

# ANCHOR: basic
conn.execute("drop database if exists connwspy")
conn.execute("create database if not exists connwspy wal_retention_period 3600 keep 36500 ")
conn.execute("use connwspy")
conn.execute("create table if not exists stb (ts timestamp, c1 int) tags (t1 int)")
conn.execute("create table if not exists tb1 using stb tags (1)")
conn.execute("insert into tb1 values (now, 1)")
conn.execute("insert into tb1 values (now+1s, 2)")
conn.execute("insert into tb1 values (now+2s, 3)")

r = conn.execute("select * from stb")
result = conn.query("select * from stb")
num_of_fields = result.field_count
print(num_of_fields)

for row in result:
    print(row)

# output:
# 3
# ('2023-02-28 15:56:13.329 +08:00', 1, 1)
# ('2023-02-28 15:56:13.333 +08:00', 2, 1)
# ('2023-02-28 15:56:13.337 +08:00', 3, 1)
