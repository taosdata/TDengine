import taosws
import datetime

conn = taosws.connect()
cursor = conn.cursor()

cursor.execute("select 1")
db = "t_ws"
cursor.execute("drop database if exists {}", db)
cursor.execute("create database {}", db)
cursor.execute("use {name}", name=db)
cursor.execute("create stable stb (ts timestamp, v1 int) tags(t1 int)")

data = [{"name": "tb1", "t1": 1}, {"name": "tb2", "t1": 2}]
cursor.execute_many("create table {name} using stb tags({t1})", data)

ts = datetime.datetime.now().astimezone()
data = [("tb1", ts, 1), ("tb2", ts, 2)]
cursor.execute_many("insert into {} values('{}', {})", data)
