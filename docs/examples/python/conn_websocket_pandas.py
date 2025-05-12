import pandas
from sqlalchemy import create_engine, text
import taos

taos_conn = taos.connect()
taos_conn.execute('drop database if exists power')
taos_conn.execute('create database if not exists power wal_retention_period 3600 keep 36500 ')
taos_conn.execute("use power")
taos_conn.execute(
    "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)")
# insert data
taos_conn.execute("""INSERT INTO power.d1001 USING power.meters TAGS('California.SanFrancisco', 2) 
VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) 
('2018-10-03 14:38:16.800', 12.30000, 221, 0.31000)
    power.d1002 USING power.meters TAGS('California.SanFrancisco', 3) 
    VALUES ('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000)
    power.d1003 USING power.meters TAGS('California.LosAngeles', 2) 
    VALUES ('2018-10-03 14:38:05.500', 11.80000, 221, 0.28000) ('2018-10-03 14:38:16.600', 13.40000, 223, 0.29000)
    power.d1004 USING power.meters TAGS('California.LosAngeles', 3) 
    VALUES ('2018-10-03 14:38:05.000', 10.80000, 223, 0.29000) ('2018-10-03 14:38:06.500', 11.50000, 221, 0.35000)""")

engine = create_engine("taosws://root:taosdata@localhost:6041")
conn = engine.connect()
df: pandas.DataFrame = pandas.read_sql(text("SELECT * FROM power.meters"), conn)
conn.close()

# print index
print(df.index)
# print data type  of element in ts column
print(type(df.ts[0]))
print(df.head(3))

# output:
# RangeIndex(start=0, stop=8, step=1)
# <class 'pandas._libs.tslibs.timestamps.Timestamp'>
#                        ts  current  ...                 location  groupid
# 0 2018-10-03 14:38:05.000     10.3  ...  California.SanFrancisco        2
# 1 2018-10-03 14:38:15.000     12.6  ...  California.SanFrancisco        2
# 2 2018-10-03 14:38:16.800     12.3  ...  California.SanFrancisco        2
