import pandas
from sqlalchemy import create_engine, text

engine = create_engine("taos://root:taosdata@localhost:6030/power")
conn = engine.connect()
df = pandas.read_sql(text("SELECT * FROM power.meters"), conn)
conn.close()


# print index
print(df.index)
# print data type  of element in ts column
print(type(df.ts[0]))
print(df.head(3))

# output:
# RangeIndex(start=0, stop=8, step=1)
# <class 'pandas._libs.tslibs.timestamps.Timestamp'>
#                        ts  current  ...               location  groupid
# 0 2018-10-03 14:38:05.500     11.8  ...  california.losangeles        2
# 1 2018-10-03 14:38:16.600     13.4  ...  california.losangeles        2
# 2 2018-10-03 14:38:05.000     10.8  ...  california.losangeles        3
