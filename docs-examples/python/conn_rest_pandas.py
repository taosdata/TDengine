import pandas
from sqlalchemy import create_engine

engine = create_engine("taosrest://root:taosdata@localhost:6041")
df: pandas.DataFrame = pandas.read_sql("SELECT * FROM power.meters", engine)

# print index
print(df.index)
# print data type  of element in ts column
print(type(df.ts[0]))
print(df.head(3))

# output:
# <class 'datetime.datetime'>
# RangeIndex(start=0, stop=8, step=1)
#                                  ts  current  ...          location  groupid
# 0         2018-10-03 14:38:05+08:00     10.3  ...  beijing.chaoyang        2
# 1         2018-10-03 14:38:15+08:00     12.6  ...  beijing.chaoyang        2
# 2  2018-10-03 14:38:16.800000+08:00     12.3  ...  beijing.chaoyang        2
