# ANCHOR: connect
from taosrest import connect, TaosRestConnection, TaosRestCursor

conn = connect(url="http://localhost:6041",
               user="root",
               password="taosdata",
               timeout=30)

# ANCHOR_END: connect
# ANCHOR: basic
# create STable
cursor = conn.cursor()
cursor.execute("DROP DATABASE IF EXISTS power", req_id=1)
cursor.execute("CREATE DATABASE power keep 36500", req_id=2)
cursor.execute(
    "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)", req_id=3)

# insert data
cursor.execute("""INSERT INTO power.d1001 USING power.meters TAGS('California.SanFrancisco', 2) VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) ('2018-10-03 14:38:16.800', 12.30000, 221, 0.31000)
    power.d1002 USING power.meters TAGS('California.SanFrancisco', 3) VALUES ('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000)
    power.d1003 USING power.meters TAGS('California.LosAngeles', 2) VALUES ('2018-10-03 14:38:05.500', 11.80000, 221, 0.28000) ('2018-10-03 14:38:16.600', 13.40000, 223, 0.29000)
    power.d1004 USING power.meters TAGS('California.LosAngeles', 3) VALUES ('2018-10-03 14:38:05.000', 10.80000, 223, 0.29000) ('2018-10-03 14:38:06.500', 11.50000, 221, 0.35000)""", req_id=4)
print("inserted row count:", cursor.rowcount)

# query data
cursor.execute("SELECT * FROM power.meters LIMIT 3", req_id=5)
# get total rows
print("queried row count:", cursor.rowcount)
# get column names from cursor
column_names = [meta[0] for meta in cursor.description]
# get rows
data = cursor.fetchall()
print(column_names)
for row in data:
    print(row)
# close cursor
cursor.close()

# output:
# inserted row count: 8
# queried row count: 3
# ['ts', 'current', 'voltage', 'phase', 'location', 'groupid']
# [datetime.datetime(2018, 10, 3, 14, 38, 5, 500000, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800), '+08:00')), 11.8, 221, 0.28, 'california.losangeles', 2]
# [datetime.datetime(2018, 10, 3, 14, 38, 16, 600000, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800), '+08:00')), 13.4, 223, 0.29, 'california.losangeles', 2]
# [datetime.datetime(2018, 10, 3, 14, 38, 5, tzinfo=datetime.timezone(datetime.timedelta(seconds=28800), '+08:00')), 10.8, 223, 0.29, 'california.losangeles', 3]
# ANCHOR_END: basic
