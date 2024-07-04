import taosrest

conn = taosrest.connect(url="http://localhost:6041")

db = "power"

conn.execute(f"DROP DATABASE IF EXISTS {db}")
conn.execute(f"CREATE DATABASE {db}")

# create super table
conn.execute(
    "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)"
)

# ANCHOR: insert
# rest insert data
sql = """
INSERT INTO 
power.d1001 USING power.meters TAGS('California.SanFrancisco', 2)
    VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) 
    ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) ('2018-10-03 14:38:16.800', 12.30000, 221, 0.31000)
power.d1002 USING power.meters TAGS('California.SanFrancisco', 3)
    VALUES ('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000)
power.d1003 USING power.meters TAGS('California.LosAngeles', 2) 
    VALUES ('2018-10-03 14:38:05.500', 11.80000, 221, 0.28000) ('2018-10-03 14:38:16.600', 13.40000, 223, 0.29000)
power.d1004 USING power.meters TAGS('California.LosAngeles', 3) 
    VALUES ('2018-10-03 14:38:05.000', 10.80000, 223, 0.29000) ('2018-10-03 14:38:06.500', 11.50000, 221, 0.35000)
"""

inserted = conn.execute(sql)
assert inserted == 8
# ANCHOR_END: insert

# ANCHOR: query
client = taosrest.RestClient("http://localhost:6041")
result = client.sql(f"SELECT * from {db}.meters")
print(result)

"""
output:
{'code': 0, 'column_meta': [['ts', 'TIMESTAMP', 8], ['current', 'FLOAT', 4], ['voltage', 'INT', 4], ['phase', 'FLOAT', 4], ['location', 'VARCHAR', 64], ['groupid', 'INT', 4]], 'data': [[datetime.datetime(2018, 10, 3, 14, 38, 5), 10.3, 219, 0.31, 'California.SanFrancisco', 2], [datetime.datetime(2018, 10, 3, 14, 38, 15), 12.6, 218, 0.33, 'California.SanFrancisco', 2], [datetime.datetime(2018, 10, 3, 14, 38, 16, 800000), 12.3, 221, 0.31, 'California.SanFrancisco', 2], [datetime.datetime(2018, 10, 3, 14, 38, 16, 650000), 10.3, 218, 0.25, 'California.SanFrancisco', 3], [datetime.datetime(2018, 10, 3, 14, 38, 5, 500000), 11.8, 221, 0.28, 'California.LosAngeles', 2], [datetime.datetime(2018, 10, 3, 14, 38, 16, 600000), 13.4, 223, 0.29, 'California.LosAngeles', 2], [datetime.datetime(2018, 10, 3, 14, 38, 5), 10.8, 223, 0.29, 'California.LosAngeles', 3], [datetime.datetime(2018, 10, 3, 14, 38, 6, 500000), 11.5, 221, 0.35, 'California.LosAngeles', 3]], 'rows': 8}
"""
# ANCHOR_END: query

# ANCHOR: req_id
result = client.sql(f"SELECT * from {db}.meters", req_id=1)
# ANCHOR_END: req_id
conn.close()
