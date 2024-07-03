import taosws

dsn = "taosws://root:taosdata@localhost:6041"
conn = taosws.connect(dsn)

db = "power"

conn.execute(f"DROP DATABASE IF EXISTS {db}")
conn.execute(f"CREATE DATABASE {db}")

# change database.
conn.execute(f"USE {db}")

# create super table
conn.execute(
    "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)"
)

# ANCHOR: insert
# ws insert data
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
# Execute a sql and get its result set. It's useful for SELECT statement
result = conn.query("SELECT * from meters")

# Get fields from result
fields = result.fields
for field in fields:
    print(field)

"""
output:
{name: ts, type: TIMESTAMP, bytes: 8}
{name: current, type: FLOAT, bytes: 4}
{name: voltage, type: INT, bytes: 4}
{name: phase, type: FLOAT, bytes: 4}
{name: location, type: BINARY, bytes: 64}
{name: groupid, type: INT, bytes: 4}
"""

# Get rows from result
for row in result:
    print(row)

"""
output:
('2018-10-03 14:38:05 +08:00', 10.300000190734863, 219, 0.3100000023841858, 'California.SanFrancisco', 2)
...
"""
# ANCHOR_END: query

# ANCHOR: req_id
result = conn.query_with_req_id("SELECT * from meters", req_id=1)
# ANCHOR_END: req_id
conn.close()
