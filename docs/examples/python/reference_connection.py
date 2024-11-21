from dotenv import load_dotenv

# read .env file from current working directory
load_dotenv()

# ANCHOR: example
import taosrest
# ANCHOR: connect
import os

url = os.environ["TDENGINE_CLOUD_URL"]
token = os.environ["TDENGINE_CLOUD_TOKEN"]

conn = taosrest.connect(url=url, token=token)
# test the connection by getting version info
print("server version:", conn.server_info)
# ANCHOR_END: connect
affected_row = conn.execute("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)")
print("affected_row", affected_row)  # 0
affected_row = conn.execute("""INSERT INTO 
    power.d1001 USING power.meters TAGS('California.SanFrancisco', 2) VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) ('2018-10-03 14:38:20.800', 12.30000, 221, 0.31000)
    power.d1002 USING power.meters TAGS('California.SanFrancisco', 3) VALUES ('2018-10-03 14:39:19.650', 10.30000, 218, 0.25000)
    power.d1003 USING power.meters TAGS('California.LosAngeles', 2) VALUES ('2018-10-03 14:41:05.500', 11.80000, 221, 0.28000) ('2018-10-03 14:42:16.600', 13.40000, 223, 0.29000)
    power.d1004 USING power.meters TAGS('California.LosAngeles', 3) VALUES ('2018-10-03 14:48:05.000', 10.80000, 223, 0.29000) ('2018-10-03 14:55:06.500', 11.50000, 221, 0.35000)
               """)
print("affected_row", affected_row)  # 8

result = conn.query("SELECT ts, current FROM power.meters LIMIT 2")

print("metadata of each column:\n", result.fields)  
# [{'name': 'ts', 'type': 'TIMESTAMP', 'bytes': 8}, {'name': 'current', 'type': 'FLOAT', 'bytes': 4}]

print("total rows:", result.rows)  # 2

# Iterate over result.
for row in result:
    print(row)
# output:
# [datetime.datetime(2018, 10, 3, 14, 38, 5), 10.3]
# [datetime.datetime(2018, 10, 3, 14, 38, 15), 12.6]

# Or get all rows as a list
print(result.data)  
# [[datetime.datetime(2018, 10, 3, 14, 38, 5), 10.3], [datetime.datetime(2018, 10, 3, 14, 38, 15), 12.6]]

# ANCHOR_END: example

