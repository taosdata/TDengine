from dotenv import load_dotenv

# read .env file from current working directory
load_dotenv()

# ANCHOR: connect
import taosrest
import os

url = os.environ["TDENGINE_CLOUD_URL"]
token = os.environ["TDENGINE_CLOUD_TOKEN"]

conn = taosrest.connect(url=url, token=token)
# test the connection by getting version info
print("server version:", conn.server_info)
# ANCHOR_END: connect
# ANCHOR: insert
# create super table
conn.execute("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)")
# insert multiple rows into multiple tables at once. subtables will be created automatically.
affected_row = conn.execute("""INSERT INTO power.d1001 USING power.meters TAGS('California.SanFrancisco', 2) VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) ('2018-10-03 14:38:16.800', 12.30000, 221, 0.31000)
    power.d1002 USING power.meters TAGS('California.SanFrancisco', 3) VALUES ('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000)
    """)
print("affected_row", affected_row)  # 4
# ANCHOR_END: insert
# ANCHOR: query
result = conn.query("SELECT ts, current FROM power.meters LIMIT 2")
# ANCHOR_END: query

# ANCHOR: fields
print(result.fields)
# output: [{'name': 'ts', 'type': 9, 'bytes': 8}, {'name': 'current', 'type': 6, 'bytes': 4}]
# ANCHOR_END: fields

# ANCHOR: rows
print(result.rows)
# output: 3
# ANCHOR_END: rows
# ANCHOR: iter
for row in result:
    print(row)
# output:
# [datetime.datetime(2018, 10, 3, 14, 38, 5, tzinfo=datetime.timezone.utc), 10.3]
# [datetime.datetime(2018, 10, 3, 14, 38, 15, tzinfo=datetime.timezone.utc), 12.6]
