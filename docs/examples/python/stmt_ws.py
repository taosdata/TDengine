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

# ANCHOR: stmt
sql = "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)"
stmt = conn.statement()
stmt.prepare(sql)

tbname = "power.d1001"

tags = [
    taosws.varchar_to_tag("California.SanFrancisco"),
    taosws.int_to_tag(2),
]

stmt.set_tbname_tags(tbname, tags)

stmt.bind_param(
    [
        taosws.millis_timestamps_to_column(
            [1626861392589, 1626861392591, 1626861392592]
        ),
        taosws.floats_to_column([10.3, 12.6, 12.3]),
        taosws.ints_to_column([194, 200, 201]),
        taosws.floats_to_column([0.31, 0.33, 0.31]),
    ]
)

stmt.add_batch()
rows = stmt.execute()

assert rows == 3

stmt.close()
# ANCHOR_END: stmt

conn.close()
