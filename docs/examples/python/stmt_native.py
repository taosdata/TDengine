import taos

conn = taos.connect(
    host="localhost",
    user="root",
    password="taosdata",
    port=6030,
)

db = "power"

conn.execute(f"DROP DATABASE IF EXISTS {db}")
conn.execute(f"CREATE DATABASE {db}")

# change database. same as execute "USE db"
conn.select_db(db)

# create super table
conn.execute(
    "CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)"
)

# ANCHOR: stmt
sql = "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)"
stmt = conn.statement(sql)

tbname = "power.d1001"

tags = taos.new_bind_params(2)
tags[0].binary(["California.SanFrancisco"])
tags[1].int([2])

stmt.set_tbname_tags(tbname, tags)

params = taos.new_bind_params(4)
params[0].timestamp((1626861392589, 1626861392591, 1626861392592))
params[1].float((10.3, 12.6, 12.3))
params[2].int([194, 200, 201])
params[3].float([0.31, 0.33, 0.31])

stmt.bind_param_batch(params)

stmt.execute()

stmt.close()
# ANCHOR_END: stmt

result = conn.query("SELECT * from meters")

for row in result.fetch_all():
    print(row)

conn.close()
