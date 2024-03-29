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

lineDemo = [
    "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1626006833639000000"
]
telnetDemo = ["stb0_0 1707095283260 4 host=host0 interface=eth0"]
jsonDemo = [
    '{"metric": "meter_current","timestamp": 1626846400,"value": 10.3, "tags": {"groupid": 2, "location": "California.SanFrancisco", "id": "d1001"}}'
]

conn.schemaless_insert(
    lineDemo, taos.SmlProtocol.LINE_PROTOCOL, taos.SmlPrecision.MILLI_SECONDS
)
conn.schemaless_insert(
    telnetDemo, taos.SmlProtocol.TELNET_PROTOCOL, taos.SmlPrecision.MICRO_SECONDS
)
conn.schemaless_insert(
    jsonDemo, taos.SmlProtocol.JSON_PROTOCOL, taos.SmlPrecision.MILLI_SECONDS
)

conn.close()
