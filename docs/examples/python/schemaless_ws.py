import taosws

dsn = "taosws://root:taosdata@localhost:6041"
conn = taosws.connect(dsn)

db = "power"

conn.execute(f"DROP DATABASE IF EXISTS {db}")
conn.execute(f"CREATE DATABASE {db}")

# change database.
conn = taosws.connect(f"{dsn}/{db}")

lineDemo = [
    "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1626006833639000000"
]
telnetDemo = ["stb0_0 1707095283260 4 host=host0 interface=eth0"]
jsonDemo = [
    '{"metric": "meter_current","timestamp": 1626846400,"value": 10.3, "tags": {"groupid": 2, "location": "California.SanFrancisco", "id": "d1001"}}'
]

conn.schemaless_insert(
    lines=lineDemo,
    protocol=taosws.PySchemalessProtocol.Line,
    precision=taosws.PySchemalessPrecision.Millisecond,
    ttl=1,
    req_id=1,
)

conn.schemaless_insert(
    lines=telnetDemo,
    protocol=taosws.PySchemalessProtocol.Telnet,
    precision=taosws.PySchemalessPrecision.Microsecond,
    ttl=1,
    req_id=2,
)

conn.schemaless_insert(
    lines=jsonDemo,
    protocol=taosws.PySchemalessProtocol.Json,
    precision=taosws.PySchemalessPrecision.Millisecond,
    ttl=1,
    req_id=3,
)

conn.close()
