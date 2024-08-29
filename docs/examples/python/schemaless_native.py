import taos

lineDemo = [
    "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1626006833639"
]

telnetDemo = ["metric_telnet 1707095283260 4 host=host0 interface=eth0"]

jsonDemo = [
    '{"metric": "metric_json","timestamp": 1626846400,"value": 10.3, "tags": {"groupid": 2, "location": "California.SanFrancisco", "id": "d1001"}}'
]
host = "localhost"
port = 6030
try:
    conn = taos.connect(
        user="root",
        password="taosdata",
        host=host,
        port=port
    )

    conn.execute("CREATE DATABASE IF NOT EXISTS power")
    # change database. same as execute "USE db"
    conn.select_db("power")

    conn.schemaless_insert(
        lineDemo, taos.SmlProtocol.LINE_PROTOCOL, taos.SmlPrecision.MILLI_SECONDS
    )
    conn.schemaless_insert(
        telnetDemo, taos.SmlProtocol.TELNET_PROTOCOL, taos.SmlPrecision.MICRO_SECONDS
    )
    conn.schemaless_insert(
        jsonDemo, taos.SmlProtocol.JSON_PROTOCOL, taos.SmlPrecision.MILLI_SECONDS
    )
    print("Inserted data with schemaless successfully.");
except Exception as err:
    print(f"Failed to insert data with schemaless, ErrMessage:{err}")
finally:
    if conn:
        conn.close()

