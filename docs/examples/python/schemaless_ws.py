import taosws

conn = None

lineDemo = [
    "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1626006833639"
]

telnetDemo = ["metric_telnet 1707095283260 4 host=host0 interface=eth0"]

jsonDemo = [
    '{"metric": "metric_json","timestamp": 1626846400,"value": 10.3, "tags": {"groupid": 2, "location": "California.SanFrancisco", "id": "d1001"}}'
]

try:
    conn = taosws.connect(user="root",
                          password="taosdata",
                          host="localhost",
                          port=6041)

    conn.execute("CREATE DATABASE IF NOT EXISTS power")
    conn = conn.execute("USE power")

    conn.schemaless_insert(
        lines=lineDemo,
        protocol=taosws.PySchemalessProtocol.Line,
        precision=taosws.PySchemalessPrecision.Millisecond
    )

    conn.schemaless_insert(
        lines=telnetDemo,
        protocol=taosws.PySchemalessProtocol.Telnet,
        precision=taosws.PySchemalessPrecision.Microsecond
    )

    conn.schemaless_insert(
        lines=jsonDemo,
        protocol=taosws.PySchemalessProtocol.Json,
        precision=taosws.PySchemalessPrecision.Millisecond
    )
except Exception as err:
    print(err)
finally:
    if conn:
        conn.close()

