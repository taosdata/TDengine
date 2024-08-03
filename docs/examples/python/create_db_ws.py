import taosws

db = "power"

def prepare():
    conn = None

    try:
        conn = taosws.connect(user="root",
                              password="taosdata",
                              host="192.168.1.98",
                              port=6041)


        # create database
        conn.execute(f"drop database if exists {db}")
        conn.execute(f"create database {db}")
    except Exception as err:
        print(err)
    finally:
        if conn:
            conn.close()

def schemaless_insert():
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
                              host="192.168.1.98",
                              port=6041,
                              database=db)

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
    except Exception as err:
        print(err)
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    prepare()
    schemaless_insert()