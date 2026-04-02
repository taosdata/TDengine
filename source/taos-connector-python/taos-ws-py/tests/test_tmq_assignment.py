import taosws


def pre_test():
    conn = taosws.connect()
    conn.execute("drop topic if exists topic_1")
    conn.execute("drop database if exists tmq_test")
    conn.execute("create database if not exists tmq_test wal_retention_period 3600")
    conn.execute("use tmq_test")
    conn.execute(
        "create table if not exists tb1 (ts timestamp, c1 int, c2 float, c3 binary(10), geo geometry(512), vbinary varbinary(32)) tags(t1 int)"
    )
    conn.execute("create topic if not exists topic_1 as select ts, c1, c2, c3, geo, vbinary from tb1")
    conn.execute(
        "insert into d0 using tb1 tags (0) values (now-2s, 1, 1.0, 'tmq test', 'POINT (4.0 8.0)', '0x7661726332')"
    )
    conn.execute(
        "insert into d0 using tb1 tags (0) values (now-1s, 2, 2.0, 'tmq test', 'POINT (4.0 8.0)', '0x7661726332')"
    )
    conn.execute(
        "insert into d0 using tb1 tags (0) values (now,    3, 3.0, 'tmq test', 'POINT (4.0 8.0)', '0x7661726332')"
    )


def after_test():
    conn = taosws.connect()
    conn.execute("drop topic if exists topic_1")
    conn.execute("drop database if exists tmq_test")


def test_tmq_assignment():
    pre_test()

    consumer = taosws.Consumer(
        conf={
            "td.connect.websocket.scheme": "ws",
            "experimental.snapshot.enable": "false",  # should disable snapshot
            "group.id": "0",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(["topic_1"])

    while 1:
        message = consumer.poll(timeout=1.0)
        if message:
            id = message.vgroup()
            topic = message.topic()
            database = message.database()
            print(f"vgroup: {id}, topic: {topic}, database: {database}")

            for block in message:
                nrows = block.nrows()
                ncols = block.ncols()
                for row in block:
                    print(row)
                values = block.fetchall()
                print(f"nrows: {nrows}, ncols: {ncols}, values: {values}")

            consumer.commit(message)
        else:
            break

    assignments = consumer.assignment()
    for assignment in assignments:
        assert assignment.topic() == "topic_1"
        assert assignment.assignments()[0].offset() >= 0

    consumer.unsubscribe()
