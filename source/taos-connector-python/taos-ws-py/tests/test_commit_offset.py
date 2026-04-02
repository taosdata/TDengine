import taosws
from taosws import Consumer


def prepare(topic_name):
    conn = taosws.connect("taosws://localhost:6041")
    cursor = conn.cursor()
    dbname = f"{topic_name}_db"
    statements = [
        f"drop topic if exists {topic_name}",
        f"drop database if exists {dbname}",
        f"create database {dbname} wal_retention_period 3600",
        f"create topic {topic_name} with meta as database {dbname}",
        f"use {dbname}",
        "create table stb1(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 int)",
        "create table tb0 using stb1 tags(1000)",
        "create table tb1 using stb1 tags(NULL)",
        """insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)
            tb1 values(now, true, -2, -3, -4, -5, 
            '2024-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',
            254, 65534, 1, 1)""",
        "create table stb2(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(10),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 bool, t2 tinyint, t3 smallint, t4 int, t5 bigint,\
            t6 timestamp, t7 float, t8 double, t9 varchar(10), t10 nchar(16),\
            t11 tinyint unsigned, t12 smallint unsigned, t13 int unsigned, t14 bigint unsigned)",
        "create table tb2 using stb2 tags(true, -2, -3, -4, -5, \
            '2024-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
        "create table tb3 using stb2 tags( NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        "create table `table` (ts timestamp, v int)",
    ]
    for statement in statements:
        cursor.execute(statement)


def clear(topic_name):
    conn = taosws.connect("taosws://localhost:6041")
    cursor = conn.cursor()
    dbname = f"{topic_name}_db"
    statements = [
        f"drop topic if exists {topic_name}",
        f"drop database if exists {dbname}",
    ]
    for statement in statements:
        cursor.execute(statement)


def test_commit_offset():
    topic_name = "topic_offset"
    prepare(topic_name)
    conf = {
        "td.connect.websocket.scheme": "ws",
        "auto.offset.reset": "earliest",
        "group.id": "0",
    }
    consumer = Consumer(conf)
    TSDB_CODE_TMQ_NO_COMMITTED = -2147467247

    consumer.subscribe([topic_name])
    print(f"subscribed {topic_name}")
    while 1:
        message = consumer.poll(timeout=1.0)
        if message:
            id = message.vgroup()
            topic = message.topic()
            database = message.database()
            print(f"vgroup: {id}, topic: {topic}, database: {database}")

            committed = consumer.committed(topic, id)
            assert committed >= 0 or committed == TSDB_CODE_TMQ_NO_COMMITTED

            position = consumer.position(topic, id)
            assert position >= 0

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
    print(f"assignments: {assignments}")
    for assignment in assignments:
        topic = assignment.topic()
        print(f"topic: {topic}")
        print(assignment.assignments())
        for assign in assignment.assignments():
            vg_id = assign.vg_id()
            offset = assign.offset()
            begin = assign.begin()
            end = assign.end()
            print(f"vg_id: {vg_id}, offset: {offset}, begin: {begin}, end: {end}")

            committed = consumer.committed(topic, vg_id)
            position = consumer.position(topic, vg_id)
            print(f"committed: {committed}, position: {position}")

            print(f"seek to begin: {begin}")
            consumer.seek(topic, vg_id, begin)

            committed = consumer.committed(topic, vg_id)
            position = consumer.position(topic, vg_id)
            print(f"after seek committed: {committed}, position: {position}")

            print(f"commit offset: {end}")
            consumer.commit_offset(topic, vg_id, end)

            committed = consumer.committed(topic, vg_id)
            position = consumer.position(topic, vg_id)
            print(f"after commit_offset committed: {committed}, position: {position}")

    topics = consumer.list_topics()
    print(f"list topics: {topics}")

    consumer.unsubscribe()


def show_env():
    import os

    print("-" * 40)
    print("TAOS_LIBRARY_PATH: ", os.environ.get("TAOS_LIBRARY_PATH"))
    print("TAOS_CONFIG_DIR: ", os.environ.get("TAOS_CONFIG_DIR"))
    print("TAOS_C_CLIENT_VERSION: ", os.environ.get("TAOS_C_CLIENT_VERSION"))
    print("TAOS_C_CLIENT_VERSION_STR: ", os.environ.get("TAOS_C_CLIENT_VERSION_STR"))
    print("taosws.__version__", taosws.__version__)
    print("-" * 40)


if __name__ == "__main__":
    show_env()
    test_commit_offset()
    clear()
