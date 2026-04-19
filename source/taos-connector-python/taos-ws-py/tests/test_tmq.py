import pytest
import taosws
import time
import utils
from taosws import Consumer


def setup():
    conn = taosws.connect("taosws://localhost:6041")
    cursor = conn.cursor()
    statements = [
        "drop topic if exists ws_tmq_meta",
        "drop database if exists ws_tmq_meta",
        "create database ws_tmq_meta wal_retention_period 3600",
        "create topic ws_tmq_meta with meta as database ws_tmq_meta",
        "use ws_tmq_meta",
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
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',
            254, 65534, 1, 1)""",
        "create table stb2(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(10),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 bool, t2 tinyint, t3 smallint, t4 int, t5 bigint,\
            t6 timestamp, t7 float, t8 double, t9 varchar(10), t10 nchar(16),\
            t11 tinyint unsigned, t12 smallint unsigned, t13 int unsigned, t14 bigint unsigned)",
        "create table tb2 using stb2 tags(true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
        "create table tb3 using stb2 tags( NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        "create table `table` (ts timestamp, v int)",
        "alter table stb1 add column new1 bool",
        "alter table stb1 add column new2 tinyint",
        "alter table stb1 add column new10 nchar(16)",
        "alter table stb1 modify column new10 nchar(32)",
        "alter table stb1 drop column new10",
        "alter table stb1 drop column new2",
        "alter table stb1 drop column new1",
        "alter table `stb2` add tag new1 bool",
        "alter table `stb2` rename tag new1 new1_new",
        "alter table `stb2` modify tag t10 nchar(32)",
        "alter table `stb2` drop tag new1_new",
        "alter table `table` add column new1 bool",
        "alter table `table` add column new2 tinyint",
        "alter table `table` add column new10 nchar(16)",
        "alter table `table` modify column new10 nchar(32)",
        "alter table `table` rename column new10 new10_new",
        "alter table `table` drop column new10_new",
        "alter table `table` drop column new2",
        "alter table `table` drop column new1",
    ]
    for statement in statements:
        cursor.execute(statement)


def test_tmq():
    setup()
    conf = {
        "td.connect.websocket.scheme": "ws",
        "group.id": "0",
        # 3.3.6.0 support
        "fetch.max.wait.ms": "3001",
        "min.poll.rows": "129",
    }
    consumer = Consumer(conf)
    consumer.subscribe(["ws_tmq_meta"])

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

    consumer.unsubscribe()


@pytest.mark.skipif(not utils.TEST_TD_ENTERPRISE, reason="only for TDengine Enterprise")
def test_tmq_with_token():
    conn = taosws.connect("ws://localhost:6041")
    try:
        conn.execute("drop token if exists token_1772607422")
        conn.execute("drop topic if exists topic_1772607422")
        conn.execute("drop database if exists test_1772607422")
        conn.execute("create database test_1772607422")
        conn.execute("create topic topic_1772607422 as database test_1772607422")

        rs = conn.query(f"create token token_1772607422 from user {utils.test_username()}")
        token = next(iter(rs))[0]

        consumer = Consumer(
            conf={
                "td.connect.websocket.scheme": "ws",
                "td.connect.ip": "localhost",
                "td.connect.port": 6041,
                "td.connect.user": "invalid_user",
                "td.connect.pass": "invalid_pass",
                "td.connect.bearer_token": token,
                "group.id": "1001",
                "client.id": "1001",
            }
        )
        consumer.subscribe(["topic_1772607422"])
        consumer.unsubscribe()

        consumer2 = Consumer(
            conf={
                "td.connect.websocket.scheme": "ws",
                "td.connect.ip": "localhost",
                "td.connect.port": 6041,
                "td.connect.user": "invalid_user",
                "td.connect.pass": "invalid_pass",
                "bearer_token": token,
                "group.id": "1001",
                "client.id": "1001",
            }
        )
        consumer2.subscribe(["topic_1772607422"])
        consumer2.unsubscribe()

        consumer3 = Consumer(
            conf={
                "td.connect.websocket.scheme": "ws",
                "td.connect.ip": "localhost",
                "td.connect.port": 6041,
                "td.connect.user": "invalid_user",
                "td.connect.pass": "invalid_pass",
                "td.connect.bearer_token": token,
                "bearer_token": "invalid_token",
                "group.id": "1001",
                "client.id": "1001",
            }
        )
        consumer3.subscribe(["topic_1772607422"])
        consumer3.unsubscribe()

        consumer4 = Consumer(
            dsn=f"ws://invalid_user:invalid_pass@localhost:6041?group.id=1001&client.id=1001&td.connect.bearer_token={token}"
        )
        consumer4.subscribe(["topic_1772607422"])
        consumer4.unsubscribe()

        consumer5 = Consumer(
            dsn=f"ws://invalid_user:invalid_pass@localhost:6041?group.id=1001&client.id=1001&bearer_token={token}"
        )
        consumer5.subscribe(["topic_1772607422"])
        consumer5.unsubscribe()

        consumer6 = Consumer(
            dsn=f"ws://invalid_user:invalid_pass@localhost:6041?group.id=1001&client.id=1001&td.connect.bearer_token={token}&bearer_token=invalid_token"
        )
        consumer6.subscribe(["topic_1772607422"])
        consumer6.unsubscribe()
    finally:
        time.sleep(3)
        conn.execute("drop token if exists token_1772607422")
        conn.execute("drop topic if exists topic_1772607422")
        conn.execute("drop database if exists test_1772607422")
        conn.close()


@pytest.mark.skipif(not utils.TEST_TD_ENTERPRISE, reason="only for TDengine Enterprise")
def test_tmq_with_invalid_token():
    conn = taosws.connect("ws://localhost:6041")
    try:
        conn.execute("drop topic if exists topic_1772611547")
        conn.execute("drop database if exists test_1772611547")
        conn.execute("create database test_1772611547")
        conn.execute("create topic topic_1772611547 as database test_1772611547")

        consumer = Consumer(
            conf={
                "td.connect.websocket.scheme": "ws",
                "td.connect.ip": "localhost",
                "td.connect.port": 6041,
                "td.connect.user": "invalid_user",
                "td.connect.pass": "invalid_pass",
                "td.connect.bearer_token": "invalid_token",
                "group.id": "1001",
                "client.id": "1001",
            }
        )
        with pytest.raises(Exception, match=r"Invalid token"):
            consumer.subscribe(["topic_1772611547"])

        consumer2 = Consumer(
            conf={
                "td.connect.websocket.scheme": "ws",
                "td.connect.ip": "localhost",
                "td.connect.port": 6041,
                "td.connect.user": "invalid_user",
                "td.connect.pass": "invalid_pass",
                "bearer_token": "invalid_token",
                "group.id": "1001",
                "client.id": "1001",
            }
        )
        with pytest.raises(Exception, match=r"Invalid token"):
            consumer2.subscribe(["topic_1772611547"])

        consumer3 = Consumer(
            dsn=f"ws://invalid_user:invalid_pass@localhost:6041?group.id=1001&client.id=1001&td.connect.bearer_token=invalid_token"
        )
        with pytest.raises(Exception, match=r"Invalid token"):
            consumer3.subscribe(["topic_1772611547"])

        consumer4 = Consumer(
            dsn=f"ws://invalid_user:invalid_pass@localhost:6041?group.id=1001&client.id=1001&bearer_token=invalid_token"
        )
        with pytest.raises(Exception, match=r"Invalid token"):
            consumer4.subscribe(["topic_1772611547"])
    finally:
        time.sleep(3)
        conn.execute("drop topic if exists topic_1772611547")
        conn.execute("drop database if exists test_1772611547")
        conn.close()


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
    print("forbid test_tmq because run long time\n")
