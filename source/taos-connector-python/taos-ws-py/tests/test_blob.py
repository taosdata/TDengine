import datetime
import time
import taosws
from taosws import Consumer
import os


def test_blob_sql():
    value = os.getenv("TEST_TD_3360")
    if value is not None:
        return

    conn = taosws.connect("ws://localhost:6041")
    cursor = conn.cursor()

    try:
        cursor.execute("drop database if exists test_1753269319")
        cursor.execute("create database test_1753269319")
        cursor.execute("use test_1753269319")
        cursor.execute("create table t0(ts timestamp, c1 blob)")
        cursor.execute("insert into t0 values(1752218982761, null)")
        cursor.execute("insert into t0 values(1752218982762, '')")
        cursor.execute("insert into t0 values(1752218982763, 'hello')")
        cursor.execute("insert into t0 values(1752218982764, '\\x12345678')")

        cursor.execute("select * from t0")
        rows = cursor.fetchall()

        assert len(rows) == 4

        assert rows[0][0].timestamp() * 1000 == 1752218982761
        assert rows[1][0].timestamp() * 1000 == 1752218982762
        assert rows[2][0].timestamp() * 1000 == 1752218982763
        assert rows[3][0].timestamp() * 1000 == 1752218982764

        assert rows[0][1] is None
        assert rows[1][1] == b""
        assert rows[2][1] == b"hello"
        assert rows[3][1] == b"\x124Vx"

    finally:
        cursor.execute("drop database test_1753269319")
        conn.close()


def test_blob_stmt2():
    value = os.getenv("TEST_TD_3360")
    if value is not None:
        return

    conn = taosws.connect("ws://localhost:6041")
    try:
        conn.execute("drop database if exists test_1753269333")
        conn.execute("create database test_1753269333")
        conn.execute("use test_1753269333")
        conn.execute("create table t0 (ts timestamp, c1 blob)")

        test_timestamps = [1726803356466, 1726803356467, 1726803356468, 1726803356469]
        test_blobs = [None, b"", b"hello", b"\x124Vx"]

        stmt2 = conn.stmt2_statement()
        stmt2.prepare("insert into t0 values (?, ?)")

        param = taosws.stmt2_bind_param_view(
            table_name="",
            tags=None,
            columns=[
                taosws.millis_timestamps_to_column(test_timestamps),
                taosws.blob_to_column(test_blobs),
            ],
        )
        stmt2.bind([param])

        affected_rows = stmt2.execute()
        assert affected_rows == 4

        stmt2.prepare("select * from t0 where ts > ?")

        param = taosws.stmt2_bind_param_view(
            table_name="",
            tags=None,
            columns=[taosws.millis_timestamps_to_column([1726803356465])],
        )
        stmt2.bind([param])
        stmt2.execute()

        result = stmt2.result_set()
        expected_results = list(zip(test_timestamps, test_blobs))

        actual_results = []
        for row in result:
            dt = datetime.datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f %z")
            timestamp = int(dt.timestamp() * 1000)
            actual_results.append((timestamp, row[1]))

        assert actual_results == expected_results

    finally:
        conn.execute("drop database test_1753269333")
        conn.close()


def test_blob_tmq():
    value = os.getenv("TEST_TD_3360")
    if value is not None:
        return

    conn = taosws.connect("ws://localhost:6041")
    try:
        conn.execute("drop topic if exists topic_1753270984")
        conn.execute("drop database if exists test_1753270984")
        conn.execute("create database test_1753270984")
        conn.execute("create topic topic_1753270984 as database test_1753270984")
        conn.execute("use test_1753270984")
        conn.execute("create table t0 (ts timestamp, c1 int, c2 blob)")

        num = 100

        sql = "insert into t0 values "
        for i in range(num):
            ts = 1726803356466 + i
            sql += f"({ts}, {i}, 'blob_{i}'), "
        conn.execute(sql)

        consumer = Consumer(dsn="ws://localhost:6041?group.id=10&auto.offset.reset=earliest")
        consumer.subscribe(["topic_1753270984"])

        cnt = 0

        while 1:
            message = consumer.poll(timeout=5.0)
            if message:
                for block in message:
                    cnt += block.nrows()
                consumer.commit(message)
            else:
                break

        assert cnt == num

        consumer.unsubscribe()

    finally:
        time.sleep(3)
        conn.execute("drop topic topic_1753270984")
        conn.execute("drop database test_1753270984")
        conn.close()
