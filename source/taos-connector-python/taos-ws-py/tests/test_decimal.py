import datetime
import sys
import types
import pytest
import taosws
import time
import utils

from decimal import Decimal
from taosws import Consumer


@pytest.mark.skipif(utils.TEST_TD_3360, reason="skip for TD-3360")
def test_decimal_sql():
    conn = taosws.connect("ws://localhost:6041")
    cursor = conn.cursor()

    try:
        cursor.execute("drop database if exists test_1775631230")
        cursor.execute("create database test_1775631230")
        cursor.execute("use test_1775631230")
        cursor.execute("create table t0 (ts timestamp, c1 decimal(10, 2), c2 decimal(20, 10))")
        cursor.execute("insert into t0 values(1752218982761, null, null)")
        cursor.execute("insert into t0 values(1752218982762, 99.9876, 1234567890.1234567890)")
        cursor.execute("insert into t0 values(1752218982763, 1.0234, 1.23E+5)")
        cursor.execute("insert into t0 values(1752218982764, -123.456, 0.1234567890123)")

        cursor.execute("select * from t0")
        rows = cursor.fetchall()

        assert len(rows) == 4

        assert rows[0][0].timestamp() * 1000 == 1752218982761
        assert rows[1][0].timestamp() * 1000 == 1752218982762
        assert rows[2][0].timestamp() * 1000 == 1752218982763
        assert rows[3][0].timestamp() * 1000 == 1752218982764

        assert rows[0][1] is None
        assert rows[0][2] is None

        assert rows[1][1] == Decimal("99.99")
        assert rows[1][2] == Decimal("1234567890.1234567890")

        assert rows[2][1] == Decimal("1.02")
        assert rows[2][2] == Decimal("123000")

        assert rows[3][1] == Decimal("-123.46")
        assert rows[3][2] == Decimal("0.1234567890")

    finally:
        cursor.execute("drop database if exists test_1775631230")
        conn.close()


@pytest.mark.skipif(utils.TEST_TD_3360, reason="skip for TD-3360")
def test_decimal_stmt2():
    conn = taosws.connect("ws://localhost:6041")
    try:
        conn.execute("drop database if exists test_1775631629")
        conn.execute("create database test_1775631629")
        conn.execute("use test_1775631629")
        conn.execute("create table t0 (ts timestamp, c1 decimal(10, 2), c2 decimal(20, 10))")

        test_timestamps = [1726803356466, 1726803356467, 1726803356468, 1726803356469]
        test_c1s = [None, Decimal("99.9876"), Decimal("1.0234"), Decimal("-123.456")]
        test_c2s = [None, Decimal("1234567890.1234567890"), Decimal("1.23E+5"), Decimal("0.1234567890123")]

        stmt2 = conn.stmt2_statement()
        stmt2.prepare("insert into t0 values (?, ?, ?)")

        param = taosws.stmt2_bind_param_view(
            table_name=None,
            tags=None,
            columns=[
                taosws.millis_timestamps_to_column(test_timestamps),
                taosws.decimal64_to_column(test_c1s),
                taosws.decimal_to_column(test_c2s),
            ],
        )
        stmt2.bind([param])

        affected_rows = stmt2.execute()
        assert affected_rows == 4

        stmt2.prepare("select * from t0 where ts > ?")
        param = taosws.stmt2_bind_param_view(
            table_name=None,
            tags=None,
            columns=[taosws.millis_timestamps_to_column([1726803356465])],
        )
        stmt2.bind([param])
        stmt2.execute()

        result = stmt2.result_set()
        expected_results = [
            (1726803356466, None, None),
            (1726803356467, Decimal("99.99"), Decimal("1234567890.1234567890")),
            (1726803356468, Decimal("1.02"), Decimal("123000")),
            (1726803356469, Decimal("-123.46"), Decimal("0.1234567890")),
        ]

        actual_results = []
        for row in result:
            dt = datetime.datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f %z")
            timestamp = int(dt.timestamp() * 1000)

            c1 = None if row[1] is None else row[1]
            c2 = None if row[2] is None else row[2]
            actual_results.append((timestamp, c1, c2))

        assert actual_results == expected_results

    finally:
        conn.execute("drop database if exists test_1775631629")
        conn.close()


@pytest.mark.skipif(utils.TEST_TD_3360, reason="skip for TD-3360")
def test_decimal_tmq():
    conn = taosws.connect("ws://localhost:6041")
    try:
        conn.execute("drop topic if exists topic_1775631845")
        conn.execute("drop database if exists test_1775631845")
        conn.execute("create database test_1775631845")
        conn.execute("create topic topic_1775631845 as database test_1775631845")
        conn.execute("use test_1775631845")
        conn.execute("create table t0 (ts timestamp, c1 int, c2 decimal(10, 2), c3 decimal(20, 10))")

        num = 100

        sql = "insert into t0 values "
        for i in range(num):
            ts = 1726803356466 + i
            c2 = i + 0.9876
            c3 = i + 0.1234567890123
            sql += f"({ts}, {i}, {c2}, {c3}), "
        conn.execute(sql)

        consumer = Consumer(dsn="ws://localhost:6041?group.id=1001&auto.offset.reset=earliest")
        consumer.subscribe(["topic_1775631845"])

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
        conn.execute("drop topic if exists topic_1775631845")
        conn.execute("drop database if exists test_1775631845")
        conn.close()


@pytest.mark.skipif(utils.TEST_TD_3360, reason="skip for TD-3360")
def test_decimal_fetch_reports_python_error_when_decimal_class_missing(monkeypatch):
    conn = taosws.connect("ws://localhost:6041")
    cursor = conn.cursor()
    try:
        cursor.execute("drop database if exists test_1775636199")
        cursor.execute("create database test_1775636199")
        cursor.execute("use test_1775636199")
        cursor.execute("create table t0 (ts timestamp, c1 decimal(10, 2))")
        cursor.execute("insert into t0 values(1752218982762, 99.9876)")

        fake_decimal_module = types.ModuleType("decimal")
        monkeypatch.setitem(sys.modules, "decimal", fake_decimal_module)

        cursor.execute("select c1 from t0")
        with pytest.raises(AttributeError, match=r"Decimal"):
            cursor.fetchall()
    finally:
        cursor.execute("drop database if exists test_1775636199")
        conn.close()
