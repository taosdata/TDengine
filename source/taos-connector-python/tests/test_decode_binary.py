import taos
import pytest
from taos.tmq import Consumer
from utils import IS_WS


def before():
    conn = taos.connect()
    conn.execute("drop topic if exists test_decode_binary_topic")
    conn.execute("drop database if exists test_decode_binary")
    conn.execute("create database if not exists test_decode_binary")
    conn.execute("use test_decode_binary")

    conn.execute("create table test_decode_binary (ts timestamp, c1 nchar(10), c2 int, c3 binary(50))")
    conn.execute("insert into test_decode_binary values ('2020-01-01 00:00:00', 'hello', 1, 'world')")
    conn.execute(
        "insert into test_decode_binary values ('2020-01-01 00:00:01', 'hello', 2,'\\x00\\x01\\x02\\x03\\x04\\x05\\x06\\x07\\x08\\x09')"
    )


def after():
    conn = taos.connect()
    conn.execute("drop topic if exists test_decode_binary_topic")
    conn.execute("drop database if exists test_decode_binary")


@pytest.mark.skipif(IS_WS, reason="Skip WS")
def test_decode_binary():
    if not taos.IS_V3:
        return
    before()
    conn = taos.connect(decode_binary=False)
    result = conn.query("select * from test_decode_binary.test_decode_binary")
    vals = result.fetch_all()
    assert len(vals) == 2
    assert vals[0][1] == "hello"
    assert vals[1][1] == "hello"
    assert vals[0][3] == b"world"
    assert vals[1][3] == b"\\x00\\x01\\x02\\x03\\x04\\x05\\x06\\x07\\x08\\x09"
    after()


@pytest.mark.skipif(IS_WS, reason="Skip WS")
def test_decode_binary_in_tmq():
    if not taos.IS_V3:
        return
    before()
    conn = taos.connect()
    conn.execute(
        "create topic if not exists test_decode_binary_topic as select * from test_decode_binary.test_decode_binary"
    )

    consumer = Consumer({"group.id": "1", "decode_binary": False, "auto.offset.reset": "earliest"})
    consumer.subscribe(["test_decode_binary_topic"])

    try:
        res = consumer.poll(1)
        values = res.value()
        all_rows = []
        for value in values:
            all_rows.extend(value.fetchall())

        assert len(all_rows) == 2
        assert all_rows[0][1] == "hello"
        assert all_rows[1][1] == "hello"
        assert all_rows[0][3] == b"world"
        assert all_rows[1][3] == b"\\x00\\x01\\x02\\x03\\x04\\x05\\x06\\x07\\x08\\x09"
    finally:
        consumer.unsubscribe()
        consumer.close()
        after()
