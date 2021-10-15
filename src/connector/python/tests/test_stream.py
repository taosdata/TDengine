from taos.cinterface import *
from taos.precision import *
from taos.bind import *
from taos import *
from ctypes import *
import time
import pytest


@pytest.fixture
def conn():
    return connect()


def stream_callback(p_param, p_result, p_row):
    # type: (c_void_p, c_void_p, c_void_p) -> None

    if p_result == None or p_row == None:
        return
    result = TaosResult(p_result)
    row = TaosRow(result, p_row)
    try:
        ts, count = row()
        p = cast(p_param, POINTER(Counter))
        p.contents.count += count
        print("[%s] inserted %d in 5s, total count: %d" % (ts.strftime("%Y-%m-%d %H:%M:%S"), count, p.contents.count))

    except Exception as err:
        print(err)
        raise err


class Counter(ctypes.Structure):
    _fields_ = [
        ("count", c_int),
    ]

    def __str__(self):
        return "%d" % self.count


def test_stream(conn):
    # type: (TaosConnection) -> None
    dbname = "pytest_taos_stream"
    try:
        conn.execute("drop database if exists %s" % dbname)
        conn.execute("create database if not exists %s" % dbname)
        conn.select_db(dbname)
        conn.execute("create table if not exists log(ts timestamp, n int)")

        result = conn.query("select count(*) from log interval(5s)")
        assert result.field_count == 2
        counter = Counter()
        counter.count = 0
        stream = conn.stream("select count(*) from log interval(5s)", stream_callback, param=byref(counter))

        for _ in range(0, 20):
            conn.execute("insert into log values(now,0)(now+1s, 1)(now + 2s, 2)")
            time.sleep(2)
        stream.close()
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        raise err


if __name__ == "__main__":
    test_stream(connect())
