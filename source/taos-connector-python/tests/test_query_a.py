import taos
import time
import pytest
from taos import *
from ctypes import *
from utils import tear_down_database
from taos.error import InterfaceError


@pytest.fixture
def conn():
    return taos.connect()


def fetch_callback(p_param, p_result, num_of_rows):
    print("fetched ", num_of_rows, "rows")
    p = cast(p_param, POINTER(Counter))
    result = TaosResult(p_result)

    if num_of_rows == 0:
        print("fetching completed")
        p.contents.done = True
        result.close()
        return
    if num_of_rows < 0:
        p.contents.done = True
        result.check_error(num_of_rows)
        result.close()
        return None

    for row in result.rows_iter(num_of_rows):
        # print(row)
        None
    p.contents.count += result.row_count
    result.fetch_rows_a(fetch_callback, p_param)


def query_callback(p_param, p_result, code):
    # type: (c_void_p, c_void_p, c_int) -> None
    if p_result is None:
        return
    result = TaosResult(p_result)
    if code == 0:
        result.fetch_rows_a(fetch_callback, p_param)
    result.check_error(code)


class Counter(Structure):
    _fields_ = [("count", c_int), ("done", c_bool)]

    def __str__(self):
        return "{ count: %d, done: %s }" % (self.count, self.done)


def test_query(conn):
    # type: (TaosConnection) -> None
    print("ignore test_query... \n")
    return
    counter = Counter(count=0)
    conn.execute("drop database if exists pytestquerya")
    conn.execute("create database pytestquerya")
    conn.execute("use pytestquerya")
    cols = [
        "bool",
        "tinyint",
        "smallint",
        "int",
        "bigint",
        "tinyint unsigned",
        "smallint unsigned",
        "int unsigned",
        "bigint unsigned",
        "float",
        "double",
        "binary(100)",
        "nchar(100)",
    ]
    s = ",".join("c%d %s" % (i, t) for i, t in enumerate(cols))
    print(s)
    conn.execute("create table tb1(ts timestamp, %s)" % s)
    for _ in range(100):
        s = ",".join("null" for c in cols)
        conn.execute("insert into tb1 values(now, %s)" % s)
    conn.query_a("select * from tb1", query_callback, byref(counter))

    while not counter.done:
        print("wait query callback")
        time.sleep(1)
    print(counter)
    db_name = "pytestquerya"
    tear_down_database(conn, db_name)
    conn.close()


def test_query_with_req_id(conn):
    # type: (TaosConnection) -> None
    print("ignore test_query_with_req_id... \n")
    return

    db_name = "pytestquerya"
    try:
        counter = Counter(count=0)
        conn.execute("drop database if exists pytestquerya")
        conn.execute("create database pytestquerya")
        conn.execute("use pytestquerya")
        cols = [
            "bool",
            "tinyint",
            "smallint",
            "int",
            "bigint",
            "tinyint unsigned",
            "smallint unsigned",
            "int unsigned",
            "bigint unsigned",
            "float",
            "double",
            "binary(100)",
            "nchar(100)",
        ]
        s = ",".join("c%d %s" % (i, t) for i, t in enumerate(cols))
        print(s)
        conn.execute("create table tb1(ts timestamp, %s)" % s)
        for _ in range(100):
            s = ",".join("null" for c in cols)
            conn.execute("insert into tb1 values(now, %s)" % s)
        req_id = utils.gen_req_id()
        conn.query_a("select * from tb1", query_callback, byref(counter), req_id)

        while not counter.done:
            print("wait query callback")
            time.sleep(1)
        print(counter)
        tear_down_database(conn, db_name)
        conn.close()
    except InterfaceError as e:
        print(e)
        tear_down_database(conn, db_name)
        conn.close()
    except Exception as e:
        print(e)
        tear_down_database(conn, db_name)
        conn.close()
        raise e


if __name__ == "__main__":
    test_query(taos.connect())
