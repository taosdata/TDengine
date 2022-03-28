from taos import *
from ctypes import *

import time


def subscribe_callback(p_sub, p_result, p_param, errno):
    # type: (c_void_p, c_void_p, c_void_p, c_int) -> None
    print("# fetch in callback")
    result = TaosResult(c_void_p(p_result))
    result.check_error(errno)
    for row in result.rows_iter():
        ts, n = row()
        print(ts, n)


def test_subscribe_callback(conn):
    # type: (TaosConnection) -> None
    dbname = "pytest_taos_subscribe_callback"
    try:
        print("drop if exists")
        conn.execute("drop database if exists %s" % dbname)
        print("create database")
        conn.execute("create database if not exists %s" % dbname)
        print("create table")
        # conn.execute("use %s" % dbname)
        conn.execute("create table if not exists %s.log(ts timestamp, n int)" % dbname)

        print("# subscribe with callback")
        sub = conn.subscribe(False, "test", "select * from %s.log" % dbname, 1000, subscribe_callback)

        for i in range(10):
            conn.execute("insert into %s.log values(now, %d)" % (dbname, i))
            time.sleep(0.7)
        sub.close()

        conn.execute("drop database if exists %s" % dbname)
        # conn.close()
    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        # conn.close()
        raise err


if __name__ == "__main__":
    test_subscribe_callback(connect())
