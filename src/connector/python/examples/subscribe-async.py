from taos import *
from ctypes import *

import time


def subscribe_callback(p_sub, p_result, p_param, errno):
    # type: (c_void_p, c_void_p, c_void_p, c_int) -> None
    print("# fetch in callback")
    result = TaosResult(p_result)
    result.check_error(errno)
    for row in result.rows_iter():
        ts, n = row()
        print(ts, n)


def test_subscribe_callback(conn):
    # type: (TaosConnection) -> None
    dbname = "pytest_taos_subscribe_callback"
    try:
        conn.exec("drop database if exists %s" % dbname)
        conn.exec("create database if not exists %s" % dbname)
        conn.select_db(dbname)
        conn.exec("create table if not exists log(ts timestamp, n int)")

        print("# subscribe with callback")
        sub = conn.subscribe(False, "test", "select * from log", 1000, subscribe_callback)

        for i in range(10):
            conn.exec("insert into log values(now, %d)" % i)
            time.sleep(0.7)
        sub.close()

        conn.exec("drop database if exists %s" % dbname)
        conn.close()
    except Exception as err:
        conn.exec("drop database if exists %s" % dbname)
        conn.close()
        raise err


if __name__ == "__main__":
    test_subscribe_callback(connect())
