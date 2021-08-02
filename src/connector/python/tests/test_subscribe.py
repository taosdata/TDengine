from taos.subscription import TaosSubscription
from taos import *
from ctypes import *
import taos
import pytest
import time
from random import random


@pytest.fixture
def conn():
    return taos.connect()


def test_subscribe(conn):
    # type: (TaosConnection) -> None

    dbname = "pytest_taos_subscribe_callback"
    try:
        conn.execute("drop database if exists %s" % dbname)
        conn.execute("create database if not exists %s" % dbname)
        conn.select_db(dbname)
        conn.execute("create table if not exists log(ts timestamp, n int)")
        for i in range(10):
            conn.execute("insert into log values(now, %d)" % i)

        sub = conn.subscribe(True, "test", "select * from log", 1000)
        print("# consume from begin")
        for ts, n in sub.consume():
            print(ts, n)
        
        print("# consume new data")
        for i in range(5):
            conn.execute("insert into log values(now, %d)(now+1s, %d)" % (i, i))
            result = sub.consume()
            for ts, n in result:
                print(ts, n)
        
        print("# consume with a stop condition")
        for i in range(10):
            conn.execute("insert into log values(now, %d)" % int(random() * 10))
            result = sub.consume()
            try:
                ts, n = next(result)
                print(ts, n)
                if n > 5:
                    result.stop_query()
                    print("## stopped")
                    break
            except StopIteration:
                continue

        sub.close()

        conn.execute("drop database if exists %s" % dbname)
        conn.close()
    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        raise err


def subscribe_callback(p_sub, p_result, p_param, errno):
    # type: (c_void_p, c_void_p, c_void_p, c_int) -> None
    print("callback")
    result = TaosResult(p_result)
    result.check_error(errno)
    for row in result.rows_iter():
        ts, n = row()
        print(ts, n)


def test_subscribe_callback(conn):
    # type: (TaosConnection) -> None
    dbname = "pytest_taos_subscribe_callback"
    try:
        conn.execute("drop database if exists %s" % dbname)
        conn.execute("create database if not exists %s" % dbname)
        conn.select_db(dbname)
        conn.execute("create table if not exists log(ts timestamp, n int)")

        print("# subscribe with callback")
        sub = conn.subscribe(False, "test", "select * from log", 1000, subscribe_callback)

        for i in range(10):
            conn.execute("insert into log values(now, %d)" % i)
            time.sleep(0.7)
        sub.close()

        conn.execute("drop database if exists %s" % dbname)
        conn.close()
    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        raise err


if __name__ == "__main__":
    test_subscribe(taos.connect())
    test_subscribe_callback(taos.connect())
