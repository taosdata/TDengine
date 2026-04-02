import taosws
import time


def test_fetchmany():
    try:
        conn = taosws.connect("taosws://localhost:6041")
        conn.execute("drop database if exists test_173942943")
        conn.execute("create database test_173942943")
        conn.execute("use test_173942943")
        conn.execute("create table t0 (ts timestamp, c1 int)")

        num = 7000
        ts = time.time_ns() // 1000000
        for i in range(num):
            conn.execute(f"insert into t0 values({ts+i}, {i})")

        cursor = conn.cursor()
        cursor.execute("select * from test_173942943.t0")

        rows = 0
        while True:
            res = cursor.fetchmany(3345)
            if res is None or len(res) == 0:
                break
            else:
                rows += len(res)

        assert rows == num

        conn.execute("drop database test_173942943")
    except Exception as err:
        print(f"fetchmany failed, err: {err}")
        raise err
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    test_fetchmany()
