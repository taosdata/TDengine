from taos.error import OperationalError
from taos import connect, new_bind_params, PrecisionEnum
from taos import *

from ctypes import *
import taos
import pytest


@pytest.fixture
def conn():
    # type: () -> taos.TaosConnection
    return connect()


def test_insert_lines(conn):
    # type: (TaosConnection) -> None

    dbname = "pytest_taos_insert_lines"
    try:
        conn.execute("drop database if exists %s" % dbname)
        conn.execute("create database if not exists %s precision 'us'" % dbname)
        conn.select_db(dbname)

        lines = [
            'st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000ns',
            'st,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000ns',
            'stf,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000ns',
        ]
        conn.insert_lines(lines)
        print("inserted")

        lines = [
            'stf,t1=5i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000ns',
        ]
        conn.insert_lines(lines)
        print("inserted")
        result = conn.query("select * from st")
        print(*result.fields)
        all = result.rows_iter()
        for row in all:
            print(row)
        result.close()
        print(result.row_count)

        conn.execute("drop database if exists %s" % dbname)
        conn.close()

    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print(err)
        raise err


if __name__ == "__main__":
    test_insert_lines(connect())
