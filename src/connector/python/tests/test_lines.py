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
        conn.exec("drop database if exists %s" % dbname)
        conn.exec("create database if not exists %s precision 'us'" % dbname)
        conn.select_db(dbname)
        conn.exec("create stable ste(ts timestamp, f int) tags(t1 bigint)")

        lines = [
            'st,t1=3i,t2=4,t3="t3" c1=3i,c3=L"passit",c2=false,c4=4 1626006833639000000',
            'st,t1=4i,t3="t4",t2=5,t4=5 c1=3i,c3=L"passitagin",c2=true,c4=5,c5=5 1626006833640000000',
            'ste,t2=5,t3=L"ste" c1=true,c2=4,c3="iam" 1626056811823316532',
            'st,t1=4i,t2=5,t3="t4" c1=3i,c3=L"passitagain",c2=true,c4=5 1626006833642000000',
            'ste,t2=5,t3=L"ste2" c3="iamszhou",c4=false 1626056811843316532',
            'ste,t2=5,t3=L"ste2" c3="iamszhou",c4=false,c5=32b,c6=64s,c7=32w,c8=88.88f 1626056812843316532',
            'st,t1=4i,t3="t4",t2=5,t4=5 c1=3i,c3=L"passitagin",c2=true,c4=5,c5=5,c6=7u 1626006933640000000',
            'stf,t1=4i,t3="t4",t2=5,t4=5 c1=3i,c3=L"passitagin",c2=true,c4=5,c5=5,c6=7u 1626006933640000000',
            'stf,t1=4i,t3="t4",t2=5,t4=5 c1=3i,c3=L"passitagin_stf",c2=false,c5=5,c6=7u 1626006933641a',
        ]
        conn.insert_lines(lines)

        lines = [
            'ste,t2=5,t3=L"ste" c1=true,c2=4,c3="iam" 1626056811855516532',
        ]
        conn.insert_lines(lines)
        for row in conn.query("select * from ste"):
            print(row)
        for row in conn.query("select * from st"):
            print(row)
        for row in conn.query("select * from stf"):
            print(row)


        # conn.exec("drop database if exists %s" % dbname)
        conn.close()

    except Exception as err:
        conn.exec("drop database if exists %s" % dbname)
        conn.close()
        raise err


if __name__ == "__main__":
    test_insert_lines(connect())
