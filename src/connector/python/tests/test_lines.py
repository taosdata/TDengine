from taos.error import OperationalError, SchemalessError
from taos import connect, new_bind_params, PrecisionEnum
from taos import *

from ctypes import *
import taos
import pytest


@pytest.fixture
def conn():
    # type: () -> taos.TaosConnection
    return connect()


def test_schemaless_insert_update_2(conn):
    # type: (TaosConnection) -> None

    dbname = "test_schemaless_insert_update_2"
    try:
        conn.execute("drop database if exists %s" % dbname)
        conn.execute("create database if not exists %s update 2 precision 'ns'" % dbname)
        conn.select_db(dbname)

        lines = [
            'st,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin, abc",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000',
        ]
        res = conn.schemaless_insert(lines, 1, 0)
        print("affected rows: ", res)
        assert(res == 1)

        result = conn.query("select * from st")
        [before] = result.fetch_all_into_dict()
        assert(before["c3"] == "passitagin, abc")

        lines = [
            'st,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000',
        ]
        res = conn.schemaless_insert(lines, 1, 0)
        result = conn.query("select * from st")
        [after] = result.fetch_all_into_dict()
        assert(after["c3"] == "passitagin")

        conn.execute("drop database if exists %s" % dbname)
        conn.close()

    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print(err)
        raise err

def test_schemaless_insert(conn):
    # type: (TaosConnection) -> None

    dbname = "pytest_taos_schemaless_insert"
    try:
        conn.execute("drop database if exists %s" % dbname)
        conn.execute("create database if not exists %s update 2 precision 'ns'" % dbname)
        conn.select_db(dbname)

        lines = [
            'st,t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000',
            'st,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin, abc",c2=true,c4=5f64,c5=5f64,c6=7u64 1626006933640000000',
            'stf,t1=4i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000',
        ]
        res = conn.schemaless_insert(lines, 1, 0)
        print("affected rows: ", res)
        assert(res == 3)

        lines = [
            'stf,t1=5i64,t3="t4",t2=5f64,t4=5f64 c1=3i64,c3=L"passitagin_stf",c2=false,c5=5f64,c6=7u64 1626006933641000000',
        ]
        res = conn.schemaless_insert(lines, 1, 0)
        print("affected rows: ", res)
        assert(res == 1)
        result = conn.query("select * from st")

        dict2 = result.fetch_all_into_dict()
        print(dict2)
        result.row_count
        all = result.rows_iter()
        for row in all:
            print(row)
        result.close()
        assert(result.row_count == 2)

        # error test
        lines = [
            ',t1=3i64,t2=4f64,t3="t3" c1=3i64,c3=L"passit",c2=false,c4=4f64 1626006833639000000',
        ]
        try:
            res = conn.schemaless_insert(lines, 1, 0)
            print(res)
            # assert(False)
        except SchemalessError as err:
            pass

        result = conn.query("select * from st")
        result.row_count
        all = result.rows_iter()
        for row in all:
            print(row)
        result.close()

        conn.execute("drop database if exists %s" % dbname)
        conn.close()

    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print(err)
        raise err


if __name__ == "__main__":
    test_schemaless_insert(connect())
    test_schemaless_insert_update_2(connect())
