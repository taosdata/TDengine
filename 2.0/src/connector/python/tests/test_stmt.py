from taos import *

from ctypes import *
from datetime import datetime
import taos
import pytest

@pytest.fixture
def conn():
    # type: () -> taos.TaosConnection
    return connect()

def test_stmt_insert(conn):
    # type: (TaosConnection) -> None

    dbname = "pytest_taos_stmt"
    try:
        conn.execute("drop database if exists %s" % dbname)
        conn.execute("create database if not exists %s" % dbname)
        conn.select_db(dbname)

        conn.execute(
            "create table if not exists log(ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
             bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
             ff float, dd double, bb binary(100), nn nchar(100), tt timestamp)",
        )
        conn.load_table_info("log")
        

        stmt = conn.statement("insert into log values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        params = new_bind_params(16)
        params[0].timestamp(1626861392589, PrecisionEnum.Milliseconds)
        params[1].bool(True)
        params[2].null()
        params[3].tinyint(2)
        params[4].smallint(3)
        params[5].int(4)
        params[6].bigint(5)
        params[7].tinyint_unsigned(6)
        params[8].smallint_unsigned(7)
        params[9].int_unsigned(8)
        params[10].bigint_unsigned(9)
        params[11].float(10.1)
        params[12].double(10.11)
        params[13].binary("hello")
        params[14].nchar("stmt")
        params[15].timestamp(1626861392589, PrecisionEnum.Milliseconds)

        stmt.bind_param(params)
        stmt.execute()

        result = stmt.use_result()
        assert result.affected_rows == 1
        result.close()
        stmt.close()

        stmt = conn.statement("select * from log")
        stmt.execute()
        result = stmt.use_result()
        row  = result.next()
        print(row)
        assert row[2] == None
        for i in range(3, 11):
            assert row[i] == i - 1
        #float == may not work as expected
        # assert row[10] == c_float(10.1)
        assert row[12] == 10.11
        assert row[13] == "hello"
        assert row[14] == "stmt"

        conn.execute("drop database if exists %s" % dbname)
        conn.close()

    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        raise err

def test_stmt_insert_multi(conn):
    # type: (TaosConnection) -> None

    dbname = "pytest_taos_stmt_multi"
    try:
        conn.execute("drop database if exists %s" % dbname)
        conn.execute("create database if not exists %s" % dbname)
        conn.select_db(dbname)

        conn.execute(
            "create table if not exists log(ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
             bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
             ff float, dd double, bb binary(100), nn nchar(100), tt timestamp)",
        )
        conn.load_table_info("log")

        start = datetime.now()
        stmt = conn.statement("insert into log values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        
        params = new_multi_binds(16)
        params[0].timestamp((1626861392589, 1626861392590, 1626861392591))
        params[1].bool((True, None, False))
        params[2].tinyint([-128, -128, None]) # -128 is tinyint null
        params[3].tinyint([0, 127, None])
        params[4].smallint([3, None, 2])
        params[5].int([3, 4, None])
        params[6].bigint([3, 4, None])
        params[7].tinyint_unsigned([3, 4, None])
        params[8].smallint_unsigned([3, 4, None])
        params[9].int_unsigned([3, 4, None])
        params[10].bigint_unsigned([3, 4, None])
        params[11].float([3, None, 1])
        params[12].double([3, None, 1.2])
        params[13].binary(["abc", "dddafadfadfadfadfa", None])
        params[14].nchar(["涛思数据", None, "a long string with 中文字符"])
        params[15].timestamp([None, None, 1626861392591])
        stmt.bind_param_batch(params)
        
        stmt.execute()
        end = datetime.now()
        print("elapsed time: ", end - start)
        result = stmt.use_result()
        assert result.affected_rows == 3
        result.close()
        stmt.close()

        stmt = conn.statement("select * from log")
        stmt.execute()
        result = stmt.use_result()
        for row in result:
            print(row)
        result.close()

        stmt.close()

        # start = datetime.now()
        # conn.query("insert into log values(1626861392660, true, NULL, 0, 3,3,3,3,3,3,3,3.0,3.0, 'abc','涛思数据',NULL)(1626861392661, true, NULL, 0, 3,3,3,3,3,3,3,3.0,3.0, 'abc','涛思数据',NULL)(1626861392662, true, NULL, 0, 3,3,3,3,3,3,3,3.0,3.0, 'abc','涛思数据',NULL)")

        # end = datetime.now()
        # print("elapsed time: ", end - start)

        conn.execute("drop database if exists %s" % dbname)
        conn.close()

    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        raise err
if __name__ == "__main__":
    test_stmt_insert(connect())
    test_stmt_insert_multi(connect())