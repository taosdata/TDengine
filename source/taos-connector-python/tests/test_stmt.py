import taos
import pytest
from ctypes import *
from taos import *
from datetime import datetime
from utils import IS_WS


@pytest.fixture
def conn():
    # type: () -> taos.TaosConnection
    return connect()


@pytest.mark.skipif(IS_WS, reason="Skip WS")
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
        # conn.load_table_info("log")

        stmt = conn.statement("insert into log values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        params = new_bind_params(16)
        params[0].timestamp(1626861392589, PrecisionEnum.Milliseconds)
        params[1].bool(True)
        params[2].tinyint(None)
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

        assert stmt.affected_rows == 1

        result = conn.query("select * from log")
        row = result.next()
        print(row)
        assert row[2] is None
        for i in range(3, 11):
            assert row[i] == i - 1
        # float == may not work as expected
        # assert row[10] == c_float(10.1)
        assert row[12] == 10.11
        assert row[13] == "hello"
        assert row[14] == "stmt"

        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print("pass test_stmt_insert")

    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        raise err


def test_stmt_insert_multi(conn):
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
        # conn.load_table_info("log")

        start = datetime.now()
        stmt = conn.statement("insert into log values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

        params = new_multi_binds(16)
        params[0].timestamp((1626861392589, 1626861392590, 1626861392591))
        params[1].bool((True, None, False))
        params[2].tinyint([-128, -128, None])  # -128 is tinyint null
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
        assert stmt.affected_rows == 3
        stmt.close()

        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print("pass test_stmt_insert_multi")

    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        raise err


@pytest.mark.skipif(IS_WS, reason="Skip WS")
def test_stmt_set_tbname_tag(conn):
    dbname = "pytest_taos_stmt_set_tbname_tag"

    try:
        conn.execute("drop database if exists %s" % dbname)
        conn.execute("create database if not exists %s" % dbname)
        conn.select_db(dbname)
        conn.execute(
            "create table if not exists log(ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
             bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
             ff float, dd double, bb binary(100), nn nchar(100), tt timestamp) tags (t1 timestamp, t2 bool,\
             t3 tinyint, t4 tinyint, t5 smallint, t6 int, t7 bigint, t8 tinyint unsigned, t9 smallint unsigned, \
             t10 int unsigned, t11 bigint unsigned, t12 float, t13 double, t14 binary(100), t15 nchar(100), t16 timestamp)"
        )

        stmt = conn.statement(
            "insert into ? using log tags (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) \
            values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        )
        tags = new_bind_params(16)
        tags[0].timestamp(1626861392589, PrecisionEnum.Milliseconds)
        tags[1].bool(True)
        tags[2].tinyint(None)
        tags[3].tinyint(2)
        tags[4].smallint(3)
        tags[5].int(4)
        tags[6].bigint(5)
        tags[7].tinyint_unsigned(6)
        tags[8].smallint_unsigned(7)
        tags[9].int_unsigned(8)
        tags[10].bigint_unsigned(9)
        tags[11].float(10.1)
        tags[12].double(10.11)
        tags[13].binary("hello")
        tags[14].nchar("stmt")
        tags[15].timestamp(1626861392589, PrecisionEnum.Milliseconds)
        stmt.set_tbname_tags("tb1", tags)
        params = new_bind_params(16)
        params[0].timestamp(1626861392589, PrecisionEnum.Milliseconds)
        params[1].bool(True)
        params[2].tinyint(None)
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

        assert stmt.affected_rows == 1

        result = conn.query("select * from log")
        row = result.next()
        assert row[2] is None
        for i in range(3, 11):
            assert row[i] == i - 1
        assert row[12] == 10.11
        assert row[13] == "hello"
        assert row[14] == "stmt"

        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print("pass test_stmt_set_tbname_tag")

    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        raise err


def test_stmt_null(conn):
    dbname = "pytest_taos_stmt_null"

    try:
        conn.execute("drop database if exists %s" % dbname)
        conn.execute("create database if not exists %s" % dbname)
        conn.select_db(dbname)
        conn.execute(
            "create table if not exists log(ts timestamp, bo bool, nil tinyint, ti tinyint, si smallint, ii int,\
             bi bigint, tu tinyint unsigned, su smallint unsigned, iu int unsigned, bu bigint unsigned, \
             ff float, dd double, bb binary(100), nn nchar(100), tt timestamp) tags (t1 timestamp, t2 bool,\
             t3 tinyint, t4 tinyint, t5 smallint, t6 int, t7 bigint, t8 tinyint unsigned, t9 smallint unsigned, \
             t10 int unsigned, t11 bigint unsigned, t12 float, t13 double, t14 binary(100), t15 nchar(100), t16 timestamp)"
        )

        stmt = conn.statement(
            "insert into ? using log tags (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) \
            values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        )
        tags = new_bind_params(16)
        tags[0].timestamp(1626861392589, PrecisionEnum.Milliseconds)
        tags[1].bool(True)
        tags[2].tinyint(None)
        tags[3].tinyint(2)
        tags[4].smallint(3)
        tags[5].int(4)
        tags[6].bigint(5)
        tags[7].tinyint_unsigned(6)
        tags[8].smallint_unsigned(7)
        tags[9].int_unsigned(8)
        tags[10].bigint_unsigned(9)
        tags[11].float(10.1)
        tags[12].double(10.11)
        tags[13].binary("hello")
        tags[14].nchar("stmt")
        tags[15].timestamp(1626861392589, PrecisionEnum.Milliseconds)
        stmt.set_tbname_tags("tb1", tags)
        params = new_multi_binds(16)
        params[0].timestamp([1626861392589, 1626861392590, 1626861392591])
        params[1].bool([None, None, None])
        params[2].tinyint([None, None, None])
        params[3].tinyint([None, None, None])
        params[4].smallint([None, None, None])
        params[5].int([None, None, None])
        params[6].bigint([None, None, None])
        params[7].tinyint_unsigned([None, None, None])
        params[8].smallint_unsigned([None, None, None])
        params[9].int_unsigned([None, None, None])
        params[10].bigint_unsigned([None, None, None])
        params[11].float([None, None, None])
        params[12].double([None, None, None])
        params[13].binary([None, None, None])
        params[14].nchar([None, None, None])
        params[15].timestamp([None, None, None])

        stmt.bind_param_batch(params)
        stmt.execute()

        assert stmt.affected_rows == 3

        result = conn.query("select * from log")
        row = result.next()
        for i in range(1, 15):
            assert row[i] is None

        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        print("pass test_stmt_null")

    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        conn.close()
        raise err


if __name__ == "__main__":
    test_stmt_insert(connect())
    test_stmt_insert_multi(connect())
    test_stmt_set_tbname_tag(connect())
    test_stmt_null(connect())
