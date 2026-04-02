import taos
import pytest
from datetime import datetime
from taos import utils, IS_V3
from taos.error import InterfaceError
from utils import tear_down_database, IS_WS


def test_query():
    """This test will use fetch_block for rows fetching, significantly faster than rows_iter"""
    conn = taos.connect()
    conn.execute("drop database if exists test_query_py")
    conn.execute("create database if not exists test_query_py")
    conn.execute("use test_query_py")
    conn.execute("create table if not exists tb1 (ts timestamp, v int) tags(jt json)")
    n = conn.execute('insert into tn1 using tb1 tags(\'{"name":"value"}\') values(now, null) (now + 10s, 1)')
    n = conn.execute('insert into tn1 using tb1 tags(\'{"name":"value"}\') values(now, null) (now + 10s, 1)')
    print("inserted %d rows" % n)
    result = conn.query("select * from tb1")
    fields = result.fields
    for field in fields:
        print("field: %s" % field)

    # test re-consume fields
    flag = 0
    for _ in fields:
        flag += 1
    assert flag == 3

    start = datetime.now()
    for row in result:
        print(row)
        None

    for row in result.rows_iter():
        print(row)

    result = conn.query("select * from tb1 limit 1")
    results = result.fetch_all_into_dict()
    print(results)

    end = datetime.now()
    elapsed = end - start
    print("elapsed time: ", elapsed)
    result.close()
    db_name = "test_query_py"
    tear_down_database(conn, db_name)
    conn.close()


@pytest.mark.skipif(IS_WS, reason="Skip WS")
def test_query_decimal():
    if not IS_V3:
        return

    conn = taos.connect()
    conn.execute("drop database if exists test_query_py")
    conn.execute("create database if not exists test_query_py")
    conn.execute("use test_query_py")
    conn.execute(
        "create table if not exists tb1 (ts timestamp, v int, dec64 decimal(10,6), dec128 decimal(24,10)) tags(jt json)"
    )
    n = conn.execute(
        'insert into tn1 using tb1 tags(\'{"name":"value"}\') values(now, null, null, null) (now + 10s, 1, "9876.123456", "123456789012.0987654321")'
    )
    n = conn.execute(
        'insert into tn1 using tb1 tags(\'{"name":"value"}\') values(now, null, null, null) (now + 10s, 1, "-9876.123456", "-123456789012.0987654321")'
    )
    print("inserted %d rows" % n)
    result = conn.query("select * from tb1")
    fields = result.fields
    for field in fields:
        print("field: %s" % field)

    # test re-consume fields
    flag = 0
    for _ in fields:
        flag += 1
    assert flag == 5

    start = datetime.now()
    for row in result:
        print(row)
        dec64 = row[2]
        dec128 = row[3]
        if dec64 is not None:
            assert str(dec64) in ["9876.123456", "-9876.123456"]
        if dec128 is not None:
            assert str(dec128) in ["123456789012.0987654321", "-123456789012.0987654321"]

    for row in result.rows_iter():
        print(row)
        dec64 = row[2]
        dec128 = row[3]
        if dec64 is not None:
            assert str(dec64) in ["9876.123456", "-9876.123456"]
        #
        if dec128 is not None:
            assert str(dec128) in ["123456789012.0987654321", "-123456789012.0987654321"]
        #

    result = conn.query("select * from tb1 limit 1")
    results = result.fetch_all_into_dict()
    print(results)

    end = datetime.now()
    elapsed = end - start
    print("elapsed time: ", elapsed)
    result.close()
    db_name = "test_query_py"
    tear_down_database(conn, db_name)
    conn.close()


def test_query_with_req_id():
    db_name = "test_query_py"
    conn = taos.connect()
    try:
        conn.execute("drop database if exists test_query_py")
        conn.execute("create database if not exists test_query_py")
        conn.execute("use test_query_py")
        conn.execute("create table if not exists tb1 (ts timestamp, v int) tags(jt json)")
        n = conn.execute('insert into tn1 using tb1 tags(\'{"name":"value"}\') values(now, null) (now + 10s, 1)')
        n = conn.execute('insert into tn1 using tb1 tags(\'{"name":"value"}\') values(now, null) (now + 10s, 1)')
        print("inserted %d rows" % n)
        req_id = utils.gen_req_id()
        result = conn.query("select * from tb1", req_id)
        fields = result.fields
        for field in fields:
            print("field: %s" % field)

        # test re-consume fields
        flag = 0
        for _ in fields:
            flag += 1
        assert flag == 3

        start = datetime.now()
        for row in result:
            print(row)
            None

        for row in result.rows_iter():
            print(row)

        req_id = utils.gen_req_id()
        result = conn.query("select * from tb1", req_id)
        results = result.fetch_all_into_dict()
        print(results)

        end = datetime.now()
        elapsed = end - start
        print("elapsed time: ", elapsed)
        result.close()
    except InterfaceError as e:
        print(e)
    except Exception as e:
        print(e)
        tear_down_database(conn, db_name)
        conn.close()
        raise e
    finally:
        tear_down_database(conn, db_name)
        conn.close()


def test_varbinary():
    if not IS_V3:
        return
    conn = taos.connect()
    conn.execute("drop database if exists test_varbinary_py")
    conn.execute("create database if not exists test_varbinary_py")
    conn.execute("use test_varbinary_py")
    conn.execute(
        "create stable if not exists stb1 (ts timestamp, v1 int, v2 varchar(50), v3 varbinary(50), v4 geometry(512)) tags(t1 int)"
    )
    conn.execute(
        "insert into tb1 using stb1 tags(1) values(now, 1, 'varchar\\x8f4e3e', '\\x8f4e3e', 'POINT (4.0 8.0)') "
        "(now + 1s, 2, 'varchar value 2', 'binary value_1', 'POINT (3.0 5.0)')"
    )
    conn.execute(
        "insert into tb2 using stb1 tags(2) values(now, 1, 'varchar value 3', '\\x8f4e3e', 'LINESTRING (1.000000 1.000000, 2.000000 2.000000, 5.000000 5.000000)') "
        "(now + 1s, 2, 'varchar value 4', 'binary value_2', 'POLYGON ((3.000000 6.000000, 5.000000 6.000000, 5.000000 8.000000, 3.000000 8.000000, 3.000000 6.000000))')"
    )
    result = conn.query("select * from stb1")

    fields = result.fields
    for field in fields:
        print("field: %s" % field)

    for row in result.rows_iter():
        print(row)

    result.close()
    conn.execute("drop database if exists test_varbinary_py")


def test_varbinary_with_cursor():
    if not IS_V3:
        return
    conn = taos.connect()
    cursor = conn.cursor()
    conn.execute("drop database if exists test_varbinary_py")
    conn.execute("create database if not exists test_varbinary_py")
    conn.execute("use test_varbinary_py")
    conn.execute("create table t(ts timestamp, c1 int, c2 binary(10), c3 varbinary(32));")
    conn.execute(
        "insert into t values(now, 1, 'aaaa', '\\x8f4e3e')(now+1s, 2, 'bbbb','\\x8f4e3e')(now+2s, 3, 'cccc','\\x8f4e3e')"
    )
    # entire columns query
    cursor.execute(f"""select * from t;""")
    res = cursor.fetchall()
    print(res)

    conn.execute("drop database if exists test_varbinary_py")
    cursor.close()


if __name__ == "__main__":
    test_query()
