import time
import taosws


url = "taosws://localhost:6041/"


def before_test(db_name):
    conn = taosws.connect()
    conn.execute("drop database if exists %s" % db_name)
    conn.execute("create database %s" % db_name)
    conn.execute("use %s" % db_name)
    conn.execute("create table t1 (ts timestamp, a int, b float, c varchar(10))")
    conn.execute("create table stb1 (ts timestamp, a int, b float, c varchar(10)) tags (t1 int, t2 binary(10))")
    conn.close()


def after_test(db_name):
    conn = taosws.connect()
    conn.execute("drop database if exists %s" % db_name)
    conn.close()


def stmt2_query(conn, sql):
    stmt2 = conn.stmt2_statement()
    stmt2.prepare(sql)
    pyStmt2Param = taosws.stmt2_bind_param_view(
        table_name="",
        tags=None,
        columns=[
            taosws.ints_to_column([2]),
        ],
    )
    stmt2.bind([pyStmt2Param])
    stmt2.execute()
    result = stmt2.result_set()
    answer = [3, 4]
    i = 0
    for row in result:
        assert row[1] == answer[i]
        i += 1


def test_stmt2_normal():
    db_name = "test_ws_stmt_{}".format(int(time.time()))
    before_test(db_name)

    conn = taosws.connect(url + db_name)

    stmt2 = conn.stmt2_statement()
    stmt2.prepare("insert into t1 values (?, ?, ?, ?)")

    pyStmt2Param = taosws.stmt2_bind_param_view(
        table_name="",
        tags=None,
        columns=[
            taosws.millis_timestamps_to_column([1686844800000, 1686844801000, 1686844802000, 1686844803000]),
            taosws.ints_to_column([1, 2, 3, 4]),
            taosws.floats_to_column([1.1, 2.2, 3.3, 4.4]),
            taosws.varchar_to_column(["a", "b", "c", "d"]),
        ],
    )

    stmt2.bind([pyStmt2Param])
    rows = stmt2.execute()
    assert rows == 4
    stmt2_query(conn, "select * from t1 where a > ?")
    after_test(db_name)


def test_stmt2_stable():
    db_name = "test_ws_stmt".format(int(time.time()))
    before_test(db_name)

    conn = taosws.connect(url + db_name)

    stmt2 = conn.stmt2_statement()
    stmt2.prepare("insert into ? using stb1 tags (?, ?) values (?, ?, ?, ?)")

    pyStmt2Param = taosws.stmt2_bind_param_view(
        table_name="stb1_1",
        tags=[
            taosws.int_to_tag(1),
            taosws.varchar_to_tag("aaa"),
        ],
        columns=[
            taosws.millis_timestamps_to_column([1686844800000, 1686844801000, 1686844802000, 1686844803000]),
            taosws.ints_to_column([1, 2, 3, 4]),
            taosws.floats_to_column([1.1, 2.2, 3.3, 4.4]),
            taosws.varchar_to_column(["a", "b", "c", "d"]),
        ],
    )

    stmt2.bind([pyStmt2Param])
    rows = stmt2.execute()
    assert rows == 4
    stmt2_query(conn, "select * from stb1 where a > ?")
    after_test(db_name)
