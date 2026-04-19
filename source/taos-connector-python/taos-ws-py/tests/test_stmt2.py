import pytest
import taosws
import time
import utils

from decimal import Decimal


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


def test_decimal_column_rejects_non_decimal_type():
    with pytest.raises(taosws.ProgrammingError, match=r"expected decimal\.Decimal or None, got int at index 0"):
        taosws.decimal64_to_column([1])


@pytest.mark.parametrize("column_builder", [taosws.decimal64_to_column, taosws.decimal_to_column])
@pytest.mark.parametrize("non_finite_value", ["NaN", "Infinity", "-Infinity", "sNaN"])
def test_decimal_column_rejects_non_finite_values(column_builder, non_finite_value):
    with pytest.raises(
        taosws.ProgrammingError,
        match=rf"decimal value must be finite, got '{non_finite_value}' at index 0",
    ):
        column_builder([Decimal(non_finite_value)])


def test_decimal_column_accepts_all_none_values():
    assert taosws.decimal64_to_column([None, None]) is not None
    assert taosws.decimal_to_column([None, None]) is not None


def test_decimal64_column_rejects_scale_overflow():
    with pytest.raises(taosws.ProgrammingError, match=r"decimal64 scale exceeds maximum 18"):
        taosws.decimal64_to_column([Decimal("0.1234567890123456789")])


def test_decimal_column_rejects_scale_overflow():
    with pytest.raises(taosws.ProgrammingError, match=r"decimal scale exceeds maximum 38"):
        taosws.decimal_to_column([Decimal("0.123456789012345678901234567890123456789")])


@pytest.mark.parametrize(
    "column_builder, error_message",
    [
        (taosws.decimal64_to_column, r"decimal64 scale exceeds maximum 18"),
        (taosws.decimal_to_column, r"decimal scale exceeds maximum 38"),
    ],
)
def test_decimal_column_rejects_tiny_scientific_notation_scale_overflow(column_builder, error_message):
    with pytest.raises(taosws.ProgrammingError, match=error_message):
        column_builder([Decimal("1E-100")])


@pytest.mark.parametrize(
    "column_builder, decimal_value, error_prefix",
    [
        (taosws.decimal64_to_column, "0.1234567890123456789", "decimal64 scale exceeds maximum 18"),
        (taosws.decimal_to_column, "1E-100", "decimal scale exceeds maximum 38"),
    ],
)
def test_decimal_column_error_includes_original_error_suffix(column_builder, decimal_value, error_prefix):
    with pytest.raises(taosws.ProgrammingError) as exc_info:
        column_builder([Decimal(decimal_value)])

    error_message = str(exc_info.value)
    assert error_message.startswith(f"{error_prefix}: ")


@pytest.mark.skipif(utils.TEST_TD_3360, reason="skip for TD-3360")
def test_stmt2_decimal():
    db_name = "test_1775627317"
    conn = taosws.connect("ws://localhost:6041")
    try:
        conn.execute(f"drop database if exists {db_name}")
        conn.execute(f"create database {db_name}")
        conn.execute(f"use {db_name}")
        conn.execute("create table t_decimal (ts timestamp, d64 decimal(10, 2), d128 decimal(20, 10))")

        stmt2 = conn.stmt2_statement()
        stmt2.prepare("insert into t_decimal values (?, ?, ?)")
        param = taosws.stmt2_bind_param_view(
            table_name=None,
            tags=None,
            columns=[
                taosws.millis_timestamps_to_column([1726803356466, 1726803356467, 1726803356468]),
                taosws.decimal64_to_column([Decimal("99.9876"), Decimal("1.0234"), None]),
                taosws.decimal_to_column(
                    [Decimal("1234567890.1234567890"), Decimal("1.23E+5"), Decimal("0.1234567890123")]
                ),
            ],
        )
        stmt2.bind([param])
        rows = stmt2.execute()
        assert rows == 3

        stmt2.prepare("select d64, d128 from t_decimal where ts >= ?")
        query_param = taosws.stmt2_bind_param_view(
            table_name=None,
            tags=None,
            columns=[taosws.millis_timestamps_to_column([1726803356466])],
        )
        stmt2.bind([query_param])
        stmt2.execute()
        data = [row for row in stmt2.result_set()]

        assert len(data) == 3
        assert isinstance(data[0][0], Decimal)
        assert isinstance(data[0][1], Decimal)
        assert isinstance(data[1][0], Decimal)
        assert isinstance(data[1][1], Decimal)
        assert isinstance(data[2][1], Decimal)
        assert data[0][0] == Decimal("99.99")
        assert data[0][1] == Decimal("1234567890.1234567890")
        assert data[1][0] == Decimal("1.02")
        assert data[1][1] == Decimal("123000")
        assert data[2][0] is None
        assert data[2][1] == Decimal("0.1234567890")

        stmt2.prepare("select d64, d128 from t_decimal where d64 >= ? and d128 >= ?")
        query_param = taosws.stmt2_bind_param_view(
            table_name=None,
            tags=None,
            columns=[
                taosws.decimal64_to_column([Decimal("1.00")]),
                taosws.decimal_to_column([Decimal("100000")]),
            ],
        )
        stmt2.bind([query_param])
        stmt2.execute()
        data = [row for row in stmt2.result_set()]

        assert len(data) == 2
        assert isinstance(data[0][0], Decimal)
        assert isinstance(data[0][1], Decimal)
        assert isinstance(data[1][0], Decimal)
        assert isinstance(data[1][1], Decimal)
        assert data[0][0] == Decimal("99.99")
        assert data[0][1] == Decimal("1234567890.1234567890")
        assert data[1][0] == Decimal("1.02")
        assert data[1][1] == Decimal("123000")

        cursor = conn.cursor()
        cursor.execute(f"select d64, d128 from {db_name}.t_decimal where ts >= 1726803356466")
        cursor_rows = cursor.fetchall()

        assert len(cursor_rows) == 3
        assert isinstance(cursor_rows[0][0], Decimal)
        assert isinstance(cursor_rows[0][1], Decimal)
        assert isinstance(cursor_rows[1][0], Decimal)
        assert isinstance(cursor_rows[1][1], Decimal)
        assert isinstance(cursor_rows[2][1], Decimal)
        assert cursor_rows[0][0] == Decimal("99.99")
        assert cursor_rows[0][1] == Decimal("1234567890.1234567890")
        assert cursor_rows[1][0] == Decimal("1.02")
        assert cursor_rows[1][1] == Decimal("123000")
        assert cursor_rows[2][0] is None
        assert cursor_rows[2][1] == Decimal("0.1234567890")

    finally:
        conn.execute(f"drop database if exists {db_name}")
        conn.close()


@pytest.mark.skipif(utils.TEST_TD_3360, reason="skip for TD-3360")
def test_stmt2_decimal_rounding_boundaries():
    db_name = "test_1775640275"
    conn = taosws.connect("ws://localhost:6041")
    try:
        conn.execute(f"drop database if exists {db_name}")
        conn.execute(f"create database {db_name}")
        conn.execute(f"use {db_name}")
        conn.execute("create table t_rounding (ts timestamp, d64 decimal(10, 2))")

        stmt2 = conn.stmt2_statement()
        stmt2.prepare("insert into t_rounding values (?, ?)")
        param = taosws.stmt2_bind_param_view(
            table_name=None,
            tags=None,
            columns=[
                taosws.millis_timestamps_to_column([1726803356470, 1726803356471, 1726803356472]),
                taosws.decimal64_to_column([Decimal("0.995"), Decimal("0.994"), Decimal("1.995")]),
            ],
        )
        stmt2.bind([param])
        rows = stmt2.execute()
        assert rows == 3

        cursor = conn.cursor()
        cursor.execute(f"select cast(d64 as varchar(32)) from {db_name}.t_rounding where ts >= 1726803356470")
        data = cursor.fetchall()

        assert [row[0] for row in data] == ["1.00", "0.99", "2.00"]
    finally:
        conn.execute(f"drop database if exists {db_name}")
        conn.close()
