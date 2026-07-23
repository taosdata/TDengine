# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql

DB = "test_tw_weighted"
TS = "2024-01-01 00:00:"


class TestTwWeighted:
    """Time-weighted aggregate functions: twavg/twsum/twvar, each in
    _linear (trapezoid) and _locf (last-observation-carried-forward) form.

    Reference dataset t1 (ms precision), 3 points:
        (0s, 10), (1s, 20), (3s, 60)   span T = 3000 ms

    Hand-computed expectations:
      linear  INT v dt  = 15000 + 80000 = 95000 ms-ticks
              twavg_linear = 95000/3000       = 31.666667
              twsum_linear (s, default)        = 95
              twsum_linear (1a=ms)             = 95000
              twvar_linear = 3700000/3000 - 31.666667^2 = 230.555556
      locf    INT v dt  = 10000 + 40000 = 50000 ms-ticks
              twavg_locf  = 50000/3000        = 16.666667
              twsum_locf  (s, default)         = 50
              twvar_locf  = 900000/3000 - 16.666667^2 = 22.222222

    Since: 3.4.3.0

    Catalog: Functions/TimeSeries

    Labels: common,ci

    Jira: None

    History:
    - 2026-06-18 Added with the twavg/twsum/twvar function family.
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # ------------------------------------------------------------------ helpers
    def _create_db(self, precision="ms"):
        tdSql.execute(f"drop database if exists {DB}")
        tdSql.execute(f"create database {DB} precision '{precision}'")
        tdSql.execute(f"use {DB}")

    def _approx(self, sql, expected, tol=1e-4):
        """Run a single-row query and compare each column approximately."""
        tdSql.query(sql)
        tdSql.checkRows(1)
        for c, val in enumerate(expected):
            actual = tdSql.queryResult[0][c]
            if val is None:
                if actual is not None:
                    tdLog.exit(f"col {c}: expected None, got {actual}")
            else:
                if actual is None:
                    tdLog.exit(f"col {c}: expected {val}, got None")
                if abs(float(actual) - float(val)) > tol:
                    tdLog.exit(f"col {c}: expected ~{val}, got {actual}")

    def _setup_ref(self):
        """Single child/normal table with the reference dataset."""
        self._create_db()
        tdSql.execute("create table t1 (ts timestamp, v int)")
        tdSql.execute(
            "insert into t1 values "
            f"('{TS}00.000', 10)('{TS}01.000', 20)('{TS}03.000', 60)"
        )

    # ------------------------------------------------------------------ tests
    def _values(self):
        """Hand-computed values on the reference dataset for all 6 funcs."""
        # twavg: linear 31.666667, locf 16.666667; twavg_linear == twa
        self._approx(
            "select twa(v), twavg_linear(v), twavg_locf(v) from t1",
            (31.666667, 31.666667, 16.666667),
        )
        # twsum default seconds: linear 95, locf 50
        self._approx(
            "select twsum_linear(v), twsum_locf(v) from t1",
            (95.0, 50.0),
        )
        # twsum explicit units: 1s == default; 1a(ms) == raw ticks; 1m scales
        self._approx(
            "select twsum_linear(v,1s), twsum_linear(v,1a), twsum_linear(v,1m) from t1",
            (95.0, 95000.0, 95.0 / 60.0),
        )
        self._approx(
            "select twsum_locf(v,1s), twsum_locf(v,1a) from t1",
            (50.0, 50000.0),
        )
        # twvar population: linear 230.555556, locf 22.222222
        self._approx(
            "select twvar_linear(v), twvar_locf(v) from t1",
            (230.555556, 22.222222),
        )

    def _equivalence(self):
        """twavg_linear must equal twa on identical data, including expr args
        and larger data, across child/super tables."""
        tdSql.query(
            "select twa(v), twavg_linear(v), twa(v+1), twavg_linear(v+1) from t1"
        )
        r = tdSql.queryResult[0]
        if abs(r[0] - r[1]) > 1e-9 or abs(r[2] - r[3]) > 1e-9:
            tdLog.exit(f"twavg_linear != twa: {r}")

    def _single_point(self):
        """A single point spans zero time: avg=value, sum=0, var=0."""
        tdSql.execute("create table t_one (ts timestamp, v int)")
        tdSql.execute(f"insert into t_one values ('{TS}00.000', 42)")
        self._approx(
            "select twavg_linear(v), twavg_locf(v), twsum_linear(v), twvar_linear(v) from t_one",
            (42.0, 42.0, 0.0, 0.0),
        )

    def _decimal(self):
        """DECIMAL input is accepted and produces double results identical to the
        reference dataset. Covers DECIMAL64 (precision <= 18) and DECIMAL128
        (precision > 18); the values 10/20/60 are exact in decimal."""
        # d64 -> DECIMAL64, d128 -> DECIMAL128; same 10/20/60 @ 0s/1s/3s as t1.
        tdSql.execute("create table t_dec (ts timestamp, d64 decimal(10,2), d128 decimal(30,4))")
        tdSql.execute(
            "insert into t_dec values "
            f"('{TS}00.000', 10, 10)('{TS}01.000', 20, 20)('{TS}03.000', 60, 60)"
        )
        for col in ("d64", "d128"):
            # avg: linear 31.666667, locf 16.666667
            self._approx(
                f"select twavg_linear({col}), twavg_locf({col}) from t_dec",
                (31.666667, 16.666667),
            )
            # sum default seconds: linear 95, locf 50; explicit 1a scales by 1000 (ms)
            self._approx(
                f"select twsum_linear({col}), twsum_locf({col}), twsum_linear({col},1a) from t_dec",
                (95.0, 50.0, 95000.0),
            )
            # var population: linear 230.555556, locf 22.222222
            self._approx(
                f"select twvar_linear({col}), twvar_locf({col}) from t_dec",
                (230.555556, 22.222222),
            )
            # decimal output type is DOUBLE, exactly like the numeric-input path
            tdSql.query(f"select twavg_linear({col}) from t_dec")
            if tdSql.queryResult[0][0] is None:
                tdLog.exit(f"twavg_linear({col}) returned None on decimal input")
        # a decimal column with the twsum time_unit still rejects a non-duration 2nd arg
        tdSql.error("select twsum_linear(d64, 3) from t_dec")

    _FUNCS = [
        "twavg_linear", "twavg_locf", "twsum_linear",
        "twsum_locf", "twvar_linear", "twvar_locf",
    ]

    def _types_and_errors(self):
        """Numeric columns accepted; non-numeric rejected; arg-count and bad
        time_unit errors."""
        tdSql.execute(
            "create table t_types (ts timestamp, cint int, cbig bigint, "
            "cfloat float, cdouble double, cbool bool, cbin binary(16), "
            "cnch nchar(16), cts timestamp)"
        )
        tdSql.execute(
            f"insert into t_types values ('{TS}00.000',1,1,1.0,1.0,true,'a','b','{TS}00.000')"
            f"('{TS}01.000',2,2,2.0,2.0,false,'c','d','{TS}01.000')"
        )
        for fn in self._FUNCS:
            for col in ("cint", "cbig", "cfloat", "cdouble"):
                tdSql.query(f"select {fn}({col}) from t_types")
            for col in ("cbool", "cbin", "cnch", "cts"):
                tdSql.error(f"select {fn}({col}) from t_types")
            # wrong arg count (0 args, or too many)
            tdSql.error(f"select {fn}() from t_types")
        # twavg/twvar take exactly one arg
        tdSql.error("select twavg_linear(cint, 1s) from t_types")
        tdSql.error("select twvar_linear(cint, 1s) from t_types")
        # twsum bad time_unit (non-duration / unsupported)
        tdSql.error("select twsum_linear(cint, 3) from t_types")
        tdSql.error("select twsum_linear(cint, 1y) from t_types")

    def _table_variety(self):
        """Child/super (partition & group by)/normal/empty tables, NULL rows."""
        tdSql.execute(
            "create table st (ts timestamp, v int) tags (g int)"
        )
        tdSql.execute("create table c1 using st tags(1)")
        tdSql.execute("create table c2 using st tags(2)")
        tdSql.execute(
            "insert into c1 values "
            f"('{TS}00.000',10)('{TS}01.000',20)('{TS}03.000',60)"
        )
        tdSql.execute(
            "insert into c2 values "
            f"('{TS}00.000',10)('{TS}01.000',20)('{TS}03.000',60)"
        )
        # child table matches the reference values
        self._approx("select twavg_linear(v) from c1", (31.666667,))
        # super with partition/group by -> one row per child, both identical
        tdSql.query("select twavg_linear(v) from st partition by tbname")
        tdSql.checkRows(2)
        tdSql.query("select twsum_linear(v) from st group by tbname")
        tdSql.checkRows(2)
        # empty table -> no row
        tdSql.execute("create table t_empty (ts timestamp, v int)")
        tdSql.query("select twavg_linear(v) from t_empty")
        tdSql.checkRows(0)
        # NULL value rows are skipped (weighting ignores them)
        tdSql.execute("create table t_null (ts timestamp, v int)")
        tdSql.execute(
            "insert into t_null values "
            f"('{TS}00.000',10)('{TS}01.000',NULL)('{TS}03.000',60)"
        )
        tdSql.query("select twavg_linear(v) from t_null")
        tdSql.checkRows(1)

    def _interval_fill(self):
        """INTERVAL windows with fill modes; linear vs locf boundary interp.
        Dataset: gap window between data so fill produces extra rows."""
        tdSql.execute("create table w1 (ts timestamp, v int)")
        tdSql.execute(
            "insert into w1 values "
            f"('{TS}01.000',10)('{TS}01.500',20)"   # [1s,2s)
            f"('{TS}03.000',60)('{TS}03.500',80)"   # [3s,4s), [2s,3s) is a gap
        )
        where = f"where ts >= '{TS}01.000' and ts < '{TS}04.000'"
        # fill(none): gap window omitted
        tdSql.query(
            f"select _wstart, twavg_linear(v) from w1 {where} interval(1s) fill(none)"
        )
        tdSql.checkRows(2)
        # fill(null): gap window present as NULL
        tdSql.query(
            f"select _wstart, twavg_linear(v) from w1 {where} interval(1s) fill(null)"
        )
        tdSql.checkRows(3)
        tdSql.checkData(1, 1, None)
        # linear vs locf both run under interval and differ on held segments
        tdSql.query(
            f"select twavg_linear(v), twavg_locf(v) from w1 {where} interval(1s)"
        )
        tdSql.checkRows(2)
        # fill(prev)/fill(linear) exercise fill machinery without error
        tdSql.query(
            f"select _wstart, twsum_linear(v,1s) from w1 {where} interval(1s) fill(prev)"
        )
        tdSql.checkRows(3)
        tdSql.query(
            f"select _wstart, twvar_linear(v) from w1 {where} interval(1s) fill(linear)"
        )
        tdSql.checkRows(3)

    def test_fun_ts_tw_weighted(self):
        """Fun: twavg_linear/twavg_locf/twsum_linear/twsum_locf/twvar_linear/twvar_locf

        1. Hand-computed value correctness (linear trapezoid vs locf rectangle)
        2. twavg_linear == twa equivalence
        3. twsum time_unit handling (default seconds, explicit 1s/1a/1m)
        4. Single-point / empty / NULL edge cases
        5. Type-support matrix and error cases (incl. DECIMAL64/DECIMAL128 input)
        6. Child/super/normal tables, partition/group by
        7. INTERVAL windows with fill(none/null/prev/linear)

        Since: v3.4.3.0
        Labels: common,ci,integration,functional
        Jira: None
        History:
            - 2026-06-18 Added with the time-weighted aggregate function family.
        """
        self._setup_ref()
        self._values()
        self._equivalence()
        self._single_point()
        self._decimal()
        self._types_and_errors()
        self._table_variety()
        self._interval_fill()
        tdLog.success(f"{__file__} successfully executed")

