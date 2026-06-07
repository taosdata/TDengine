import os
from datetime import datetime

from new_test_framework.utils import tdCom, tdLog, tdSql


class FunTsWindowCaseHelper:
    def setup_class(cls):
        cls.replicaVar = 1
        tdLog.debug(f"start to execute {__file__}")

    def create_database(self, db_name: str, vgroups: int = 1):
        tdSql.execute(f"drop database if exists {db_name}")
        tdSql.execute(f"create database {db_name} vgroups {vgroups}")
        tdSql.execute(f"use {db_name}")

    def drop_database(self, db_name: str):
        tdSql.execute(f"drop database if exists {db_name}")

    def to_sql_literal(self, value):
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        return str(value)

    def insert_rows(self, table_name: str, rows, batch_size: int = 500):
        for start in range(0, len(rows), batch_size):
            batch = rows[start : start + batch_size]
            values = []
            for row in batch:
                values.append(
                    "(" + ", ".join(self.to_sql_literal(value) for value in row) + ")"
                )
            tdSql.execute(f"insert into {table_name} values " + ",".join(values))

    def format_ts(self, ts_ms: int) -> str:
        return datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def _base_rows(self, base: int):
        return [
            (base + 1000, 1, 1, 10),
            (base + 2000, 1, 3, 20),
            (base + 3000, 2, 4, 35),
            (base + 20000, 3, 1, 50),
            (base + 21000, 3, 3, 65),
            (base + 22000, 4, 4, 85),
        ]

    def _rate_rows(self, base: int):
        return [
            (base + 1000, 1, 1, 10),
            (base + 2000, 1, 3, 20),
            (base + 3000, 2, 4, 30),
            (base + 20000, 3, 1, 40),
            (base + 21000, 3, 3, 50),
            (base + 22000, 4, 4, 60),
        ]

    def _partition_rows(self, base: int):
        return [
            (base + 1000, 1, 3, 5),
            (base + 2000, 1, 4, 15),
            (base + 15000, 2, 1, 25),
            (base + 16000, 2, 3, 35),
            (base + 17000, 3, 4, 45),
        ]

    def _create_partition_stable(self, stable_name: str, base: int):
        tdSql.execute("drop table if exists ct1")
        tdSql.execute("drop table if exists ct2")
        tdSql.execute(f"drop table if exists {stable_name}")
        tdSql.execute(
            f"create stable {stable_name}(ts timestamp, state_val int, marker int, val int) tags(site int)"
        )
        tdSql.execute(f"create table ct1 using {stable_name} tags(1)")
        tdSql.execute(f"create table ct2 using {stable_name} tags(2)")
        self.insert_rows("ct1", self._rate_rows(base), batch_size=10)
        self.insert_rows("ct2", self._partition_rows(base), batch_size=10)

    def _check_csum_basic_execution(self):
        base = 1704067200000
        tdSql.execute("create table t_csum_basic(ts timestamp, state_val int, marker int, val int)")
        self.insert_rows("t_csum_basic", self._base_rows(base), batch_size=10)

        ws_interval_0 = self.format_ts(base)
        ws_session_0 = self.format_ts(base + 1000)
        ws_interval_1 = self.format_ts(base + 20000)
        ws_state_1 = self.format_ts(base + 3000)
        ws_state_2 = self.format_ts(base + 22000)
        ws_event_0 = self.format_ts(base + 2000)
        ws_event_1 = self.format_ts(base + 21000)

        tdSql.queryAndCheckResult(
            [
                "select * from (select _wstart as ws, csum(val) as cv from t_csum_basic interval(10s)) order by ws, cv",
                "select * from (select _wstart as ws, csum(val) as cv from t_csum_basic session(ts, 10s)) order by ws, cv",
                "select * from (select _wstart as ws, csum(val) as cv from t_csum_basic state_window(state_val)) order by ws, cv",
                "select * from (select _wstart as ws, csum(val) as cv from t_csum_basic event_window start with marker >= 3 end with marker >= 4) order by ws, cv",
                "select * from (select _wstart as ws, csum(val) as cv from t_csum_basic count_window(3)) order by ws, cv",
            ],
            [
                [
                    (ws_interval_0, 10),
                    (ws_interval_0, 30),
                    (ws_interval_0, 65),
                    (ws_interval_1, 50),
                    (ws_interval_1, 115),
                    (ws_interval_1, 200),
                ],
                [
                    (ws_session_0, 10),
                    (ws_session_0, 30),
                    (ws_session_0, 65),
                    (ws_interval_1, 50),
                    (ws_interval_1, 115),
                    (ws_interval_1, 200),
                ],
                [
                    (ws_session_0, 10),
                    (ws_session_0, 30),
                    (ws_state_1, 35),
                    (ws_interval_1, 50),
                    (ws_interval_1, 115),
                    (ws_state_2, 85),
                ],
                [
                    (ws_event_0, 20),
                    (ws_event_0, 55),
                    (ws_event_1, 65),
                    (ws_event_1, 150),
                ],
                [
                    (ws_session_0, 10),
                    (ws_session_0, 30),
                    (ws_session_0, 65),
                    (ws_interval_1, 50),
                    (ws_interval_1, 115),
                    (ws_interval_1, 200),
                ],
            ],
        )

    def _check_csum_null_and_partition(self):
        base = 1704067200000
        tdSql.execute("create table t_csum_null(ts timestamp, state_val int, marker int, val int)")
        self.insert_rows(
            "t_csum_null",
            [
                (base + 1000, 1, 1, 10),
                (base + 2000, 1, 3, None),
                (base + 3000, 1, 4, 35),
                (base + 20000, 2, 1, None),
                (base + 21000, 2, 3, 65),
                (base + 22000, 2, 4, None),
                (base + 40000, 3, 1, None),
                (base + 41000, 3, 3, None),
                (base + 42000, 3, 4, 85),
            ],
            batch_size=20,
        )

        tdSql.queryAndCheckResult(
            [
                "select * from (select _wstart as ws, csum(val) as cv from t_csum_null interval(10s)) order by ws, cv",
                "select * from (select _wstart as ws, csum(val) as cv from t_csum_null session(ts, 10s)) order by ws, cv",
                "select * from (select _wstart as ws, csum(val) as cv from t_csum_null state_window(state_val)) order by ws, cv",
                "select * from (select _wstart as ws, csum(val) as cv from t_csum_null event_window start with marker >= 3 end with marker >= 4) order by ws, cv",
                "select * from (select _wstart as ws, csum(val) as cv from t_csum_null count_window(3)) order by ws, cv",
            ],
            [
                [
                    (self.format_ts(base), 10),
                    (self.format_ts(base), 45),
                    (self.format_ts(base + 20000), 65),
                    (self.format_ts(base + 40000), 85),
                ],
                [
                    (self.format_ts(base + 1000), 10),
                    (self.format_ts(base + 1000), 45),
                    (self.format_ts(base + 20000), 65),
                    (self.format_ts(base + 40000), 85),
                ],
                [
                    (self.format_ts(base + 1000), 10),
                    (self.format_ts(base + 1000), 45),
                    (self.format_ts(base + 20000), 65),
                    (self.format_ts(base + 40000), 85),
                ],
                [
                    (self.format_ts(base + 2000), 35),
                    (self.format_ts(base + 21000), 65),
                    (self.format_ts(base + 41000), 85),
                ],
                [
                    (self.format_ts(base + 1000), 10),
                    (self.format_ts(base + 1000), 45),
                    (self.format_ts(base + 20000), 65),
                    (self.format_ts(base + 40000), 85),
                ],
            ],
        )

        tdSql.execute(
            "create stable st_csum_part(ts timestamp, state_val int, marker int, val int) tags(site int)"
        )
        tdSql.execute("create table ct1 using st_csum_part tags(1)")
        tdSql.execute("create table ct2 using st_csum_part tags(2)")
        self.insert_rows(
            "ct1",
            self._base_rows(base),
            batch_size=10,
        )
        self.insert_rows(
            "ct2",
            [
                (base + 1000, 2, 3, 5),
                (base + 2000, 2, 4, 15),
                (base + 15000, 3, 1, 25),
                (base + 16000, 3, 3, 35),
                (base + 17000, 4, 4, 45),
            ],
            batch_size=10,
        )

        tdSql.queryAndCheckResult(
            [
                "select * from (select tbname, _wstart as ws, csum(val) as cv from st_csum_part partition by tbname interval(10s)) order by tbname, ws, cv",
                "select * from (select tbname, _wstart as ws, csum(val) as cv from st_csum_part partition by tbname session(ts, 10s)) order by tbname, ws, cv",
                "select * from (select tbname, _wstart as ws, csum(val) as cv from st_csum_part partition by tbname state_window(state_val)) order by tbname, ws, cv",
                "select * from (select tbname, _wstart as ws, csum(val) as cv from st_csum_part partition by tbname event_window start with marker >= 3 end with marker >= 4) order by tbname, ws, cv",
                "select * from (select tbname, _wstart as ws, csum(val) as cv from st_csum_part partition by tbname count_window(2)) order by tbname, ws, cv",
            ],
            [
                [
                    ("ct1", self.format_ts(base), 10),
                    ("ct1", self.format_ts(base), 30),
                    ("ct1", self.format_ts(base), 65),
                    ("ct1", self.format_ts(base + 20000), 50),
                    ("ct1", self.format_ts(base + 20000), 115),
                    ("ct1", self.format_ts(base + 20000), 200),
                    ("ct2", self.format_ts(base), 5),
                    ("ct2", self.format_ts(base), 20),
                    ("ct2", self.format_ts(base + 10000), 25),
                    ("ct2", self.format_ts(base + 10000), 60),
                    ("ct2", self.format_ts(base + 10000), 105),
                ],
                [
                    ("ct1", self.format_ts(base + 1000), 10),
                    ("ct1", self.format_ts(base + 1000), 30),
                    ("ct1", self.format_ts(base + 1000), 65),
                    ("ct1", self.format_ts(base + 20000), 50),
                    ("ct1", self.format_ts(base + 20000), 115),
                    ("ct1", self.format_ts(base + 20000), 200),
                    ("ct2", self.format_ts(base + 1000), 5),
                    ("ct2", self.format_ts(base + 1000), 20),
                    ("ct2", self.format_ts(base + 15000), 25),
                    ("ct2", self.format_ts(base + 15000), 60),
                    ("ct2", self.format_ts(base + 15000), 105),
                ],
                [
                    ("ct1", self.format_ts(base + 1000), 10),
                    ("ct1", self.format_ts(base + 1000), 30),
                    ("ct1", self.format_ts(base + 3000), 35),
                    ("ct1", self.format_ts(base + 20000), 50),
                    ("ct1", self.format_ts(base + 20000), 115),
                    ("ct1", self.format_ts(base + 22000), 85),
                    ("ct2", self.format_ts(base + 1000), 5),
                    ("ct2", self.format_ts(base + 1000), 20),
                    ("ct2", self.format_ts(base + 15000), 25),
                    ("ct2", self.format_ts(base + 15000), 60),
                    ("ct2", self.format_ts(base + 17000), 45),
                ],
                [
                    ("ct1", self.format_ts(base + 2000), 20),
                    ("ct1", self.format_ts(base + 2000), 55),
                    ("ct1", self.format_ts(base + 21000), 65),
                    ("ct1", self.format_ts(base + 21000), 150),
                    ("ct2", self.format_ts(base + 1000), 5),
                    ("ct2", self.format_ts(base + 1000), 20),
                    ("ct2", self.format_ts(base + 16000), 35),
                    ("ct2", self.format_ts(base + 16000), 80),
                ],
                [
                    ("ct1", self.format_ts(base + 1000), 10),
                    ("ct1", self.format_ts(base + 1000), 30),
                    ("ct1", self.format_ts(base + 3000), 35),
                    ("ct1", self.format_ts(base + 3000), 85),
                    ("ct1", self.format_ts(base + 21000), 65),
                    ("ct1", self.format_ts(base + 21000), 150),
                    ("ct2", self.format_ts(base + 1000), 5),
                    ("ct2", self.format_ts(base + 1000), 20),
                    ("ct2", self.format_ts(base + 15000), 25),
                    ("ct2", self.format_ts(base + 15000), 60),
                    ("ct2", self.format_ts(base + 17000), 45),
                ],
            ],
        )

    def _check_csum_numeric_matrix(self):
        base = 1704067200000
        tdSql.execute(
            """create table t_csum_types(
            ts timestamp,
            c_tiny tinyint,
            c_small smallint,
            c_int int,
            c_big bigint,
            c_float float,
            c_double double,
            c_tiny_u tinyint unsigned,
            c_small_u smallint unsigned,
            c_int_u int unsigned,
            c_big_u bigint unsigned
            )"""
        )
        self.insert_rows(
            "t_csum_types",
            [
                (base + 1000, 1, 10, 100, 1000, 1.0, 10.0, 1, 10, 100, 1000),
                (base + 2000, 2, 20, 200, 2000, 2.0, 20.0, 2, 20, 200, 2000),
                (base + 11000, 4, 40, 400, 4000, 4.0, 40.0, 4, 40, 400, 4000),
                (base + 12000, 7, 70, 700, 7000, 7.0, 70.0, 7, 70, 700, 7000),
            ],
            batch_size=10,
        )

        tdSql.queryAndCheckResult(
            [
                "select * from ("
                "select _wstart as ws, "
                "csum(c_tiny) as s_tiny, csum(c_small) as s_small, csum(c_int) as s_int, "
                "csum(c_big) as s_big, csum(c_float) as s_float, csum(c_double) as s_double, "
                "csum(c_tiny_u) as s_tiny_u, csum(c_small_u) as s_small_u, "
                "csum(c_int_u) as s_int_u, csum(c_big_u) as s_big_u "
                "from t_csum_types interval(10s)"
                ") order by ws, s_int"
            ],
            [[
                (self.format_ts(base), 1, 10, 100, 1000, 1.0, 10.0, 1, 10, 100, 1000),
                (self.format_ts(base), 3, 30, 300, 3000, 3.0, 30.0, 3, 30, 300, 3000),
                (self.format_ts(base + 10000), 4, 40, 400, 4000, 4.0, 40.0, 4, 40, 400, 4000),
                (self.format_ts(base + 10000), 11, 110, 1100, 11000, 11.0, 110.0, 11, 110, 1100, 11000),
            ]],
        )

    def _check_csum_interval_specific(self):
        base = 1704067200000
        tdSql.execute("create table t_csum_slide(ts timestamp, val int)")
        self.insert_rows(
            "t_csum_slide",
            [
                (base + 1000, 1),
                (base + 6000, 2),
                (base + 11000, 3),
                (base + 16000, 4),
            ],
            batch_size=10,
        )
        tdSql.queryAndCheckResult(
            [
                "select * from (select _wstart as ws, csum(val) as cv from t_csum_slide interval(10s) sliding(5s)) order by ws, cv"
            ],
            [[
                (self.format_ts(base - 5000), 1),
                (self.format_ts(base), 1),
                (self.format_ts(base), 3),
                (self.format_ts(base + 5000), 2),
                (self.format_ts(base + 5000), 5),
                (self.format_ts(base + 10000), 3),
                (self.format_ts(base + 10000), 7),
                (self.format_ts(base + 15000), 4),
            ]],
        )

        tdSql.execute("create table t_csum_width(ts timestamp, val int)")
        self.insert_rows(
            "t_csum_width",
            [
                (base + 1000, 1),
                (base + 4500, 2),
                (base + 5200, 3),
                (base + 9800, 4),
                (base + 10050, 5),
                (base + 10100, 6),
                (base + 11500, 7),
            ],
            batch_size=10,
        )
        tdSql.queryAndCheckResult(
            [
                "select * from (select _wstart as ws, csum(val) as cv from t_csum_width interval(10s)) order by ws, cv",
                "select * from (select _wstart as ws, csum(val) as cv from t_csum_width interval(5s)) order by ws, cv",
                "select * from (select _wstart as ws, csum(val) as cv from t_csum_width interval(1s)) order by ws, cv",
            ],
            [
                [
                    (self.format_ts(base), 1),
                    (self.format_ts(base), 3),
                    (self.format_ts(base), 6),
                    (self.format_ts(base), 10),
                    (self.format_ts(base + 10000), 5),
                    (self.format_ts(base + 10000), 11),
                    (self.format_ts(base + 10000), 18),
                ],
                [
                    (self.format_ts(base), 1),
                    (self.format_ts(base), 3),
                    (self.format_ts(base + 5000), 3),
                    (self.format_ts(base + 5000), 7),
                    (self.format_ts(base + 10000), 5),
                    (self.format_ts(base + 10000), 11),
                    (self.format_ts(base + 10000), 18),
                ],
                [
                    (self.format_ts(base + 1000), 1),
                    (self.format_ts(base + 4000), 2),
                    (self.format_ts(base + 5000), 3),
                    (self.format_ts(base + 9000), 4),
                    (self.format_ts(base + 10000), 5),
                    (self.format_ts(base + 10000), 11),
                    (self.format_ts(base + 11000), 7),
                ],
            ],
        )

    def _check_diff_basic_execution(self):
        base = 1704067200000
        tdSql.execute("create table t_diff_window(ts timestamp, state_val int, marker int, val int)")
        self.insert_rows("t_diff_window", self._base_rows(base), batch_size=10)

        tdSql.queryAndCheckResult(
            [
                "select * from (select _wstart as ws, diff(val) as dv from t_diff_window interval(10s)) order by ws, dv",
                "select * from (select _wstart as ws, diff(val) as dv from t_diff_window session(ts, 10s)) order by ws, dv",
                "select * from (select _wstart as ws, diff(val) as dv from t_diff_window state_window(state_val)) order by ws, dv",
                "select * from (select _wstart as ws, diff(val) as dv from t_diff_window event_window start with marker >= 3 end with marker >= 4) order by ws, dv",
                "select * from (select _wstart as ws, diff(val) as dv from t_diff_window count_window(3)) order by ws, dv",
            ],
            [
                [
                    (self.format_ts(base), 10),
                    (self.format_ts(base), 15),
                    (self.format_ts(base + 20000), 15),
                    (self.format_ts(base + 20000), 20),
                ],
                [
                    (self.format_ts(base + 1000), 10),
                    (self.format_ts(base + 1000), 15),
                    (self.format_ts(base + 20000), 15),
                    (self.format_ts(base + 20000), 20),
                ],
                [
                    (self.format_ts(base + 1000), 10),
                    (self.format_ts(base + 20000), 15),
                ],
                [
                    (self.format_ts(base + 2000), 15),
                    (self.format_ts(base + 21000), 20),
                ],
                [
                    (self.format_ts(base + 1000), 10),
                    (self.format_ts(base + 1000), 15),
                    (self.format_ts(base + 20000), 15),
                    (self.format_ts(base + 20000), 20),
                ],
            ],
        )

    def _check_diff_numeric_matrix(self):
        base = 1704067200000
        tdSql.execute(
            """create table t_diff_types(
            ts timestamp,
            c_tiny tinyint,
            c_small smallint,
            c_int int,
            c_big bigint,
            c_float float,
            c_double double,
            c_tiny_u tinyint unsigned,
            c_small_u smallint unsigned,
            c_int_u int unsigned,
            c_big_u bigint unsigned
            )"""
        )
        self.insert_rows(
            "t_diff_types",
            [
                (base + 1000, 1, 10, 100, 1000, 1.0, 10.0, 1, 10, 100, 1000),
                (base + 2000, 2, 20, 200, 2000, 2.0, 20.0, 2, 20, 200, 2000),
                (base + 11000, 4, 40, 400, 4000, 4.0, 40.0, 4, 40, 400, 4000),
                (base + 12000, 7, 70, 700, 7000, 7.0, 70.0, 7, 70, 700, 7000),
            ],
            batch_size=10,
        )

        tdSql.queryAndCheckResult(
            [
                "select * from ("
                "select _wstart as ws, "
                "diff(c_tiny) as d_tiny, diff(c_small) as d_small, diff(c_int) as d_int, "
                "diff(c_big) as d_big, diff(c_float) as d_float, diff(c_double) as d_double, "
                "diff(c_tiny_u) as d_tiny_u, diff(c_small_u) as d_small_u, "
                "diff(c_int_u) as d_int_u, diff(c_big_u) as d_big_u "
                "from t_diff_types interval(10s)"
                ") order by ws, d_int"
            ],
            [[
                (self.format_ts(base), 1, 10, 100, 1000, 1.0, 10.0, 1, 10, 100, 1000),
                (self.format_ts(base + 10000), 3, 30, 300, 3000, 3.0, 30.0, 3, 30, 300, 3000),
            ]],
        )

    def _check_diff_partition(self):
        base = 1704067200000
        self._create_partition_stable("st_diff_part", base)

        tdSql.queryAndCheckResult(
            [
                "select * from (select tbname, _wstart as ws, diff(val) as dv from st_diff_part partition by tbname session(ts, 10s)) order by tbname, ws, dv"
            ],
            [[
                ("ct1", self.format_ts(base + 1000), 10),
                ("ct1", self.format_ts(base + 1000), 10),
                ("ct1", self.format_ts(base + 20000), 10),
                ("ct1", self.format_ts(base + 20000), 10),
                ("ct2", self.format_ts(base + 1000), 10),
                ("ct2", self.format_ts(base + 15000), 10),
                ("ct2", self.format_ts(base + 15000), 10),
            ]],
        )

    def _check_derivative_window(self):
        base = 1704067200000
        tdSql.execute("create table t_derivative(ts timestamp, state_val int, marker int, val int)")
        self.insert_rows("t_derivative", self._rate_rows(base), batch_size=10)

        tdSql.queryAndCheckResult(
            [
                "select * from (select _wstart as ws, derivative(val, 1s, 0) as dv from t_derivative interval(10s)) order by ws, dv",
                "select * from (select _wstart as ws, derivative(val, 1s, 0) as dv from t_derivative session(ts, 10s)) order by ws, dv",
                "select * from (select _wstart as ws, derivative(val, 1s, 0) as dv from t_derivative state_window(state_val)) order by ws, dv",
                "select * from (select _wstart as ws, derivative(val, 1s, 0) as dv from t_derivative event_window start with marker >= 3 end with marker >= 4) order by ws, dv",
                "select * from (select _wstart as ws, derivative(val, 1s, 0) as dv from t_derivative count_window(3)) order by ws, dv",
            ],
            [
                [
                    (self.format_ts(base), 10.0),
                    (self.format_ts(base), 10.0),
                    (self.format_ts(base + 20000), 10.0),
                    (self.format_ts(base + 20000), 10.0),
                ],
                [
                    (self.format_ts(base + 1000), 10.0),
                    (self.format_ts(base + 1000), 10.0),
                    (self.format_ts(base + 20000), 10.0),
                    (self.format_ts(base + 20000), 10.0),
                ],
                [
                    (self.format_ts(base + 1000), 10.0),
                    (self.format_ts(base + 20000), 10.0),
                ],
                [
                    (self.format_ts(base + 2000), 10.0),
                    (self.format_ts(base + 21000), 10.0),
                ],
                [
                    (self.format_ts(base + 1000), 10.0),
                    (self.format_ts(base + 1000), 10.0),
                    (self.format_ts(base + 20000), 10.0),
                    (self.format_ts(base + 20000), 10.0),
                ],
            ],
        )

    def _check_derivative_partition_and_errors(self):
        base = 1704067200000
        self._create_partition_stable("st_derivative_part", base)
        tdSql.queryAndCheckResult(
            [
                "select * from (select tbname, _wstart as ws, derivative(val, 1s, 0) as dv from st_derivative_part partition by tbname session(ts, 10s)) order by tbname, ws, dv"
            ],
            [[
                ("ct1", self.format_ts(base + 1000), 10.0),
                ("ct1", self.format_ts(base + 1000), 10.0),
                ("ct1", self.format_ts(base + 20000), 10.0),
                ("ct1", self.format_ts(base + 20000), 10.0),
                ("ct2", self.format_ts(base + 1000), 10.0),
                ("ct2", self.format_ts(base + 15000), 10.0),
                ("ct2", self.format_ts(base + 15000), 10.0),
            ]],
        )

        tdSql.execute("create table t_derivative_error(ts timestamp, c_bool bool, c_bin binary(8))")
        tdSql.execute(
            f"""insert into t_derivative_error values
            ({base + 1000}, true, 'a')
            ({base + 2000}, false, 'b')
            ({base + 3000}, true, 'c')
            """
        )
        tdSql.error("select derivative(c_bool, 1s, 0) from t_derivative_error interval(10s)")
        tdSql.error("select derivative(c_bin, 1s, 0) from t_derivative_error session(ts, 10s)")

    def _check_mavg_window(self):
        base = 1704067200000
        tdSql.execute("create table t_mavg(ts timestamp, state_val int, marker int, val int)")
        self.insert_rows("t_mavg", self._rate_rows(base), batch_size=10)

        tdSql.queryAndCheckResult(
            [
                "select * from (select _wstart as ws, mavg(val, 2) as mv from t_mavg interval(10s)) order by ws, mv",
                "select * from (select _wstart as ws, mavg(val, 2) as mv from t_mavg session(ts, 10s)) order by ws, mv",
                "select * from (select _wstart as ws, mavg(val, 2) as mv from t_mavg state_window(state_val)) order by ws, mv",
                "select * from (select _wstart as ws, mavg(val, 2) as mv from t_mavg event_window start with marker >= 3 end with marker >= 4) order by ws, mv",
                "select * from (select _wstart as ws, mavg(val, 2) as mv from t_mavg count_window(3)) order by ws, mv",
            ],
            [
                [
                    (self.format_ts(base), 15.0),
                    (self.format_ts(base), 25.0),
                    (self.format_ts(base + 20000), 45.0),
                    (self.format_ts(base + 20000), 55.0),
                ],
                [
                    (self.format_ts(base + 1000), 15.0),
                    (self.format_ts(base + 1000), 25.0),
                    (self.format_ts(base + 20000), 45.0),
                    (self.format_ts(base + 20000), 55.0),
                ],
                [
                    (self.format_ts(base + 1000), 15.0),
                    (self.format_ts(base + 20000), 45.0),
                ],
                [
                    (self.format_ts(base + 2000), 25.0),
                    (self.format_ts(base + 21000), 55.0),
                ],
                [
                    (self.format_ts(base + 1000), 15.0),
                    (self.format_ts(base + 1000), 25.0),
                    (self.format_ts(base + 20000), 45.0),
                    (self.format_ts(base + 20000), 55.0),
                ],
            ],
        )

    def _check_mavg_partition_and_size(self):
        base = 1704067200000
        self._create_partition_stable("st_mavg_part", base)
        tdSql.queryAndCheckResult(
            [
                "select * from (select tbname, _wstart as ws, mavg(val, 2) as mv from st_mavg_part partition by tbname session(ts, 10s)) order by tbname, ws, mv"
            ],
            [[
                ("ct1", self.format_ts(base + 1000), 15.0),
                ("ct1", self.format_ts(base + 1000), 25.0),
                ("ct1", self.format_ts(base + 20000), 45.0),
                ("ct1", self.format_ts(base + 20000), 55.0),
                ("ct2", self.format_ts(base + 1000), 10.0),
                ("ct2", self.format_ts(base + 15000), 30.0),
                ("ct2", self.format_ts(base + 15000), 40.0),
            ]],
        )

        tdSql.execute("create table t_mavg_size(ts timestamp, val int)")
        self.insert_rows(
            "t_mavg_size",
            [
                (base + 1000, 10),
                (base + 2000, 20),
                (base + 3000, 30),
                (base + 20000, 40),
                (base + 21000, 50),
                (base + 22000, 60),
            ],
            batch_size=10,
        )
        tdSql.queryAndCheckResult(
            [
                "select * from (select _wstart as ws, mavg(val, 3) as mv from t_mavg_size interval(10s)) order by ws, mv"
            ],
            [[
                (self.format_ts(base), 20.0),
                (self.format_ts(base + 20000), 50.0),
            ]],
        )

    def _check_statecount_window(self):
        base = 1704067200000
        tdSql.execute("create table t_statecount(ts timestamp, state_val int, marker int, val int)")
        self.insert_rows("t_statecount", self._rate_rows(base), batch_size=10)

        tdSql.queryAndCheckResult(
            [
                "select * from (select _wstart as ws, statecount(val, 'GE', 20) as sc from t_statecount interval(10s)) order by ws, sc",
                "select * from (select _wstart as ws, statecount(val, 'GE', 20) as sc from t_statecount session(ts, 10s)) order by ws, sc",
                "select * from (select _wstart as ws, statecount(val, 'GE', 20) as sc from t_statecount state_window(state_val)) order by ws, sc",
                "select * from (select _wstart as ws, statecount(val, 'GE', 20) as sc from t_statecount event_window start with marker >= 3 end with marker >= 4) order by ws, sc",
                "select * from (select _wstart as ws, statecount(val, 'GE', 20) as sc from t_statecount count_window(3)) order by ws, sc",
            ],
            [
                [
                    (self.format_ts(base), -1),
                    (self.format_ts(base), 1),
                    (self.format_ts(base), 2),
                    (self.format_ts(base + 20000), 1),
                    (self.format_ts(base + 20000), 2),
                    (self.format_ts(base + 20000), 3),
                ],
                [
                    (self.format_ts(base + 1000), -1),
                    (self.format_ts(base + 1000), 1),
                    (self.format_ts(base + 1000), 2),
                    (self.format_ts(base + 20000), 1),
                    (self.format_ts(base + 20000), 2),
                    (self.format_ts(base + 20000), 3),
                ],
                [
                    (self.format_ts(base + 1000), -1),
                    (self.format_ts(base + 1000), 1),
                    (self.format_ts(base + 3000), 1),
                    (self.format_ts(base + 20000), 1),
                    (self.format_ts(base + 20000), 2),
                    (self.format_ts(base + 22000), 1),
                ],
                [
                    (self.format_ts(base + 2000), 1),
                    (self.format_ts(base + 2000), 2),
                    (self.format_ts(base + 21000), 1),
                    (self.format_ts(base + 21000), 2),
                ],
                [
                    (self.format_ts(base + 1000), -1),
                    (self.format_ts(base + 1000), 1),
                    (self.format_ts(base + 1000), 2),
                    (self.format_ts(base + 20000), 1),
                    (self.format_ts(base + 20000), 2),
                    (self.format_ts(base + 20000), 3),
                ],
            ],
        )

    def _check_statecount_partition_and_operator(self):
        base = 1704067200000
        self._create_partition_stable("st_statecount_part", base)
        tdSql.queryAndCheckResult(
            [
                "select * from (select tbname, _wstart as ws, statecount(val, 'GE', 20) as sc from st_statecount_part partition by tbname session(ts, 10s)) order by tbname, ws, sc"
            ],
            [[
                ("ct1", self.format_ts(base + 1000), -1),
                ("ct1", self.format_ts(base + 1000), 1),
                ("ct1", self.format_ts(base + 1000), 2),
                ("ct1", self.format_ts(base + 20000), 1),
                ("ct1", self.format_ts(base + 20000), 2),
                ("ct1", self.format_ts(base + 20000), 3),
                ("ct2", self.format_ts(base + 1000), -1),
                ("ct2", self.format_ts(base + 1000), -1),
                ("ct2", self.format_ts(base + 15000), 1),
                ("ct2", self.format_ts(base + 15000), 2),
                ("ct2", self.format_ts(base + 15000), 3),
            ]],
        )

        tdSql.execute("create table t_statecount_op(ts timestamp, val int)")
        self.insert_rows(
            "t_statecount_op",
            [
                (base + 1000, 10),
                (base + 2000, 30),
                (base + 3000, 15),
                (base + 4000, 5),
            ],
            batch_size=10,
        )
        tdSql.query("select statecount(val, 'LT', 20) from t_statecount_op interval(10s)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, -1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 2)

    def _check_stateduration_window(self):
        base = 1704067200000
        tdSql.execute("create table t_stateduration(ts timestamp, state_val int, marker int, val int)")
        self.insert_rows("t_stateduration", self._rate_rows(base), batch_size=10)

        tdSql.queryAndCheckResult(
            [
                "select * from (select _wstart as ws, stateduration(val, 'GE', 20, 1s) as sd from t_stateduration interval(10s)) order by ws, sd",
                "select * from (select _wstart as ws, stateduration(val, 'GE', 20, 1s) as sd from t_stateduration session(ts, 10s)) order by ws, sd",
                "select * from (select _wstart as ws, stateduration(val, 'GE', 20, 1s) as sd from t_stateduration state_window(state_val)) order by ws, sd",
                "select * from (select _wstart as ws, stateduration(val, 'GE', 20, 1s) as sd from t_stateduration event_window start with marker >= 3 end with marker >= 4) order by ws, sd",
                "select * from (select _wstart as ws, stateduration(val, 'GE', 20, 1s) as sd from t_stateduration count_window(3)) order by ws, sd",
            ],
            [
                [
                    (self.format_ts(base), -1),
                    (self.format_ts(base), 0),
                    (self.format_ts(base), 1),
                    (self.format_ts(base + 20000), 0),
                    (self.format_ts(base + 20000), 1),
                    (self.format_ts(base + 20000), 2),
                ],
                [
                    (self.format_ts(base + 1000), -1),
                    (self.format_ts(base + 1000), 0),
                    (self.format_ts(base + 1000), 1),
                    (self.format_ts(base + 20000), 0),
                    (self.format_ts(base + 20000), 1),
                    (self.format_ts(base + 20000), 2),
                ],
                [
                    (self.format_ts(base + 1000), -1),
                    (self.format_ts(base + 1000), 0),
                    (self.format_ts(base + 3000), 0),
                    (self.format_ts(base + 20000), 0),
                    (self.format_ts(base + 20000), 1),
                    (self.format_ts(base + 22000), 0),
                ],
                [
                    (self.format_ts(base + 2000), 0),
                    (self.format_ts(base + 2000), 1),
                    (self.format_ts(base + 21000), 0),
                    (self.format_ts(base + 21000), 1),
                ],
                [
                    (self.format_ts(base + 1000), -1),
                    (self.format_ts(base + 1000), 0),
                    (self.format_ts(base + 1000), 1),
                    (self.format_ts(base + 20000), 0),
                    (self.format_ts(base + 20000), 1),
                    (self.format_ts(base + 20000), 2),
                ],
            ],
        )

    def _check_stateduration_partition_and_operator(self):
        base = 1704067200000
        self._create_partition_stable("st_stateduration_part", base)
        tdSql.queryAndCheckResult(
            [
                "select * from (select tbname, _wstart as ws, stateduration(val, 'GE', 20, 1s) as sd from st_stateduration_part partition by tbname session(ts, 10s)) order by tbname, ws, sd"
            ],
            [[
                ("ct1", self.format_ts(base + 1000), -1),
                ("ct1", self.format_ts(base + 1000), 0),
                ("ct1", self.format_ts(base + 1000), 1),
                ("ct1", self.format_ts(base + 20000), 0),
                ("ct1", self.format_ts(base + 20000), 1),
                ("ct1", self.format_ts(base + 20000), 2),
                ("ct2", self.format_ts(base + 1000), -1),
                ("ct2", self.format_ts(base + 1000), -1),
                ("ct2", self.format_ts(base + 15000), 0),
                ("ct2", self.format_ts(base + 15000), 1),
                ("ct2", self.format_ts(base + 15000), 2),
            ]],
        )

        tdSql.execute("create table t_stateduration_op(ts timestamp, val int)")
        self.insert_rows(
            "t_stateduration_op",
            [
                (base + 1000, 10),
                (base + 2000, 30),
                (base + 3000, 15),
                (base + 4000, 5),
            ],
            batch_size=10,
        )
        tdSql.query("select stateduration(val, 'LT', 20, 1s) from t_stateduration_op interval(10s)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, -1)
        tdSql.checkData(2, 0, 0)
        tdSql.checkData(3, 0, 1)

    def _check_twa_window(self):
        base = 1704067200000
        tdSql.execute("create table t_twa(ts timestamp, state_val int, marker int, val int)")
        self.insert_rows("t_twa", self._rate_rows(base), batch_size=10)

        tdSql.queryAndCheckResult(
            [
                "select * from (select _wstart as ws, twa(val) as tv from t_twa interval(10s)) order by ws",
                "select * from (select _wstart as ws, twa(val) as tv from t_twa session(ts, 10s)) order by ws",
                "select * from (select _wstart as ws, twa(val) as tv from t_twa state_window(state_val)) order by ws",
                "select * from (select _wstart as ws, twa(val) as tv from t_twa event_window start with marker >= 3 end with marker >= 4) order by ws",
                "select * from (select _wstart as ws, twa(val) as tv from t_twa count_window(3)) order by ws",
            ],
            [
                [
                    (self.format_ts(base), 29.3785584345973),
                    (self.format_ts(base + 20000), 50.0),
                ],
                [
                    (self.format_ts(base + 1000), 20.0),
                    (self.format_ts(base + 20000), 50.0),
                ],
                [
                    (self.format_ts(base + 1000), 15.0),
                    (self.format_ts(base + 3000), 30.0),
                    (self.format_ts(base + 20000), 45.0),
                    (self.format_ts(base + 22000), 60.0),
                ],
                [
                    (self.format_ts(base + 2000), 25.0),
                    (self.format_ts(base + 21000), 55.0),
                ],
                [
                    (self.format_ts(base + 1000), 20.0),
                    (self.format_ts(base + 20000), 50.0),
                ],
            ],
        )

    def _check_twa_partition(self):
        base = 1704067200000
        self._create_partition_stable("st_twa_part", base)
        tdSql.queryAndCheckResult(
            [
                "select * from (select tbname, _wstart as ws, twa(val) as tv from st_twa_part partition by tbname session(ts, 10s)) order by tbname, ws"
            ],
            [[
                ("ct1", self.format_ts(base + 1000), 20.0),
                ("ct1", self.format_ts(base + 20000), 50.0),
                ("ct2", self.format_ts(base + 1000), 10.0),
                ("ct2", self.format_ts(base + 15000), 35.0),
            ]],
        )

    def _check_irate_window(self):
        base = 1704067200000
        tdSql.execute("create table t_irate(ts timestamp, state_val int, marker int, val int)")
        self.insert_rows("t_irate", self._rate_rows(base), batch_size=10)

        tdSql.queryAndCheckResult(
            [
                "select * from (select _wstart as ws, irate(val) as iv from t_irate interval(10s)) order by ws",
                "select * from (select _wstart as ws, irate(val) as iv from t_irate session(ts, 10s)) order by ws",
                "select * from (select _wstart as ws, irate(val) as iv from t_irate state_window(state_val)) order by ws",
                "select * from (select _wstart as ws, irate(val) as iv from t_irate event_window start with marker >= 3 end with marker >= 4) order by ws",
                "select * from (select _wstart as ws, irate(val) as iv from t_irate count_window(3)) order by ws",
            ],
            [
                [
                    (self.format_ts(base), 10.0),
                    (self.format_ts(base + 20000), 10.0),
                ],
                [
                    (self.format_ts(base + 1000), 10.0),
                    (self.format_ts(base + 20000), 10.0),
                ],
                [
                    (self.format_ts(base + 1000), 10.0),
                    (self.format_ts(base + 3000), 0.0),
                    (self.format_ts(base + 20000), 10.0),
                    (self.format_ts(base + 22000), 0.0),
                ],
                [
                    (self.format_ts(base + 2000), 10.0),
                    (self.format_ts(base + 21000), 10.0),
                ],
                [
                    (self.format_ts(base + 1000), 10.0),
                    (self.format_ts(base + 20000), 10.0),
                ],
            ],
        )

    def _check_irate_partition_and_errors(self):
        base = 1704067200000
        self._create_partition_stable("st_irate_part", base)
        tdSql.queryAndCheckResult(
            [
                "select * from (select tbname, _wstart as ws, irate(val) as iv from st_irate_part partition by tbname session(ts, 10s)) order by tbname, ws"
            ],
            [[
                ("ct1", self.format_ts(base + 1000), 10.0),
                ("ct1", self.format_ts(base + 20000), 10.0),
                ("ct2", self.format_ts(base + 1000), 10.0),
                ("ct2", self.format_ts(base + 15000), 10.0),
            ]],
        )

        tdSql.execute("create table t_irate_error(ts timestamp, c_bool bool)")
        tdSql.execute(
            f"""insert into t_irate_error values
            ({base + 1000}, true)
            ({base + 2000}, false)
            ({base + 3000}, true)
            """
        )
        tdSql.error("select irate(c_bool) from t_irate_error interval(10s)")

    def _check_sample_window(self):
        base = 1704067200000
        tdSql.execute("create table t_sample(ts timestamp, state_val int, marker int, val int)")
        self.insert_rows("t_sample", self._rate_rows(base), batch_size=10)

        tdSql.queryAndCheckResult(
            [
                "select * from (select _wstart as ws, sample(val, 3) as sv from t_sample interval(10s)) order by ws, sv",
                "select * from (select _wstart as ws, sample(val, 3) as sv from t_sample session(ts, 10s)) order by ws, sv",
                "select * from (select _wstart as ws, sample(val, 2) as sv from t_sample state_window(state_val)) order by ws, sv",
                "select * from (select _wstart as ws, sample(val, 2) as sv from t_sample event_window start with marker >= 3 end with marker >= 4) order by ws, sv",
                "select * from (select _wstart as ws, sample(val, 3) as sv from t_sample count_window(3)) order by ws, sv",
            ],
            [
                [
                    (self.format_ts(base), 10),
                    (self.format_ts(base), 20),
                    (self.format_ts(base), 30),
                    (self.format_ts(base + 20000), 40),
                    (self.format_ts(base + 20000), 50),
                    (self.format_ts(base + 20000), 60),
                ],
                [
                    (self.format_ts(base + 1000), 10),
                    (self.format_ts(base + 1000), 20),
                    (self.format_ts(base + 1000), 30),
                    (self.format_ts(base + 20000), 40),
                    (self.format_ts(base + 20000), 50),
                    (self.format_ts(base + 20000), 60),
                ],
                [
                    (self.format_ts(base + 1000), 10),
                    (self.format_ts(base + 1000), 20),
                    (self.format_ts(base + 3000), 30),
                    (self.format_ts(base + 20000), 40),
                    (self.format_ts(base + 20000), 50),
                    (self.format_ts(base + 22000), 60),
                ],
                [
                    (self.format_ts(base + 2000), 20),
                    (self.format_ts(base + 2000), 30),
                    (self.format_ts(base + 21000), 50),
                    (self.format_ts(base + 21000), 60),
                ],
                [
                    (self.format_ts(base + 1000), 10),
                    (self.format_ts(base + 1000), 20),
                    (self.format_ts(base + 1000), 30),
                    (self.format_ts(base + 20000), 40),
                    (self.format_ts(base + 20000), 50),
                    (self.format_ts(base + 20000), 60),
                ],
            ],
        )

    def _check_sample_partition(self):
        base = 1704067200000
        self._create_partition_stable("st_sample_part", base)
        tdSql.queryAndCheckResult(
            [
                "select * from (select tbname, _wstart as ws, sample(val, 3) as sv from st_sample_part partition by tbname session(ts, 10s)) order by tbname, ws, sv"
            ],
            [[
                ("ct1", self.format_ts(base + 1000), 10),
                ("ct1", self.format_ts(base + 1000), 20),
                ("ct1", self.format_ts(base + 1000), 30),
                ("ct1", self.format_ts(base + 20000), 40),
                ("ct1", self.format_ts(base + 20000), 50),
                ("ct1", self.format_ts(base + 20000), 60),
                ("ct2", self.format_ts(base + 1000), 5),
                ("ct2", self.format_ts(base + 1000), 15),
                ("ct2", self.format_ts(base + 15000), 25),
                ("ct2", self.format_ts(base + 15000), 35),
                ("ct2", self.format_ts(base + 15000), 45),
            ]],
        )

        tdSql.error("select sample(val, 1) from t_sample interval(10s) fill(next)")

        tdSql.execute("create table t_sample_dup(ts timestamp, state_val int, marker int, val int)")
        tdSql.execute(
            f"""insert into t_sample_dup values
            ({base + 1000}, 1, 1, 10)
            ({base + 1000}, 1, 3, 20)
            ({base + 2000}, 2, 4, 30)
            """
        )
        tdSql.queryAndCheckResult(
            [
                "select * from (select _wstart as ws, sample(val, 2) as sv from t_sample_dup state_window(state_val)) order by ws, sv",
                "select * from (select _wstart as ws, sample(val, 2) as sv from t_sample_dup count_window(2)) order by ws, sv",
            ],
            [
                [
                    (self.format_ts(base + 1000), 20),
                    (self.format_ts(base + 2000), 30),
                ],
                [
                    (self.format_ts(base + 1000), 20),
                    (self.format_ts(base + 1000), 30),
                ],
            ],
        )

    def _check_interp_window_errors(self):
        base = 1704067200000
        tdSql.execute("create table t_interp(ts timestamp, state_val int, marker int, val int)")
        self.insert_rows("t_interp", self._rate_rows(base), batch_size=10)

        tdSql.error("select interp(val) from t_interp interval(10s)")
        tdSql.error("select interp(val) from t_interp session(ts, 10s)")
        tdSql.error("select interp(val) from t_interp state_window(state_val)")
        tdSql.error(
            "select interp(val) from t_interp event_window start with marker >= 3 end with marker >= 4"
        )
        tdSql.error("select interp(val) from t_interp count_window(2)")

    def _check_interp_partition_errors(self):
        base = 1704067200000
        self._create_partition_stable("st_interp_part", base)
        tdSql.error("select interp(val) from st_interp_part partition by tbname interval(10s)")
        tdSql.error("select interp(val) from st_interp_part partition by tbname session(ts, 10s)")
        tdSql.error("select interp(val) from st_interp_part partition by tbname count_window(2)")
        tdSql.error("select interp(val) from st_interp_part partition by tbname state_window(state_val)")
        tdSql.error("select interp(val) from t_interp interval(10s) fill(null)")

    def _check_mix_cross_block_compare(self):
        base = 1704067200000
        self.create_database("db_indef_block")

        tdSql.execute(
            "create table t_window_large(ts timestamp, state_val int, marker int, val int)"
        )
        self.insert_rows(
            "t_window_large",
            [
                (
                    base + idx,
                    1,
                    1 if idx == 0 else 2 if idx == 4097 else 0,
                    1,
                )
                for idx in range(4098)
            ]
            + [
                (base + 30000, 2, 1, 5),
                (base + 30001, 2, 2, 5),
            ],
        )

        count_start = 1704070800000
        tdSql.execute("create table t_count_large(ts timestamp, val int)")
        self.insert_rows(
            "t_count_large",
            [(count_start + idx * 1000, 1) for idx in range(4098)],
        )

        metric_start = 1704074400000
        tdSql.execute(
            "create table t_metric_large(ts timestamp, state_val int, marker int, val int)"
        )
        self.insert_rows(
            "t_metric_large",
            [
                (
                    metric_start + idx * 1000,
                    1 if idx < 4098 else 2,
                    1 if idx == 0 else 2 if idx == 4999 else 0,
                    idx + 1,
                )
                for idx in range(5000)
            ],
        )

        sql_file = os.path.join(
            os.path.dirname(__file__),
            "in",
            "test_fun_ts_mix_with_window_cross_block.in",
        )
        ans_file = os.path.join(
            os.path.dirname(__file__),
            "ans",
            "test_fun_ts_mix_with_window_cross_block.ans",
        )
        tdCom.compare_testcase_result(
            sql_file, ans_file, "test_fun_ts_mix_with_window_cross_block"
        )

    def _check_mix_groupid_cross_block_compare(self):
        base = 1704081600000
        self.create_database("db_indef_groupid_block", vgroups=4)

        tdSql.execute(
            "create stable st_group_large(ts timestamp, state_val int, marker int, val int) tags(groupid int, device int)"
        )

        row_count = 8192
        for groupid in range(1, 5):
            for device in range(2):
                table_name = f"g{groupid}_d{device}"
                tdSql.execute(
                    f"create table {table_name} using st_group_large tags({groupid}, {device})"
                )
                self.insert_rows(
                    table_name,
                    [
                        (
                            base + idx * 1000 + device * 400 + groupid * 50,
                            1 + ((idx // 1024) % 4),
                            1 if idx % 1500 == 0 else 2 if idx % 1500 == 1499 else 0,
                            groupid * 100000 + idx * 4 + device * 2 + 1,
                        )
                        for idx in range(row_count)
                    ],
                    batch_size=1000,
                )

        self._validate_mix_groupid_cross_block_query_results(base, row_count)

    def _validate_mix_groupid_cross_block_query_results(self, base: int, row_count: int):
        interval_ms = 300000
        threshold = 250000
        queries = [
            "select groupid as gid, cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, _wduration as wd, csum(val) as cv from st_group_large partition by groupid interval(5m) order by gid, ws, cv",
            "select groupid as gid, cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, _wduration as wd, diff(val) as dv from st_group_large partition by groupid interval(5m) order by gid, ws, dv",
            "select groupid as gid, cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, _wduration as wd, derivative(val, 1s, 0) as drv from st_group_large partition by groupid interval(5m) order by gid, ws, drv",
            "select groupid as gid, cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, _wduration as wd, mavg(val, 2) as mv from st_group_large partition by groupid interval(5m) order by gid, ws, mv",
            "select groupid as gid, cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, _wduration as wd, statecount(val, 'GE', 250000) as sc from st_group_large partition by groupid interval(5m) order by gid, ws, sc",
            "select groupid as gid, cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, _wduration as wd, stateduration(val, 'GE', 250000, 1s) as sd from st_group_large partition by groupid interval(5m) order by gid, ws, sd",
            "select tbname, groupid as gid, cast(_wstart as bigint) as ws, cast(_wend as bigint) as we, _wduration as wd, csum(val) as cv from st_group_large partition by tbname interval(5m) order by tbname, ws, cv",
        ]

        rows_by_group = {groupid: [] for groupid in range(1, 5)}
        rows_by_table = {}
        for groupid in range(1, 5):
            for device in range(2):
                table_name = f"g{groupid}_d{device}"
                table_rows = [
                    (
                        base + idx * 1000 + device * 400 + groupid * 50,
                        groupid * 100000 + idx * 4 + device * 2 + 1,
                    )
                    for idx in range(row_count)
                ]
                rows_by_group[groupid].extend(table_rows)
                rows_by_table[table_name] = table_rows

        for groupid in rows_by_group:
            rows_by_group[groupid].sort()

        def iter_interval_rows(sorted_rows):
            windows = {}
            for ts_ms, value in sorted_rows:
                ws = ts_ms - ts_ms % interval_ms
                windows.setdefault(ws, []).append((ts_ms, value))
            for ws in sorted(windows.keys()):
                yield ws, windows[ws]

        expected_rows = {query: [] for query in queries}

        for groupid in range(1, 5):
            for ws, entries in iter_interval_rows(rows_by_group[groupid]):
                we = ws + interval_ms - 1
                running_sum = 0
                state_counter = -1
                duration_start_ts = -1
                for _, value in entries:
                    running_sum += value
                    expected_rows[queries[0]].append(
                        (
                            groupid,
                            ws,
                            we,
                            interval_ms - 1,
                            running_sum,
                        )
                    )

                for idx in range(1, len(entries)):
                    curr_ts, curr_value = entries[idx]
                    prev_ts, prev_value = entries[idx - 1]
                    expected_rows[queries[1]].append(
                        (
                            groupid,
                            ws,
                            we,
                            interval_ms - 1,
                            curr_value - prev_value,
                        )
                    )
                    expected_rows[queries[2]].append(
                        (
                            groupid,
                            ws,
                            we,
                            interval_ms - 1,
                            (curr_value - prev_value) / ((curr_ts - prev_ts) / 1000.0),
                        )
                    )
                    expected_rows[queries[3]].append(
                        (
                            groupid,
                            ws,
                            we,
                            interval_ms - 1,
                            (curr_value + prev_value) / 2.0,
                        )
                    )

                for ts_ms, value in entries:
                    if value >= threshold:
                        state_counter = 1 if state_counter < 0 else state_counter + 1
                        duration_start_ts = (
                            ts_ms if duration_start_ts < 0 else duration_start_ts
                        )
                        state_duration = int((ts_ms - duration_start_ts) / 1000)
                    else:
                        state_counter = -1
                        state_duration = -1
                        duration_start_ts = -1
                    expected_rows[queries[4]].append(
                        (
                            groupid,
                            ws,
                            we,
                            interval_ms - 1,
                            state_counter,
                        )
                    )
                    expected_rows[queries[5]].append(
                        (
                            groupid,
                            ws,
                            we,
                            interval_ms - 1,
                            state_duration,
                        )
                    )

        for table_name in sorted(rows_by_table.keys()):
            groupid = int(table_name[1])
            for ws, entries in iter_interval_rows(rows_by_table[table_name]):
                we = ws + interval_ms - 1
                running_sum = 0
                for _, value in entries:
                    running_sum += value
                    expected_rows[queries[6]].append(
                        (
                            table_name,
                            groupid,
                            ws,
                            we,
                            interval_ms - 1,
                            running_sum,
                        )
                    )

        for query in queries:
            expected_rows[query].sort()
            actual_rows = tdSql.query(query, row_tag=True)
            expected_for_query = expected_rows[query]
            if len(actual_rows) != len(expected_for_query):
                raise AssertionError(
                    f"Row-count mismatch for query '{query}': "
                    f"actual_rows={len(actual_rows)}, expected_rows={len(expected_for_query)}"
                )

            mismatch_idx = -1
            for idx, (actual, expected) in enumerate(zip(actual_rows, expected_for_query)):
                if len(actual) != len(expected):
                    mismatch_idx = idx
                    break
                row_match = True
                for col_idx, (actual_val, expected_val) in enumerate(zip(actual, expected)):
                    if isinstance(expected_val, float):
                        if abs(actual_val - expected_val) > 1e-9:
                            row_match = False
                            break
                    elif actual_val != expected_val:
                        row_match = False
                        break
                if not row_match:
                    mismatch_idx = idx
                    break

            if mismatch_idx != -1:
                raise AssertionError(
                    f"Formula mismatch for query '{query}' at row {mismatch_idx}: "
                    f"actual={actual_rows[mismatch_idx]}, "
                    f"expected={expected_for_query[mismatch_idx]}"
                )

    def _check_mix_same_rowcount_compatible(self):
        base = 1704067200000
        self.create_database("db_ts_mix_same_rows")

        tdSql.execute(
            "create table t_mix_rows(ts timestamp, state_val int, marker int, val int)"
        )
        self.insert_rows("t_mix_rows", self._rate_rows(base), batch_size=10)

        tdSql.queryAndCheckResult(
            [
                "select * from (select _wstart as ws, twa(val) as tv, irate(val) as iv from t_mix_rows interval(10s)) order by ws",
                "select * from (select _wstart as ws, twa(val) as tv, irate(val) as iv from t_mix_rows session(ts, 10s)) order by ws",
                "select * from (select _wstart as ws, twa(val) as tv, irate(val) as iv from t_mix_rows state_window(state_val)) order by ws",
                "select * from (select _wstart as ws, twa(val) as tv, irate(val) as iv from t_mix_rows event_window start with marker >= 3 end with marker >= 4) order by ws",
                "select * from (select _wstart as ws, twa(val) as tv, irate(val) as iv from t_mix_rows count_window(3)) order by ws",
            ],
            [
                [
                    (self.format_ts(base), 29.3785584345973, 10.0),
                    (self.format_ts(base + 20000), 50.0, 10.0),
                ],
                [
                    (self.format_ts(base + 1000), 20.0, 10.0),
                    (self.format_ts(base + 20000), 50.0, 10.0),
                ],
                [
                    (self.format_ts(base + 1000), 15.0, 10.0),
                    (self.format_ts(base + 3000), 30.0, 0.0),
                    (self.format_ts(base + 20000), 45.0, 10.0),
                    (self.format_ts(base + 22000), 60.0, 0.0),
                ],
                [
                    (self.format_ts(base + 2000), 25.0, 10.0),
                    (self.format_ts(base + 21000), 55.0, 10.0),
                ],
                [
                    (self.format_ts(base + 1000), 20.0, 10.0),
                    (self.format_ts(base + 20000), 50.0, 10.0),
                ],
            ],
        )

    def _check_mix_lag_lead_and_outer_query(self):
        base = 1704067200000
        self.create_database("db_ts_mix_window")

        tdSql.execute("create table t_lag_lead(ts timestamp, state_val int, marker int, val int)")
        self.insert_rows(
            "t_lag_lead",
            [
                (base + 1000, 1, 1, 10),
                (base + 2000, 1, 3, 20),
                (base + 3000, 2, 4, 30),
                (base + 20000, 3, 1, 100),
                (base + 21000, 3, 3, 200),
                (base + 22000, 4, 4, 300),
            ],
            batch_size=10,
        )
        tdSql.queryAndCheckResult(
            [
                "select * from (select _wstart as ws, lag(val, 1, -1) as prev_v, lead(val, 1, -1) as next_v from t_lag_lead interval(10s)) order by ws, prev_v, next_v",
                "select * from (select _wstart as ws, lag(val, 1, -1) as prev_v, lead(val, 1, -1) as next_v from t_lag_lead session(ts, 10s)) order by ws, prev_v, next_v",
                "select * from (select _wstart as ws, lag(val, 1, -1) as prev_v, lead(val, 1, -1) as next_v from t_lag_lead state_window(state_val)) order by ws, prev_v, next_v",
                "select * from (select _wstart as ws, lag(val, 1, -1) as prev_v, lead(val, 1, -1) as next_v from t_lag_lead event_window start with marker >= 3 end with marker >= 4) order by ws, prev_v, next_v",
                "select * from (select _wstart as ws, lag(val, 1, -1) as prev_v, lead(val, 1, -1) as next_v from t_lag_lead count_window(3)) order by ws, prev_v, next_v",
            ],
            [
                [
                    (self.format_ts(base), -1, 20),
                    (self.format_ts(base), 10, 30),
                    (self.format_ts(base), 20, -1),
                    (self.format_ts(base + 20000), -1, 200),
                    (self.format_ts(base + 20000), 100, 300),
                    (self.format_ts(base + 20000), 200, -1),
                ],
                [
                    (self.format_ts(base + 1000), -1, 20),
                    (self.format_ts(base + 1000), 10, 30),
                    (self.format_ts(base + 1000), 20, -1),
                    (self.format_ts(base + 20000), -1, 200),
                    (self.format_ts(base + 20000), 100, 300),
                    (self.format_ts(base + 20000), 200, -1),
                ],
                [
                    (self.format_ts(base + 1000), -1, 20),
                    (self.format_ts(base + 1000), 10, -1),
                    (self.format_ts(base + 3000), -1, -1),
                    (self.format_ts(base + 20000), -1, 200),
                    (self.format_ts(base + 20000), 100, -1),
                    (self.format_ts(base + 22000), -1, -1),
                ],
                [
                    (self.format_ts(base + 2000), -1, 30),
                    (self.format_ts(base + 2000), 20, -1),
                    (self.format_ts(base + 21000), -1, 300),
                    (self.format_ts(base + 21000), 200, -1),
                ],
                [
                    (self.format_ts(base + 1000), -1, 20),
                    (self.format_ts(base + 1000), 10, 30),
                    (self.format_ts(base + 1000), 20, -1),
                    (self.format_ts(base + 20000), -1, 200),
                    (self.format_ts(base + 20000), 100, 300),
                    (self.format_ts(base + 20000), 200, -1),
                ],
            ],
        )

        tdSql.execute("create table t_sub(ts timestamp, state_val int, marker int, val int)")
        self.insert_rows(
            "t_sub",
            [
                (base + 1000, 1, 1, 5),
                (base + 2000, 1, 3, -3),
                (base + 3000, 2, 4, 4),
                (base + 20000, 3, 1, 1),
                (base + 21000, 3, 3, 2),
                (base + 22000, 4, 4, 3),
            ],
            batch_size=10,
        )
        tdSql.queryAndCheckResult(
            [
                "select ws, max(cv), min(cv) from (select _wstart as ws, csum(val) as cv from t_sub interval(10s)) t group by ws order by ws",
                "select ws, max(cv), min(cv) from (select _wstart as ws, csum(val) as cv from t_sub session(ts, 10s)) t group by ws order by ws",
                "select ws, max(cv), min(cv) from (select _wstart as ws, csum(val) as cv from t_sub state_window(state_val)) t group by ws order by ws",
                "select ws, max(cv), min(cv) from (select _wstart as ws, csum(val) as cv from t_sub event_window start with marker >= 3 end with marker >= 4) t group by ws order by ws",
                "select ws, max(cv), min(cv) from (select _wstart as ws, csum(val) as cv from t_sub count_window(3)) t group by ws order by ws",
            ],
            [
                [
                    (self.format_ts(base), 6, 2),
                    (self.format_ts(base + 20000), 6, 1),
                ],
                [
                    (self.format_ts(base + 1000), 6, 2),
                    (self.format_ts(base + 20000), 6, 1),
                ],
                [
                    (self.format_ts(base + 1000), 5, 2),
                    (self.format_ts(base + 3000), 4, 4),
                    (self.format_ts(base + 20000), 3, 1),
                    (self.format_ts(base + 22000), 3, 3),
                ],
                [
                    (self.format_ts(base + 2000), 1, -3),
                    (self.format_ts(base + 21000), 5, 2),
                ],
                [
                    (self.format_ts(base + 1000), 6, 2),
                    (self.format_ts(base + 20000), 6, 1),
                ],
            ],
        )

    def _check_mix_incompatible_and_illegal(self):
        base = 1704067200000
        tdSql.execute(
            "create table t_return_rows(ts timestamp, state_val int, marker int, v1 int, v2 int)"
        )
        self.insert_rows(
            "t_return_rows",
            [
                (base + 1000, 1, 1, 1, 10),
                (base + 2000, 1, 3, 2, 20),
                (base + 3000, 2, 4, 3, 30),
                (base + 20000, 3, 1, 4, 40),
                (base + 21000, 3, 3, 5, 50),
            ],
            batch_size=10,
        )
        tdSql.error("select csum(v1), lag(v2, 1, -1) from t_return_rows interval(10s)")
        tdSql.error("select csum(v1), lag(v2, 1, -1) from t_return_rows session(ts, 10s)")
        tdSql.error("select csum(v1), lag(v2, 1, -1) from t_return_rows state_window(state_val)")
        tdSql.error(
            "select csum(v1), lag(v2, 1, -1) from t_return_rows event_window start with marker >= 3 end with marker >= 4"
        )
        tdSql.error("select csum(v1), lag(v2, 1, -1) from t_return_rows count_window(2)")
        tdSql.error(
            "select csum(v1), statecount(v2, 'GE', 20) from t_return_rows interval(10s)"
        )
        tdSql.error(
            "select diff(v1), derivative(v2, 1s, 0) from t_return_rows session(ts, 10s)"
        )
        tdSql.error(
            "select statecount(v1, 'GE', 2), sample(v2, 2) from t_return_rows count_window(2)"
        )

        tdSql.execute(
            "create table t_illegal(ts timestamp, state_val int, marker int, val int)"
        )
        tdSql.execute(
            f"""insert into t_illegal values
            ({base + 1000}, 1, 1, 1)
            ({base + 2000}, 1, 3, 2)
            ({base + 3000}, 2, 4, 3)
            ({base + 20000}, 3, 1, 4)
            ({base + 21000}, 3, 3, 5)
            """
        )
        tdSql.error("select csum(val) from t_illegal group by val")
        tdSql.error("select lag(val, 1, -1) from t_illegal group by val")
        tdSql.error("select csum(val), count(*) from t_illegal interval(10s)")
        tdSql.error("select csum(val), count(*) from t_illegal session(ts, 10s)")
        tdSql.error("select csum(val), count(*) from t_illegal state_window(state_val)")
        tdSql.error(
            "select csum(val), count(*) from t_illegal event_window start with marker >= 3 end with marker >= 4"
        )
        tdSql.error("select csum(val), count(*) from t_illegal count_window(2)")
        tdSql.error("select lag(val, 1, -1), sum(val) from t_illegal interval(10s)")
        tdSql.error("select lag(val, 1, -1), sum(val) from t_illegal session(ts, 10s)")
        tdSql.error(
            "select lag(val, 1, -1), sum(val) from t_illegal state_window(state_val)"
        )
        tdSql.error(
            "select lag(val, 1, -1), sum(val) from t_illegal event_window start with marker >= 3 end with marker >= 4"
        )
        tdSql.error("select lag(val, 1, -1), sum(val) from t_illegal count_window(2)")
        tdSql.error("select csum(val) from t_illegal interval(10s) fill(null)")
        tdSql.error("select diff(val) from t_illegal interval(10s) fill(prev)")
        tdSql.error("select lead(val, 1, -1) from t_illegal interval(10s) fill(linear)")
        tdSql.error("select lag(val, 1, -1) from t_illegal interval(10s) fill(value, 0)")

    def run_csum_with_window_case(self):
        tdLog.info("verify csum ordinary-window coverage")
        self.create_database("db_ts_csum_window", vgroups=4)
        self._check_csum_basic_execution()
        self._check_csum_null_and_partition()
        self._check_csum_numeric_matrix()
        self._check_csum_interval_specific()
        self.drop_database("db_ts_csum_window")

    def run_diff_with_window_case(self):
        tdLog.info("verify diff ordinary-window coverage")
        self.create_database("db_ts_diff_window", vgroups=2)
        self._check_diff_basic_execution()
        self._check_diff_numeric_matrix()
        self._check_diff_partition()
        self.drop_database("db_ts_diff_window")

    def run_derivative_with_window_case(self):
        tdLog.info("verify derivative ordinary-window coverage")
        self.create_database("db_ts_derivative_window")
        self._check_derivative_window()
        self._check_derivative_partition_and_errors()
        self.drop_database("db_ts_derivative_window")

    def run_interp_with_window_case(self):
        tdLog.info("verify interp rejects ordinary-window queries")
        self.create_database("db_ts_interp_window")
        self._check_interp_window_errors()
        self._check_interp_partition_errors()
        self.drop_database("db_ts_interp_window")

    def run_irate_with_window_case(self):
        tdLog.info("verify irate ordinary-window coverage")
        self.create_database("db_ts_irate_window")
        self._check_irate_window()
        self._check_irate_partition_and_errors()
        self.drop_database("db_ts_irate_window")

    def run_mavg_with_window_case(self):
        tdLog.info("verify mavg ordinary-window coverage")
        self.create_database("db_ts_mavg_window")
        self._check_mavg_window()
        self._check_mavg_partition_and_size()
        self.drop_database("db_ts_mavg_window")

    def run_sample_with_window_case(self):
        tdLog.info("verify sample ordinary-window coverage")
        self.create_database("db_ts_sample_window")
        self._check_sample_window()
        self._check_sample_partition()
        self.drop_database("db_ts_sample_window")

    def run_statecount_with_window_case(self):
        tdLog.info("verify statecount ordinary-window coverage")
        self.create_database("db_ts_statecount_window")
        self._check_statecount_window()
        self._check_statecount_partition_and_operator()
        self.drop_database("db_ts_statecount_window")

    def run_stateduration_with_window_case(self):
        tdLog.info("verify stateduration ordinary-window coverage")
        self.create_database("db_ts_stateduration_window")
        self._check_stateduration_window()
        self._check_stateduration_partition_and_operator()
        self.drop_database("db_ts_stateduration_window")

    def run_twa_with_window_case(self):
        tdLog.info("verify twa ordinary-window coverage")
        self.create_database("db_ts_twa_window")
        self._check_twa_window()
        self._check_twa_partition()
        self.drop_database("db_ts_twa_window")

    def run_mix_with_window_case(self):
        tdLog.info("verify mixed indefinite-rows window coverage")
        self._check_mix_cross_block_compare()
        self._check_mix_groupid_cross_block_compare()
        self._check_mix_same_rowcount_compatible()
        self._check_mix_lag_lead_and_outer_query()
        self._check_mix_incompatible_and_illegal()
