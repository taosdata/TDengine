from new_test_framework.utils import tdLog, tdSql

import uuid
from datetime import datetime, timedelta


class TestCorrelatedScalarTimeRange:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def check_result(self, sql, expected):
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(len(expected))
        for row, values in enumerate(expected):
            for col, value in enumerate(values):
                tdSql.checkData(row, col, value)

    def do_time_bounds(self):
        self.check_result(
            "select r.v, (select avg(h.v) from history h "
            "where h.device = r.device and h.ts > r.ts - 10m "
            "and h.ts <= r.ts) from readings r order by r.ts",
            [(10, 20), (20, 25), (50, 60)],
        )
        self.check_result(
            "select r.v, (select h.v from history h "
            "where h.device = r.device and h.ts <= r.ts "
            "order by h.ts desc limit 1) from readings r order by r.ts",
            [(10, 20), (20, 30), (50, 60)],
        )
        print("per-row time bounds ...................... [ passed ]")

    def do_mixed_predicate(self):
        self.check_result(
            "select r.v, (select count(*) from history h "
            "where h.ts > r.ts - 10m and h.ts <= r.ts "
            "and (h.device = r.device or h.v = 999)) "
            "from readings r order by r.ts",
            [(10, 1), (20, 3), (50, 2)],
        )
        print("time range with correlated OR ........... [ passed ]")

    def do_query_pseudocolumns(self):
        bounds = (
            "h.device = r.device and h.ts > r.ts - 10m and h.ts <= r.ts"
        )
        for pseudo in ("_qstart", "_qend", "_qduration"):
            tdSql.query(
                f"select (select {pseudo} from history h where {bounds} limit 1) "
                "from readings r order by r.ts",
                queryTimes=1,
            )
            tdSql.checkRows(3)
            actual = [row[0] for row in tdSql.queryResult]
            for index, (ts, device) in enumerate(
                (
                    ("2026-09-11 00:00:10", "d1"),
                    ("2026-09-11 00:00:20", "d1"),
                    ("2026-09-11 00:01:10", "d2"),
                )
            ):
                lower = (datetime.strptime(ts, "%Y-%m-%d %H:%M:%S") - timedelta(minutes=10)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                tdSql.query(
                    f"select {pseudo} from history h where h.device = '{device}' "
                    f"and h.ts > '{lower}' and h.ts <= '{ts}' limit 1",
                    queryTimes=1,
                )
                tdSql.checkRows(1)
                assert actual[index] == tdSql.queryResult[0][0], (pseudo, index, actual)
        print("per-row query pseudocolumns ............. [ passed ]")

    def do_auto_interval_and_fill(self):
        for projection, suffix in (
            ("_wstart", "interval(1m, auto) limit 1"),
            ("count(*)", "interval(1m, auto) fill(value, -1) limit 1"),
        ):
            tdSql.query(
                f"select (select {projection} from history h where h.device = r.device "
                f"and h.ts > r.ts - 10m and h.ts <= r.ts {suffix}) "
                "from readings r order by r.ts",
                queryTimes=1,
            )
            tdSql.checkRows(3)
            actual = [row[0] for row in tdSql.queryResult]
            for index, (ts, device) in enumerate(
                (
                    ("2026-09-11 00:00:10", "d1"),
                    ("2026-09-11 00:00:20", "d1"),
                    ("2026-09-11 00:01:10", "d2"),
                )
            ):
                lower = (datetime.strptime(ts, "%Y-%m-%d %H:%M:%S") - timedelta(minutes=10)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                tdSql.query(
                    f"select {projection} from history h where h.device = '{device}' "
                    f"and h.ts > '{lower}' and h.ts <= '{ts}' {suffix}",
                    queryTimes=1,
                )
                tdSql.checkRows(1)
                assert actual[index] == tdSql.queryResult[0][0], (suffix, index, actual)
        print("per-row AUTO interval and FILL .......... [ passed ]")

    def do_invalid_fill_range(self):
        tdSql.execute(
            "create table fill_ranges "
            "(ts timestamp, start_time timestamp, device varchar(16))"
        )
        tdSql.execute(
            "insert into fill_ranges values "
            "('2026-09-11 00:00:20', '2026-09-11 00:00:19', 'd1') "
            "('2026-09-11 00:01:10', '2025-09-11 00:01:10', 'd2')"
        )
        inner = (
            "select count(*) from history h where h.device = r.device "
            "and h.ts >= r.start_time and h.ts <= r.ts "
            "interval(1a) fill(value, -1) limit 1"
        )
        tdSql.query(
            f"select ({inner}) from fill_ranges r where r.device = 'd1'",
            queryTimes=1,
        )
        tdSql.checkRows(1)
        actual = tdSql.queryResult[0][0]
        tdSql.query(
            "select count(*) from history h where h.device = 'd1' "
            "and h.ts >= '2026-09-11 00:00:19' "
            "and h.ts <= '2026-09-11 00:00:20' "
            "interval(1a) fill(value, -1) limit 1",
            queryTimes=1,
        )
        tdSql.checkRows(1)
        assert actual == tdSql.queryResult[0][0]
        tdSql.error(
            f"select ({inner}) from fill_ranges r where r.device = 'd2'",
            expectedErrno=0x263B,
        )
        tdSql.error(
            "select count(*) from history h where h.device = 'd2' "
            "and h.ts >= '2025-09-11 00:01:10' "
            "and h.ts <= '2026-09-11 00:01:10' "
            "interval(1a) fill(value, -1) limit 1",
            expectedErrno=0x263B,
        )
        print("valid and invalid FILL ranges ........... [ passed ]")

    def do_tag_and_tbname_predicates(self):
        tdSql.execute("create stable history_st (ts timestamp, v int) tags (device varchar(16))")
        tdSql.execute("create table hist_d1 using history_st tags ('d1')")
        tdSql.execute("create table hist_d2 using history_st tags ('d2')")
        tdSql.execute(
            "insert into hist_d1 values "
            "('2026-09-11 00:00:10', 20) ('2026-09-11 00:00:20', 30)"
        )
        tdSql.execute("insert into hist_d2 values ('2026-09-11 00:01:10', 60)")
        self.check_result(
            "select r.v, (select count(*) from history_st h "
            "where h.ts > r.ts - 10m and h.ts <= r.ts "
            "and (h.device = r.device or h.v = 999)) "
            "from readings r order by r.ts",
            [(10, 1), (20, 2), (50, 1)],
        )
        tdSql.execute("create table routing (ts timestamp, target varchar(16))")
        tdSql.execute(
            "insert into routing values "
            "('2026-09-11 00:00:10', 'hist_d1') "
            "('2026-09-11 00:00:20', 'hist_d1') "
            "('2026-09-11 00:01:10', 'hist_d2')"
        )
        self.check_result(
            "select (select count(*) from history_st h "
            "where h.ts > r.ts - 10m and h.ts <= r.ts "
            "and (tbname = r.target or h.v = 999)) "
            "from routing r order by r.ts",
            [(1,), (2,), (1,)],
        )
        print("time, tag and tbname predicates .......... [ passed ]")

    def do_ranges(self):
        database = f"correlated_scalar_e3_{uuid.uuid4().hex[:8]}"
        tdSql.execute(f"create database {database} precision 'ms'")
        try:
            tdSql.execute(f"use {database}")
            tdSql.execute("create table readings (ts timestamp, device varchar(16), v int)")
            tdSql.execute("create table history (ts timestamp, device varchar(16), v int)")
            tdSql.execute(
                "insert into readings values "
                "('2026-09-11 00:00:10', 'd1', 10) "
                "('2026-09-11 00:00:20', 'd1', 20) "
                "('2026-09-11 00:01:10', 'd2', 50)"
            )
            tdSql.execute(
                "insert into history values "
                "('2026-09-11 00:00:10', 'd1', 20) "
                "('2026-09-11 00:00:15', 'other', 999) "
                "('2026-09-11 00:00:20', 'd1', 30) "
                "('2026-09-11 00:01:10', 'd2', 60)"
            )
            self.do_time_bounds()
            self.do_mixed_predicate()
            self.do_query_pseudocolumns()
            self.do_auto_interval_and_fill()
            self.do_invalid_fill_range()
            self.do_tag_and_tbname_predicates()
        finally:
            tdSql.execute(f"drop database {database}")

    def do_timestamp_precisions(self):
        for precision, first, second in (
            ("ms", ".123", ".124"),
            ("us", ".123456", ".123457"),
            ("ns", ".123456789", ".123456790"),
        ):
            database = f"correlated_scalar_{precision}_{uuid.uuid4().hex[:8]}"
            tdSql.execute(f"create database {database} precision '{precision}'")
            try:
                tdSql.execute(f"use {database}")
                tdSql.execute("create table readings (ts timestamp, device varchar(16), v int)")
                tdSql.execute("create table history (ts timestamp, device varchar(16), v int)")
                tdSql.execute(
                    "insert into readings values "
                    f"('2026-09-11 00:00:00{first}', 'd1', 1) "
                    f"('2026-09-11 00:00:00{second}', 'd2', 2)"
                )
                tdSql.execute(
                    "insert into history values "
                    f"('2026-09-11 00:00:00{first}', 'd1', 7) "
                    f"('2026-09-11 00:00:00{second}', 'd2', 8)"
                )
                self.check_result(
                    "select r.v, (select h.v from history h "
                    "where h.ts = r.ts and h.device = r.device) "
                    "from readings r order by r.ts",
                    [(1, 7), (2, 8)],
                )
            finally:
                tdSql.execute(f"drop database {database}")
        print("timestamp ms/us/ns parameters ........... [ passed ]")

    def test_correlated_time_range(self):
        """Time bounds and mixed predicates vary with the outer row

        1. Changing outer timestamps alter aggregate and latest row.
        2. Time, tag and tbname AND/OR branches remain in the query.
        3. Range pseudocolumns, AUTO offset and FILL match literal controls,
           including an invalid range after per-row parameter binding.

        Catalog:
            - Query:SubQuery

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-09-17 Codex Add dependent time range checks.

        """
        self.do_ranges()
        self.do_timestamp_precisions()
