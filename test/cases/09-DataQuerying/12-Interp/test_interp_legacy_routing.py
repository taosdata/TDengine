import os
import re
import time
from datetime import datetime

from new_test_framework.utils import ci_scaled_retry, tdLog, tdSql
from new_test_framework.utils.srvCtl import sc


class TestInterpLegacyRouting:
    updatecfgDict = {
        "asynclog": 0,
        "qDebugFlag": 135,
    }
    DB = "interp_legacy_routing"
    PRIMARY_RANGE = (
        "range('2024-01-01 00:00:01', "
        "'2024-01-01 00:00:05') every(1s) fill(linear)"
    )
    EVENT_RANGE = (
        "range('2024-06-01 00:00:01', "
        "'2024-06-01 00:00:05') every(1s) fill(linear)"
    )
    RUNTIME_RE = re.compile(
        r"TimeSlice runtime interpMode=(-?\d+) "
        r"scanKind=([a-z_]+) scanInterpMode=(-?\d+)"
    )

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def prepare(self):
        tdSql.execute(f"drop database if exists {self.DB}")
        tdSql.execute(f"create database {self.DB} vgroups 1")
        tdSql.execute(f"use {self.DB}")
        tdSql.execute(
            "create table primary_src (ts timestamp, val int)"
        )
        tdSql.execute(
            "insert into primary_src values "
            "('2024-01-01 00:00:01', 10) "
            "('2024-01-01 00:00:02', 20) "
            "('2024-01-01 00:00:03', 30) "
            "('2024-01-01 00:00:04', 40) "
            "('2024-01-01 00:00:05', 50)"
        )
        tdSql.execute(
            "create table composite_src "
            "(ts timestamp, pk int primary key, val int)"
        )
        tdSql.execute(
            "insert into composite_src values "
            "('2024-01-01 00:00:01', 1, 10) "
            "('2024-01-01 00:00:01', 2, 15) "
            "('2024-01-01 00:00:02', 1, 20) "
            "('2024-01-01 00:00:02', 2, 25) "
            "('2024-01-01 00:00:03', 1, 30)"
        )
        tdSql.execute(
            "create table event_src "
            "(ts timestamp, event_time timestamp, val int)"
        )
        tdSql.execute(
            "insert into event_src values "
            "('2024-01-01 00:00:01', "
            "'2024-06-01 00:00:01', 100) "
            "('2024-01-01 00:00:02', "
            "'2024-06-01 00:00:03', 300) "
            "('2024-01-01 00:00:03', "
            "'2024-06-01 00:00:05', 500)"
        )
        tdSql.execute(
            "create table join_src (ts timestamp, val int)"
        )
        tdSql.execute(
            "insert into join_src values "
            "('2024-01-01 00:00:01', 1) "
            "('2024-01-01 00:00:02', 2) "
            "('2024-01-01 00:00:03', 3) "
            "('2024-01-01 00:00:04', 4) "
            "('2024-01-01 00:00:05', 5)"
        )
        tdSql.execute(
            "create vtable virtual_src "
            f"(ts timestamp, val int from {self.DB}.primary_src.val)"
        )
        tdSql.execute(
            "create stable virtual_stb (ts timestamp, val int) "
            "tags (gid int) virtual 1"
        )
        tdSql.execute(
            "create vtable virtual_child "
            f"(val from {self.DB}.primary_src.val) "
            "using virtual_stb tags (1)"
        )

    def query_with_route(self, sql, expected_mode,
                         expected_route=None):
        log_file = sc.dnodeLogPath(1) + "/taosdlog.0"
        offset = os.path.getsize(log_file)
        tdSql.query(sql)
        rows = [tuple(row) for row in tdSql.queryResult]

        deadline = time.monotonic() + ci_scaled_retry(5)
        evidence = ""
        while time.monotonic() < deadline:
            with open(
                log_file, "r", encoding="utf-8", errors="replace"
            ) as log:
                log.seek(offset)
                evidence = log.read()
            routes = [
                (int(match.group(1)), match.group(2),
                 int(match.group(3)))
                for match in self.RUNTIME_RE.finditer(evidence)
            ]
            if routes and all(
                route[0] == expected_mode for route in routes
            ):
                if expected_route is None or expected_route in routes:
                    return rows
            time.sleep(0.1)

        raise AssertionError(
            "missing expected interp runtime route: "
            f"mode={expected_mode}, route={expected_route}, "
            f"observed={routes}\nlog tail:\n{evidence[-4000:]}"
        )

    def check_group_first_controls(self):
        direct_sql = (
            "select interp(val) from primary_src "
            f"{self.PRIMARY_RANGE}"
        )
        rows = self.query_with_route(
            direct_sql, 3, (3, "table", 3)
        )
        assert [row[0] for row in rows] == [10, 20, 30, 40, 50]

        pushdown_sql = (
            "select interp(val) from "
            "(select ts, val from primary_src where val >= 0) "
            f"{self.PRIMARY_RANGE}"
        )
        rows = self.query_with_route(
            pushdown_sql, 3, (3, "other", -1)
        )
        assert [row[0] for row in rows] == [10, 20, 30, 40, 50]

        composite_sql = (
            "select _irowts, interp(val) from composite_src "
            "range('2024-01-01 00:00:01', "
            "'2024-01-01 00:00:03') every(1s) fill(linear)"
        )
        rows = self.query_with_route(
            composite_sql, 3, (3, "table", 3)
        )
        assert rows == [
            (datetime(2024, 1, 1, 0, 0, 1), 10),
            (datetime(2024, 1, 1, 0, 0, 2), 20),
            (datetime(2024, 1, 1, 0, 0, 3), 30),
        ]

    def check_permanent_legacy_routes(self):
        non_primary_sql = (
            "select interp(val) from "
            "(select event_time, val from event_src) "
            f"{self.EVENT_RANGE}"
        )
        rows = self.query_with_route(non_primary_sql, 0)
        assert [row[0] for row in rows] == [100, 200, 300, 400, 500]

        ordered_sql = (
            "select interp(val) from "
            "(select * from primary_src order by ts desc) "
            f"{self.PRIMARY_RANGE}"
        )
        rows = self.query_with_route(ordered_sql, 0)
        assert [row[0] for row in rows] == [50]

        join_sql = (
            "select interp(val) from "
            "(select a.ts, a.val + b.val val from primary_src a "
            "join join_src b on a.ts = b.ts) "
            f"{self.PRIMARY_RANGE}"
        )
        rows = self.query_with_route(join_sql, 0)
        assert [row[0] for row in rows] == [11, 22, 33, 44, 55]

        virtual_sql = (
            "select interp(val) from virtual_src "
            f"{self.PRIMARY_RANGE}"
        )
        rows = self.query_with_route(virtual_sql, 0)
        assert [row[0] for row in rows] == [10, 20, 30, 40, 50]

        virtual_child_sql = (
            "select interp(val) from virtual_child "
            f"{self.PRIMARY_RANGE}"
        )
        rows = self.query_with_route(virtual_child_sql, 0)
        assert [row[0] for row in rows] == [10, 20, 30, 40, 50]

    def test_interp_legacy_routing(self):
        """Verify optimizer routing at the permanent LEGACY boundary.

        1. Direct scans, pushed projections, and composite primary keys use
           GROUP_FIRST.
        2. Non-primary timelines use LEGACY.
        3. Non-eliminated ordered subqueries and joins use LEGACY.
        4. Normal and child virtual tables use LEGACY.

        Catalog:
            - Query:Interp

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        History:
            - 2026-08-19 Tony Zhang added optimizer routing coverage
        """
        self.prepare()
        self.check_group_first_controls()
        self.check_permanent_legacy_routes()
