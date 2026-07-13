###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import os
import tempfile

from new_test_framework.utils import tdLog, tdSql, tdCom, etool


class TestFunTsNoTimelineRowOrder:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # --- util ---
    def prepare(self):
        tdSql.execute("DROP DATABASE IF EXISTS test_no_timeline_row_order")
        tdSql.execute("CREATE DATABASE test_no_timeline_row_order")
        tdSql.execute("USE test_no_timeline_row_order")
        tdSql.execute("CREATE TABLE t_base (ts TIMESTAMP, seq INT, val INT, nullable_val INT, status NCHAR(16))")
        tdSql.execute(
            "INSERT INTO t_base VALUES "
            "('2024-01-01 00:00:01', 1, 10, 10, 'open') "
            "('2024-01-01 00:00:02', 2, 15, NULL, 'closed') "
            "('2024-01-01 00:00:03', 3, 12, 12, 'open') "
            "('2024-01-01 00:00:04', 4, 20, NULL, 'idle') "
            "('2024-01-01 00:00:05', 5, 25, 25, 'closed')"
        )
        tdSql.execute(
            "CREATE TABLE t_unique "
            "(ts TIMESTAMP, seq INT, val INT, bucket INT, aux INT, marker NCHAR(16), "
            "label NCHAR(16), nullable_label NCHAR(16))"
        )
        tdSql.execute(
            "INSERT INTO t_unique VALUES "
            "('2024-01-01 00:00:06', 1, 10, 1, 101, 'a-first', 'alpha', NULL) "
            "('2024-01-01 00:00:05', 2, 20, 1, 202, 'b-first', 'beta', 'beta') "
            "('2024-01-01 00:00:04', 3, 10, 9, 909, 'a-later', 'alpha', 'alpha-later') "
            "('2024-01-01 00:00:03', 4, 10, 2, 404, 'g-first', 'gamma', NULL) "
            "('2024-01-01 00:00:02', 5, 20, 8, 808, 'b-later', 'beta', 'beta-later') "
            "('2024-01-01 00:00:01', 6, 30, 3, 606, 'n-first', NULL, NULL)"
        )

    def check_single_col(self, sql, expected):
        tdSql.query(sql)
        tdSql.checkRows(len(expected))
        for row, value in enumerate(expected):
            tdSql.checkData(row, 0, value)

    def check_rows(self, sql, expected):
        tdSql.query(sql)
        tdSql.checkRows(len(expected))
        for row, values in enumerate(expected):
            for col, value in enumerate(values):
                tdSql.checkData(row, col, value)

    def write_temp_csv(self, rows):
        fd, path = tempfile.mkstemp(suffix=".csv", prefix="no_timeline_row_order_")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(",".join(str(cell) for cell in row))
                f.write("\n")
        return path

    # --- impl ---
    def do_row_order_functions(self):
        source = "(SELECT seq, val, nullable_val, status FROM t_base ORDER BY seq)"
        self.check_single_col(f"SELECT diff(val) FROM {source}", [5, -3, 8, 5])
        self.check_single_col(f"SELECT csum(val) FROM {source}", [10, 25, 37, 57, 82])
        self.check_single_col(f"SELECT mavg(val, 2) FROM {source}", [12.5, 13.5, 16.0, 22.5])
        self.check_single_col(f"SELECT statecount(val, 'GT', 12) FROM {source}", [-1, 1, -1, 1, 2])
        self.check_single_col(f"SELECT lag(val, 1) FROM {source}", [None, 10, 15, 12, 20])
        self.check_single_col(f"SELECT lead(val, 1) FROM {source}", [15, 12, 20, 25, None])
        self.check_single_col(f"SELECT fill_forward(nullable_val) FROM {source}", [10, 10, 12, 12, 25])
        self.check_single_col(
            f"SELECT v FROM (SELECT unique(status) AS v FROM {source}) ORDER BY v",
            ["closed", "idle", "open"],
        )

        print("row order functions without timestamp ......................... [ passed ]")

    def do_unique_no_timeline(self):
        source = "(SELECT seq, val, bucket, aux, marker, label, nullable_label FROM t_unique ORDER BY seq)"
        self.check_single_col(
            f"SELECT v FROM (SELECT unique(label) AS v FROM {source}) ORDER BY v NULLS FIRST",
            [None, "alpha", "beta", "gamma"],
        )
        self.check_rows(
            f"SELECT v, seq, bucket, aux, marker FROM "
            f"(SELECT unique(label) AS v, seq, bucket, aux, marker FROM {source}) "
            "ORDER BY v NULLS FIRST",
            [
                (None, 6, 3, 606, "n-first"),
                ("alpha", 1, 1, 101, "a-first"),
                ("beta", 2, 1, 202, "b-first"),
                ("gamma", 4, 2, 404, "g-first"),
            ],
        )
        self.check_single_col(
            f"SELECT v FROM (SELECT unique(nullable_label) AS v FROM {source}) ORDER BY v NULLS FIRST",
            [None, "alpha-later", "beta", "beta-later"],
        )
        self.check_rows(
            f"SELECT v, seq FROM (SELECT unique(label) AS v, seq FROM {source}) WHERE v = 'alpha'",
            [("alpha", 1)],
        )
        tdSql.query(f"SELECT unique(label) FROM (SELECT label FROM t_unique)")
        tdSql.checkRows(4)
        self.check_single_col(
            "SELECT v FROM (SELECT unique(label) AS v FROM "
            "(SELECT label FROM t_unique ORDER BY seq LIMIT 4)) ORDER BY v",
            ["alpha", "beta", "gamma"],
        )
        tdSql.error("SELECT unique(label) FROM (SELECT label FROM t_unique) INTERVAL(1s)")
        self.check_single_col(
            f"SELECT v FROM (SELECT unique(val + bucket) AS v FROM {source}) ORDER BY v",
            [11, 12, 19, 21, 28, 33],
        )
        self.check_rows(
            "SELECT v, seq, bucket, aux, marker FROM "
            "(SELECT unique(label) AS v, seq, bucket, aux, marker FROM "
            "(SELECT ts, seq, bucket, aux, marker, label FROM t_unique ORDER BY seq)) "
            "WHERE v = 'alpha'",
            [("alpha", 3, 9, 909, "a-later")],
        )
        self.check_rows(
            f"SELECT v, seq, bucket, aux, marker FROM "
            f"(SELECT unique(label) AS v, seq, bucket, aux, marker FROM {source}) "
            "WHERE v = 'beta'",
            [("beta", 2, 1, 202, "b-first")],
        )
        self.check_rows(
            f"SELECT v, seq, nullable_label, marker FROM "
            f"(SELECT unique(val) AS v, seq, nullable_label, marker FROM {source}) "
            "WHERE v = 10",
            [(10, 1, None, "a-first")],
        )

        print("unique without timestamp projection ........................... [ passed ]")

    def do_unique_no_timeline_ans(self):
        case_dir = os.path.dirname(os.path.abspath(__file__))
        input_file = os.path.join(case_dir, "in", "test_unique_no_timeline_row_order.in")
        expected_file = os.path.join(case_dir, "ans", "test_unique_no_timeline_row_order.ans")
        tdCom.compare_testcase_result(
            input_file, expected_file, "test_unique_no_timeline_row_order"
        )

        print("unique without timestamp projection ans ....................... [ passed ]")

    def do_order_by_controls_row_order(self):
        source = "(SELECT seq, val FROM t_base ORDER BY seq DESC)"
        self.check_single_col(f"SELECT diff(val) FROM {source}", [-5, -8, 3, -5])
        self.check_single_col(f"SELECT lag(val, 2) FROM {source}", [None, None, 25, 20, 12])

        print("non timestamp order by controls row order ..................... [ passed ]")

    def do_set_operator_input(self):
        source = "(SELECT 1 ord, 10 val UNION ALL SELECT 2, 20 UNION ALL SELECT 3, 15 ORDER BY ord)"
        self.check_single_col(f"SELECT diff(val) FROM {source}", [10, -5])
        self.check_single_col(f"SELECT csum(val) FROM {source}", [10, 30, 45])
        self.check_single_col(f"SELECT lag(val, 1) FROM {source}", [None, 10, 20])

        print("union all input without timestamp ............................. [ passed ]")

    def do_inline_source_inputs(self):
        text_source = (
            "(SELECT val FROM TEXT(ts TIMESTAMP, val INT) "
            "VALUES ('2024-01-01 00:00:01', 10) "
            "('2024-01-01 00:00:02', 20) "
            "('2024-01-01 00:00:03', 30) t ORDER BY val)"
        )
        self.check_single_col(f"SELECT csum(val) FROM {text_source}", [10, 30, 60])

        csv = self.write_temp_csv(
            [
                ("2024-01-01 00:00:01", 10),
                ("2024-01-01 00:00:02", 20),
                ("2024-01-01 00:00:03", 15),
            ]
        )
        try:
            file_source = (
                f"(SELECT val FROM FILE('{csv}', 'ts TIMESTAMP, val INT') f "
                "ORDER BY val)"
            )
            self.check_single_col(f"SELECT lag(val, 1) FROM {file_source}", [None, 10, 15])
        finally:
            os.remove(csv)

        print("text and file inputs without timestamp projection ............. [ passed ]")

    def do_nested_subquery_input(self):
        source = (
            "(SELECT val FROM "
            "(SELECT seq, val FROM "
            "(SELECT seq, val FROM t_base ORDER BY seq) s1 "
            "WHERE val >= 12 ORDER BY seq) s2)"
        )
        self.check_single_col(f"SELECT csum(val) FROM {source}", [15, 27, 47, 72])
        self.check_single_col(f"SELECT diff(val) FROM {source}", [-3, 8, 5])

        print("nested subquery input without timestamp ....................... [ passed ]")

    def do_timeline_functions_still_rejected(self):
        source = "(SELECT seq, val FROM t_base ORDER BY seq)"
        tdSql.error(f"SELECT tail(val, 2) FROM {source}")
        tdSql.error(f"SELECT derivative(val, 1s, 0) FROM {source}")
        tdSql.error(f"SELECT irate(val) FROM {source}")
        tdSql.error(f"SELECT stateduration(val, 'GT', 12, 1s) FROM {source}")
        tdSql.error(f"SELECT twa(val) FROM {source}")
        tdSql.error(f"SELECT elapsed(val) FROM {source}")
        tdSql.error(f"SELECT interp(val) FROM {source} RANGE('2024-01-01 00:00:00', '2024-01-01 00:00:10') EVERY(1s) FILL(NULL)")
        tdSql.error(f"SELECT diff(val) FROM {source} INTERVAL(1s)")
        tdSql.error(f"SELECT count(*) FROM {source} INTERVAL(1s)")
        tdSql.error(f"SELECT count(*) FROM {source} SESSION(seq, 1s)")
        tdSql.error(f"SELECT count(*) FROM {source} STATE_WINDOW(val)")
        tdSql.error(f"SELECT count(*) FROM {source} EVENT_WINDOW START WITH val >= 15 END WITH val >= 20")
        tdSql.error(f"SELECT count(*) FROM {source} COUNT_WINDOW(2)")

        print("time value functions still require timeline ................... [ passed ]")

    def do_projected_timestamp_input_order(self):
        source = "(SELECT ts, seq, val FROM t_base ORDER BY seq DESC)"
        self.check_single_col(f"SELECT diff(val) FROM {source}", [-5, -8, 3, -5])

        print("projected timestamp input order ............................... [ passed ]")

    # --- main ---
    def test_no_timeline_row_order(self):
        """No timeline row-order functions execute by upstream input order.

        1. Covers row-order functions when the input projection has no TIMESTAMP column.
        2. Verifies non-timestamp ORDER BY controls the function input order.
        3. Covers UNION ALL, TEXT, FILE, and nested-subquery rowset inputs.
        4. Covers no-timeline unique representative-row semantics.
        5. Verifies time-value functions and window clauses are still rejected.

        Catalog:
            - Functions/TimeSeries

        Since: v3.4.3.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-07-02 Ren Xinsheng Add no-timeline row-order function coverage

        """
        self.prepare()
        self.do_row_order_functions()
        self.do_order_by_controls_row_order()
        self.do_unique_no_timeline()
        self.do_unique_no_timeline_ans()
        self.do_set_operator_input()
        self.do_inline_source_inputs()
        self.do_nested_subquery_input()
        self.do_timeline_functions_still_rejected()
        self.do_projected_timestamp_input_order()
