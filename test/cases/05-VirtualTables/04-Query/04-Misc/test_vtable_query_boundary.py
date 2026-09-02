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
from collections import Counter

from new_test_framework.utils import tdLog, tdSql
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from vtable_util import VtableQueryUtil


class TestVTableQueryBoundary:
    @staticmethod
    def _prepare_data():
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_same_db_vtables()

    def setup_class(cls):
        cls._prepare_data()

    def init(self, conn=None, logSql=None, replicaVar=1):
        tdLog.debug("start to execute %s" % __file__)
        self._prepare_data()

    def _plan_contains(self, sql: str, keyword: str) -> bool:
        tdSql.query(f"explain verbose true {sql}")
        for row in tdSql.queryResult:
            for col in row:
                if col is not None and keyword in str(col):
                    return True
        return False

    def _fetch_rows(self, sql: str):
        tdSql.query(sql)
        return [tuple(tdSql.getData(i, j) for j in range(tdSql.queryCols)) for i in range(tdSql.queryRows)]

    @staticmethod
    def _to_int_or_none(value):
        return None if value is None else int(value)

    def test_state_window_pseudocolumns_respect_referenced_filter(self):
        """State window pseudo columns respect referenced-column filters.

        A normal virtual table state-window query that only selects pseudo
        columns used to build an invalid empty merge path and could also lose
        the referenced-column filter during virtual-scan optimization.

        Since: v3.4.0.0

        Catalog: query/window

        Labels: boundary_case,state_window,virtual_table,window_optimize,integration,functional
        History:
            - 2026-05-29 Codex Added regression case

        """
        tdSql.execute("drop database if exists test_vtable_state_window_pseudo")
        tdSql.execute("create database test_vtable_state_window_pseudo")
        tdSql.execute("use test_vtable_state_window_pseudo")

        tdSql.execute("create table origin_t(ts timestamp, status int)")
        tdSql.execute(
            "insert into origin_t values "
            "('2026-05-29 00:00:00.000', 1) "
            "('2026-05-29 00:00:01.000', 1) "
            "('2026-05-29 00:00:02.000', 2) "
            "('2026-05-29 00:00:03.000', 2) "
            "('2026-05-29 00:00:04.000', 1)"
        )
        tdSql.execute("create vtable vt(ts timestamp, status int from origin_t.status)")

        tdSql.query("select _wstart, _wend from vt where status = 2 state_window(status)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2026-05-29 00:00:02.000")
        tdSql.checkData(0, 1, "2026-05-29 00:00:03.000")

        tdSql.query("select _wstart, _wduration, _wend from vt where status = 2 state_window(status)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1000)

        tdSql.query("select _wend from vt where status = 2 state_window(status)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2026-05-29 00:00:03.000")

        tdSql.query("select _wstart, _wduration, _wend from vt where status = 2 state_window(status, 1)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2026-05-29 00:00:02.000")
        tdSql.checkData(0, 1, 1000)
        tdSql.checkData(0, 2, "2026-05-29 00:00:03.000")

        tdSql.query("select _wstart, _wduration, _wend from vt where status = 2 state_window(status, 2)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "2026-05-29 00:00:02.000")
        tdSql.checkData(0, 1, 1000)
        tdSql.checkData(0, 2, "2026-05-29 00:00:03.000")

    def test_event_window_no_window_not_out_of_range(self):
        """Event window empty result should not crash.

        Validate the optimized virtual-table event window path when start/end
        predicates never form any window. The query should return zero rows
        and must not raise out-of-range executor errors.

        Since: v3.3.8.0

        Catalog: query/window

        Labels: boundary_case,event_window,virtual_table,window_optimize,integration,functional
        History:
            - 2026-03-05 Jing Sima Added boundary regression case
            - 2026-03-05 Codex Rewrote metadata to align with guideline

        """
        sql = (
            "select _wstart, _wend, first(u_tinyint_col), last(u_tinyint_col), count(u_tinyint_col) "
            "from test_vtable_select.vtb_virtual_stb "
            "event_window start with u_tinyint_col > 200 and u_tinyint_col < 100 end with u_tinyint_col > 200 "
        )
        keyword = "Dynamic Query Control for Virtual Table Window"
        if not self._plan_contains(sql, keyword):
            tdLog.exit(f"expected optimized event-window path, but '{keyword}' not found in explain plan")

        tdSql.query(sql)
        tdSql.checkRows(0)

    def test_event_window_last_wend_not_plus_one(self):
        """Optimized event window keeps final _wend consistent.

        Compare baseline and optimized event-window query paths for the same
        window definition, and verify the last window's `_wstart` and `_wend`
        remain identical between both paths.

        Since: v3.3.8.0

        Catalog: query/window

        Labels: boundary_case,event_window,virtual_table,window_optimize,integration,functional
        History:
            - 2026-03-05 Jing Sima Added boundary regression case
            - 2026-03-05 Codex Rewrote metadata to align with guideline

        """
        baseline_sql = (
            "select _wstart, _wend, first(*), last(*), count(*) "
            "from test_vtable_select.vtb_virtual_stb "
            "event_window start with u_tinyint_col > 50 end with u_tinyint_col > 200 "
        )
        optimized_sql = (
            "select _wstart, _wend, first(u_tinyint_col), last(u_tinyint_col), count(u_tinyint_col) "
            "from test_vtable_select.vtb_virtual_stb "
            "event_window start with u_tinyint_col > 50 end with u_tinyint_col > 200 "
        )
        keyword = "Dynamic Query Control for Virtual Table Window"

        if self._plan_contains(baseline_sql, keyword):
            tdLog.exit("baseline query unexpectedly hits optimized dynamic window path")
        if not self._plan_contains(optimized_sql, keyword):
            tdLog.exit("optimized query did not hit dynamic window path")

        tdSql.query(baseline_sql)
        baseline_rows = tdSql.queryRows
        tdSql.checkEqual(baseline_rows > 0, True)
        baseline_last_wstart = tdSql.getData(baseline_rows - 1, 0)
        baseline_last_wend = tdSql.getData(baseline_rows - 1, 1)

        tdSql.query(optimized_sql)
        tdSql.checkRows(baseline_rows)
        tdSql.checkData(baseline_rows - 1, 0, baseline_last_wstart)
        tdSql.checkData(baseline_rows - 1, 1, baseline_last_wend)

    def test_event_window_true_for_definition(self):
        """Event window TRUE_FOR should follow duration/count definition.

        Build baseline event-window results without TRUE_FOR, then apply
        duration/count predicates in test code and compare with SQL results of
        each TRUE_FOR expression.

        Since: v3.3.8.0

        Catalog: query/window

        Labels: boundary_case,event_window,true_for,virtual_table,integration,functional
        History:
            - 2026-03-05 Codex Added semantic cross-check for TRUE_FOR

        """
        keyword = "Dynamic Query Control for Virtual Table Window"
        base_sql = (
            "select _wstart, _wend, _wduration, count(*) "
            "from test_vtable_select.vtb_virtual_stb "
            "event_window start with u_tinyint_col > 50 end with u_tinyint_col > 200 "
        )
        base_rows = self._fetch_rows(base_sql)
        normalized_base = [
            (
                str(row[0]),
                str(row[1]),
                self._to_int_or_none(row[2]),
                self._to_int_or_none(row[3]),
            )
            for row in base_rows
        ]
        tdSql.checkEqual(len(normalized_base) > 0, True)

        case_defs = [
            ("2a", lambda d, c: d >= 2),
            ("10a", lambda d, c: d >= 10),
            ("30a", lambda d, c: d >= 30),
            ("COUNT 2", lambda d, c: c >= 2),
            ("COUNT 4", lambda d, c: c >= 4),
            ("COUNT 12", lambda d, c: c >= 12),
            ("2a AND COUNT 2", lambda d, c: d >= 2 and c >= 2),
            ("10a AND COUNT 4", lambda d, c: d >= 10 and c >= 4),
            ("30a AND COUNT 12", lambda d, c: d >= 30 and c >= 12),
            ("2a OR COUNT 2", lambda d, c: d >= 2 or c >= 2),
            ("10a OR COUNT 4", lambda d, c: d >= 10 or c >= 4),
            ("30a OR COUNT 12", lambda d, c: d >= 30 or c >= 12),
        ]

        for expr, predicate in case_defs:
            sql = (
                "select _wstart, _wend, first(u_tinyint_col), last(u_tinyint_col), count(u_tinyint_col) "
                "from test_vtable_select.vtb_virtual_stb "
                "event_window start with u_tinyint_col > 50 end with u_tinyint_col > 200 "
                f"TRUE_FOR({expr}) "
            )
            if not self._plan_contains(sql, keyword):
                tdLog.exit(f"TRUE_FOR({expr}) did not hit dynamic window optimization path")

            actual_rows = [
                (
                    str(r[0]),
                    str(r[1]),
                )
                for r in self._fetch_rows(sql)
            ]
            expected_rows = [
                (wstart, wend)
                for (wstart, wend, duration, cnt) in normalized_base
                if predicate(0 if duration is None else duration, 0 if cnt is None else cnt)
            ]
            tdSql.checkEqual(Counter(actual_rows), Counter(expected_rows))

    def run(self):
        self.test_state_window_pseudocolumns_respect_referenced_filter()
        self.test_event_window_no_window_not_out_of_range()
        self.test_event_window_last_wend_not_plus_one()
        self.test_event_window_true_for_definition()

    def stop(self):
        tdLog.success("%s successfully executed" % __file__)
