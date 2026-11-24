# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-
"""state_window aggregates with expression parameters on a multi-origin virtual table.

The VtableWindow optimizer rewrites a state_window query over a virtual normal
table by splitting each aggregate function onto the source scan of its column
parameter.  Aggregates over expressions (sum(case when
... end), sum(attr1 + attr4)) cannot be split that way and used to be silently
dropped from the plan, so their output columns could no longer be resolved and
the query failed with TSDB_CODE_PLAN_SLOT_NOT_FOUND
("Planner slot key not found").

These expression aggregates use the VirtualScan -> StateWindow path and
produce the same result as the aggregate over the merged virtual rows.

Catalog:
    - VirtualTable

Since: v3.4.4.0

Labels: virtual,state_window,ci,functional

Jira: None

Feishu: https://project.feishu.cn/taosdata_td/defect/detail/7177829996

History:
    - 2026-11-23 tdcrew created
"""
from new_test_framework.utils import tdLog, tdSql


DB = "test_vt_state_win_case"
BASE_TS = 1577840401000  # 2020-01-01 01:00:01 UTC


class TestVtableStateWindowCaseWhen:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.execute(f"drop database if exists {DB};")
        tdSql.execute(f"create database {DB} vgroups 2;")
        # motor1: ts 01..04, MillRun = 1,1,2,2
        tdSql.execute(
            f"create table {DB}.origin1 (ts timestamp, MillRun int, attr1 int, attr2 int, attr3 int)"
        )
        tdSql.execute(
            f"insert into {DB}.origin1 values "
            f"({BASE_TS + 0}, 1, 10, 100, 1000) "
            f"({BASE_TS + 1000}, 1, 20, 200, 2000) "
            f"({BASE_TS + 2000}, 2, 30, 300, 3000) "
            f"({BASE_TS + 3000}, 2, 40, 400, 4000)"
        )
        # motor2: only rows at ts 02 and ts 04, so the merged virtual rows carry
        # NULL for attr4..attr6 at ts 01 and ts 03.
        tdSql.execute(f"create table {DB}.origin2 (ts timestamp, attr4 int, attr5 int, attr6 int)")
        tdSql.execute(
            f"insert into {DB}.origin2 values "
            f"({BASE_TS + 1000}, 50, 500, 5000) "
            f"({BASE_TS + 3000}, 60, 600, 6000)"
        )
        tdSql.execute(
            f"create vtable {DB}.vt_test (ts timestamp, "
            f"MillRun int from {DB}.origin1.MillRun, "
            f"attr1 int from {DB}.origin1.attr1, "
            f"attr2 int from {DB}.origin1.attr2, "
            f"attr3 int from {DB}.origin1.attr3, "
            f"attr4 int from {DB}.origin2.attr4, "
            f"attr5 int from {DB}.origin2.attr5, "
            f"attr6 int from {DB}.origin2.attr6)"
        )

    @staticmethod
    def _check_windows(sql, expected, split):
        tdSql.query(f"explain verbose true {sql}")
        uses_split = any(
            "Dynamic Query Control for Virtual Table Window" in row[0] for row in tdSql.queryResult
        )
        assert uses_split == split, tdSql.queryResult

        tdSql.query(sql)
        tdSql.checkRows(len(expected))
        for row, (start_ms, *values) in enumerate(expected):
            # Native returns local datetimes and REST returns timezone-aware ones.
            tdSql.checkEqual(round(tdSql.getData(row, 0).timestamp() * 1000), start_ms)
            for col, value in enumerate(values, start=1):
                tdSql.checkData(row, col, value)

    # --- impl ---
    def do_case_when_agg_query(self):
        # The customer query shape: pseudo-column expression, case-when
        # aggregates and extend, over a multi-origin virtual table.
        sql = (
            f"select _wstart + 0s as output_timestamp, "
            f"sum(case when attr1 is null then 0 else attr1 end) as m1_c1, "
            f"sum(case when attr2 is null then 0 else attr2 end) as m1_c2, "
            f"max(case when attr3 is null then 0 else attr3 end) as m1_cmax, "
            f"sum(case when attr4 is null then 0 else attr4 end) as m2_c1, "
            f"sum(case when attr5 is null then 0 else attr5 end) as m2_c2, "
            f"max(case when attr6 is null then 0 else attr6 end) as m2_cmax "
            f"from {DB}.vt_test where ts >= {BASE_TS} "
            f"state_window (MillRun) extend (1)"
        )
        self._check_windows(sql, [
            (BASE_TS, 30, 300, 2000, 50, 500, 5000),
            (BASE_TS + 2000, 70, 700, 4000, 60, 600, 6000),
        ], split=False)

    def do_expression_param_agg_query(self):
        sql = f"select _wstart, sum(attr1 + attr4) from {DB}.vt_test state_window(MillRun)"
        self._check_windows(sql, [(BASE_TS, 70), (BASE_TS + 2000, 100)], split=False)

    def do_two_column_agg_query(self):
        sql = f"select _wstart, corr(attr1, attr2), sum(attr4) from {DB}.vt_test state_window(MillRun)"
        self._check_windows(sql, [(BASE_TS, 1.0, 50), (BASE_TS + 2000, 1.0, 60)], split=False)

    def do_count_star_query(self):
        sql = f"select _wstart, count(*), sum(attr4) from {DB}.vt_test state_window(MillRun)"
        self._check_windows(sql, [(BASE_TS, 2, 50), (BASE_TS + 2000, 2, 60)], split=False)

    def do_plain_column_agg_query(self):
        sql = (
            f"select _wstart, sum(attr1), sum(attr2), max(attr3), sum(attr4), sum(attr5), max(attr6) "
            f"from {DB}.vt_test state_window(MillRun)"
        )
        self._check_windows(sql, [
            (BASE_TS, 30, 300, 2000, 50, 500, 5000),
            (BASE_TS + 2000, 70, 700, 4000, 60, 600, 6000),
        ], split=True)

    def do_percentile_agg_query(self):
        window = f"from {DB}.vt_test state_window(MillRun)"
        approx = f"select _wstart, sum(attr1), apercentile(attr4, 95) {window}"
        self._check_windows(approx, [(BASE_TS, 30, 50), (BASE_TS + 2000, 70, 60)], split=True)

        # Exact percentiles require both scan phases, including in a mixed window.
        exact = f"select _wstart, percentile(attr1, 50), apercentile(attr4, 95) {window}"
        self._check_windows(exact, [(BASE_TS, 15, 50), (BASE_TS + 2000, 35, 60)], split=False)

    # --- main ---
    def test_state_window_case_when(self):
        """Case-when aggregates over a virtual table state_window query

        1. customer query shape: _wstart + 0s, sum/max(case when ... end),
           extend(1) over a multi-origin virtual normal table
        2. cross-origin expression and two-column aggregate arguments keep the original window
        3. count(*) alongside a cross-origin aggregate
        4. plain single-column aggregates still follow the optimized path
        5. approximate percentiles can be split; exact percentiles retain both scan phases

        Catalog:
            - VirtualTable

        Since: v3.4.4.0

        Labels: virtual,state_window,ci,functional

        Jira: None

        History:
            - 2026-11-23 tdcrew created
        """
        self.do_case_when_agg_query()
        self.do_expression_param_agg_query()
        self.do_two_column_agg_query()
        self.do_count_star_query()
        self.do_plain_column_agg_query()
        self.do_percentile_agg_query()
