###################################################################
#           Copyright (c) 2025 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql, etool


TSDB_CODE_PAR_INVALID_STATE_WIN_TYPE = -2147473880


class TestStateWindowMultiColOptimize:
    @classmethod
    def setup_class(cls):
        cls.replicaVar = 1
        tdLog.debug(f"start to execute {__file__}")

    def test_state_window_multi_col_optimize(self):
        """Verify vstable state-window optimization for multi-column keys

        1. Build a real virtual stable and virtual child table for the optimizer
        2. Verify dual-column state_window hits vstableWindowOptimize and matches baseline
        3. Verify triple-column state_window hits vstableWindowOptimize and matches baseline
        4. Verify non-column state expressions do not hit the optimized path

        Catalog:
            - TimeSeriesExt:StateWindow

        Since: v3.4.0.0

        Labels: common,ci,state_window,virtual_table,optimizer

        Jira: None

        History:
            - 2026-04-09 Tony Zhang Initial version for dual-column state window optimization
            - 2026-04-09 GitHub Copilot Reworked case to cover real vstableWindowOptimize path

        """
        self.do_prepare_data()
        self.do_dual_col_vstable_optimization()
        self.do_triple_col_vstable_optimization()
        self.do_non_column_state_expr_not_optimized()

    # --- util ---

    def do_prepare_data(self):
        tdSql.execute("drop database if exists test_sw_opt")
        tdSql.execute("create database test_sw_opt keep 3650")
        tdSql.execute("use test_sw_opt")

        tdSql.execute(
            "create stable src_stb ("
            "ts timestamp, "
            "state_col1 int, "
            "state_col2 int, "
            "state_col3 int, "
            "agg_val1 double, "
            "agg_val2 double) "
            "tags (gid int)"
        )

        tdSql.execute("create table src_ctb1 using src_stb tags (1)")
        tdSql.execute("create table src_ctb2 using src_stb tags (2)")

        tdSql.execute("""insert into src_ctb1 values
            ('2025-10-01 10:00:00', 1, 10, 100, 100.0, 200.0),
            ('2025-10-01 10:00:01', 1, 10, 100, 110.0, 210.0),
            ('2025-10-01 10:00:02', 1, 20, 100, 120.0, 220.0),
            ('2025-10-01 10:00:03', 2, 20, 100, 130.0, 230.0)
        """)

        tdSql.execute("""insert into src_ctb2 values
            ('2025-10-01 10:00:04', 2, 20, 200, 140.0, 240.0),
            ('2025-10-01 10:00:05', 2, 30, 200, 150.0, 250.0),
            ('2025-10-01 10:00:06', 1, 10, 100, 160.0, 260.0),
            ('2025-10-01 10:00:07', 1, 10, 100, 170.0, 270.0)
        """)

        tdSql.execute(
            "create stable vstb_sw_opt ("
            "ts timestamp, "
            "state_col1 int, "
            "state_col2 int, "
            "state_col3 int, "
            "agg_val1 double, "
            "agg_val2 double) "
            "tags (gid int) virtual 1"
        )

        tdSql.execute(
            "create vtable vctb_sw_opt_1 ("
            "state_col1 from test_sw_opt.src_ctb1.state_col1, "
            "state_col2 from test_sw_opt.src_ctb1.state_col2, "
            "state_col3 from test_sw_opt.src_ctb1.state_col3, "
            "agg_val1 from test_sw_opt.src_ctb1.agg_val1, "
            "agg_val2 from test_sw_opt.src_ctb1.agg_val2) "
            "using vstb_sw_opt tags (1)"
        )

        tdSql.execute(
            "create vtable vctb_sw_opt_2 ("
            "state_col1 from test_sw_opt.src_ctb2.state_col1, "
            "state_col2 from test_sw_opt.src_ctb2.state_col2, "
            "state_col3 from test_sw_opt.src_ctb2.state_col3, "
            "agg_val1 from test_sw_opt.src_ctb2.agg_val1, "
            "agg_val2 from test_sw_opt.src_ctb2.agg_val2) "
            "using vstb_sw_opt tags (2)"
        )

        print("prepare virtual stable data .......... [passed]")

    def _plan_contains(self, sql: str, keyword: str) -> bool:
        tdSql.query(f"explain verbose true {sql}")
        for row in tdSql.queryResult or []:
            for col in row:
                if col is not None and keyword in str(col):
                    return True
        return False

    def _fetch_rows(self, sql: str):
        tdSql.query(sql, show=True)
        return [tuple(tdSql.getData(i, j) for j in range(tdSql.queryCols)) for i in range(tdSql.queryRows)]

    def _check_same_rows(self, baseline_sql: str, optimized_sql: str):
        baseline_rows = self._fetch_rows(baseline_sql)
        optimized_rows = self._fetch_rows(optimized_sql)
        tdSql.checkEqual(len(optimized_rows), len(baseline_rows))
        tdSql.checkEqual(optimized_rows, baseline_rows)

    # --- impl ---

    def do_dual_col_vstable_optimization(self):
        keyword = "Dynamic Query Control for Virtual Table Window"
        baseline_sql = (
            "select _wstart, _wend, count(state_col2), avg(agg_val1) "
            "from test_sw_opt.src_stb state_window(state_col1, state_col2)"
        )
        optimized_sql = (
            "select _wstart, _wend, count(state_col2), avg(agg_val1) "
            "from test_sw_opt.vstb_sw_opt state_window(state_col1, state_col2)"
        )

        if self._plan_contains(baseline_sql, keyword):
            tdLog.exit("baseline physical-table query unexpectedly hit vstable optimized path")
        if not self._plan_contains(optimized_sql, keyword):
            tdLog.exit("dual-column virtual-stable query did not hit vstableWindowOptimize path")

        self._check_same_rows(baseline_sql, optimized_sql)
        print("dual-col vstable optimize ............ [passed]")

    def do_triple_col_vstable_optimization(self):
        keyword = "Dynamic Query Control for Virtual Table Window"
        baseline_sql = (
            "select _wstart, _wend, count(state_col3), avg(agg_val1) "
            "from test_sw_opt.src_stb state_window(state_col1, state_col2, state_col3)"
        )
        optimized_sql = (
            "select _wstart, _wend, count(state_col3), avg(agg_val1) "
            "from test_sw_opt.vstb_sw_opt state_window(state_col1, state_col2, state_col3)"
        )

        if not self._plan_contains(optimized_sql, keyword):
            tdLog.exit("triple-column virtual-stable query did not hit vstableWindowOptimize path")

        self._check_same_rows(baseline_sql, optimized_sql)
        print("triple-col vstable optimize .......... [passed]")

    def do_non_column_state_expr_not_optimized(self):
        sql = (
            "select _wstart, _wend, count(*), state_col3 "
            "from test_sw_opt.vstb_sw_opt state_window(state_col1 + state_col2, state_col3)"
        )

        tdSql.error(f"explain verbose true {sql}", expectedErrno=TSDB_CODE_PAR_INVALID_STATE_WIN_TYPE)
        tdSql.error(sql, expectedErrno=TSDB_CODE_PAR_INVALID_STATE_WIN_TYPE)
        print("non-column state expr rejected ....... [passed]")
