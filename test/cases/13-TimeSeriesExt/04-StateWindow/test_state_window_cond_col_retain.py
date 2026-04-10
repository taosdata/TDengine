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


class TestStateWindowCondColRetain:
    """Regression: vstable state-window optimization must retain condition columns."""

    DB = "test_sw_cond"

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_state_window_cond_col_retain(self):
        """Multi-col state_window optimization on vstable and vtable, plus WHERE correctness

        1. Prepare source tables, virtual stable and virtual normal table
        2. Verify multi-col STATE_WINDOW on vstable hits VStableWindow optimized path (no WHERE)
        3. Verify multi-col STATE_WINDOW on vtable correctness (single-source, VirtualScan eliminated)
        4. Verify multi-col STATE_WINDOW on multi-source vtable hits VtableWindow optimized path
        5. Verify WHERE + STATE_WINDOW on vstable gives correct result (non-optimized path)
        6. Verify WHERE + STATE_WINDOW on vtable gives correct result (non-optimized path)

        Catalog:
            - TimeSeriesExt:StateWindow

        Since: v3.4.0.0

        Labels: common,ci,state_window,virtual_table,optimizer

        Jira: None

        History:
            - 2026-04-10 Tony Zhang Multi-col state window optimization for vstable and vtable

        """
        self.do_prepare_data()
        self.do_vstable_multi_col_optimization()
        self.do_vtable_multi_col_optimization()
        self.do_vtable_multi_src_optimization()
        self.do_vstable_where_correctness()
        self.do_vtable_where_correctness()

    # --- util ---

    def _plan_contains(self, sql, keyword):
        tdSql.query(f"explain verbose true {sql}")
        for row in tdSql.queryResult or []:
            for col in row:
                if col is not None and keyword in str(col):
                    return True
        return False

    def _fetch_sorted(self, sql):
        tdSql.query(sql)
        rows = []
        for i in range(tdSql.queryRows):
            rows.append(tuple(tdSql.getData(i, j) for j in range(tdSql.queryCols)))
        rows.sort()
        return rows

    def _assert_same_result(self, baseline_sql, optimized_sql):
        baseline = self._fetch_sorted(baseline_sql)
        optimized = self._fetch_sorted(optimized_sql)
        tdSql.checkEqual(len(optimized), len(baseline))
        tdSql.checkEqual(optimized, baseline)

    # --- data ---

    def do_prepare_data(self):
        db = self.DB
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} keep 3650")
        tdSql.execute(f"use {db}")

        tdSql.execute(
            "create stable src_stb ("
            "ts timestamp, "
            "sk1 int, "
            "sk2 int, "
            "filter_col int, "
            "filter_col2 varchar(16), "
            "val double) "
            "tags (gid int)"
        )

        tdSql.execute("create table src_ct1 using src_stb tags (1)")
        tdSql.execute("create table src_ct2 using src_stb tags (2)")

        tdSql.execute(f"""insert into src_ct1 values
            ('2025-10-01 10:00:00', 1, 10, 5,  'A', 100.0),
            ('2025-10-01 10:00:01', 1, 10, 20, 'A', 110.0),
            ('2025-10-01 10:00:02', 1, 20, 30, 'B', 120.0),
            ('2025-10-01 10:00:03', 2, 20, 40, 'B', 130.0),
            ('2025-10-01 10:00:04', 2, 20, 50, 'A', 140.0),
            ('2025-10-01 10:00:05', 1, 10, 60, 'C', 150.0)
        """)

        tdSql.execute(f"""insert into src_ct2 values
            ('2025-10-01 10:00:06', 3, 30, 10, 'A', 200.0),
            ('2025-10-01 10:00:07', 3, 30, 70, 'B', 210.0),
            ('2025-10-01 10:00:08', 1, 10, 80, 'C', 220.0),
            ('2025-10-01 10:00:09', 1, 10, 20, 'A', 230.0)
        """)

        tdSql.execute(
            "create stable vstb ("
            "ts timestamp, "
            "sk1 int, "
            "sk2 int, "
            "filter_col int, "
            "filter_col2 varchar(16), "
            "val double) "
            "tags (gid int) virtual 1"
        )

        tdSql.execute(
            "create vtable vct1 ("
            f"sk1 from {db}.src_ct1.sk1, "
            f"sk2 from {db}.src_ct1.sk2, "
            f"filter_col from {db}.src_ct1.filter_col, "
            f"filter_col2 from {db}.src_ct1.filter_col2, "
            f"val from {db}.src_ct1.val) "
            "using vstb tags (1)"
        )

        tdSql.execute(
            "create vtable vct2 ("
            f"sk1 from {db}.src_ct2.sk1, "
            f"sk2 from {db}.src_ct2.sk2, "
            f"filter_col from {db}.src_ct2.filter_col, "
            f"filter_col2 from {db}.src_ct2.filter_col2, "
            f"val from {db}.src_ct2.val) "
            "using vstb tags (2)"
        )

        # --- vtable (virtual normal table, single source) for VtableWindow path ---
        tdSql.execute(
            f"create vtable vtb_norm ("
            "ts timestamp, "
            f"sk1 int from {db}.src_ct1.sk1, "
            f"sk2 int from {db}.src_ct1.sk2, "
            f"filter_col int from {db}.src_ct1.filter_col, "
            f"filter_col2 varchar(16) from {db}.src_ct1.filter_col2, "
            f"val double from {db}.src_ct1.val)"
        )

        # --- vtable (virtual normal table, multi source) ---
        # VirtualScan has >1 children so EliminateVirtualScan won't remove it;
        # VtableWindow optimization can match.
        tdSql.execute(
            f"create vtable vtb_multi_src ("
            "ts timestamp, "
            f"sk1 int from {db}.src_ct1.sk1, "
            f"sk2 int from {db}.src_ct1.sk2, "
            f"val double from {db}.src_ct1.val, "
            f"filter_col int from {db}.src_ct2.filter_col)"
        )

        print("prepare data ............................. [passed]")

    # --- impl ---

    def do_vstable_multi_col_optimization(self):
        """Multi-col STATE_WINDOW on vstable must hit VStableWindow optimized path."""
        db = self.DB
        opt_kw = "Dynamic Query Control for Virtual Table Window"

        optimized_sql = (
            f"select _wstart, _wend, count(sk2), avg(val) "
            f"from {db}.vstb state_window(sk1, sk2)"
        )
        baseline_sql = (
            f"select _wstart, _wend, count(sk2), avg(val) "
            f"from {db}.src_stb state_window(sk1, sk2)"
        )

        if not self._plan_contains(optimized_sql, opt_kw):
            tdLog.exit("vstable multi-col state_window did not hit VStableWindow optimized path")

        self._assert_same_result(baseline_sql, optimized_sql)
        print("vstable multi-col optimization ........... [passed]")

    def do_vtable_multi_col_optimization(self):
        """Multi-col STATE_WINDOW on vtable — correctness check (VirtualScan may be eliminated)."""
        db = self.DB

        optimized_sql = (
            f"select _wstart, _wend, count(sk2), avg(val) "
            f"from {db}.vtb_norm state_window(sk1, sk2)"
        )
        baseline_sql = (
            f"select _wstart, _wend, count(sk2), avg(val) "
            f"from {db}.src_ct1 state_window(sk1, sk2)"
        )

        self._assert_same_result(baseline_sql, optimized_sql)
        print("vtable multi-col correctness ............. [passed]")

    def do_vtable_multi_src_optimization(self):
        """Multi-source vtable: VirtualScan not eliminated, VtableWindow optimization must trigger."""
        db = self.DB
        opt_kw = "Dynamic Query Control for Virtual Table Window"

        optimized_sql = (
            f"select _wstart, _wend, count(sk2), avg(val), count(filter_col) "
            f"from {db}.vtb_multi_src state_window(sk1, sk2)"
        )
        baseline_sql = (
            f"select _wstart, _wend, count(sk2), avg(val) "
            f"from {db}.src_ct1 state_window(sk1, sk2)"
        )

        if not self._plan_contains(optimized_sql, opt_kw):
            tdLog.exit("multi-source vtable did not hit VtableWindow optimized path")

        # compare the shared columns (first 4) against src_ct1 baseline
        tdSql.query(optimized_sql)
        opt_rows = []
        for i in range(tdSql.queryRows):
            opt_rows.append((tdSql.getData(i, 0), tdSql.getData(i, 1),
                             tdSql.getData(i, 2), tdSql.getData(i, 3)))
        opt_rows.sort()

        base_rows = self._fetch_sorted(baseline_sql)
        tdSql.checkEqual(len(opt_rows), len(base_rows))
        tdSql.checkEqual(opt_rows, base_rows)
        print("vtable multi-src optimization ............ [passed]")

    def do_vstable_where_correctness(self):
        """WHERE on non-state-key col with multi-col STATE_WINDOW on vstable — correctness only."""
        db = self.DB

        optimized_sql = (
            f"select _wstart, _wend, count(sk2), avg(val) "
            f"from {db}.vstb where filter_col > 20 state_window(sk1, sk2)"
        )
        baseline_sql = (
            f"select _wstart, _wend, count(sk2), avg(val) "
            f"from {db}.src_stb where filter_col > 20 state_window(sk1, sk2)"
        )

        self._assert_same_result(baseline_sql, optimized_sql)
        print("vstable WHERE correctness ................ [passed]")

    def do_vtable_where_correctness(self):
        """WHERE on non-state-key col with multi-col STATE_WINDOW on vtable — correctness only."""
        db = self.DB

        optimized_sql = (
            f"select _wstart, _wend, count(sk2), avg(val) "
            f"from {db}.vtb_norm where filter_col > 20 state_window(sk1, sk2)"
        )
        baseline_sql = (
            f"select _wstart, _wend, count(sk2), avg(val) "
            f"from {db}.src_ct1 where filter_col > 20 state_window(sk1, sk2)"
        )

        self._assert_same_result(baseline_sql, optimized_sql)
        print("vtable WHERE correctness ................. [passed]")
