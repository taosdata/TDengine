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
from new_test_framework.utils import tdLog, tdSql
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

    def test_event_window_no_window_not_out_of_range(self):
        """Event window empty result should not crash.

        Validate the optimized virtual-table event window path when start/end
        predicates never form any window. The query should return zero rows
        and must not raise out-of-range executor errors.

        Since: v3.3.8.0

        Catalog: query/window

        Labels: virtual_table,event_window,boundary_case,window_optimize

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

        Labels: virtual_table,event_window,boundary_case,window_optimize

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

    def run(self):
        self.test_event_window_no_window_not_out_of_range()
        self.test_event_window_last_wend_not_plus_one()

    def stop(self):
        tdLog.success("%s successfully executed" % __file__)
