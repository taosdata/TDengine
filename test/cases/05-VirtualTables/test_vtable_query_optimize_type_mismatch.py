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


TSDB_CODE_VTABLE_COLUMN_TYPE_MISMATCH = -2147458552  # 0x80006208


class TestVTableQueryOptimizeTypeMismatch:
    @staticmethod
    def _prepare_data(mode=1):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_same_db_vtables(mode=mode)

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

    def _recreate_source_table_with_mismatch(self):
        tdSql.execute("use test_vtable_select;")
        tdSql.execute("drop table if exists vtb_org_normal_0;")
        tdSql.execute(
            "create table vtb_org_normal_0 ("
            "ts timestamp, "
            "u_tinyint_col int unsigned, "
            "u_smallint_col smallint unsigned, "
            "u_int_col int unsigned, "
            "u_bigint_col bigint unsigned, "
            "tinyint_col tinyint, "
            "smallint_col smallint, "
            "int_col int, "
            "bigint_col bigint, "
            "float_col float, "
            "double_col double, "
            "bool_col bool, "
            "binary_16_col binary(16), "
            "binary_32_col binary(32), "
            "nchar_16_col nchar(16), "
            "nchar_32_col nchar(32))"
        )
        tdSql.execute(
            "insert into vtb_org_normal_0 values "
            "(now, 1, 2, 3, 4, -1, -2, -3, -4, 1.25, 2.5, true, 'b16', 'b32', 'n16', 'n32')"
        )

    def _assert_optimized_query_reports_mismatch(self, sql: str, keyword: str, mode=1):
        self._prepare_data(mode=mode)

        if not self._plan_contains(sql, keyword):
            tdLog.exit(f"expected optimized path, but '{keyword}' not found in explain plan")

        self._recreate_source_table_with_mismatch()
        tdSql.error(sql, expectedErrno=TSDB_CODE_VTABLE_COLUMN_TYPE_MISMATCH)

    def test_interval_batch_scan_reports_column_type_mismatch(self):
        """Optimized interval scan should reject source-column type drift.

        Verify the virtual super table interval optimization still returns
        `TSDB_CODE_VTABLE_COLUMN_TYPE_MISMATCH` after a referenced source
        table is recreated with the same column name but a different type.

        Since: v3.3.8.0

        Catalog: query/window

        Labels: virtual_table,interval,optimize,batch_scan,regression

        Jira: None

        History:
            - 2026-03-25 Jing Sima Added optimized batch-scan mismatch regression

        """
        sql = (
            "select _wstart, _wend, count(u_tinyint_col), count(u_smallint_col) "
            "from test_vtable_select.vtb_virtual_stb interval(1s)"
        )
        keyword = "Dynamic Query Control for Virtual Stable Interval (has_partition=0 batch_process_child=1"

        self._assert_optimized_query_reports_mismatch(sql, keyword)

    def test_event_window_reports_column_type_mismatch(self):
        """Optimized event window should reject source-column type drift.

        Verify the optimized virtual super table event-window path still
        returns `TSDB_CODE_VTABLE_COLUMN_TYPE_MISMATCH` after a referenced
        source table is recreated with the same column name but a different
        type.

        Since: v3.3.8.0

        Catalog: query/window

        Labels: virtual_table,event_window,optimize,batch_scan,regression

        Jira: None

        History:
            - 2026-03-25 Jing Sima Added optimized event-window mismatch regression

        """
        sql = (
            "select _wstart, _wend, first(u_tinyint_col), last(u_tinyint_col), count(u_tinyint_col) "
            "from test_vtable_select.vtb_virtual_stb "
            "event_window start with u_tinyint_col > 50 end with u_tinyint_col > 200 limit 100"
        )
        keyword = "Dynamic Query Control for Virtual Table Window"

        self._assert_optimized_query_reports_mismatch(sql, keyword)

    def test_session_window_reports_column_type_mismatch(self):
        """Optimized session window should reject source-column type drift.

        Verify the optimized virtual super table session-window path still
        returns `TSDB_CODE_VTABLE_COLUMN_TYPE_MISMATCH` after a referenced
        source table is recreated with the same column name but a different
        type.

        Since: v3.3.8.0

        Catalog: query/window

        Labels: virtual_table,session_window,optimize,batch_scan,regression

        Jira: None

        History:
            - 2026-03-25 Jing Sima Added optimized session-window mismatch regression

        """
        sql = (
            "select _wstart, _wend, first(u_tinyint_col), last(u_tinyint_col), count(u_tinyint_col) "
            "from test_vtable_select.vtb_virtual_stb session(ts, 1a) limit 100"
        )
        keyword = "Dynamic Query Control for Virtual Table Window"

        self._assert_optimized_query_reports_mismatch(sql, keyword)

    def test_state_window_reports_column_type_mismatch(self):
        """Optimized state window should reject source-column type drift.

        Verify the optimized virtual super table state-window path still
        returns `TSDB_CODE_VTABLE_COLUMN_TYPE_MISMATCH` after a referenced
        source table is recreated with the same column name but a different
        type.

        Since: v3.3.8.0

        Catalog: query/window

        Labels: virtual_table,state_window,optimize,batch_scan,regression

        Jira: None

        History:
            - 2026-03-25 Jing Sima Added optimized state-window mismatch regression

        """
        sql = (
            "select _wstart, _wend, count(bool_col), avg(u_tinyint_col) "
            "from test_vtable_select.vtb_virtual_stb state_window(bool_col)"
        )
        keyword = "Dynamic Query Control for Virtual Table Window"

        self._assert_optimized_query_reports_mismatch(sql, keyword)

    def test_agg_reports_column_type_mismatch(self):
        """Optimized aggregate should reject source-column type drift.

        Verify the optimized virtual super table aggregate path still returns
        `TSDB_CODE_VTABLE_COLUMN_TYPE_MISMATCH` after a referenced source
        table is recreated with the same column name but a different type.

        Since: v3.3.8.0

        Catalog: query/aggregate

        Labels: virtual_table,aggregate,optimize,batch_scan,regression

        Jira: None

        History:
            - 2026-03-25 Jing Sima Added optimized aggregate mismatch regression

        """
        sql = "select avg(u_tinyint_col) from test_vtable_select.vtb_virtual_stb"
        keyword = "Dynamic Query Control for Virtual Stable Agg (has_partition=0 batch_process_child=1"

        self._assert_optimized_query_reports_mismatch(sql, keyword, mode=2)

    def test_partitioned_agg_reports_column_type_mismatch(self):
        """Optimized partitioned aggregate should reject source-column type drift.

        Verify the optimized virtual super table partitioned aggregate path
        still returns `TSDB_CODE_VTABLE_COLUMN_TYPE_MISMATCH` after a
        referenced source table is recreated with the same column name but a
        different type.

        Since: v3.3.8.0

        Catalog: query/aggregate

        Labels: virtual_table,aggregate,partition,optimize,batch_scan,regression

        Jira: None

        History:
            - 2026-03-25 Jing Sima Added optimized partitioned-aggregate mismatch regression

        """
        sql = (
            "select avg(u_tinyint_col) from test_vtable_select.vtb_virtual_stb "
            "partition by bool_tag order by 1"
        )
        keyword = "Dynamic Query Control for Virtual Stable Agg (has_partition=1 batch_process_child=1"

        self._assert_optimized_query_reports_mismatch(sql, keyword, mode=2)

    def run(self):
        self.test_interval_batch_scan_reports_column_type_mismatch()
        self.test_event_window_reports_column_type_mismatch()
        self.test_session_window_reports_column_type_mismatch()
        self.test_state_window_reports_column_type_mismatch()
        self.test_agg_reports_column_type_mismatch()
        self.test_partitioned_agg_reports_column_type_mismatch()

    def stop(self):
        tdLog.success("%s successfully executed" % __file__)
