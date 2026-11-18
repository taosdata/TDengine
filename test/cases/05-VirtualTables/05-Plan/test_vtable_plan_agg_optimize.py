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
from new_test_framework.utils import tdLog, tdSql, etool, tdCom
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from vtable_util import (
    VTABLE_COLUMN_TYPES,
    VtableQueryUtil,
    build_vstable_sql,
    build_vtable_child_sql,
    build_vtable_normal_sql,
)

class TestVTablePlanAggOptimize:
    def setup_class(cls):
        vtbUtil = VtableQueryUtil()
        vtbUtil.prepare_same_db_vtables(mode = 2)

    def run_normal_query(self, testCase, result_transform=None):
        # read sql from .sql file and execute
        tdLog.info(f"test case : {testCase}.")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", f"{testCase}.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", f"{testCase}.ans")

        tdCom.compare_testcase_result(
            self.sqlFile, self.ansFile, testCase,
            result_transform=result_transform)

    def do_select_function_pushdown(self, test_case):
        self.run_normal_query(test_case)
        print(f"{test_case} [ passed ]")

    def do_unreferenced_column_null(self, test_case):
        db = VtableQueryUtil.VTABLE_DB_NAME
        column_types = VTABLE_COLUMN_TYPES + (("aux_ts", "timestamp"),)
        source_map = {
            "int_col": f"{db}.vtb_org_tb_0.int_col",
            "float_col": f"{db}.vtb_org_tb_1.float_col",
        }
        tdSql.execute(f"use {db}")
        null_ts = "2025-06-17 04:00:00.000"
        try:
            tdSql.execute(build_vstable_sql(column_types, stb="vtb_null_stb"))
            tdSql.execute(build_vtable_normal_sql("vtb_null_ntb", source_map, column_types))
            tdSql.execute(build_vtable_child_sql(
                "vtb_null_ctb", source_map, "7, true, 7, 7, 'null', 'null'",
                stb="vtb_null_stb"))
            # A trailing NULL must not change LAST's chosen value or timestamp.
            tdSql.execute(
                f"insert into {db}.vtb_org_tb_0 (ts, int_col) "
                f"values ('{null_ts}', null)")
            for flushed in (False, True):
                if flushed:
                    tdSql.execute(f"flush database {db}")
                self.run_normal_query(
                    test_case,
                    result_transform=lambda output: output.rstrip("\r\n") + "\n")

            for table in ("vtb_null_ntb", "vtb_null_ctb"):
                tdSql.query(
                    f"select last(int_col) as val, _c0 as ts, "
                    f"bool_col as q, nchar_16_col as label, "
                    f"u_bigint_col as counter from {db}.{table}")
                tdSql.checkRows(1)
                tdSql.checkResColNameList(["val", "ts", "q", "label", "counter"])
                for column, data_type in ((2, "BOOL"), (3, "NCHAR"),
                                           (4, "BIGINT UNSIGNED")):
                    tdSql.checkData(0, column, None)
                    assert tdSql.getType(column) == data_type
        finally:
            tdSql.execute(f"delete from {db}.vtb_org_tb_0 where ts = '{null_ts}'")
            tdSql.execute(f"drop table if exists {db}.vtb_null_ctb")
            tdSql.execute(f"drop table if exists {db}.vtb_null_ntb")
            tdSql.execute(f"drop stable if exists {db}.vtb_null_stb")
        print(f"{test_case} [ passed ]")

    def do_partitioned_aggregate_iteration(self):
        db = VtableQueryUtil.VTABLE_DB_NAME
        full_counts = []
        for index in range(3):
            tdSql.query(
                f"select count(int_col) from {db}.vtb_virtual_ctb_full_{index}"
            )
            full_counts.append(tdSql.getData(0, 0))

        selected = []
        for kind, tag in (("full", 0), ("half_full", 1)):
            table = f"vtb_virtual_ctb_{kind}_1"
            tdSql.query(f"select count(int_col) from {db}.{table}")
            selected.append((tdSql.getData(0, 0), tag, table))

        for _ in range(2):
            # Batch dispatch advances through groups without aggregate tag arguments.
            tdSql.query(
                f"select count(int_col) from {db}.vtb_virtual_stb "
                "where int_tag = 0 partition by tbname order by 1"
            )
            tdSql.checkRows(3)
            for row, expected in enumerate(sorted(full_counts)):
                tdSql.checkData(row, 0, expected)

            # Single-child dispatch skips tag groups excluded by the source map.
            tdSql.query(
                f"select count(int_col), last(int_tag), last(tbname) "
                f"from {db}.vtb_virtual_stb where binary_32_tag = 'child1' "
                "partition by tbname order by 3"
            )
            tdSql.checkRows(2)
            for row, expected in enumerate(sorted(selected, key=lambda item: item[2])):
                for column, value in enumerate(expected):
                    tdSql.checkData(row, column, value)

            # One group retains its iterator while consuming multiple virtual children.
            tdSql.query(
                f"select count(int_col), last(int_tag) "
                f"from {db}.vtb_virtual_stb where int_tag = 0 "
                "partition by nchar_32_tag"
            )
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, sum(full_counts))
            tdSql.checkData(0, 1, 0)

            tdSql.query(
                f"select count(int_col), last(int_tag) "
                f"from {db}.vtb_virtual_stb where int_tag = 0 "
                "partition by tbname slimit 1"
            )
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, 0)

        tdSql.query(
            f"select count(int_col), last(int_tag) "
            f"from {db}.vtb_virtual_stb where int_tag = 2 partition by tbname"
        )
        tdSql.checkRows(0)
        print("partitioned aggregate iteration [ passed ]")

    def do_last_dynamic_prune_precision(self):
        source_db = VtableQueryUtil.VTABLE_DB_NAME
        base_ts = 1750046400000
        for index in range(6):
            tdSql.execute(
                f"create table {source_db}.prune_source_{index} "
                "(ts timestamp, v int, sparse int)"
            )
            tdSql.execute(
                f"insert into {source_db}.prune_source_{index} values "
                f"({base_ts + index * 1000}, {index}, {index + 100}) "
                f"({base_ts + index * 1000 + 1}, {index + 10}, null)"
            )

        for precision in ("ms", "us", "ns"):
            target_db = f"vtb_prune_{precision}"
            tdSql.execute(f"drop database if exists {target_db}")
            tdSql.execute(f"create database {target_db} precision '{precision}' vgroups 1")
            try:
                tdSql.execute(
                    f"create stable {target_db}.vstb (ts timestamp, v int, sparse int) "
                    "tags(g int) virtual 1"
                )
                for index in range(6):
                    tdSql.execute(
                        f"create vtable {target_db}.child_{index} "
                        f"(v from {source_db}.prune_source_{index}.v, "
                        f"sparse from {source_db}.prune_source_{index}.sparse) "
                        f"using {target_db}.vstb tags({index})"
                    )
                for flushed in (False, True):
                    if flushed:
                        tdSql.execute(f"flush database {source_db}")
                    for function, expected in (("last", (15, 105)), ("first", (0, 100))):
                        tdSql.query(
                            f"select {function}(v), {function}(sparse) from {target_db}.vstb"
                        )
                        tdSql.checkRows(1)
                        tdSql.checkData(0, 0, expected[0])
                        tdSql.checkData(0, 1, expected[1])
            finally:
                tdSql.execute(f"drop database {target_db}")
        print("LAST dynamic prune precision [ passed ]")

    def test_last_dynamic_prune_precision(self):
        """Query: virtual aggregate pruning across timestamp precisions

        1. Check FIRST and LAST across six source tables with sparse values.
        2. Cover ms, us and ns virtual databases before and after flushing sources.

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci,virtual

        Jira: None

        History:
            - 2026-11-13 Joey Sima Created
        """
        self.do_last_dynamic_prune_precision()

    def test_partitioned_aggregate_iteration(self):
        """Query: virtual aggregate group iterator lifetime

        1. Check batch and single-child dispatch with filtered tag groups.
        2. Consume multiple children in one group and stop after a limited group.
        3. Repeat queries and cover empty virtual children.

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci,virtual

        Jira: None

        History:
            - 2026-11-13 Joey Sima Created
        """
        self.do_partitioned_aggregate_iteration()

    def test_select_function_pushdown_result(self):
        """Query: independent selection aggregates on multi-source virtual tables.

        1. Compare MIN, MAX, FIRST, LAST and MODE results with golden answers.
        2. Cover sparse timestamps, NULLs, empty sources, scalar expressions and time ranges.
        3. Check mixed aggregates and row-dependent selectors before and after flush.

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci,virtual

        Jira: None

        History:
            - 2026-11-10 Joey Sima Created
        """
        self.do_select_function_pushdown("test_vtable_select_function_pushdown")

    def test_unreferenced_column_null_result(self):
        """Query: replace unreferenced virtual data columns with typed NULLs.

        1. Reuse shared source data for virtual normal and child tables.
        2. Check LAST companions, expressions, predicates, grouping and subqueries.
        3. Preserve NULL-only and empty inputs, aliases and result types across flush.
        4. Check the count when every SESSION timestamp is NULL.
        5. Check positional FILL values, TIME_ALIGN inputs, WINDOW_TIME keys and correlated parameters.

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci,virtual

        Jira: None

        History:
            - 2026-11-16 Joey Sima Created for defect 7170417049
        """
        self.do_unreferenced_column_null("test_vtable_unreferenced_column_null")

    def test_unreferenced_column_null_plan(self):
        """Plan: eliminate single-source virtual scans with unreferenced columns.

        1. Check descending LAST scans after rewriting selection companions to NULL.
        2. Cover virtual normal and child tables with scalar NULL expressions.
        3. Preserve virtual supertable bindings and multiple-source row merging.
        4. Retain the scan that supplies the unbound SESSION time key.

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci,virtual

        Jira: None

        History:
            - 2026-11-16 Joey Sima Created for defect 7170417049
        """
        self.do_unreferenced_column_null("test_vtable_plan_unreferenced_column_null")

    def test_select_function_pushdown_plan(self):
        """Plan: selection aggregate pushdown and merged-row safety boundaries.

        1. Compare EXPLAIN plans for five independent single-row selectors.
        2. Keep companions, LAST_ROW, multi-row selectors and virtual timestamps above merging.
        3. Preserve filters and aggregates with cross-source expressions.

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: common,ci,virtual

        Jira: None

        History:
            - 2026-11-10 Joey Sima Created
        """
        self.do_select_function_pushdown("test_vtable_plan_select_function_pushdown")

    def test_virtual_table_plan(self):
        """Query: virtual stable query plan

        1. test select super table agg's plan

        Catalog:
            - VirtualTable

        Since: v3.3.8.0

        Labels: virtual,integration,functional

        Jira: None

        History:
            - 2025-12-19 Jing Sima created

        """

        self.run_normal_query("test_vstable_plan_test_agg")
        self.run_normal_query("test_vstable_plan_test_agg_group_by")

