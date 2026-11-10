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

from vtable_util import VtableQueryUtil

class TestVTablePlanAggOptimize:
    def setup_class(cls):
        vtbUtil = VtableQueryUtil()
        vtbUtil.prepare_same_db_vtables(mode = 2)

    def run_normal_query(self, testCase):
        # read sql from .sql file and execute
        tdLog.info(f"test case : {testCase}.")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", f"{testCase}.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", f"{testCase}.ans")

        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

    def do_select_function_pushdown(self, test_case):
        self.run_normal_query(test_case)
        print(f"{test_case} [ passed ]")

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

