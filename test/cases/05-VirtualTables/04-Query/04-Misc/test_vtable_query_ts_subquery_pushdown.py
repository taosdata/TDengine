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
from new_test_framework.utils import tdLog, tdSql, tdCom
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from vtable_util import VtableQueryUtil


class TestVTableQueryTsSubqueryPushdown:
    def setup_class(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_ts_subquery_pushdown_env()

    def run_normal_query(self, test_case):
        tdLog.info(f"test case : {test_case}.")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", f"{test_case}.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", f"{test_case}.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, test_case)

    def test_vtable_ts_subquery_query(self):
        """Query: virtual table ts filter with renamed origin ts column

        1. validate direct ts range filter when origin ts column is not named ts
        2. validate scalar-subquery ts range filter on virtual normal/child/super tables
        3. validate scalar-subquery ts range filter on multi-origin virtual normal table
        4. validate scalar subqueries using filtered `first/last(...)` timestamps

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2026-3-9 Jing Sima Added

        """

        self.run_normal_query("test_vtable_ts_subquery_query")

    def test_union_join_ts_filter_with_vtable(self):
        """Query: UNION/JOIN timestamp filters with virtual table branches

        1. validate UNION ALL outer ts filter/order keeps projected ts values
        2. validate JOIN-side ts filter/order keeps projected ts values
        3. validate virtual-table branches can still apply timestamp pushdown

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-6-5 Added regression for UNION ALL set-op ts pushdown with vtable branch
            - 2026-6-6 Added JOIN timestamp pushdown scenarios

        """

        self.run_normal_query("test_vtable_union_join_ts_pushdown")
