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
from new_test_framework.utils import tdLog, tdSql, tdCom
from vtable_util import VtableQueryUtil
import os


class TestVTablePlanPushdownTsSubquery:
    def setup_class(cls):
        vtb_util = VtableQueryUtil()
        vtb_util.prepare_ts_subquery_pushdown_env()

    def run_normal_query(self, test_case):
        tdLog.info(f"test case : {test_case}.")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", f"{test_case}.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", f"{test_case}.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, test_case)

    def test_vtable_plan_ts_subquery(self):
        """Query: virtual table ts subquery explain plan

        1. validate explain/analyze plan with ts scalar-subquery range conditions
        2. validate multi-origin virtual table can push ts scalar-subquery conditions to every origin scan
        3. validate analyze output shows origin table scans only read filtered rows
        4. validate filtered `first/last(...)` scalar subqueries keep the same pushdown shape

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-9 Jing Sima Added

        """

        self.run_normal_query("test_vtable_plan_ts_subquery")
