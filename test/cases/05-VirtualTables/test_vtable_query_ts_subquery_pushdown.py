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

        Labels: common,ci

        Jira: None

        History:
            - 2026-3-9 Jing Sima Added

        """

        self.run_normal_query("test_vtable_ts_subquery_query")
