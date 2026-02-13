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
from new_test_framework.utils import tdLog, tdSql, etool, tdCom
import os
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

    def test_virtual_table_plan(self):
        """Query: virtual stable query plan

        1. test select super table agg's plan

        Catalog:
            - VirtualTable

        Since: v3.3.8.0

        Labels: virtual

        Jira: None

        History:
            - 2025-12-19 Jing Sima created

        """

        self.run_normal_query("test_vstable_plan_test_agg")

