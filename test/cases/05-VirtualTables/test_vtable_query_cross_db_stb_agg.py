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
from test_vtable_util import TestVtableQueryUtil

class TestVTableQuerySameDBStbProject:

    def setup_class(cls):
        vtbUtil = TestVtableQueryUtil()
        vtbUtil.prepare_cross_db_vtables(mode = 2)

    def run_normal_query(self, testCase):
        # read sql from .sql file and execute
        tdLog.info(f"test case : {testCase}.")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", f"{testCase}.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", f"{testCase}.ans")

        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

    def test_select_virtual_super_table(self):
        """Query: virtual stable from different db

        1. test vstable select super table agg
        2. test vstable select super table agg with tag condition
        3. test vstable select super table agg with time condition
        4. test vstable select super table agg with partition expression

        Catalog:
            - VirtualTable

        Since: v3.3.8.0

        Labels: virtual

        Jira: None

        History:
            - 2025-12-19 Jing Sima created

        """
        self.run_normal_query("test_vstable_select_test_agg")
        self.run_normal_query("test_vstable_select_test_agg_tag_cond")
        self.run_normal_query("test_vstable_select_test_agg_time_cond")
        self.run_normal_query("test_vstable_select_test_agg_partition_expr")

