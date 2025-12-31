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
        vtbUtil.prepare_same_db_vtables()
        tdSql.execute(f'alter local "multiResultFunctionStarReturnTags" "1";')
    def teardown_class(cls):
        vtbUtil = TestVtableQueryUtil()
        #vtbUtil.clean_up_same_db_vtables()

    def run_normal_query(self, testCase):
        # read sql from .sql file and execute
        tdLog.info(f"test case : {testCase}.")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", f"{testCase}.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", f"{testCase}.ans")

        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

    def test_select_virtual_super_table(self):
        """Query: virtual stable from same db

        1. test vstable select super table projection
        2. test vstable select super table projection filter
        3. test vstable select super table projection timerange filter
        4. test vstable select super table function

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2025-3-15 Jing Sima Created
            - 2025-5-6 Huo Hong Migrated to new test framework
            - 2025-10-23 Jing Sima Split from test_vtable_query.py

        """
        self.run_normal_query("test_vstable_select_test_projection")
        self.run_normal_query("test_vstable_select_test_projection_filter")
        self.run_normal_query("test_vstable_select_test_projection_timerange_filter")
        self.run_normal_query("test_vstable_select_test_function")

