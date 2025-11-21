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

class TestVtableQueryCrossDBNtb:
    updatecfgDict = {
        "supportVnodes":"1000",
    }
    def setup_class(cls):
        vtbUtil = TestVtableQueryUtil()
        vtbUtil.prepare_same_db_vtables()

    def run_normal_query(self, testCase):
        # read sql from .sql file and execute
        tdLog.info(f"test case : {testCase}.")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", f"{testCase}.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", f"{testCase}.ans")

        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)


    def test_select_virtual_normal_table(self):
        """Query: v-ntable crossdb query

        1. test vntable select normal table cross db projection
        2. test vntable select normal table cross db projection filter
        3. test vntable select normal table cross db projection timerange filter
        4. test vntable select normal table cross db function
        5. test vntable select normal table cross db interval
        6. test vntable select normal table cross db state mode 0
        7. test vntable select normal table cross db state mode 1
        8. test vntable select normal table cross db state mode 2
        9. test vntable select normal table cross db session
        10. test vntable select normal table cross db event
        11. test vntable select normal table cross db count
        12. test vntable select normal table cross db partition
        13. test vntable select normal table cross db group
        14. test vntable select normal table cross db orderby

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2025-3-15 Jing Sima Created
            - 2025-5-6 Huo Hong Migrated to new test framework
            - 2025-11-21 Jing Sima Split Add state different mode test

        """
        self.run_normal_query("test_vtable_select_test_projection")
        self.run_normal_query("test_vtable_select_test_projection_filter")
        self.run_normal_query("test_vtable_select_test_projection_timerange_filter")
        #self.run_normal_query("test_vtable_select_test_function")

        self.run_normal_query("test_vtable_select_test_interval")
        self.run_normal_query("test_vtable_select_test_state_mode_0")
        self.run_normal_query("test_vtable_select_test_state_mode_1")
        self.run_normal_query("test_vtable_select_test_state_mode_2")
        self.run_normal_query("test_vtable_select_test_session")
        self.run_normal_query("test_vtable_select_test_event")
        self.run_normal_query("test_vtable_select_test_count")

        self.run_normal_query("test_vtable_select_test_partition")
        self.run_normal_query("test_vtable_select_test_group")
        self.run_normal_query("test_vtable_select_test_orderby")
