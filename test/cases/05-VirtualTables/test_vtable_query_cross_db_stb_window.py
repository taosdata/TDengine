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

class TestVtableQueryCrossDbStbWindow:
    updatecfgDict = {
        "supportVnodes":"1000",
    }
    def setup_class(cls):
        vtbUtil = VtableQueryUtil()
        vtbUtil.prepare_same_db_vtables()

    def run_normal_query(self, testCase):
        # read sql from .sql file and execute
        tdLog.info(f"test case : {testCase}.")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", f"{testCase}.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", f"{testCase}.ans")

        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

    def test_select_virtual_super_table(self):
        """Query: v-stable crossdb window query

        1. test vstable select super table cross db interval
        2. test vstable select super table cross db session
        3. test vstable select super table cross db event
        4. test vstable select super table cross db count
        5. test vstable select super table same db state in mode 0
        6. test vstable select super table same db state in mode 1
        7. test vstable select super table same db state in mode 2

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2025-3-15 Jing Sima Created
            - 2025-5-6 Huo Hong Migrated to new test framework
            - 2025-11-21 Jing Sima Remove state test case, split to test_vtable_query_cross_db_stb_window_state.py
            - 2026-1-14 Jing Sima Add back state different mode test
        """

        self.run_normal_query("test_vstable_select_test_interval")
        self.run_normal_query("test_vstable_select_test_session")
        self.run_normal_query("test_vstable_select_test_event")
        self.run_normal_query("test_vstable_select_test_count")
        self.run_normal_query("test_vstable_select_test_state_mode_0")
        self.run_normal_query("test_vstable_select_test_state_mode_1")
        self.run_normal_query("test_vstable_select_test_state_mode_2")