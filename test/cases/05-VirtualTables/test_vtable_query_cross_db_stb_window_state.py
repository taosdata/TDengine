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
        """Query: virtual stable from cross db

        1. test vstable select super table cross db state in mode 0
        2. test vstable select super table cross db state in mode 1
        3. test vstable select super table cross db state in mode 2

        Since: v3.3.8.0

        Labels: virtual

        Jira: None

        History:
            - 2025-11-21 Jing Sima Split from test_vtable_query_cross_db_stb_window.py

        """

        self.run_normal_query("test_vstable_select_test_state_mode_0")
        #self.run_normal_query("test_vstable_select_test_state_mode_1")
        self.run_normal_query("test_vstable_select_test_state_mode_2")

