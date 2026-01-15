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

class TestVTableQuerySameDBNtbWindowState:
    def setup_class(cls):
        vtbUtil = VtableQueryUtil()
        vtbUtil.prepare_same_db_vtables()

    def run_normal_query(self, testCase):
        # read sql from .sql file and execute
        tdLog.info(f"test case : {testCase}.")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", f"{testCase}.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", f"{testCase}.ans")

        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)


    def test_select_virtual_normal_table_window_state(self):
        """Query: virtual normal table

        1. test vntable select normal table state mode 0
        2. test vntable select normal table state mode 1
        3. test vntable select normal table state mode 2

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual

        Jira: None

        History:
            - 2026-1-6 Jing Sima Split Add state different mode test
        """

        self.run_normal_query("test_vtable_select_test_state_mode_0")
        self.run_normal_query("test_vtable_select_test_state_mode_1")
        self.run_normal_query("test_vtable_select_test_state_mode_2")
        self.run_normal_query("test_vtable_select_test_state_mode_0_batch_hint")
        self.run_normal_query("test_vtable_select_test_state_mode_1_batch_hint")
        self.run_normal_query("test_vtable_select_test_state_mode_2_batch_hint")
        self.run_normal_query("test_vtable_select_test_state_mode_0_single_hint")
        self.run_normal_query("test_vtable_select_test_state_mode_1_single_hint")
        self.run_normal_query("test_vtable_select_test_state_mode_2_single_hint")


