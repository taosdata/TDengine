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
from new_test_framework.utils import tdLog, tdCom
import os
from vtable_util import VtableQueryUtil


class TestVTableQuerySameDBStbWindowVtbRef:

    def setup_class(cls):
        vtbUtil = VtableQueryUtil()
        vtbUtil.prepare_same_db_vtables(ref_mode="virtual_ref")

    def run_normal_query(self, testCase):
        tdLog.info(f"test case : {testCase}.")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", f"{testCase}.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", f"{testCase}.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

    def test_select_virtual_super_table(self):
        """Query: same db virtual stable window query with virtual table ref

        1. test vstable select super table interval
        2. test vstable select super table session
        3. test vstable select super table event
        4. test vstable select super table count
        5. test vstable select super table state in mode 0
        6. test vstable select super table state in mode 1
        7. test vstable select super table state in mode 2

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-26 Jing Sima created

        """
        self.run_normal_query("test_vstable_select_test_interval")
        self.run_normal_query("test_vstable_select_test_session")
        self.run_normal_query("test_vstable_select_test_event")
        self.run_normal_query("test_vstable_select_test_count")
        self.run_normal_query("test_vstable_select_test_state_mode_0")
        self.run_normal_query("test_vstable_select_test_state_mode_1")
        self.run_normal_query("test_vstable_select_test_state_mode_2")
