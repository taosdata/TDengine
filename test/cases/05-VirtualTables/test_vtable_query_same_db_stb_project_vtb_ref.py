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
import os
from vtable_util import VtableQueryUtil


class TestVTableQuerySameDBStbProjectVtbRef:

    def setup_class(cls):
        vtbUtil = VtableQueryUtil()
        vtbUtil.prepare_same_db_vtables(ref_mode="virtual_ref")
        tdSql.execute('alter local "multiResultFunctionStarReturnTags" "1";')

    def run_normal_query(self, testCase):
        tdLog.info(f"test case : {testCase}.")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", f"{testCase}.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", f"{testCase}.ans")
        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

    def test_select_virtual_super_table(self):
        """Query: same db virtual stable project query with virtual table ref

        1. test vstable select super table projection
        2. test vstable select super table projection filter
        3. test vstable select super table projection timerange filter
        4. test vstable select super table function

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-26 Jing Sima created

        """
        self.run_normal_query("test_vstable_select_test_projection")
        self.run_normal_query("test_vstable_select_test_projection_filter")
        self.run_normal_query("test_vstable_select_test_projection_timerange_filter")
        self.run_normal_query("test_vstable_select_test_function")
