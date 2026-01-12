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
class TestVTableQuerySameDBCtb:

    def setup_class(cls):
        vtbUtil = VtableQueryUtil()
        vtbUtil.prepare_same_db_vtables()

    def run_normal_query(self, testCase):
        # read sql from .sql file and execute
        tdLog.info(f"test case : {testCase}.")
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", f"{testCase}.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", f"{testCase}.ans")

        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

    def test_select_virtual_child_table(self):
        """Query: virtual child table

        1. test vctable select child table projection
        2. test vctable select child table projection filter
        3. test vctable select child table projection timerange filter
        4. test vctable select child table interval
        5. test vctable select child table session
        6. test vctable select child table event
        7. test vctable select child table count
        8. test vctable select child table partition
        9. test vctable select child table group
        10. test vctable select child table orderby

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2025-3-15 Jing Sima Created
            - 2025-5-6 Huo Hong Migrated to new test framework
            - 2025-10-17 Jing Sima Add timerange filter test case
            - 2025-11-21 Jing Sima Split Add state different mode test
            - 2026-1-6 Jing Sima Split state window test to another file
        """
        self.run_normal_query("test_vctable_select_test_projection")
        self.run_normal_query("test_vctable_select_test_projection_filter")
        self.run_normal_query("test_vctable_select_test_projection_timerange_filter")
        self.run_normal_query("test_vctable_select_test_function")

        self.run_normal_query("test_vctable_select_test_interval")
        self.run_normal_query("test_vctable_select_test_session")
        self.run_normal_query("test_vctable_select_test_event")
        self.run_normal_query("test_vctable_select_test_count")

        self.run_normal_query("test_vctable_select_test_partition")
        self.run_normal_query("test_vctable_select_test_group")
        self.run_normal_query("test_vctable_select_test_orderby")

