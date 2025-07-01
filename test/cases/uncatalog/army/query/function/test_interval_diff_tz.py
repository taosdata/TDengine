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

class TestIntervalDiffTz:
    clientCfgDict = { "timezone": "UTC" }
    updatecfgDict = {
        "timezone": "UTC-8",
        "clientCfg": clientCfgDict,
    }

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def insert_data(self):
        tdLog.info("insert interval test data.")
        # taosBenchmark run
        json = etool.curFile(__file__, "interval.json")
        etool.benchMark(json = json)

    def query_run(self):
        # read sql from .sql file and execute
        tdLog.info("test normal query.")
        self.sqlFile = etool.curFile(__file__, f"in/interval.in")
        self.ansFile = etool.curFile(__file__, f"ans/interval_diff_tz.csv")

        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "interval_diff_tz")

    def test_interval_diff_tz(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """
        self.insert_data()
        self.query_run()

        tdLog.success(f"{__file__} successfully executed")

