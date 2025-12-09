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
    clientCfgDict = {"timezone": "UTC"}
    updatecfgDict = {
        "timezone": "UTC-8",
        "clientCfg": clientCfgDict,
    }

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        tdLog.info("insert interval test data.")
        # taosBenchmark run
        json = etool.curFile(__file__, "interval.json")
        etool.benchMark(json=json)

    def test_interval_diff_tz(self):
        """Interval: timezone

        test interval with client and server using different timezone

        Catalog:
            - Timeseries:TimeWindow

        Since: v3.3.0.0

        Labels: decimal

        History:
            - 2024-9-14 Feng Chao Created
            - 2025-5-08 Huo Hong Migrated to new test framework

        """
        # read sql from .sql file and execute
        tdLog.info("test normal query.")
        self.sqlFile = etool.curFile(__file__, f"in/interval.in")
        self.ansFile = etool.curFile(__file__, f"ans/interval_diff_tz.csv")

        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "interval_diff_tz")
