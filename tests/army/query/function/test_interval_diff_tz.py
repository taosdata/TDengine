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

from frame import etool
from frame.etool import *
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame.common import *

class TDTestCase(TBase):
    clientCfgDict = { "timezone": "UTC" }
    updatecfgDict = {
        "timezone": "UTC-8",
        "clientCfg": clientCfgDict,
    }

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    def insert_data(self):
        tdLog.info("insert interval test data.")
        # taosBenchmark run
        json = etool.curFile(__file__, "interval.json")
        etool.benchMark(json = json)

    def query_test(self):
        # read sql from .sql file and execute
        tdLog.info("test normal query.")
        self.sqlFile = etool.curFile(__file__, f"in/interval.in")
        self.ansFile = etool.curFile(__file__, f"ans/interval_diff_tz.csv")

        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, "interval_diff_tz")

    def run(self):
        self.insert_data()
        self.query_test()

    def stop(self):
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
