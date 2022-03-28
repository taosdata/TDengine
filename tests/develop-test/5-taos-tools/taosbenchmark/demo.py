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
import os
import subprocess
import time

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def caseDescription(self):
        '''
        [TD-13823] taosBenchmark test cases
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        cmd = "taosBenchmark -n 100 -t 100 -y"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("use test")
        tdSql.query("select count(*) from test.meters")
        tdSql.checkData(0, 0, 10000)

        tdSql.query("describe meters")
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "current")
        tdSql.checkData(1, 1, "FLOAT")
        tdSql.checkData(2, 0, "voltage")
        tdSql.checkData(2, 1, "INT")
        tdSql.checkData(3, 0, "phase")
        tdSql.checkData(3, 1, "FLOAT")
        tdSql.checkData(4, 0, "groupid")
        tdSql.checkData(4, 1, "INT")
        tdSql.checkData(4, 3, "TAG")
        tdSql.checkData(5, 0, "location")
        tdSql.checkData(5, 1, "BINARY")
        tdSql.checkData(5, 2, 16)
        tdSql.checkData(5, 3, "TAG")

        tdSql.query("select count(*) from test.meters where groupid >= 0")
        tdSql.checkData(0, 0, 10000)

        tdSql.query("select count(*) from test.meters where location = 'beijing' or location = 'shanghai'")
        tdSql.checkData(0, 0, 10000)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())