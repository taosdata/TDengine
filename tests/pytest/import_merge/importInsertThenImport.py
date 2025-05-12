###################################################################
#       Copyright (c) 2016 by TAOS Technologies, Inc.
#             All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import sys
import taos
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def run(self):
        self.ntables = 1
        self.startTime = 1520000010000

        tdSql.prepare()

        tdLog.info("================= step1")
        tdLog.info("create 1 table")
        tdSql.execute('create table tb1 (ts timestamp, i int)')

        tdLog.info("================= step2")
        tdLog.info("insert 100 sequential data")
        startTime = self.startTime
        for rid in range(1, 101):
            tdSql.execute(
                'insert into tb1 values(%ld, %d)' %
                (startTime + rid, rid))

        tdLog.info("================= step3")
        tdSql.query('select * from tb1')
        tdSql.checkRows(100)

        tdLog.info("================= step4")
        tdLog.info("import 100 sequential data")
        startTime = self.startTime
        for rid in range(1, 101):
            tdSql.execute(
                'import into tb1 values(%ld, %d)' %
                (startTime + rid, 100 + rid))

        tdSql.query('select * from tb1')
        tdSql.checkRows(100)
        tdSql.checkData(0, 1, 1)

        tdLog.info("================= step5")
        tdDnodes.stop(1)
        tdDnodes.start(1)
        #tdLog.sleep(10)

        tdLog.info("================= step6")
        tdLog.info("import 100 sequential data again")
        startTime = self.startTime
        for rid in range(1, 101):
            tdSql.execute(
                'import into tb1 values(%ld, %d)' %
                (startTime + rid, 100 + rid))

        tdLog.info("================= step7")
        tdSql.query('select * from tb1')
        tdSql.checkRows(100)
        tdSql.checkData(0, 1, 1)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
