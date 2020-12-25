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
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        self.ntables = 1
        self.startTime = 1520000010000

        tdDnodes.stop(1)
        tdDnodes.deploy(1)
        tdDnodes.start(1)

        tdSql.execute('reset query cache')
        tdSql.execute('drop database if exists db')
        tdSql.execute('create database db')
        tdSql.execute('use db')

        tdLog.info("================= step1")
        tdLog.info("create 1 table")
        tdSql.execute('create table tb1 (ts timestamp, i int)')

        tdLog.info("================= step2")
        tdLog.info("import 10 sequential data")
        startTime = self.startTime
        for rid in range(1, 11):
            tdSql.execute(
                'import into tb1 values(%ld, %d)' %
                (startTime + rid, rid))

        tdLog.info("================= step3")
        tdSql.query('select * from tb1')
        tdSql.checkRows(10)

        tdLog.info("================= step4")
        tdLog.info("import 1 data after")
        startTime = self.startTime + 11
        tdSql.execute('import into tb1 values(%ld, %d)' % (startTime, rid))

        tdLog.info("================= step5")
        tdDnodes.forcestop(1)
        tdDnodes.start(1)
        #tdLog.sleep(10)

        tdLog.info("================= step6")
        tdSql.query('select * from tb1')
        tdSql.checkRows(11)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
