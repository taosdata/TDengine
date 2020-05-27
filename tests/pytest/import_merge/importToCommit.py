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
        tdSql.execute('create database db cache 128 maxtables 10')
        tdSql.execute('use db')

        tdLog.info("================= step1")
        tdLog.info("create 1 table")
        tdSql.execute('create table tb1 (ts timestamp, i int)')
        tdLog.info(
            "one block can import 38 records and totally there are 40 blocks")

        tdLog.info("================= step2")
        tdLog.info('insert data until the first commit')
        dnodesDir = tdDnodes.getDnodesRootDir()
        dataDir = dnodesDir + '/dnode1/data/vnode'
        startTime = self.startTime
        rid0 = 1
        while (True):
            sqlcmd = 'insert into tb1 values(%ld, %d)' % (
                startTime + rid0 * 2, rid0)
            tdSql.execute(sqlcmd)
            rid0 += 1
            vnodes = os.listdir(dataDir)
            if (len(vnodes) > 0):
                tdLog.info("data is committed, stop inserting")
                break

        tdLog.info("================= step5")
        tdLog.info("import 1 data before ")
        startTime = self.startTime
        sqlcmd = ['import into tb1 values']
        for rid in range(3, 4):
            sqlcmd.append('(%ld, %d)' % (startTime + rid, rid))
        tdSql.execute(" ".join(sqlcmd))

        tdLog.info("================= step6")
        tdSql.execute('reset query cache')
        tdSql.query('select * from tb1 order by ts desc')
        tdSql.checkRows(rid0 - 1 + 1)

        tdLog.info("================= step7")
        tdSql.execute('reset query cache')
        tdSql.query('select count(*) from tb1')
        tdSql.checkData(0, 0, rid0 - 1 + 1)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
