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

import sys
import time
import taos
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        rowNum = 200
        totalNum = 200
        tdSql.prepare()

        tdLog.info("=============== step1")
        tdSql.execute("create table mt(ts timestamp, tbcol int, tbcol2 float) TAGS(tgcol int)")
        for i in range(5):
            tdSql.execute("create table tb%d using mt tags(%d)" % (i, i))
            for j in range(rowNum):
                tdSql.execute("insert into tb%d values(now + %ds, %d, %d)" % (i, j, j, j))
        time.sleep(0.1)

        tdLog.info("=============== step2")
        tdSql.query("select count(*), count(tbcol), count(tbcol2) from mt interval(10s)")
        tdSql.execute("create table st as select count(*), count(tbcol), count(tbcol2) from mt interval(10s)")

        tdLog.info("=============== step3")
        tdSql.waitedQuery("select * from st", 1, 120)
        v = tdSql.getData(0, 3)
        if v >= 51:
            tdLog.exit("value is %d, which is larger than 51" % v)

        tdLog.info("=============== step4")
        for i in range(5, 10):
            tdSql.execute("create table tb%d using mt tags(%d)" % (i, i))
            for j in range(rowNum):
                tdSql.execute("insert into tb%d values(now + %ds, %d, %d)" % (i, j, j, j))

        tdLog.info("=============== step5")
        tdLog.sleep(30)
        tdSql.waitedQuery("select * from st order by ts desc", 1, 120)
        v = tdSql.getData(0, 3)
        if v <= 51:
            tdLog.exit("value is %d, which is smaller than 51" % v)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())


