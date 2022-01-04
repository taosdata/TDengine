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
        tdSql.prepare()
        ts_now = 1633017600000
        tdLog.info("=============== step1")
        tdSql.execute("create table mt(ts timestamp, tbcol int, tbcol2 float) TAGS(tgcol int)")
        for i in range(5):
            tdSql.execute("create table tb%d using mt tags(%d)" % (i, i))
            for j in range(rowNum):
                tdSql.execute("insert into tb%d values(%d, %d, %d)" % (i, ts_now, j, j))
                ts_now += 1000
        time.sleep(0.1)

        tdLog.info("=============== step2")
        tdSql.query("select count(*), count(tbcol), count(tbcol2) from mt interval(10s)")
        tdSql.execute("create table st as select count(*), count(tbcol), count(tbcol2) from mt interval(10s)")

        tdLog.info("=============== step3")
        start = time.time()
        tdSql.waitedQuery("select * from st", 1, 180)
        delay = int(time.time() - start) + 80
        v = tdSql.getData(0, 3)
        if v != 10:
            tdLog.exit("value is %d, expect is 10." % v)

        tdLog.info("=============== step4")
        for i in range(5, 10):
            tdSql.execute("create table tb%d using mt tags(%d)" % (i, i))
            for j in range(rowNum):
                tdSql.execute("insert into tb%d values(%d, %d, %d)" % (i, ts_now, j, j))
                ts_now += 1000                

        tdLog.info("=============== step5")
        maxValue = 0
        for i in range(delay):
            time.sleep(1)
            tdSql.query("select * from st order by ts desc")
            v = tdSql.getData(0, 3)
            if v > maxValue:
                maxValue = v
            if v >= 10:
                break

        if maxValue < 10:
            tdLog.exit("value is %d, expect is 10" % maxValue)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())


