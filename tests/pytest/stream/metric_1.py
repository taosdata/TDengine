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

    def createFuncStream(self, expr, suffix, value):
        tbname = "strm_" + suffix
        tdLog.info("create stream table %s" % tbname)
        tdSql.query("select %s from stb interval(1d)" % expr)
        tdSql.checkData(0, 1, value)
        tdSql.execute("create table %s as select %s from stb interval(1d)" % (tbname, expr))

    def checkStreamData(self, suffix, value):
        sql = "select * from strm_" + suffix
        tdSql.waitedQuery(sql, 1, 120)
        tdSql.checkData(0, 1, value)

    def run(self):
        tbNum = 10
        rowNum = 20
        ts_begin = 1633017600000

        tdSql.prepare()

        tdLog.info("===== preparing data =====")
        tdSql.execute(
            "create table stb(ts timestamp, tbcol int, tbcol2 float) tags(tgcol int)")
        for i in range(tbNum):
            tdSql.execute("create table tb%d using stb tags(%d)" % (i, i))
            for j in range(rowNum):
                tdSql.execute(
                    "insert into tb%d values (%d, %d, %d)" %
                    (i, ts_begin + j, j, j))
        time.sleep(0.1)

        self.createFuncStream("count(*)", "c1", 200)
        self.createFuncStream("count(tbcol)", "c2", 200)
        self.createFuncStream("count(tbcol2)", "c3", 200)
        self.createFuncStream("avg(tbcol)", "av", 9.5)
        self.createFuncStream("sum(tbcol)", "su", 1900)
        self.createFuncStream("min(tbcol)", "mi", 0)
        self.createFuncStream("max(tbcol)", "ma", 19)
        self.createFuncStream("first(tbcol)", "fi", 0)
        self.createFuncStream("last(tbcol)", "la", 19)
        #tdSql.query("select stddev(tbcol) from stb interval(1d)")
        #tdSql.query("select leastsquares(tbcol, 1, 1) from stb interval(1d)")
        tdSql.query("select top(tbcol, 1) from stb interval(1d)")
        tdSql.query("select bottom(tbcol, 1) from stb interval(1d)")
        #tdSql.query("select percentile(tbcol, 1) from stb interval(1d)")
        #tdSql.query("select diff(tbcol) from stb interval(1d)")

        tdSql.query("select count(tbcol) from stb where ts < now + 4m interval(1d)")
        tdSql.checkData(0, 1, 200)
        #tdSql.execute("create table strm_wh as select count(tbcol) from stb where ts < now + 4m interval(1d)")

        self.createFuncStream("count(tbcol)", "as", 200)

        tdSql.query("select count(tbcol) from stb interval(1d) group by tgcol")
        tdSql.checkData(0, 1, 20)

        tdSql.query("select count(tbcol) from stb where ts < now + 4m interval(1d) group by tgcol")
        tdSql.checkData(0, 1, 20)

        self.checkStreamData("c1", 200)
        self.checkStreamData("c2", 200)
        self.checkStreamData("c3", 200)
        self.checkStreamData("av", 9.5)
        self.checkStreamData("su", 1900)
        self.checkStreamData("mi", 0)
        self.checkStreamData("ma", 19)
        self.checkStreamData("fi", 0)
        self.checkStreamData("la", 19)
        #self.checkStreamData("wh", 200)
        self.checkStreamData("as", 200)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())


