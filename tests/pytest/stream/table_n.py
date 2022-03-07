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
        tbNum = 10
        rowNum = 20

        tdSql.prepare()

        tdLog.info("===== preparing data =====")
        tdSql.execute(
            "create table stb(ts timestamp, tbcol int, tbcol2 float) tags(tgcol int)")
        for i in range(tbNum):
            tdSql.execute("create table tb%d using stb tags(%d)" % (i, i))
            for j in range(rowNum):
                tdSql.execute(
                    "insert into tb%d values (now - %dm, %d, %d)" %
                    (i, 1440 - j, j, j))
        time.sleep(0.1)

        tdLog.info("===== step 1 =====")
        tdSql.query("select count(*), count(tbcol), count(tbcol2) from tb1 interval(1d)")
        tdSql.checkData(0, 1, rowNum)
        tdSql.checkData(0, 2, rowNum)
        tdSql.checkData(0, 3, rowNum)

        tdLog.info("===== step 2 =====")
        tdSql.execute("create table strm_c3 as select count(*), count(tbcol), count(tbcol2) from tb1 interval(1d)")

        tdLog.info("===== step 3 =====")
        tdSql.execute("create table strm_c32 as select count(*), count(tbcol) as c1, count(tbcol2) as c2, count(tbcol) as c3, count(tbcol) as c4, count(tbcol) as c5, count(tbcol) as c6, count(tbcol) as c7, count(tbcol) as c8, count(tbcol) as c9, count(tbcol) as c10, count(tbcol) as c11, count(tbcol) as c12, count(tbcol) as c13, count(tbcol) as c14, count(tbcol) as c15, count(tbcol) as c16, count(tbcol) as c17, count(tbcol) as c18, count(tbcol) as c19, count(tbcol) as c20, count(tbcol) as c21, count(tbcol) as c22, count(tbcol) as c23, count(tbcol) as c24, count(tbcol) as c25, count(tbcol) as c26, count(tbcol) as c27, count(tbcol) as c28, count(tbcol) as c29, count(tbcol) as c30 from tb1 interval(1d)")

        tdLog.info("===== step 4 =====")
        tdSql.query("select count(*), count(tbcol) as c1, count(tbcol2) as c2, count(tbcol) as c3, count(tbcol) as c4, count(tbcol) as c5, count(tbcol) as c6, count(tbcol) as c7, count(tbcol) as c8, count(tbcol) as c9, count(tbcol) as c10, count(tbcol) as c11, count(tbcol) as c12, count(tbcol) as c13, count(tbcol) as c14, count(tbcol) as c15, count(tbcol) as c16, count(tbcol) as c17, count(tbcol) as c18, count(tbcol) as c19, count(tbcol) as c20, count(tbcol) as c21, count(tbcol) as c22, count(tbcol) as c23, count(tbcol) as c24, count(tbcol) as c25, count(tbcol) as c26, count(tbcol) as c27, count(tbcol) as c28, count(tbcol) as c29, count(tbcol) as c30  from tb1 interval(1d)")
        tdSql.checkData(0, 1, rowNum)
        tdSql.checkData(0, 2, rowNum)
        tdSql.checkData(0, 3, rowNum)

        tdLog.info("===== step 5 =====")
        tdSql.execute("create table strm_c31 as select count(*), count(tbcol) as c1, count(tbcol2) as c2, count(tbcol) as c3, count(tbcol) as c4, count(tbcol) as c5, count(tbcol) as c6, count(tbcol) as c7, count(tbcol) as c8, count(tbcol) as c9, count(tbcol) as c10, count(tbcol) as c11, count(tbcol) as c12, count(tbcol) as c13, count(tbcol) as c14, count(tbcol) as c15, count(tbcol) as c16, count(tbcol) as c17, count(tbcol) as c18, count(tbcol) as c19, count(tbcol) as c20, count(tbcol) as c21, count(tbcol) as c22, count(tbcol) as c23, count(tbcol) as c24, count(tbcol) as c25, count(tbcol) as c26, count(tbcol) as c27, count(tbcol) as c28, count(tbcol) as c29, count(tbcol) as c30 from tb1 interval(1d)")

        tdLog.info("===== step 6 =====")
        tdSql.query("select avg(tbcol), sum(tbcol), min(tbcol), max(tbcol),  first(tbcol), last(tbcol) from tb1 interval(1d)")
        tdSql.checkData(0, 1, 9.5)
        tdSql.checkData(0, 2, 190)
        tdSql.checkData(0, 3, 0)
        tdSql.checkData(0, 4, 19)
        tdSql.checkData(0, 5, 0)
        tdSql.checkData(0, 6, 19)
        tdSql.execute("create table strm_avg as select avg(tbcol), sum(tbcol), min(tbcol), max(tbcol),  first(tbcol), last(tbcol) from tb1 interval(1d)")

        tdLog.info("===== step 7 =====")
        tdSql.query("select stddev(tbcol), leastsquares(tbcol, 1, 1), percentile(tbcol, 1) from tb1 interval(1d)")
        tdSql.checkData(0, 1, 5.766281297335398)
        tdSql.checkData(0, 3, 0.19)
        tdSql.execute("create table strm_ot as select stddev(tbcol), leastsquares(tbcol, 1, 1), percentile(tbcol, 1) from tb1 interval(1d)")

        tdLog.info("===== step 8 =====")
        tdSql.query("select avg(tbcol), sum(tbcol), min(tbcol), max(tbcol),  first(tbcol), last(tbcol), stddev(tbcol), percentile(tbcol, 1), count(tbcol), leastsquares(tbcol, 1, 1) from tb1 interval(1d)")
        tdSql.checkData(0, 1, 9.5)
        tdSql.checkData(0, 2, 190)
        tdSql.checkData(0, 3, 0)
        tdSql.checkData(0, 4, 19)
        tdSql.checkData(0, 5, 0)
        tdSql.checkData(0, 6, 19)
        tdSql.checkData(0, 7, 5.766281297335398)
        tdSql.checkData(0, 8, 0.19)
        tdSql.checkData(0, 9, rowNum)
        tdSql.execute("create table strm_to as select avg(tbcol), sum(tbcol), min(tbcol), max(tbcol),  first(tbcol), last(tbcol), stddev(tbcol), percentile(tbcol, 1), count(tbcol), leastsquares(tbcol, 1, 1) from tb1 interval(1d)")

        tdLog.info("===== step 9 =====")
        tdSql.query("select avg(tbcol), sum(tbcol), min(tbcol), max(tbcol),  first(tbcol), last(tbcol), stddev(tbcol), percentile(tbcol, 1), count(tbcol), leastsquares(tbcol, 1, 1) from tb1 where ts < now + 4m interval(1d)")
        tdSql.checkData(0, 9, rowNum)
        tdSql.execute("create table strm_wh as select avg(tbcol), sum(tbcol), min(tbcol), max(tbcol),  first(tbcol), last(tbcol), stddev(tbcol), percentile(tbcol, 1), count(tbcol), leastsquares(tbcol, 1, 1) from tb1 where ts < now + 4m interval(1d)")

        tdLog.info("===== step 10 =====")
        tdSql.waitedQuery("select * from strm_c3", 1, 120)
        tdSql.checkData(0, 1, rowNum)
        tdSql.checkData(0, 2, rowNum)
        tdSql.checkData(0, 3, rowNum)

        tdLog.info("===== step 11 =====")
        tdSql.waitedQuery("select * from strm_c31", 1, 30)
        for i in range(1, 10):
            tdSql.checkData(0, i, rowNum)

        tdLog.info("===== step 12 =====")
        tdSql.waitedQuery("select * from strm_avg", 1, 20)
        tdSql.checkData(0, 1, 9.5)
        tdSql.checkData(0, 2, 190)
        tdSql.checkData(0, 3, 0)
        tdSql.checkData(0, 4, 19)
        tdSql.checkData(0, 5, 0)
        tdSql.checkData(0, 6, 19)

        tdLog.info("===== step 13 =====")
        tdSql.waitedQuery("select * from strm_ot", 1, 20)
        tdSql.checkData(0, 1, 5.766281297335398)
        tdSql.checkData(0, 3, 0.19)

        tdLog.info("===== step 14 =====")
        tdSql.waitedQuery("select * from strm_to", 1, 20)
        tdSql.checkData(0, 1, 9.5)
        tdSql.checkData(0, 2, 190)
        tdSql.checkData(0, 3, 0)
        tdSql.checkData(0, 4, 19)
        tdSql.checkData(0, 5, 0)
        tdSql.checkData(0, 6, 19)
        tdSql.checkData(0, 7, 5.766281297335398)
        tdSql.checkData(0, 8, 0.19)
        tdSql.checkData(0, 9, rowNum)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
