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
import taos
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()
        tdSql.execute(
            'create table st (ts timestamp, v1 int, v2 int, v3 int, v4 int, v5 int) tags (t int)')

        totalTables = 100
        batchSize = 500
        totalBatch = 60

        tdLog.info(
            "create %d tables, insert %d rows per table" %
            (totalTables, batchSize * totalBatch))

        for t in range(0, totalTables):
            tdSql.execute('create table t%d using st tags(%d)' % (t, t))
            # 2019-06-10 00:00:00
            beginTs = 1560096000000
            interval = 10000
            for r in range(0, totalBatch):
                sql = 'insert into t%d values ' % (t)
                for b in range(0, batchSize):
                    ts = beginTs + (r * batchSize + b) * interval
                    sql += '(%d, 1, 2, 3, 4, 5)' % (ts)
                tdSql.execute(sql)

        tdLog.info("insert data finished")
        tdSql.execute('alter table st add column v6 int')
        tdLog.sleep(5)
        tdLog.info("alter table finished")

        tdSql.query("select count(*) from t50")
        tdSql.checkData(0, 0, (int)(batchSize * totalBatch))

        tdLog.info("insert")
        tdSql.execute(
            "insert into t50 values ('2019-06-13 07:59:55.000', 1, 2, 3, 4, 5, 6)")

        tdLog.info("import")
        tdSql.execute(
            "import into t50 values ('2019-06-13 07:59:55.000', 1, 2, 3, 4, 5, 6)")

        tdLog.info("query")
        tdSql.query("select count(*) from t50")
        tdSql.checkData(0, 0, batchSize * totalBatch + 1)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
