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
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts = 1537146000000

    def run(self):
        tdSql.prepare()

        print("==============step1")
        tdSql.execute(
            "create table if not exists stb (ts timestamp, col1 int, col2 int, col3 int) tags(loc nchar(20), id int)")
        tdSql.execute(
            "insert into tb0 using stb tags('beijing', 1) values(%s, 1, 1, 1)(%s, 2, 2, 2)(%s, 3, 3, 3)(%s, 4, 4, 4)" % (self.ts, self.ts + 1000000, self.ts + 2000000, self.ts + 3000000))
        tdSql.execute(
            "insert into tb1 using stb tags('beijing', 2) values(%s, 1, 1, 1)(%s, 2, 2, 2)(%s, 3, 3, 3)(%s, 4, 4, 4)" % (self.ts + 4000000, self.ts + 5000000, self.ts + 6000000, self.ts + 7000000))
        tdSql.execute(
            "insert into tb2 using stb tags('shanghai', 1) values(%s, 1, 1, 1)(%s, 2, 2, 2)(%s, 3, 3, 3)(%s, 4, 4, 4)" % (self.ts + 8000000, self.ts + 9000000, self.ts + 10000000, self.ts + 11000000))
        tdSql.execute(
            "insert into tb3 using stb tags('shanghai', 2) values(%s, 1, 1, 1)(%s, 2, 2, 2)(%s, 3, 3, 3)(%s, 4, 4, 4)" % (self.ts + 12000000, self.ts + 13000000, self.ts + 14000000, self.ts + 15000000))
        tdSql.execute(
            "insert into tb4 using stb tags('shanghai', 3) values(%s, null, null, null)(%s, null, null, null)(%s, null, null, null)(%s, null, null, null)" % (self.ts + 16000000, self.ts + 17000000, self.ts + 18000000, self.ts + 19000000))
        
        tdSql.query("select first(col1) - avg(col1) from stb where ts > '2018-09-17 08:00:00.000' and ts < '2018-09-17 14:16:41.000' interval(1h)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, -1.5)
        tdSql.checkData(1, 1, -1.5)
        tdSql.checkData(2, 1, -1.0)
        tdSql.checkData(3, 1, 1.5)
        tdSql.checkData(4, 1, 0)

        tdSql.query("select first(col1) - avg(col1) from stb where ts > '2018-09-17 08:00:00.000' and ts < '2018-09-17 14:16:41.000' interval(1h) fill(null)")
        tdSql.checkRows(7)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(6, 1, None)       

        tdSql.query("select max(col1) - min(col1) from stb where ts > '2018-09-17 08:00:00.000' and ts < '2018-09-17 14:16:41.000' and id = 1 group by loc, id")
        tdSql.checkRows(2)

        tdSql.query("select spread(col1) from stb where ts > '2018-09-17 08:00:00.000' and ts < '2018-09-17 14:16:41.000' and id = 1 group by loc, id")
        tdSql.checkRows(2)
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())