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
                
        currTs = self.ts

        for i in range(100):
            sql = "create table tb%d using stb tags('city%d', 1)" % (i, i)
            tdSql.execute(sql)

            sql = "insert into tb%d values" % i            
            for j in range(5):
                val = 1 + j
                sql += "(%d, %d, %d, %d)" % (currTs, val, val, val)
                currTs += 1000000
            tdSql.execute(sql)    

        tdSql.query("select first(col1) - avg(col1) from stb where ts > '2018-09-17 08:00:00.000' and ts < '2018-09-23 04:36:40.000' interval(1h)")
        tdSql.checkRows(139)        

        tdSql.query("select first(col1) - avg(col1) from stb where ts > '2018-09-17 08:00:00.000' and ts < '2018-09-23 04:36:40.000' interval(1h) fill(null)")
        tdSql.checkRows(141)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(140, 1, None)       

        tdSql.query("select max(col1) - min(col1) from stb where ts > '2018-09-17 08:00:00.000' and ts < '2018-09-23 04:36:40.000' and id = 1 group by loc, id")
        rows = tdSql.queryRows

        tdSql.query("select spread(col1) from stb where ts > '2018-09-17 08:00:00.000' and ts < '2018-09-23 04:36:40.000' and id = 1 group by loc, id")
        tdSql.checkRows(rows)
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())