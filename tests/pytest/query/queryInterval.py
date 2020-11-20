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

        self.ts = 1593548685000

    def run(self):
        tdSql.prepare()

        tdSql.execute("create table st (ts timestamp, voltage int) tags (loc nchar(30))")
        tdSql.execute("insert into t0 using st tags('beijing') values(%d, 220) (%d, 221) (%d, 225) (%d, 228) (%d, 222)" 
                        % (self.ts, self.ts + 1000000000, self.ts + 2000000000, self.ts + 3000000000, self.ts + 6000000000))
        tdSql.execute("insert into t1 using st tags('shanghai') values(%d, 220) (%d, 221) (%d, 225) (%d, 228) (%d, 222)" 
                        % (self.ts, self.ts + 2000000000, self.ts + 4000000000, self.ts + 5000000000, self.ts + 7000000000))             
                

        tdSql.query("select avg(voltage) from st interval(1n)")
        tdSql.checkRows(3)        
        tdSql.checkData(0, 0, "2020-07-01 00:00:00")
        tdSql.checkData(0, 1, 221.4)        
        tdSql.checkData(1, 0, "2020-08-01 00:00:00")
        tdSql.checkData(1, 1, 227.0)        
        tdSql.checkData(2, 0, "2020-09-01 00:00:00")
        tdSql.checkData(2, 1, 222.0)
        
        tdSql.query("select avg(voltage) from st interval(1n, 15d)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "2020-06-16 00:00:00")
        tdSql.checkData(0, 1, 220.333333)
        tdSql.checkData(1, 0, "2020-07-16 00:00:00")
        tdSql.checkData(1, 1, 224.666666)
        tdSql.checkData(2, 0, "2020-08-16 00:00:00")
        tdSql.checkData(2, 1, 225.0)
        tdSql.checkData(3, 0, "2020-09-16 00:00:00")
        tdSql.checkData(3, 1, 222.0)

        tdSql.query("select avg(voltage) from st interval(1n, 15d) group by loc")
        tdSql.checkRows(7)
        tdSql.checkData(0, 0, "2020-06-16 00:00:00")
        tdSql.checkData(0, 1, 220.5)
        tdSql.checkData(1, 0, "2020-07-16 00:00:00")
        tdSql.checkData(1, 1, 226.5)
        tdSql.checkData(2, 0, "2020-08-16 00:00:00")
        tdSql.checkData(2, 1, 222.0)
        tdSql.checkData(3, 0, "2020-06-16 00:00:00")
        tdSql.checkData(3, 1, 220.0)
        tdSql.checkData(4, 0, "2020-07-16 00:00:00")
        tdSql.checkData(4, 1, 221.0)
        tdSql.checkData(5, 0, "2020-08-16 00:00:00")
        tdSql.checkData(5, 1, 226.5)
        tdSql.checkData(6, 0, "2020-09-16 00:00:00")
        tdSql.checkData(6, 1, 222.0)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
