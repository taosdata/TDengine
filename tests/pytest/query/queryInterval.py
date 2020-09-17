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

        self.ts = 1538548685000

    def run(self):
        tdSql.prepare()

        tdSql.execute("create table st (ts timestamp, voltage int) tags (loc nchar(30))")
        tdSql.execute("insert into t0 using st tags('beijing') values(now, 220) (now - 15d, 221) (now - 30d, 225) (now - 35d, 228) (now - 45d, 222)" )

        tdSql.query("select avg(voltage) from st interval(1n)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 225.0)
        tdSql.checkData(1, 1, 220.5)
        
        tdSql.query("select avg(voltage) from st interval(1n, 15d)")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 225.0)
        tdSql.checkData(1, 1, 223.0)
        tdSql.checkData(2, 1, 220.0)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
