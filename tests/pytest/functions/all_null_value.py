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
        tdSql.init(conn.cursor())

        self.rowNum = 10
        self.ts = 1537146000000
        
    def run(self):
        tdSql.prepare()
        
        tdSql.execute("create table st(ts timestamp, c1 int, c2 int)")        
        for i in range(self.rowNum):
            tdSql.execute("insert into st values(%d, null, null)" % (self.ts + i))

        tdSql.query("select avg(c1) from st")
        tdSql.checkRows(0)

        tdSql.query("select max(c1) from st")
        tdSql.checkRows(0)

        tdSql.query("select min(c1) from st")
        tdSql.checkRows(0)

        tdSql.query("select bottom(c1, 1) from st")
        tdSql.checkRows(0)

        tdSql.query("select top(c1, 1) from st")
        tdSql.checkRows(0)

        tdSql.query("select diff(c1) from st")
        tdSql.checkRows(0)

        tdSql.query("select first(c1) from st")
        tdSql.checkRows(0)

        tdSql.query("select last(c1) from st")
        tdSql.checkRows(0)

        tdSql.query("select last_row(c1) from st")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query("select count(c1) from st")
        tdSql.checkRows(0)

        tdSql.query("select leastsquares(c1, 1, 1) from st")
        tdSql.checkRows(0)    

        tdSql.query("select c1 + c2 from st")
        tdSql.checkRows(10)

        tdSql.query("select spread(c1) from st")
        tdSql.checkRows(0)

        # tdSql.query("select stddev(c1) from st")
        # tdSql.checkRows(0)

        tdSql.query("select sum(c1) from st")
        tdSql.checkRows(0)

        tdSql.query("select twa(c1) from st")
        tdSql.checkRows(0)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
