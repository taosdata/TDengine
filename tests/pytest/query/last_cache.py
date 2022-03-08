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
from util.dnodes import tdDnodes

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.tables = 10
        self.rows = 20
        self.perfix = 't'
        self.ts = 1601481600000
    
    def insertData(self):
        print("==============step1")        
        tdSql.execute("create table st (ts timestamp, c1 int) tags(t1 int)")
        
        for i in range(self.tables):
            tdSql.execute("create table %s%d using st tags(%d)" % (self.perfix, i, i))
            for j in range(self.rows):
                tc = self.ts + j * 60000
                tdSql.execute("insert into %s%d values(%d, %d)" %(self.perfix, i, tc, j))

    def executeQueries(self):
        print("==============step2")
        tdSql.query("select last(c1) from %s%d" % (self.perfix, 1))
        tdSql.checkData(0, 0, 19)

        tdSql.query("select last(c1) from %s%d where ts <= %d" % (self.perfix, 1, self.ts + 4 * 60000))
        tdSql.checkData(0, 0, 4)

        tdSql.query("select last(c1) as b from %s%d" % (self.perfix, 1))
        tdSql.checkData(0, 0, 19)

        tdSql.query("select last(c1) from %s%d interval(1m)" % (self.perfix, 1))
        tdSql.checkData(1, 1, 1)

        tdSql.query("select last(c1) from %s%d interval(1d)" % (self.perfix, 1))
        tdSql.checkData(0, 1, 19)

        tdSql.query("select last(c1) from %s%d where ts <= %d interval(1m)" % (self.perfix, 1, self.ts + 4 * 60000))
        tdSql.checkRows(5)
        tdSql.checkData(1, 1, 1)

        tdSql.query("select last(c1) from st")
        tdSql.checkData(0, 0, 19)

        tdSql.query("select last(c1) as c from st where ts <= %d" % (self.ts + 4 * 60000))
        tdSql.checkData(0, 0, 4)

        tdSql.query("select last(c1) as c from st where t1 <= 5")
        tdSql.checkData(0, 0, 19)

        tdSql.query("select last(c1) as c from st where t1 <= 5 and ts <= %d" % (self.ts + 4 * 60000))
        tdSql.checkData(0, 0, 4)

        tdSql.query("select last(c1) from st interval(1m)")        
        tdSql.checkData(1, 1, 1)

        tdSql.query("select last(c1) from st interval(1d)")        
        tdSql.checkData(0, 1, 19)

        tdSql.query("select last(c1) from st group by t1")
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, 19)

        tdSql.query("select last(c1) as c from st where ts <= %d interval(1m) group by t1" % (self.ts + 4 * 60000))
        tdSql.checkData(1, 1, 1)
        tdSql.checkRows(50)

    def run(self):
        tdSql.prepare()

        # last_cache_0.sim
        tdSql.execute("create database test1 cachelast 0")
        tdSql.execute("use test1")
        self.insertData()
        self.executeQueries()
        
        tdSql.execute("alter database test1 cachelast 1")        
        self.executeQueries()
        tdDnodes.stop(1)
        tdDnodes.start(1)
        self.executeQueries()

        tdSql.execute("alter database test1 cachelast 0")        
        self.executeQueries()
        tdDnodes.stop(1)
        tdDnodes.start(1)
        self.executeQueries()

        # last_cache_1.sim
        tdSql.execute("create database test2 cachelast 1")
        tdSql.execute("use test2")
        self.insertData()
        self.executeQueries()
        
        tdSql.execute("alter database test2 cachelast 0")        
        self.executeQueries()
        tdDnodes.stop(1)
        tdDnodes.start(1)
        self.executeQueries()

        tdSql.execute("alter database test2 cachelast 1")        
        self.executeQueries()
        tdDnodes.stop(1)
        tdDnodes.start(1)
        self.executeQueries()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
