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
        self.columns = 50
        self.perfix = 't'
        self.ts = 1601481600000
    
    def insertData(self):
        print("==============step1")
        sql = "create table st(ts timestamp, "
        for i in range(self.columns - 1):
            sql += "c%d int, " % (i + 1)
        sql += "c50 int) tags(t1 int)"
        tdSql.execute(sql)
        
        for i in range(self.tables):
            tdSql.execute("create table %s%d using st tags(%d)" % (self.perfix, i, i))
            for j in range(self.rows):
                tc = self.ts + j * 60000
                tdSql.execute("insert into %s%d(ts, c1) values(%d, %d)" %(self.perfix, i, tc, j))    

    def executeQueries(self):
        print("==============step2")
        tdSql.query("select last_row(c1) from %s%d" % (self.perfix, 1))
        tdSql.checkData(0, 0, 19)

        tdSql.query("select last_row(c1) from %s%d where ts <= %d" % (self.perfix, 1, self.ts + 4 * 60000))
        tdSql.checkData(0, 0, 4)

        tdSql.query("select last_row(c1) as b from %s%d" % (self.perfix, 1))
        tdSql.checkData(0, 0, 19)

        tdSql.query("select last_row(c1) from st")
        tdSql.checkData(0, 0, 19)

        tdSql.query("select last_row(c1) as c from st where ts <= %d" % (self.ts + 4 * 60000))
        tdSql.checkData(0, 0, 4)

        tdSql.query("select last_row(c1) as c from st where t1 < 5")
        tdSql.checkData(0, 0, 19)

        tdSql.query("select last_row(c1) as c from st where t1 <= 5 and ts <= %d" % (self.ts + 4 * 60000))
        tdSql.checkData(0, 0, 4)        

        tdSql.query("select last_row(c1) as c from st group by t1")
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, 19)        

        tc = self.ts + 1 * 3600000
        tdSql.execute("insert into %s%d(ts, c1) values(%d, %d)" %(self.perfix, 1, tc, 10))

        tc = self.ts + 3 * 3600000
        tdSql.execute("insert into %s%d(ts, c1) values(%d, null)" %(self.perfix, 1, tc))

        tc = self.ts + 5 * 3600000
        tdSql.execute("insert into %s%d(ts, c1) values(%d, %d)" %(self.perfix, 1, tc, -1))

        tc = self.ts + 7 * 3600000
        tdSql.execute("insert into %s%d(ts, c1) values(%d, null)" %(self.perfix, 1, tc))

    def insertData2(self):
        tc = self.ts + 1 * 3600000
        tdSql.execute("insert into %s%d(ts, c1) values(%d, %d)" %(self.perfix, 1, tc, 10))

        tc = self.ts + 3 * 3600000
        tdSql.execute("insert into %s%d(ts, c1) values(%d, null)" %(self.perfix, 1, tc))

        tc = self.ts + 5 * 3600000
        tdSql.execute("insert into %s%d(ts, c1) values(%d, %d)" %(self.perfix, 1, tc, -1))

        tc = self.ts + 7 * 3600000
        tdSql.execute("insert into %s%d(ts, c1) values(%d, null)" %(self.perfix, 1, tc))

    def executeQueries2(self): 
        # For stable
        tc = self.ts + 6 * 3600000
        tdSql.query("select last_row(c1) from st where ts < %d " % tc)        
        tdSql.checkData(0, 0, -1)

        tc = self.ts + 8 * 3600000
        tdSql.query("select last_row(*) from st where ts < %d " % tc)        
        tdSql.checkData(0, 1, None)

        tdSql.query("select last_row(*) from st")        
        tdSql.checkData(0, 1, None)

        tc = self.ts + 4 * 3600000
        tdSql.query("select last_row(*) from st where ts < %d " % tc)        
        tdSql.checkData(0, 1, None)

        tc1 = self.ts + 1 * 3600000
        tc2 = self.ts + 4 * 3600000
        tdSql.query("select last_row(*) from st where ts > %d and ts <= %d" % (tc1, tc2))
        tdSql.checkData(0, 1, None)

        # For table
        tc = self.ts + 6 * 3600000
        tdSql.query("select last_row(*) from %s%d where ts <= %d" % (self.perfix, 1, tc))
        tdSql.checkData(0, 1, -1)

        tc = self.ts + 8 * 3600000
        tdSql.query("select last_row(*) from %s%d where ts <= %d" % (self.perfix, 1, tc))
        tdSql.checkData(0, 1, None)

        tdSql.query("select last_row(*) from %s%d" % (self.perfix, 1))
        tdSql.checkData(0, 1, None)

        tc = self.ts + 4 * 3600000
        tdSql.query("select last_row(*) from %s%d where ts <= %d" % (self.perfix, 1, tc))
        tdSql.checkData(0, 1, None)

        tc1 = self.ts + 1 * 3600000
        tc2 = self.ts + 4 * 3600000
        tdSql.query("select last_row(*) from st where ts > %d and ts <= %d" % (tc1, tc2))
        tdSql.checkData(0, 1, None)
        
    def run(self):
        tdSql.prepare()
        
        print("============== last_row_cache_0.sim")
        tdSql.execute("create database test1 cachelast 0")
        tdSql.execute("use test1")
        self.insertData()
        self.executeQueries()
        self.insertData2()
        self.executeQueries2()
        
        print("============== alter last cache")
        tdSql.execute("alter database test1 cachelast 1")                
        self.executeQueries2()
        tdDnodes.stop(1)
        tdDnodes.start(1)
        self.executeQueries2()

        tdSql.execute("alter database test1 cachelast 0")        
        self.executeQueries2()
        tdDnodes.stop(1)
        tdDnodes.start(1)
        self.executeQueries2()

        print("============== last_row_cache_1.sim")
        tdSql.execute("create database test2 cachelast 1")
        tdSql.execute("use test2")
        self.insertData()
        self.executeQueries()
        self.insertData2()
        self.executeQueries2()
        tdDnodes.stop(1)
        tdDnodes.start(1)
        self.executeQueries2()
        
        tdSql.execute("alter database test2 cachelast 0")        
        self.executeQueries2()
        tdDnodes.stop(1)
        tdDnodes.start(1)
        self.executeQueries2()

        tdSql.execute("alter database test2 cachelast 1")        
        self.executeQueries2()
        tdDnodes.stop(1)
        tdDnodes.start(1)
        self.executeQueries2()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
