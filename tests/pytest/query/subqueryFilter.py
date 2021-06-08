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

        self.ts = 1601481600000
        self.tables = 10
        self.perfix = 'dev'
    
    def insertData(self):
        print("==============step1")
        tdSql.execute(
            "create table if not exists st (ts timestamp, tagtype int) tags(dev nchar(50))")
        
        for i in range(self.tables):
            tdSql.execute("create table %s%d using st tags(%d)" % (self.perfix, i, i))
            rows = 15 + i
            for j in range(rows):                
                tdSql.execute("insert into %s%d values(%d, %d)" %(self.perfix, i, self.ts + i * 20 * 10000 + j * 10000, j))  

    def run(self):
        tdSql.prepare()

        self.insertData()

        tdSql.query("select count(*) val from st group by tbname")
        tdSql.checkRows(10)

        tdSql.query("select * from (select count(*) val from st group by tbname)")
        tdSql.checkRows(10)

        tdSql.query("select * from (select count(*) val from st group by tbname) a where a.val < 20")
        tdSql.checkRows(5)

        tdSql.query("select * from (select count(*) val from st group by tbname) a where a.val > 20")
        tdSql.checkRows(4)

        tdSql.query("select * from (select count(*) val from st group by tbname) a where a.val = 20")
        tdSql.checkRows(1)

        tdSql.query("select * from (select count(*) val from st group by tbname) a where a.val <= 20")
        tdSql.checkRows(6)

        tdSql.query("select * from (select count(*) val from st group by tbname) a where a.val >= 20")
        tdSql.checkRows(5)

        tdSql.query("select count(*) from (select first(tagtype) val from st interval(30s)) a where a.val > 20")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query("select count(*) from (select first(tagtype) val from st interval(30s)) a where a.val >= 20")
        tdSql.checkData(0, 0, 2)

        tdSql.query("select count(*) from (select first(tagtype) val from st interval(30s)) a where a.val < 20")
        tdSql.checkData(0, 0, 63)

        tdSql.query("select count(*) from (select first(tagtype) val from st interval(30s)) a where a.val <= 20")
        tdSql.checkData(0, 0, 64)

        tdSql.query("select count(*) from (select first(tagtype) val from st interval(30s)) a where a.val = 20")
        tdSql.checkData(0, 0, 1)
        
        tdSql.query("select count(*) from (select first(tagtype) val from st interval(30s)) a where a.val > 20")        
        tdSql.checkData(0, 0, 1)

        tdSql.query("select count(*) from (select first(tagtype) val from st interval(30s)) a where a.val >= 20")
        tdSql.checkData(0, 0, 2)

        tdSql.query("select count(*) from (select first(tagtype) val from st interval(30s)) a where a.val < 20")
        tdSql.checkData(0, 0, 63)

        tdSql.query("select count(*) from (select first(tagtype) val from st interval(30s)) a where a.val <= 20")
        tdSql.checkData(0, 0, 64)

        tdSql.query("select count(*) from (select first(tagtype) val from st interval(30s)) a where a.val = 20")
        tdSql.checkData(0, 0, 1)

        tdSql.query("select count(*) from (select last(tagtype) val from st interval(30s)) a where a.val > 20")        
        tdSql.checkData(0, 0, 3)

        tdSql.query("select count(*) from (select last(tagtype) val from st interval(30s)) a where a.val >= 20")
        tdSql.checkData(0, 0, 5)

        tdSql.query("select count(*) from (select last(tagtype) val from st interval(30s)) a where a.val < 20")
        tdSql.checkData(0, 0, 60)

        tdSql.query("select count(*) from (select last(tagtype) val from st interval(30s)) a where a.val <= 20")
        tdSql.checkData(0, 0, 62)

        tdSql.query("select count(*) from (select last(tagtype) val from st interval(30s)) a where a.val = 20")
        tdSql.checkData(0, 0, 2)


        
        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
