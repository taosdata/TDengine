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
import os
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        
        self.ts = 1538548685000
        self.numberOfTables = 10000
        self.numberOfRecords = 100
    
    def run(self):
        tdSql.prepare()

        tdSql.execute("create table st(ts timestamp, c1 int, c2 nchar(10)) tags(t1 int, t2 binary(10))")
        tdSql.execute("create table t1 using st tags(1, 'beijing')")        
        sql = "insert into t1 values"
        currts = self.ts
        for i in range(100):
            sql += "(%d, %d, 'nchar%d')" % (currts + i, i % 100, i % 100)
        tdSql.execute(sql)

        tdSql.execute("create table t2 using st tags(2, 'shanghai')")        
        sql = "insert into t2 values"
        currts = self.ts
        for i in range(100):
            sql += "(%d, %d, 'nchar%d')" % (currts + i, i % 100, i % 100)
        tdSql.execute(sql)

        os.system("taosdump --databases db -o /tmp")
        
        tdSql.execute("drop database db")
        tdSql.query("show databases")
        tdSql.checkRows(0)
        
        os.system("taosdump -i /tmp")

        tdSql.query("show databases")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'db')
        
        tdSql.execute("use db")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'st')

        tdSql.query("show tables")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 't2')
        tdSql.checkData(1, 0, 't1')

        tdSql.query("select * from t1")
        tdSql.checkRows(100)
        for i in range(100):
            tdSql.checkData(i, 1, i)
            tdSql.checkData(i, 2, "nchar%d" % i)

        tdSql.query("select * from t2")
        tdSql.checkRows(100)
        for i in range(100):
            tdSql.checkData(i, 1, i)
            tdSql.checkData(i, 2, "nchar%d" % i)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())