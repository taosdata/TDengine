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
import random


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts = 1500000000000

    def run(self):
        tdSql.prepare()
        
        tdSql.execute("create table meters(ts timestamp, col1 int) tags(id int, loc nchar(20))")
        sql = "insert into t0 using meters tags(1, 'beijing') values"
        for i in range(100):
            sql += "(%d, %d)" % (self.ts + i * 1000, random.randint(1, 100))        
        tdSql.execute(sql)   

        sql = "insert into t1 using meters tags(2, 'shanghai') values"
        for i in range(100):
            sql += "(%d, %d)" % (self.ts + i * 1000, random.randint(1, 100))        
        tdSql.execute(sql)
        
        tdSql.query("select count(*) from meters interval(10s) sliding(5s)")
        tdSql.checkRows(21)

        tdSql.error("select count(*) from meters sliding(5s)")

        tdSql.error("select count(*) from meters sliding(5s) interval(10s)")

        tdSql.error("select * from meters sliding(5s) order by ts desc")
                
        tdSql.query("select count(*) from meters group by loc")
        tdSql.checkRows(2)

        tdSql.error("select * from meters group by loc sliding(5s)")   

        # Fix defect: https://jira.taosdata.com:18080/browse/TD-2700
        tdSql.execute("create database test")
        tdSql.execute("use test")
        tdSql.execute("create table t1(ts timestamp, k int)")
        tdSql.execute("insert into t1 values(1500000001000, 0)")
        tdSql.query("select sum(k) from t1 interval(1d) sliding(1h)")
        tdSql.checkRows(24)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
