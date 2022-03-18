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

        # test case for https://jira.taosdata.com:18080/browse/TD-5338
        tdSql.query("select loc,max(voltage) from st  interval(1m);")
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2020-07-01 04:24:00.000")
        tdSql.checkData(0, 1, "beijing")
        tdSql.checkData(0, 2, 220)
        tdSql.checkData(1, 0, "2020-07-12 18:11:00.000")
        tdSql.checkData(1, 1, "beijing")
        tdSql.checkData(1, 2, 221)
        tdSql.checkData(2, 0, "2020-07-24 07:58:00.000")
        tdSql.checkData(2, 1, "beijing")
        tdSql.checkData(2, 2, 225)
        tdSql.checkData(3, 0, "2020-08-04 21:44:00.000")
        tdSql.checkData(2, 1, "beijing")
        tdSql.checkData(3, 2, 228)
        tdSql.checkData(4, 0, "2020-08-16 11:31:00.000")
        tdSql.checkData(4, 1, "shanghai")
        tdSql.checkData(4, 2, 225)
        tdSql.checkData(5, 0, "2020-08-28 01:18:00.000")
        tdSql.checkData(5, 1, "shanghai")
        tdSql.checkData(5, 2, 228)
        tdSql.checkData(6, 0, "2020-09-08 15:04:00.000")
        tdSql.checkData(6, 1, "beijing")
        tdSql.checkData(6, 2, 222)
        tdSql.checkData(7, 0, "2020-09-20 04:51:00.000")
        tdSql.checkData(7, 1, "shanghai")
        tdSql.checkData(7, 2, 222)
        tdSql.query("select loc,max(voltage) from t0  interval(1m);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2020-07-01 04:24:00.000")
        tdSql.checkData(0, 1, "beijing")
        tdSql.checkData(0, 2, 220)
        tdSql.checkData(1, 0, "2020-07-12 18:11:00.000")
        tdSql.checkData(1, 1, "beijing")
        tdSql.checkData(1, 2, 221)
        tdSql.checkData(2, 0, "2020-07-24 07:58:00.000")
        tdSql.checkData(2, 1, "beijing")
        tdSql.checkData(2, 2, 225)
        tdSql.checkData(3, 0, "2020-08-04 21:44:00.000")
        tdSql.checkData(2, 1, "beijing")
        tdSql.checkData(3, 2, 228)       
        tdSql.checkData(4, 0, "2020-09-08 15:04:00.000")
        tdSql.checkData(4, 1, "beijing")
        tdSql.checkData(4, 2, 222)
    

        # test case for https://jira.taosdata.com:18080/browse/TD-2298
        tdSql.execute("create database test keep 36500")
        tdSql.execute("use test")
        tdSql.execute("create table t (ts timestamp, voltage int)")
        for i in range(10000):
            tdSql.execute("insert into t values(%d, 0)" % (1000000 + i * 6000))
        
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query("select last(*) from t interval(1s)")
        tdSql.checkRows(10000)

        # test case for https://jira.taosdata.com:18080/browse/TD-2601
        newTs = 1601481600000

        tdSql.execute("create database test2")
        tdSql.execute("use test2")
        tdSql.execute("create table t (ts timestamp, voltage int)")
        for i in range(100):
            tdSql.execute("insert into t values(%d, %d)" % (newTs + i * 10000000, i))
        
        tdSql.query("select sum(voltage) from t where ts >='2020-10-01 00:00:00' and ts <='2020-12-01 00:00:00' interval(1n) fill(NULL)")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 4950)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(2, 1, None)

         # test case for https://jira.taosdata.com:18080/browse/TD-2659， https://jira.taosdata.com:18080/browse/TD-2660
        tdSql.execute("create database test3")
        tdSql.execute("use test3")
        tdSql.execute("create table tb(ts timestamp, c int)")
        tdSql.execute("insert into tb values('2020-10-30 18:11:56.680', -111)")
        tdSql.execute("insert into tb values('2020-11-19 18:11:45.773', null)")
        tdSql.execute("insert into tb values('2020-12-09 18:11:17.098', null)")
        tdSql.execute("insert into tb values('2020-12-29 11:00:49.412', 1)")
        tdSql.execute("insert into tb values('2020-12-29 11:00:50.412', 2)")
        tdSql.execute("insert into tb values('2020-12-29 11:00:52.412', 3)")

        tdSql.query("select first(ts),twa(c) from tb interval(14a)")
        tdSql.checkRows(6)

        tdSql.error("select twa(c) from tb group by c")


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
