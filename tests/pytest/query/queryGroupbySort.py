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

        self.ts = 1537146000000

    def run(self):
        tdSql.prepare()

        tdSql.execute("CREATE TABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int, t3 float, t4 double)")
        tdSql.execute("CREATE TABLE D1001 USING meters TAGS ('Beijing.Chaoyang', 2 , NULL, NULL)")
        tdSql.execute("CREATE TABLE D1002 USING meters TAGS ('Beijing.Chaoyang', 3 , NULL , 1.7)")
        tdSql.execute("CREATE TABLE D1003 USING meters TAGS ('Beijing.Chaoyang', 3 , 1.1 , 1.7)")
        tdSql.execute("INSERT INTO D1001 VALUES (1538548685000, 10.3, 219, 0.31) (1538548695000, 12.6, 218, 0.33) (1538548696800, 12.3, 221, 0.31)")
        tdSql.execute("INSERT INTO D1002 VALUES (1538548685001, 10.5, 220, 0.28)  (1538548696800, 12.3, 221, 0.31)")
        tdSql.execute("INSERT INTO D1003 VALUES (1538548685001, 10.5, 220, 0.28)  (1538548696800, 12.3, 221, 0.31)")
        tdSql.query("SELECT SUM(current), AVG(voltage) FROM meters WHERE groupId > 1 INTERVAL(1s) GROUP BY location order by ts DESC")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2018-10-03 14:38:16")
        tdSql.checkData(1, 0, "2018-10-03 14:38:15")
        tdSql.checkData(2, 0, "2018-10-03 14:38:05")

        tdSql.query("SELECT SUM(current), AVG(voltage) FROM meters WHERE groupId > 1 INTERVAL(1s) GROUP BY location order by ts ASC")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, "2018-10-03 14:38:05")
        tdSql.checkData(1, 0, "2018-10-03 14:38:15")
        tdSql.checkData(2, 0, "2018-10-03 14:38:16")

        tdSql.error("SELECT SUM(current) as s, AVG(voltage) FROM meters WHERE groupId > 1 INTERVAL(1s) GROUP BY location order by s ASC")

        tdSql.error("SELECT SUM(current) as s, AVG(voltage) FROM meters WHERE groupId > 1 INTERVAL(1s) GROUP BY location order by s DESC")

        #add for TD-3170
        tdSql.query("select avg(current) from meters group by t3;")
        tdSql.checkData(0, 0, 11.6)
        tdSql.query("select avg(current) from meters group by t4;")
        tdSql.query("select avg(current) from meters group by t3,t4;")

        # TS-899
        tdSql.execute("create table stb(ts timestamp, c0 int, c1 double, c2 binary(20), c3 int) tags(t1 int, t2 nchar(20))")
        tags = ["beijing", "shanghai"]
        cols = ["chaoyang", "haidian", "shunyi"]
        for i in range(10):
            tdSql.execute("create table t%d using stb tags(%d, '%s')" % (i, i, tags[i % 2]))
            sql = "insert into t%d values" % i
            for j in range(100):
                sql += "(%d, %d, %f, '%s', %d)" % (self.ts + j, j % 10 + 1, random.uniform(1, 10), cols[j % 3], j % 5 + 1)
            tdSql.execute(sql)
        
        tdSql.error("select count(*) from stb group by c1")
        tdSql.query("select count(*), c0, c2, c3 from stb group by c0, c2, c3")
        tdSql.checkRows(30)

        tdSql.query("select count(*), c0, c2, c3 from stb group by c0, c2, c3, t1, t2")
        tdSql.checkRows(300)

        tdSql.query("select count(*), c0 from t0 group by c0")
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, 10)

        # corner cases
        tdSql.execute("create table tb(ts timestamp, c0 int, c1 bool, c2 float, c3 double)")
        tdSql.error("select count(*) from tb group by c2")
        tdSql.error("select count(*) from tb group by c3")
        tdSql.error("select count(*) from tb group by c2, c3")
        tdSql.query("select count(*) from tb group by c1")
        tdSql.checkRows(0)
        
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
