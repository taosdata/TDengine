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

        self.ts = 1537146000000

    def run(self):
        tdSql.prepare()

        print("==============step1")
        tdSql.execute("create table st(ts timestamp, c1 float, c2 double) tags(t1 float, t2 double)")
        tdSql.execute(
            'CREATE TABLE if not exists t1 using st tags(1.023, 3.00412)')
        tdSql.execute(
            'CREATE TABLE if not exists t2 using st tags(2.005, 10.129001)')

        print("==============step2")
        for i in range(10):
            tdSql.execute("insert into t1 values(%d, %s, %s) t2 values(%d, %s, %s)" % 
                (self.ts + i, 1.0001 + i, 2.0001301 + i, self.ts + i, 1.0001 * i, 2.0001301 * i))
        
        tdSql.query("select count(*) from st")
        tdSql.checkData(0, 0, 20)

        tdSql.query("select count(*) from st where t1 > 1.023")
        tdSql.checkData(0, 0, 10)

        tdSql.query("select count(*) from st where t1 < 1.023")
        tdSql.checkRows(0)

        tdSql.query("select count(*) from st where t1 >= 1.023")
        tdSql.checkData(0, 0, 20)

        tdSql.query("select count(*) from st where t1 <= 1.023")
        tdSql.checkData(0, 0, 10)

        tdSql.query("select count(*) from st where t1 = 1.023")
        tdSql.checkData(0, 0, 10)

        tdSql.query("select count(*) from st where t1 <> 1.023")
        tdSql.checkData(0, 0, 10)

        tdSql.query("select count(*) from st where t1 != 1.023")
        tdSql.checkData(0, 0, 10)

        tdSql.query("select count(*) from st where t1 >= 1.023 and t1 <= 1.023")
        tdSql.checkData(0, 0, 10)

        tdSql.query("select count(*) from st where t1 > 1.023 and t1 <= 1.023")
        tdSql.checkRows(0)

        tdSql.query("select count(*) from st where t1 >= 1.023 and t1 < 1.023")
        tdSql.checkRows(0)

        tdSql.query("select count(*) from st where t2 > 3.00412")
        tdSql.checkData(0, 0, 10)

        tdSql.query("select count(*) from st where t2 < 3.00412")
        tdSql.checkRows(0)

        tdSql.query("select count(*) from st where t2 >= 3.00412")
        tdSql.checkData(0, 0, 20)

        tdSql.query("select count(*) from st where t2 <= 3.00412")
        tdSql.checkData(0, 0, 10)

        tdSql.query("select count(*) from st where t2 = 3.00412")
        tdSql.checkData(0, 0, 10)

        tdSql.query("select count(*) from st where t2 <> 3.00412")
        tdSql.checkData(0, 0, 10)

        tdSql.query("select count(*) from st where t2 != 3.00412")
        tdSql.checkData(0, 0, 10)

        tdSql.query("select count(*) from st where t2 >= 3.00412 and t2 <= 3.00412")
        tdSql.checkData(0, 0, 10)

        tdSql.query("select count(*) from st where t2 > 3.00412 and t2 <= 3.00412")
        tdSql.checkRows(0)

        tdSql.query("select count(*) from st where t2 >= 3.00412 and t2 < 3.00412")
        tdSql.checkRows(0)

        tdSql.query("select count(*) from t1 where c1 < 2.0001")
        tdSql.checkData(0, 0, 1)

        tdSql.query("select count(*) from t1 where c1 <= 2.0001")
        tdSql.checkData(0, 0, 2)

        tdSql.query("select count(*) from t1 where c1 = 2.0001")
        tdSql.checkData(0, 0, 1)

        tdSql.query("select count(*) from t1 where c1 > 2.0001")
        tdSql.checkData(0, 0, 8)

        tdSql.query("select count(*) from t1 where c1 >= 2.0001")
        tdSql.checkData(0, 0, 9)

        tdSql.query("select count(*) from t1 where c1 <> 2.0001")
        tdSql.checkData(0, 0, 9)

        tdSql.query("select count(*) from t1 where c1 != 2.0001")
        tdSql.checkData(0, 0, 9)

        tdSql.query("select count(*) from t1 where c1 >= 2.0001 and c1 <= 2.0001")
        tdSql.checkData(0, 0, 1)

        tdSql.query("select count(*) from t1 where c1 > 2.0001 and c1 <= 2.0001")
        tdSql.checkRows(0)

        tdSql.query("select count(*) from t1 where c1 >= 2.0001 and c1 < 2.0001")
        tdSql.checkRows(0)

        tdSql.query("select count(*) from t2 where c2 < 2.0001301")
        tdSql.checkData(0, 0, 1)

        tdSql.query("select count(*) from t2 where c2 <= 2.0001301")
        tdSql.checkData(0, 0, 2)

        tdSql.query("select count(*) from t2 where c2 = 2.0001301")
        tdSql.checkData(0, 0, 1)

        tdSql.query("select count(*) from t2 where c2 > 2.0001301")
        tdSql.checkData(0, 0, 8)

        tdSql.query("select count(*) from t2 where c2 >= 2.0001301")
        tdSql.checkData(0, 0, 9)

        tdSql.query("select count(*) from t2 where c2 <> 2.0001301")
        tdSql.checkData(0, 0, 9)

        tdSql.query("select count(*) from t2 where c2 != 2.0001301")
        tdSql.checkData(0, 0, 9)

        tdSql.query("select count(*) from t2 where c2 >= 2.0001301 and c2 <= 2.0001301")
        tdSql.checkData(0, 0, 1)

        tdSql.query("select count(*) from t2 where c2 > 2.0001301 and c2 <= 2.0001301")
        tdSql.checkRows(0)

        tdSql.query("select count(*) from t2 where c2 >= 2.0001301 and c2 < 2.0001301")
        tdSql.checkRows(0)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
