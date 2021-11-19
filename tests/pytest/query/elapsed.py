###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
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

    def general(self):
        # normal table
        tdSql.execute("create database ms_test")
        tdSql.execute("use ms_test")
        tdSql.execute("create table t1 (ts timestamp, f float)")
        tdSql.execute("insert into t1 values('2021-11-18 00:00:10', 1)"
                                           "('2021-11-18 00:00:30', 2)"
                                           "('2021-11-18 00:00:40', 3)"
                                           "('2021-11-18 00:01:00', 4)")
        
        tdSql.execute("create database ns_test precision \"ns\"")
        tdSql.execute("use ns_test")
        tdSql.execute("create table t1 (ts timestamp, f float)")
        tdSql.execute("insert into t1 values('2021-11-18 00:00:00.000000100', 1)"
                                           "('2021-11-18 00:00:00' + 200b, 2)"
                                           "('2021-11-18 00:00:00' + 300b, 3)"
                                           "('2021-11-18 00:00:00' + 500b, 4)")
        
        # super table
        tdSql.execute("create stable st1(ts timestamp, f float) tags(id int)")
        tdSql.execute("create table subt1 using st1 tags(1)")
        tdSql.execute("create table subt2 using st1 tags(2)")
        tdSql.execute("insert into subt1 values('2021-11-18 00:00:00', 1)('2021-11-18 00:06:00', 2)('2021-11-18 00:12:00', 3)('2021-11-18 00:24:00', 4)")

    def normalTable(self):
        tdSql.query("select elapsed(ts) from t1")
        tdSql.checkData(0, 0, 50000)

        tdSql.query("select elapsed(ts, 20s) from t1")
        tdSql.checkData(0, 0, 2.5)

        tdSql.query("select elapsed(ts) from t1 interval(1s)")

    def superTable(self):
        tdSql.query("select elapsed(ts) from st1 group by tbname")

        tdSql.query("select elapsed(ts) from st1 interval(1s) group by tbname")

        tdSql.query("select elapsed(ts, 1s), twa(f), elapsed(ts, 1s) * twa(f) as integral from st1 interval(1s) group by tbname")


    def run(self):
        tdSql.prepare()
        self.general()
        self.normalTable()
        #self.superTable()


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
