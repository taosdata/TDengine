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
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def querysqls(self):
        tdSql.query("select stddev(c1) from t10 group by c1")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(2, 0, 0)
        tdSql.checkData(3, 0, 0)
        tdSql.checkData(4, 0, 0)
        tdSql.checkData(5, 0, 0)
        tdSql.query("select stddev(c2) from t10")
        tdSql.checkData(0, 0, 0.5)

    def run(self):
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database  if not exists db keep 36500")
        tdSql.execute("use db")

        tdLog.printNoPrefix("==========step1:create table && insert data")
        tdSql.execute("create stable stb1 (ts timestamp , c1 int ,c2 float) tags(t1 int)")
        tdSql.execute("create table t10 using stb1 tags(1)")
        tdSql.execute("insert into t10 values ('1969-12-31 00:00:00.000', 2,1)")
        tdSql.execute("insert into t10 values ('1970-01-01 00:00:00.000', 3,1)")
        tdSql.execute("insert into t10 values (0, 4,1)")
        tdSql.execute("insert into t10 values (now-18725d, 1,2)")
        tdSql.execute("insert into t10 values ('2021-04-06 00:00:00.000', 5,2)")
        tdSql.execute("insert into t10 values (now+1d,6,2)")

        tdLog.printNoPrefix("==========step2:query and check")
        self.querysqls()

        tdLog.printNoPrefix("==========step3:after wal,check again")
        tdSql.query("select * from information_schema.ins_dnodes")
        index = tdSql.getData(0, 0)
        tdDnodes.stop(index)
        tdDnodes.start(index)
        self.querysqls()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())