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
    """
    add test data before 1970s
    """
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        print("==============step1")
        tdSql.execute("create database if not exists demo keep 36500;");
        print("==============create db demo keep 365000 days")
        tdSql.execute("use demo;")
        tdSql.execute("CREATE table if not exists test (ts timestamp, f1 int);")
        print("==============create table test")

        print("==============step2")
        #TODO : should add more testcases
        tdSql.execute("insert into test values('1930-12-12 01:19:20.345', 1);")
        tdSql.execute("insert into test values('1969-12-30 23:59:59.999', 2);")
        tdSql.execute("insert into test values(-3600001, 3);")
        tdSql.execute("insert into test values('2020-10-20 14:02:53.770', 4);")
        print("==============insert data")

        # tdSql.query("select * from test;")
        #
        # tdSql.checkRows(3)
        #
        # tdSql.checkData(0,0,'1969-12-12 01:19:20.345000')
        # tdSql.checkData(1,0,'1970-01-01 07:00:00.000000')
        # tdSql.checkData(2,0,'2020-10-20 14:02:53.770000')
        print("==============step3")
        tdDnodes.stopAll()
        tdDnodes.start(1)
        print("==============restart taosd")


        print("==============step4")
        tdSql.execute("use demo;")
        tdSql.query("select * from test;")
        print(tdSql.queryResult)
        tdSql.checkRows(4)
        tdSql.checkData(0,0,'1930-12-12 01:19:20.345000')
        tdSql.checkData(1,0,'1969-12-30 23:59:59.999000')
        tdSql.checkData(2,0,'1970-01-01 06:59:59.999000')
        tdSql.checkData(3,0,'2020-10-20 14:02:53.770000')
        print("==============check data")



    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
