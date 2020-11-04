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
    kill query
    """

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        print("==============step1")
        tdSql.execute("create database if not exists demo;");
        tdSql.execute("use demo;")
        tdSql.execute("CREATE TABLE IF NOT EXISTS demo1 (ts TIMESTAMP, f1 int);")
        ts = 1300000000000

        for i in range(1000):
            tdSql.execute(" insert into demo1 values({ts}, {data});".format(ts=ts, data=i))
            ts += 1

        print("==============insert into test1 and test2 form test file")



        print("==============step2")
        while True:
            tdSql.query('select * from demo1;')
            print('==============', len(tdSql.queryResult))
        with open(ordered_csv) as f1:
            num1 = len(f1.readlines())
        tdSql.checkRows(num1)


        tdSql.query('select * from test2;')
        with open(disordered_csv) as f2:
            num2 = len(f2.readlines())
        tdSql.checkRows(num2)
        print("=============execute select count(*) from xxx")


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
