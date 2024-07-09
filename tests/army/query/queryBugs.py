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
import time
import random

import taos
import frame
import frame.etool


from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
    # fix
    def FIX_TD_30686(self):
        tdLog.info("check bug TD_30686 ...\n")
        sqls = [
            "create database db",
            "create table db.st(ts timestamp, age int) tags(area tinyint);",
            "insert into db.t1 using db.st tags(100) values('2024-01-01 10:00:01', 1);",
            "insert into db.t2 using db.st tags(110) values('2024-01-01 10:00:02', 2);",
            "insert into db.t3 using db.st tags(3) values('2024-01-01 10:00:03', 3);"
        ]
        tdSql.executes(sqls)

        sql = "select * from db.st where area < 139 order by ts;"
        results = [
            ["2024-01-01 10:00:01", 1, 100],
            ["2024-01-01 10:00:02", 2, 110],
            ["2024-01-01 10:00:03", 3, 3]
        ]
        tdSql.checkDataMem(sql, results)

    def FIX_TS_5105(self):
        tdLog.info("check bug TS_5105 ...\n")
        ts1 = "2024-07-03 10:00:00.000"
        ts2 = "2024-07-03 13:00:00.000"
        sqls = [
            "drop database if exists ts_5105",
            "create database ts_5105 cachemodel 'both';",
            "use ts_5105;",
            "CREATE STABLE meters (ts timestamp, current float) TAGS (location binary(64), groupId int);",
            "CREATE TABLE d1001 USING meters TAGS ('California.B', 2);",
            "CREATE TABLE d1002 USING meters TAGS ('California.S', 3);",
            f"INSERT INTO d1001 VALUES ('{ts1}', 10);",
            f"INSERT INTO d1002 VALUES ('{ts2}', 13);",
        ]
        tdSql.executes(sqls)

        sql = "select last(ts), last_row(ts) from meters;"

        # 执行多次，有些时候last_row(ts)会返回错误的值，详见TS-5105
        for i in range(1, 10):
            tdLog.debug(f"{i}th execute sql: {sql}")
            tdSql.query(sql)
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, ts2)
            tdSql.checkData(0, 1, ts2)

    # run
    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        # TD BUGS
        self.FIX_TD_30686()

        # TS BUGS
        self.FIX_TS_5105()

        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
