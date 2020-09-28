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

        self.powers = [7, 15, 31, 63]
        self.types = ["tinyint", "smallint", "int", "bigint"]
        self.rowNum = 10
        self.ts = 1537146000000

    def run(self):
        tdSql.prepare()

        for i in range(len(self.powers)):
            curType = self.types[i]
            print("======= Verify filter for %s type =========" % (curType))
            tdLog.debug(
                "create table st%s(ts timestamp, num %s) tags(id %s)" %
                (curType, curType, curType))
            tdSql.execute(
                "create table st%s(ts timestamp, num %s) tags(id %s)" %
                (curType, curType, curType))

            # create 10 tables, insert 10 rows for each table
            for j in range(self.rowNum):
                tdSql.execute(
                    "create table st%s%d using st%s tags(%d)" %
                    (curType, j + 1, curType, j + 1))
                for k in range(self.rowNum):
                    tdSql.execute(
                        "insert into st%s%d values(%d, %d)" %
                        (curType, j + 1, self.ts + k + 1, j * 10 + k + 1))

            tdSql.error("insert into st%s10 values(%d, %d)" %
                        (curType, self.ts + 11, pow(2, self.powers[i])))
            tdSql.execute("insert into st%s10 values(%d, %d)" %
                          (curType, self.ts + 12, pow(2, self.powers[i]) - 1))
            tdSql.error("insert into st%s10 values(%d, %d)" %
                        (curType, self.ts + 13, pow(-2, self.powers[i])))
            tdSql.execute("insert into st%s10 values(%d, %d)" %
                          (curType, self.ts + 14, pow(-2, self.powers[i]) + 1))

            # > for int type on column
            tdSql.query("select * from st%s where num > 50" % curType)
            tdSql.checkRows(51)

            # >= for int type on column
            tdSql.query("select * from st%s where num >= 50" % curType)
            tdSql.checkRows(52)

            # = for int type on column
            tdSql.query("select * from st%s where num = 50" % curType)
            tdSql.checkRows(1)

            # < for int type on column
            tdSql.query("select * from st%s where num < 50" % curType)
            tdSql.checkRows(50)

            # <= for int type on column
            tdSql.query("select * from st%s where num <= 50" % curType)
            tdSql.checkRows(51)

            # <> for int type on column
            tdSql.query("select * from st%s where num <> 50" % curType)
            tdSql.checkRows(101)

            # != for int type on column
            tdSql.query("select * from st%s where num != 50" % curType)
            tdSql.checkRows(101)

            # range for int type on column
            tdSql.query(
                "select * from st%s where num > 50 and num < 100" %
                curType)
            tdSql.checkRows(49)

            tdSql.query(
                "select * from st%s where num >= 50 and num < 100" %
                curType)
            tdSql.checkRows(50)

            tdSql.query(
                "select * from st%s where num > 50 and num <= 100" %
                curType)
            tdSql.checkRows(50)

            tdSql.query(
                "select * from st%s where num >= 50 and num <= 100" %
                curType)
            tdSql.checkRows(51)

            # > for int type on tag
            tdSql.query("select * from st%s where id > 5" % curType)
            tdSql.checkRows(52)

            # >= for int type on tag
            tdSql.query("select * from st%s where id >= 5" % curType)
            tdSql.checkRows(62)

            # = for int type on tag
            tdSql.query("select * from st%s where id = 5" % curType)
            tdSql.checkRows(10)

            # < for int type on tag
            tdSql.query("select * from st%s where id < 5" % curType)
            tdSql.checkRows(40)

            # <= for int type on tag
            tdSql.query("select * from st%s where id <= 5" % curType)
            tdSql.checkRows(50)

            # <> for int type on tag
            tdSql.query("select * from st%s where id <> 5" % curType)
            tdSql.checkRows(92)

            # != for int type on tag
            tdSql.query("select * from st%s where id != 5" % curType)
            tdSql.checkRows(92)

            # != for int type on tag
            tdSql.query("select * from st%s where id != 5" % curType)
            tdSql.checkRows(92)

            # range for int type on tag
            tdSql.query("select * from st%s where id > 5 and id < 7" % curType)
            tdSql.checkRows(10)

            tdSql.query(
                "select * from st%s where id >= 5 and id < 7" %
                curType)
            tdSql.checkRows(20)

            tdSql.query(
                "select * from st%s where id > 5 and id <= 7" %
                curType)
            tdSql.checkRows(20)

            tdSql.query(
                "select * from st%s where id >= 5 and id <= 7" %
                curType)
            tdSql.checkRows(30)

            print(
                "======= Verify filter for %s type finished =========" %
                curType)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
