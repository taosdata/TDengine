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

        self.rowNum = 10
        self.ts = 1537146000000

    def run(self):
        tdSql.prepare()

        print("======= Verify filter for float and double type =========")
        tdLog.debug(
            "create table st(ts timestamp, num float, speed double) tags(tagcol1 float, tagcol2 double)")
        tdSql.execute(
            "create table st(ts timestamp, num float, speed double) tags(tagcol1 float, tagcol2 double)")

        for j in range(self.rowNum):
            tdSql.execute("insert into st1 using st tags(1.1, 2.3) values(%d, %f, %f)" % (
                self.ts + j + 1, 1.1 * (j + 1), 2.3 * (j + 1)))

        # > for float type on column
        tdSql.query("select * from st where num > 5.5")
        tdSql.checkRows(5)

        # >= for float type on column
        tdSql.query("select * from st where num >= 5.5")
        tdSql.checkRows(6)

        # = for float type on column
        tdSql.query("select * from st where num = 5.5")
        tdSql.checkRows(1)

        # <> for float type on column
        tdSql.query("select * from st where num <> 5.5")
        tdSql.checkRows(9)

        # != for float type on column
        tdSql.query("select * from st where num != 5.5")
        tdSql.checkRows(9)

        # <= for float type on column
        tdSql.query("select * from st where num <= 5.5")
        tdSql.checkRows(5)

        # < for float type on column
        tdSql.query("select * from st where num < 5.5")
        tdSql.checkRows(4)

        # range for float type on column
        tdSql.query("select * from st where num > 5.5 and num < 11.0")
        tdSql.checkRows(4)

        tdSql.query("select * from st where num >= 5.5 and num < 11.0")
        tdSql.checkRows(5)

        tdSql.query("select * from st where num > 5.5 and num <= 11.0")
        tdSql.checkRows(5)

        tdSql.query("select * from st where num >= 5.5 and num <= 11.0")
        tdSql.checkRows(6)

        # > for float type on tag
        tdSql.query("select * from st where tagcol1 > 1.1")
        tdSql.checkRows(0)

        # >= for float type on tag
        tdSql.query("select * from st where tagcol1 >= 1.1")
        tdSql.checkRows(10)

        # = for float type on tag
        tdSql.query("select * from st where tagcol1 = 1.1")
        tdSql.checkRows(10)

        # <> for float type on tag
        tdSql.query("select * from st where tagcol1 <> 1.1")
        tdSql.checkRows(0)

        # != for float type on tag
        tdSql.query("select * from st where tagcol1 != 1.1")
        tdSql.checkRows(0)

        # <= for float type on tag
        tdSql.query("select * from st where tagcol1 <= 1.1")
        tdSql.checkRows(10)

        # < for float type on tag
        tdSql.query("select * from st where tagcol1 < 1.1")
        tdSql.checkRows(0)

        # > for double type on column
        tdSql.query("select * from st where speed > 11.5")
        tdSql.checkRows(5)

        # >= for double type on column
        tdSql.query("select * from st where speed >= 11.5")
        tdSql.checkRows(6)

        # = for double type on column
        tdSql.query("select * from st where speed = 11.5")
        tdSql.checkRows(1)

        # <> for double type on column
        tdSql.query("select * from st where speed <> 11.5")
        tdSql.checkRows(9)

        # != for double type on column
        tdSql.query("select * from st where speed != 11.5")
        tdSql.checkRows(9)

        # <= for double type on column
        tdSql.query("select * from st where speed <= 11.5")
        tdSql.checkRows(5)

        # < for double type on column
        tdSql.query("select * from st where speed < 11.5")
        tdSql.checkRows(4)

        # range for double type on column
        tdSql.query("select * from st where speed > 11.5 and speed < 20.7")
        tdSql.checkRows(3)

        tdSql.query("select * from st where speed >= 11.5 and speed < 20.7")
        tdSql.checkRows(4)

        tdSql.query("select * from st where speed > 11.5 and speed <= 20.7")
        tdSql.checkRows(4)

        tdSql.query("select * from st where speed >= 11.5 and speed <= 20.7")
        tdSql.checkRows(5)

        # > for double type on tag
        tdSql.query("select * from st where tagcol2 > 2.3")
        tdSql.checkRows(0)

        # >= for double type on tag
        tdSql.query("select * from st where tagcol2 >= 2.3")
        tdSql.checkRows(10)

        # = for double type on tag
        tdSql.query("select * from st where tagcol2 = 2.3")
        tdSql.checkRows(10)

        # <> for double type on tag
        tdSql.query("select * from st where tagcol2 <> 2.3")
        tdSql.checkRows(0)

        # != for double type on tag
        tdSql.query("select * from st where tagcol2 != 2.3")
        tdSql.checkRows(0)

        # <= for double type on tag
        tdSql.query("select * from st where tagcol2 <= 2.3")
        tdSql.checkRows(10)

        # < for double type on tag
        tdSql.query("select * from st where tagcol2 < 2.3")
        tdSql.checkRows(0)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
