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

        print("======= step 1: create table and insert data =========")
        tdLog.debug(
            ''' create table st(ts timestamp, tbcol1 tinyint, tbcol2 smallint, tbcol3 int, tbcol4 bigint, tbcol5 float, tbcol6 double,
                tbcol7 bool, tbcol8 nchar(20), tbcol9 binary(20)) tags(tagcol1 tinyint, tagcol2 smallint, tagcol3 int, tagcol4 bigint, tagcol5 float,
                tagcol6 double, tagcol7 bool, tagcol8 nchar(20), tagcol9 binary(20))''')
        tdSql.execute(
            ''' create table st(ts timestamp, tbcol1 tinyint, tbcol2 smallint, tbcol3 int, tbcol4 bigint, tbcol5 float, tbcol6 double,
                tbcol7 bool, tbcol8 nchar(20), tbcol9 binary(20)) tags(tagcol1 tinyint, tagcol2 smallint, tagcol3 int, tagcol4 bigint, tagcol5 float,
                tagcol6 double, tagcol7 bool, tagcol8 nchar(20), tagcol9 binary(20))''')

        for i in range(self.rowNum):
            tdSql.execute("create table st%d using st tags(%d, %d, %d, %d, %f, %f, %d, 'tag%d', '标签%d')" % (
                i + 1, i + 1, i + 1, i + 1, i + 1, 1.1 * (i + 1), 1.23 * (i + 1), (i + 1) % 2, i + 1, i + 1))
            for j in range(self.rowNum):
                tdSql.execute("insert into st%d values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d')" % (
                    i + 1, self.ts + 10 * (i + 1) + j + 1, j + 1, j + 1, j + 1, j + 1, 1.1 * (j + 1), 1.23 * (j + 1), (j + 1) % 2, j + 1, j + 1))

        print("======= step 2: verify order for each column =========")
        # sort for timestamp in asc order
        tdSql.query("select * from st order by ts asc")
        tdSql.checkColumnSorted(0, "asc")

        # sort for timestamp in desc order
        tdSql.query("select * from st order by ts desc")
        tdSql.checkColumnSorted(0, "desc")

        for i in range(1, 10):
            tdSql.error("select * from st order by tbcol%d" % i)
            tdSql.error("select * from st order by tbcol%d asc" % i)
            tdSql.error("select * from st order by tbcol%d desc" % i)

            tdSql.query(
                "select avg(tbcol1) from st group by tagcol%d order by tagcol%d" %
                (i, i))
            tdSql.checkColumnSorted(1, "")

            tdSql.query(
                "select avg(tbcol1) from st group by tagcol%d order by tagcol%d asc" %
                (i, i))
            tdSql.checkColumnSorted(1, "asc")

            tdSql.query(
                "select avg(tbcol1) from st group by tagcol%d order by tagcol%d desc" %
                (i, i))
            tdSql.checkColumnSorted(1, "desc")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
