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
import numpy as np


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.rowNum = 10
        self.ts = 1537146000000

    def checkColumnSorted(self, col, order):
        frame = inspect.stack()[1]
        callerModule = inspect.getmodule(frame[0])
        callerFilename = callerModule.__file__

        if col < 0:
            tdLog.exit(
                "%s failed: sql:%s, col:%d is smaller than zero" %
                (callerFilename, tdSql.sql, col))
        if col > tdSql.queryCols:
            tdLog.exit(
                "%s failed: sql:%s, col:%d is larger than queryCols:%d" %
                (callerFilename, tdSql.sql, col, tdSql.queryCols))

        matrix = np.array(tdSql.queryResult)
        list = matrix[:, 0]

        if order == "" or order.upper() == "ASC":
            if all(sorted(list) == list):
                tdLog.info(
                    "sql:%s, column :%d is sorted in accending order as expected" %
                    (tdSql.sql, col))
            else:
                tdLog.exit(
                    "%s failed: sql:%s, col:%d is not sorted in accesnind order" %
                    (callerFilename, tdSql.sql, col))
        elif order.upper() == "DESC":
            if all(sorted(list, reverse=True) == list):
                tdLog.info(
                    "sql:%s, column :%d is sorted in decending order as expected" %
                    (tdSql.sql, col))
            else:
                tdLog.exit(
                    "%s failed: sql:%s, col:%d is not sorted in decending order" %
                    (callerFilename, tdSql.sql, col))
        else:
            tdLog.exit(
                "%s failed: sql:%s, the order provided for col:%d is not correct" %
                (callerFilename, tdSql.sql, col))

    def run(self):
        tdSql.prepare()

        print("======= step 1: create table and insert data =========")
        tdLog.debug(
            ''' create table st(ts timestamp, tbcol1 tinyint, tbcol2 smallint, tbcol3 int, tbcol4 bigint, tbcol5 float, tbcol6 double,
                tbcol7 bool, tbcol8 nchar(20), tbcol9 binary(20), tbcol11 tinyint unsigned, tbcol12 smallint unsigned, tbcol13 int unsigned, tbcol14 bigint unsigned) tags(tagcol1 tinyint, tagcol2 smallint, tagcol3 int, tagcol4 bigint, tagcol5 float,
                tagcol6 double, tagcol7 bool, tagcol8 nchar(20), tagcol9 binary(20),  tagcol11 tinyint unsigned, tagcol12 smallint unsigned, tagcol13 int unsigned, tagcol14 bigint unsigned)''')
        tdSql.execute(
            ''' create table st(ts timestamp, tbcol1 tinyint, tbcol2 smallint, tbcol3 int, tbcol4 bigint, tbcol5 float, tbcol6 double,
                tbcol7 bool, tbcol8 nchar(20), tbcol9 binary(20), tbcol11 tinyint unsigned, tbcol12 smallint unsigned, tbcol13 int unsigned, tbcol14 bigint unsigned) tags(tagcol1 tinyint, tagcol2 smallint, tagcol3 int, tagcol4 bigint, tagcol5 float,
                tagcol6 double, tagcol7 bool, tagcol8 nchar(20), tagcol9 binary(20),  tagcol11 tinyint unsigned, tagcol12 smallint unsigned, tagcol13 int unsigned, tagcol14 bigint unsigned)''')

        for i in range(self.rowNum):
            tdSql.execute("create table st%d using st tags(%d, %d, %d, %d, %f, %f, %d, 'tag%d', '标签%d', %d, %d, %d, %d)" % (
                i + 1, i + 1, i + 1, i + 1, i + 1, 1.1 * (i + 1), 1.23 * (i + 1), (i + 1) % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))
            for j in range(self.rowNum):
                tdSql.execute("insert into st%d values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)" % (
                    i + 1, self.ts + 10 * (i + 1) + j + 1, j + 1, j + 1, j + 1, j + 1, 1.1 * (j + 1), 1.23 * (j + 1), (j + 1) % 2, j + 1, j + 1, i + 1, i + 1, i + 1, i + 1))

        print("======= step 2: verify order for each column =========")
        # sort for timestamp in asc order
        tdSql.query("select * from st order by ts asc")
        self.checkColumnSorted(0, "asc")

        # sort for timestamp in desc order
        tdSql.query("select * from st order by ts desc")
        self.checkColumnSorted(0, "desc")

        print("======= step 2: verify order for special column =========")
        
        tdSql.query("select tbcol1 from st order by ts desc")

        tdSql.query("select tbcol6 from st order by ts desc")

        for i in range(1, 10):
            tdSql.error("select * from st order by tbcol%d" % i)
            tdSql.error("select * from st order by tbcol%d asc" % i)
            tdSql.error("select * from st order by tbcol%d desc" % i)

            tdSql.query(
                "select avg(tbcol1) from st group by tagcol%d order by tagcol%d" %
                (i, i))
            self.checkColumnSorted(1, "")

            tdSql.query(
                "select avg(tbcol1) from st group by tagcol%d order by tagcol%d asc" %
                (i, i))
            self.checkColumnSorted(1, "asc")

            tdSql.query(
                "select avg(tbcol1) from st group by tagcol%d order by tagcol%d desc" %
                (i, i))
            self.checkColumnSorted(1, "desc")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
