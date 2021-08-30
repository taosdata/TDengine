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

        print("======= Verify filter for bool, nchar and binary type =========")
        tdLog.debug(
            "create table st(ts timestamp, tbcol1 bool, tbcol2 binary(10), tbcol3 nchar(20), tbcol4 tinyint, tbcol5 smallint, tbcol6 int, tbcol7 bigint, tbcol8 float, tbcol9 double) tags(tagcol1 bool, tagcol2 binary(10), tagcol3 nchar(10))")
        tdSql.execute(
            "create table st(ts timestamp, tbcol1 bool, tbcol2 binary(10), tbcol3 nchar(20), tbcol4 tinyint, tbcol5 smallint, tbcol6 int, tbcol7 bigint, tbcol8 float, tbcol9 double) tags(tagcol1 bool, tagcol2 binary(10), tagcol3 nchar(10))")

        tdSql.execute("create table st1 using st tags(true, 'table1', '水表')")
        for i in range(1, 6):
            tdSql.execute(
                "insert into st1 values(%d, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d, %f, %f)" %
                (self.ts + i, i %
                 2, i, i,
                 i, i, i, i, 1.0, 1.0))

        # =============Data type keywords cannot be used in filter====================
        # timestamp
        tdSql.error("select * from st where timestamp = 1629417600")

        # bool
        tdSql.error("select * from st where bool = false")

        #binary
        tdSql.error("select * from st where binary = 'taosdata'")

        # nchar
        tdSql.error("select * from st where nchar = '涛思数据'")

        # tinyint
        tdSql.error("select * from st where tinyint = 127")

        # smallint
        tdSql.error("select * from st where smallint = 32767")

        # int
        tdSql.error("select * from st where INTEGER = 2147483647")
        tdSql.error("select * from st where int = 2147483647")

        # bigint
        tdSql.error("select * from st where bigint = 2147483647")

        # float
        tdSql.error("select * from st where float = 3.4E38")

        # double
        tdSql.error("select * from st where double = 1.7E308")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
