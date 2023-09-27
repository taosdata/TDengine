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
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def run(self):
        tdSql.prepare()

        print("==============Case 1: add column, restart taosd, drop the same colum then add it back")
        tdSql.execute(
            "create table st(ts timestamp, speed int, bb bool, ii int, dd double) tags(loc nchar(20))")
        tdSql.execute(
            "insert into t1 using st tags('beijing') values('2023-09-27 10:00:00', 1, 'true', 1, 0.123)")
        tdSql.execute(
            "insert into t1 using st tags('beijing') values('2023-09-27 11:00:00', 2, 'true', 2, 0.444)")
        tdSql.execute(
            "alter table st add column tbcol binary(20)")
        tdSql.execute(
            "alter table st drop column speed")

        # restart taosd
        tdDnodes.forcestop(1)
        tdDnodes.start(1)

        tdSql.execute(
            "alter table st drop column tbcol")
        tdSql.execute(
            "alter table st add column tbcol binary(20)")

        tdSql.query("select * from st")
        tdSql.checkRows(2)

        tdSql.query("select last(bb), stddev(dd), last(dd), last(ii) from st where ts > '2023-09-27' and ts < '2023-09-28' and bb == 'true' and ii > 0")
        tdSql.checkRows(1)

        print("==============Case 2: keep adding columns, restart taosd")
        tdSql.execute(
            "create table dt(ts timestamp, tbcol1 tinyint) tags(tgcol1 tinyint)")
        tdSql.execute(
            "alter table dt add column tbcol2 int")
        tdSql.execute(
            "alter table dt add column tbcol3 smallint")
        tdSql.execute(
            "alter table dt add column tbcol4 bigint")
        tdSql.execute(
            "alter table dt add column tbcol5 float")
        tdSql.execute(
            "alter table dt add column tbcol6 double")
        tdSql.execute(
            "alter table dt add column tbcol7 bool")
        tdSql.execute(
            "alter table dt add column tbcol8 nchar(20)")
        tdSql.execute(
            "alter table dt add column tbcol9 binary(20)")
        tdSql.execute(
            "alter table dt add column tbcol10 tinyint unsigned")
        tdSql.execute(
            "alter table dt add column tbcol11 int unsigned")
        tdSql.execute(
            "alter table dt add column tbcol12 smallint unsigned")
        tdSql.execute(
            "alter table dt add column tbcol13 bigint unsigned")

        # restart taosd
        tdDnodes.forcestop(1)
        tdDnodes.start(1)

        tdSql.query("select * from dt")
        tdSql.checkRows(0)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
