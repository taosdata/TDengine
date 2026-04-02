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
        tdLog.debug(f"start to execute {__file__}")
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database  if not exists db keep 36500")
        tdSql.execute("use db")

        tdLog.printNoPrefix("==========step1:create table && insert data")
        tdSql.execute("create table stbtag (ts timestamp, c1 int) TAGS(t1 int)")
        tdSql.execute("create table tag1 using stbtag tags(1)")

        tdLog.printNoPrefix("==========step2:alter stb add tag create new chiltable")
        tdSql.execute("alter table stbtag add tag t2 int")
        tdSql.execute("alter table stbtag add tag t3 tinyint")
        tdSql.execute("alter table stbtag add tag t4 smallint ")
        tdSql.execute("alter table stbtag add tag t5 bigint")
        tdSql.execute("alter table stbtag add tag t6 float ")
        tdSql.execute("alter table stbtag add tag t7 double ")
        tdSql.execute("alter table stbtag add tag t8 bool ")
        tdSql.execute("alter table stbtag add tag t9 binary(10) ")
        tdSql.execute("alter table stbtag add tag t10 nchar(10)")

        tdSql.execute("create table tag2 using stbtag tags(2, 22, 23, 24, 25, 26.1, 27.1, 1, 'binary9', 'nchar10')")
        tdSql.query( "select tbname, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10 from stbtag" )
        tdSql.checkData(1, 0, "tag2")
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 22)
        tdSql.checkData(1, 3, 23)
        tdSql.checkData(1, 4, 24)
        tdSql.checkData(1, 5, 25)
        tdSql.checkData(1, 6, 26.1)
        tdSql.checkData(1, 7, 27.1)
        tdSql.checkData(1, 8, 1)
        tdSql.checkData(1, 9, "binary9")
        tdSql.checkData(1, 10, "nchar10")

        tdLog.printNoPrefix("==========step3:alter stb drop tag create new chiltable")
        tdSql.execute("alter table stbtag drop tag t2 ")
        tdSql.execute("alter table stbtag drop tag t3 ")
        tdSql.execute("alter table stbtag drop tag t4 ")
        tdSql.execute("alter table stbtag drop tag t5 ")
        tdSql.execute("alter table stbtag drop tag t6 ")
        tdSql.execute("alter table stbtag drop tag t7 ")
        tdSql.execute("alter table stbtag drop tag t8 ")
        tdSql.execute("alter table stbtag drop tag t9 ")
        tdSql.execute("alter table stbtag drop tag t10 ")

        tdSql.execute("create table tag3 using stbtag tags(3)")
        tdSql.query("select * from stbtag where tbname like 'tag3' ")
        tdSql.checkCols(3)
        tdSql.query("select tbname, t1 from stbtag where tbname like 'tag3' ")
        tdSql.checkData(0, 1, 3)



    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())