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

    def insertnow(self):
        tdSql.execute("drop database if exists dbcom")
        tdSql.execute("create database  if not exists dbcom keep 36500")
        tdSql.execute("use dbcom")

        tdSql.execute(
            "create table stbcom (ts timestamp, c1 int, c2 tinyint, c3 smallint, c4 bigint, c5 float, c6 double) TAGS(t1 int)"
        )
        tdSql.execute("create table tcom1 using stbcom tags(1)")

        # timestamp list:
        #   0 -> "1970-01-01 08:00:00" | -28800000 -> "1970-01-01 00:00:00" | -946800000000 -> "1940-01-01 00:00:00"
        #   -631180800000 -> "1950-01-01 00:00:00"

        tdSql.execute("insert into tcom1 values (now-1d, 1, 11, 21, 31, 41.0, 51.1)")
        tdSql.execute("insert into tcom1 values (now-2d, 2, 12, 22, 32, 42.0, 52.1)")
        tdSql.execute("insert into tcom1 values (now-3d, 3, 13, 23, 33, 43.0, 53.1)")
        tdSql.execute("insert into tcom1 values (now-4d, 4, 14, 24, 34, 44.0, 54.1)")

    def querycom(self):
        tdSql.query("select * from tcom1 where c1=2-1")
        tdSql.checkRows(1)
        tdSql.query("select * from tcom1 where c1=-1+2")
        tdSql.checkRows(1)
        tdSql.query("select * from tcom1 where c1=1.0*1.0")
        tdSql.checkRows(1)
        tdSql.query("select * from tcom1 where c1=1.0/1.0")
        tdSql.checkRows(1)
        tdSql.query("select * from tcom1 where c1>1.0/1.0")
        tdSql.checkRows(3)
        tdSql.query("select * from tcom1 where c1<1.0/1.0")
        tdSql.checkRows(0)

        tdSql.query("select * from tcom1 where c2=12-1")
        tdSql.checkRows(1)
        tdSql.query("select * from tcom1 where c2=-1+12")
        tdSql.checkRows(1)
        tdSql.query("select * from tcom1 where c2=11.0*1.0")
        tdSql.checkRows(1)
        tdSql.query("select * from tcom1 where c2=11.0/1.0")
        tdSql.checkRows(1)
        tdSql.query("select * from tcom1 where c2>11.0/1.0")
        tdSql.checkRows(3)
        tdSql.query("select * from tcom1 where c2<11.0/1.0")
        tdSql.checkRows(0)

        tdSql.query("select * from tcom1 where c3=22-1")
        tdSql.checkRows(1)
        tdSql.query("select * from tcom1 where c3=-1+22")
        tdSql.checkRows(1)
        tdSql.query("select * from tcom1 where c3=21.0*1.0")
        tdSql.checkRows(1)
        tdSql.query("select * from tcom1 where c3=21.0/1.0")
        tdSql.checkRows(1)
        tdSql.query("select * from tcom1 where c3>21.0/1.0")
        tdSql.checkRows(3)
        tdSql.query("select * from tcom1 where c3<21.0/1.0")
        tdSql.checkRows(0)

        tdSql.query("select * from tcom1 where c4=32-1")
        tdSql.checkRows(1)
        tdSql.query("select * from tcom1 where c4=-1+32")
        tdSql.checkRows(1)
        tdSql.query("select * from tcom1 where c4=31.0*1.0")
        tdSql.checkRows(1)
        tdSql.query("select * from tcom1 where c4=31.0/1.0")
        tdSql.checkRows(1)
        tdSql.query("select * from tcom1 where c4>31.0/1.0")
        tdSql.checkRows(3)
        tdSql.query("select * from tcom1 where c4<31.0/1.0")
        tdSql.checkRows(0)

        tdSql.query("select * from tcom1 where c5=42-1")
        tdSql.checkRows(1)
        tdSql.query("select * from tcom1 where c5=-1+42")
        tdSql.checkRows(1)
        tdSql.query("select * from tcom1 where c5=41*1")
        tdSql.checkRows(1)
        tdSql.query("select * from tcom1 where c5=41/1")
        tdSql.checkRows(1)
        tdSql.query("select * from tcom1 where c5>41/1")
        tdSql.checkRows(3)
        tdSql.query("select * from tcom1 where c5<41/1")
        tdSql.checkRows(0)
        tdSql.query("select * from tcom1 where c5=42.000000008-1.0000000099999999999999")
        tdSql.checkRows(1)
        tdSql.query("select * from tcom1 where c5=42.0008-1.0000099999999999999")
        tdSql.checkRows(0)

        tdSql.query("select * from tcom1 where c6=52-0.9")
        tdSql.checkRows(1)
        tdSql.query("select * from tcom1 where c6=-0.9+52")
        tdSql.checkRows(1)
        tdSql.query("select * from tcom1 where c6=51.1*1")
        tdSql.checkRows(1)
        tdSql.query("select * from tcom1 where c6=51.1/1")
        tdSql.checkRows(1)
        tdSql.query("select * from tcom1 where c6>51.1/1")
        tdSql.checkRows(3)
        tdSql.query("select * from tcom1 where c6<51.1/1")
        tdSql.checkRows(0)
        tdSql.query("select * from tcom1 where c6=52.100000000000008-1.000000000000009")
        tdSql.checkRows(1)


    def run(self):
        self.insertnow()
        self.querycom()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())