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

import taos
import sys

from util.log import *
from util.sql import *
from util.cases import *

class TDTestCase:

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def run(self):
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database db keep 36500,36500,36500")
        tdSql.execute("use db")
        tdSql.execute("create stable if not exists stb1 (ts timestamp, c1 int) tags(t11 int)")
        tdSql.execute("create table t10 using stb1 tags(1)")
        gapdate = (datetime.datetime.now() - datetime.datetime(1970, 1, 1, 8, 0, 0)).days

        tdLog.printNoPrefix("==========step2:insert data")
        tdSql.execute(f"insert into t10 values (now-{gapdate}d, 1)")
        tdSql.execute(f"insert into t10 values (now-{gapdate + 1}d, 2)")

        tdLog.printNoPrefix("==========step3:query")
        tdSql.query("select * from t10 where c1=1")
        tdSql.checkRows(1)
        tdSql.query("select * from t10 where c1=2")
        tdSql.checkRows(1)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())