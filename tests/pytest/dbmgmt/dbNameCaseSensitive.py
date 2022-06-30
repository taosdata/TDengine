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
        self._conn = conn

    def run(self):        

        # database name
        tdSql.execute("create database db")
        tdSql.query("show databases")
        tdSql.checkRows(1)

        tdSql.error("create database Db")
        tdSql.error("create database `db`")
        tdSql.execute("create database `Db`")
        tdSql.query("show databases")
        tdSql.checkRows(2)

        tdSql.execute("alter database db cachelast 1")
        tdSql.execute("alter database `Db` cachelast 1")

        tdSql.execute("use db")
        tdSql.query("select database()")
        tdSql.checkData(0, 0, 'db');
        tdSql.query("show db.vgroups")
        tdSql.checkRows(0)

        tdSql.execute("use `Db`")
        tdSql.query("select database()")
        tdSql.checkData(0, 0, 'Db');
        tdSql.query("show `Db`.vgroups")
        tdSql.checkRows(0)
        tdSql.query("show create database `Db`")
        tdSql.checkRows(1)


        tdSql.execute("drop database db")
        tdSql.execute("drop database `Db`")

        tdSql.query("show databases")
        tdSql.checkRows(0)

        # corner cases
        tdSql.execute("create database `电力系统`")
        tdSql.query("show `电力系统`.vgroups")
        tdSql.checkRows(0)
        tdSql.query("show databases")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "电力系统")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())