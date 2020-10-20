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
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
from util.dnodes import tdDnodes


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        print("==============step1")
        tdSql.execute("create database db_vplu");
        tdSql.execute("use db_vplu")
        tdSql.execute("CREATE table if not exists st (ts timestamp, speed int) tags(id int)")
        tdSql.execute("CREATE table if not exists st_vplu (ts timestamp, speed int) tags(id int)")

        print("==============step2")

        tdSql.execute("drop table st")

        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st_vplu")

        tdDnodes.stopAll()
        tdDnodes.start(1)
        
        tdSql.execute("use db_vplu")
        tdSql.query("show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "st_vplu")

        tdSql.execute("drop database db")
        tdSql.query("show databases")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "db_vplu")

        tdDnodes.stopAll()
        tdDnodes.start(1)

        tdSql.query("show databases")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "db_vplu")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
