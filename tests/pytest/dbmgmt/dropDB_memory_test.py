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
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()
        tbNum = 10000
        insertRows = 1
        db = "db"
        loop = 2
        tdSql.execute("drop database if exists %s" % (db))
        tdSql.execute("reset query cache")
        tdLog.sleep(1)
        for k in range(1, loop + 1):
            tdLog.info("===========Loop%d starts============" % (k))
            tdSql.execute(
                "create database %s cache 163840 ablocks 40 maxtables 5000 wal 0" %
                (db))
            tdSql.execute("use %s" % (db))
            tdSql.execute(
                "create table stb (ts timestamp, c1 int) tags(t1 bigint, t2 double)")
            for j in range(1, tbNum):
                tdSql.execute(
                    "create table tb%d using stb tags(%d, %d)" %
                    (j, j, j))

            for j in range(1, tbNum):
                for i in range(0, insertRows):
                    tdSql.execute(
                        "insert into tb%d values (now + %dm, %d)" %
                        (j, i, i))
                tdSql.query("select * from tb%d" % (j))
                tdSql.checkRows(insertRows)
                tdLog.info("insert %d rows into tb%d" % (insertRows, j))
            # tdSql.sleep(3)
            tdSql.execute("drop database %s" % (db))
            tdLog.sleep(2)
            tdLog.info("===========Loop%d completed!=============" % (k))

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


#tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
