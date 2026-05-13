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


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        print("==============step1")
        tdSql.execute(
            "create table if not exists st (ts timestamp, tagtype int) tags(dev nchar(50))")
        tdSql.execute(
            'CREATE TABLE if not exists dev_001 using st tags("dev_01")')
        tdSql.execute(
            'CREATE TABLE if not exists dev_002 using st tags("dev_02")')

        print("==============step2")

        tdSql.execute(
            """INSERT INTO dev_001(ts, tagtype) VALUES('2020-05-13 10:00:00.000', 1),
            ('2020-05-13 10:00:00.001', 1)
             dev_002 VALUES('2020-05-13 10:00:00.001', 1)""")

        tdSql.query("select * from db.st where dev='dev_01'")
        tdSql.checkRows(2)

        tdSql.query("select * from db.st where dev='dev_02'")
        tdSql.checkRows(1)

        #For: https://jira.taosdata.com:18080/browse/TD-2671
        print("==============step3")
        tdSql.execute(
            "create stable if not exists stb (ts timestamp, tagtype int) tags(dev nchar(50))")
        tdSql.execute(
            'CREATE TABLE if not exists dev_01 using stb tags("dev_01")')
        tdSql.execute(
            'CREATE TABLE if not exists dev_02 using stb tags("dev_02")')

        print("==============step4")

        tdSql.execute(
            """INSERT INTO dev_01(ts, tagtype) VALUES('2020-05-13 10:00:00.000', 1),
            ('2020-05-13 10:00:00.001', 1)
             dev_02 VALUES('2020-05-13 10:00:00.001', 1)""")

        tdSql.query("select * from db.stb where dev='dev_01'")
        tdSql.checkRows(2)

        tdSql.query("select * from db.stb where dev='dev_02'")
        tdSql.checkRows(1)

        tdSql.query("describe db.stb")
        tdSql.checkRows(3)

        tdSql.error("drop stable if exists db.dev_01")
        tdSql.error("drop stable if exists db.dev_02")

        tdSql.execute("alter stable db.stb add tag t1 int")
        tdSql.query("describe db.stb")
        tdSql.checkRows(4)

        tdSql.execute("drop stable db.stb")
        tdSql.query("show stables")
        tdSql.checkRows(1)

        tdSql.error("drop stable if exists db.dev_001")
        tdSql.error("drop stable if exists db.dev_002")
        
        for i in range(10):
            tdSql.execute("drop stable if exists db.stb")
            tdSql.query("show stables")
            tdSql.checkRows(1)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
