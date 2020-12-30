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
        print("prepare data")
        tdSql.execute("create table db.st (ts timestamp, i int) tags(j int)")
        tdSql.execute("create table db.tb using st tags(1)")
        tdSql.execute("insert into db.tb values(now, 1)")

        print("==============step2")
        print("create table as select")
        try:
            tdSql.execute("create table db.test as select * from db.st")
        except Exception as e:
            tdLog.exit(e)

        # case for defect: https://jira.taosdata.com:18080/browse/TD-2560
        tdSql.execute("create table db.tb02 using st tags(2)")
        tdSql.execute("create table db.tb03 using st tags(3)")
        tdSql.execute("create table db.tb04 using st tags(4)")

        tdSql.query("show tables like 'tb%' ")
        tdSql.checkRows(4)

        tdSql.query("show tables like 'tb0%' ")
        tdSql.checkRows(3)

        tdSql.execute("create table db.st0 (ts timestamp, i int) tags(j int)")
        tdSql.execute("create table db.st1 (ts timestamp, i int, c2 int) tags(j int, loc nchar(20))")

        tdSql.query("show stables like 'st%' ")
        tdSql.checkRows(3)
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
