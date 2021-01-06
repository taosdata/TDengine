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

        tdSql.query("select * from db.st where ts='2020-05-13 10:00:00.000'")
        tdSql.checkRows(1)

        ## test case for https://jira.taosdata.com:18080/browse/TD-2488
        tdSql.execute("create table m1(ts timestamp, k int) tags(a int)")
        tdSql.execute("create table t1 using m1 tags(1)")
        tdSql.execute("create table t2 using m1 tags(2)")
        tdSql.execute("insert into t1 values('2020-1-1 1:1:1', 1)")
        tdSql.execute("insert into t1 values('2020-1-1 1:10:1', 2)")
        tdSql.execute("insert into t2 values('2020-1-1 1:5:1', 99)")
        
        tdSql.query("select count(*) from m1 where ts = '2020-1-1 1:5:1' ")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdDnodes.stop(1)
        tdDnodes.start(1)

        tdSql.query("select count(*) from m1 where ts = '2020-1-1 1:5:1' ")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
