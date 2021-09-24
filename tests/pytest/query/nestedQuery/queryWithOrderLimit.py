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
import os
import taos
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
from util.dnodes import tdDnodes
import random

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts = 1593548685000
        self.tables = 10
        self.rowsPerTable = 100

    def run(self):
        # tdSql.execute("drop database db ")
        tdSql.prepare()
        tdSql.execute("create table st (ts timestamp, num int,  value int) tags (loc nchar(30))")
        for i in range(self.tables):
            for j in range(self.rowsPerTable): 
                args1=(i, i, self.ts + i * self.rowsPerTable + j * 10000, i, random.randint(1, 100))              
                tdSql.execute("insert into t%d using st tags('beijing%d')  values(%d, %d, %d)" % args1)
                
        tdSql.query("select * from (select * from st)")
        tdSql.checkRows(self.tables * self.rowsPerTable)

        tdSql.query("select * from (select * from st limit 10)")
        tdSql.checkRows(10)

        tdSql.query("select * from (select * from st order by ts desc limit 10)")
        tdSql.checkRows(10)

        # bug: https://jira.taosdata.com:18080/browse/TD-5043
        tdSql.query("select * from (select * from st order by ts desc limit 10 offset 1000)")
        tdSql.checkRows(0)

        tdSql.query("select avg(value), sum(value) from st group by tbname")
        tdSql.checkRows(self.tables)

        tdSql.query("select * from (select avg(value), sum(value) from st group by tbname)")
        tdSql.checkRows(self.tables)

        tdSql.query("select avg(value), sum(value) from st group by tbname slimit 5")
        tdSql.checkRows(5)

        tdSql.query("select * from (select avg(value), sum(value) from st group by tbname slimit 5)")
        tdSql.checkRows(5)

        tdSql.query("select avg(value), sum(value) from st group by tbname slimit 5 soffset 7")
        tdSql.checkRows(3)

        tdSql.query("select * from (select avg(value), sum(value) from st group by tbname slimit 5 soffset 7)")
        tdSql.checkRows(3)

        # https://jira.taosdata.com:18080/browse/TD-5497
        tdSql.execute("create table tt(ts timestamp ,i int)")
        tdSql.execute("insert into tt values(now, 11)(now + 1s, -12)")
        tdSql.query("select * from (select max(i),0-min(i) from tt)")
        tdSql.checkRows(1);
        tdSql.checkData(0, 0, 11);
        tdSql.checkData(0, 1, 12.0);

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
