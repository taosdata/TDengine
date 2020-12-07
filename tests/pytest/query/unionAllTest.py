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
import random


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts = 1500000000000
        self.num = 10

    def run(self):
        tdSql.prepare()

        tdSql.execute("create table st(ts timestamp, c1 int) tags(loc nchar(20))")
        tdSql.execute("create table t0 using st tags('nchar0')")
        tdSql.execute("create table t1 using st tags('nchar1')")
        tdSql.execute("create table t2 using st tags('nchar2')")
        tdSql.execute("create table t3 using st tags('nchar3')")
        tdSql.execute("create table t4 using st tags('nchar4')")
        tdSql.execute("create table t5 using st tags('nchar5')")

        for i in range(self.num):
            tdSql.execute("insert into t0 values(%d, %d)" % (self.ts + i, i))
            tdSql.execute("insert into t1 values(%d, %d)" % (self.ts + i, i))
            tdSql.execute("insert into t2 values(%d, %d)" % (self.ts + i, i))
            tdSql.execute("insert into t3 values(%d, %d)" % (self.ts + i, i))
            tdSql.execute("insert into t4 values(%d, %d)" % (self.ts + i, i))
            tdSql.execute("insert into t5 values(%d, %d)" % (self.ts + i, i))                

        sql = ''' select * from st where loc = 'nchar0' limit 1 union all select * from st where loc = 'nchar1' limit 1 union all select * from st where loc = 'nchar2' limit 1
            union all select * from st where loc = 'nchar3' limit 1 union all select * from st where loc = 'nchar4' limit 1'''
        tdSql.query(sql)
        tdSql.checkRows(5)

        sql = ''' select * from st where loc = 'nchar0' limit 1 union all select * from st where loc = 'nchar1' limit 1 union all select * from st where loc = 'nchar2' limit 1
            union all select * from st where loc = 'nchar3' limit 1 union all select * from st where loc = 'nchar4' limit 1 union all select * from st where loc = 'nchar5''''
        tdSql.query(sql)
        tdSql.checkRows(6)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())