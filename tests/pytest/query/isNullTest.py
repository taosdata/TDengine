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

        self.ts = 1537146000000

    def run(self):
        tdSql.prepare()

        print("==============step1")
        tdSql.execute("create table st(ts timestamp, c1 int, c2 binary(20), c3 nchar(20)) tags(t1 int, t2 binary(20), t3 nchar(20))")
        tdSql.execute("create table t1 using st tags(1, 'binary1', 'nchar1')")
        tdSql.execute("insert into t2(ts, c2) using st(t2) tags('') values(%d, '')" % (self.ts + 10))
        tdSql.execute("insert into t3(ts, c2) using st(t3) tags('') values(%d, '')" % (self.ts + 10))

        for i in range(10):
            tdSql.execute("insert into t1 values(%d, %d, 'binary%d', 'nchar%d')" % (self.ts + i, i, i, i))
            tdSql.execute("insert into t2 values(%d, %d, 'binary%d', 'nchar%d')" % (self.ts + i, i, i, i))
            tdSql.execute("insert into t3 values(%d, %d, 'binary%d', 'nchar%d')" % (self.ts + i, i, i, i))

        tdSql.execute("insert into t1(ts, c2) values(%d, '')" % (self.ts + 10))
        tdSql.execute("insert into t1(ts, c3) values(%d, '')" % (self.ts + 11))        
        tdSql.execute("insert into t2(ts, c3) values(%d, '')" % (self.ts + 11))        
        tdSql.execute("insert into t3(ts, c3) values(%d, '')" % (self.ts + 11))

        tdSql.query("select count(*) from st")
        tdSql.checkData(0, 0, 36)

        tdSql.query("select count(*) from st where t1 is null")
        tdSql.checkData(0, 0, 24)

        tdSql.query("select count(*) from st where t1 is not null")
        tdSql.checkData(0, 0, 12)

        tdSql.query("select count(*) from st where t2 is null")
        tdSql.checkData(0, 0, 12)

        tdSql.query("select count(*) from st where t2 is not null")
        tdSql.checkData(0, 0, 24)

        tdSql.error("select count(*) from st where t2 <> null")
        tdSql.error("select count(*) from st where t2 = null")

        tdSql.query("select count(*) from st where t2 = '' ")
        tdSql.checkData(0, 0, 12)

        tdSql.query("select count(*) from st where t2 <> '' ")
        tdSql.checkData(0, 0, 24)

        tdSql.query("select count(*) from st where t3 is null")
        tdSql.checkData(0, 0, 12)

        tdSql.query("select count(*) from st where t3 is not null")
        tdSql.checkData(0, 0, 24)

        tdSql.error("select count(*) from st where t3 <> null")
        tdSql.error("select count(*) from st where t3 = null")

        tdSql.query("select count(*) from st where t3 = '' ")
        tdSql.checkData(0, 0, 12)

        tdSql.query("select count(*) from st where t3 <> '' ")
        tdSql.checkData(0, 0, 24)

        tdSql.query("select count(*) from st where c1 is not null")
        tdSql.checkData(0, 0, 30)

        tdSql.query("select count(*) from st where c1 is null")
        tdSql.checkData(0, 0, 6)

        tdSql.query("select count(*) from st where c2 is not null")
        tdSql.checkData(0, 0, 33)

        tdSql.query("select count(*) from st where c2 is null")
        tdSql.checkData(0, 0, 3)

        tdSql.error("select count(*) from st where c2 <> null")
        tdSql.error("select count(*) from st where c2 = null")

        tdSql.query("select count(*) from st where c2 = '' ")
        tdSql.checkData(0, 0, 3)

        tdSql.query("select count(*) from st where c2 <> '' ")
        tdSql.checkData(0, 0, 30)

        tdSql.query("select count(*) from st where c3 is not null")
        tdSql.checkData(0, 0, 33)

        tdSql.query("select count(*) from st where c3 is null")
        tdSql.checkData(0, 0, 3)

        tdSql.error("select count(*) from st where c3 <> null")
        tdSql.error("select count(*) from st where c3 = null")

        tdSql.query("select count(*) from st where c3 = '' ")
        tdSql.checkData(0, 0, 3)

        tdSql.query("select count(*) from st where c3 <> '' ")
        tdSql.checkData(0, 0, 30)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
