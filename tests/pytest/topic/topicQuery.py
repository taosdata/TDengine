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

from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts = 1538548685000

    def run(self):
        tdSql.prepare()

        # test case for https://jira.taosdata.com:18080/browse/TD-3679
        print("==============step1")
        tdSql.execute(
            "create topic tq_test partitions 10")
        tdSql.execute(
            "insert into tq_test.p1(off, ts, content) values(0, %d, 'aaaa')" % self.ts)
        tdSql.execute(
            "insert into tq_test.p1(off, ts, content) values(1, %d, 'aaaa')" % (self.ts + 1))
        tdSql.execute(
            "insert into tq_test.p1(off, ts, content) values(2, %d, 'aaaa')" % (self.ts + 2))
        tdSql.execute(
            "insert into tq_test.p1(off, ts, content) values(3, %d, 'aaaa')" % (self.ts + 3))

        print("==============step2")

        tdSql.query("select * from tq_test.p1")
        tdSql.checkRows(4)

        tdSql.query("select * from tq_test.p1 where ts >= %d" % self.ts)
        tdSql.checkRows(4)

        tdSql.query("select * from tq_test.p1 where ts > %d" % self.ts)
        tdSql.checkRows(3)

        tdSql.query("select * from tq_test.p1 where ts = %d" % self.ts)
        tdSql.checkRows(1)


        tdSql.execute("use db")
        tdSql.execute("create table test(ts timestamp, start timestamp, value int)")
        tdSql.execute("insert into test values(%d, %d, 1)" % (self.ts, self.ts))
        tdSql.execute("insert into test values(%d, %d, 1)" % (self.ts + 1, self.ts + 1))
        tdSql.execute("insert into test values(%d, %d, 1)" % (self.ts + 2, self.ts + 2))
        tdSql.execute("insert into test values(%d, %d, 1)" % (self.ts + 3, self.ts + 3))

        tdSql.query("select * from test")
        tdSql.checkRows(4)

        tdSql.query("select * from test where ts >= %d" % self.ts)
        tdSql.checkRows(4)

        tdSql.query("select * from test where ts > %d" % self.ts)
        tdSql.checkRows(3)

        tdSql.query("select * from test where ts = %d" % self.ts)
        tdSql.checkRows(1)

        tdSql.query("select * from test where start >= %d" % self.ts)
        tdSql.checkRows(4)

        tdSql.query("select * from test where start > %d" % self.ts)
        tdSql.checkRows(3)

        tdSql.query("select * from test where start = %d" % self.ts)
        tdSql.checkRows(1)

        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())