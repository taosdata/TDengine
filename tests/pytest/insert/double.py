# -*- coding: utf-8 -*-

import sys
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql, replicaVar = 1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        tdLog.info('=============== step1')
        tdLog.info('create table tb (ts timestamp, speed double)')
        tdSql.execute('create table tb (ts timestamp, speed double)')
        tdLog.info('=============== step2')
        tdLog.info('select * from tb order by ts desc')
        tdSql.query('select * from tb order by ts desc')
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        tdLog.info('=============== step3')
        tdLog.info("insert into tb values (now+2a, 2.85)")
        tdSql.execute("insert into tb values (now+2a, 2.85)")
        tdLog.info('select * from tb order by ts desc')
        tdSql.query('select * from tb order by ts desc')
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        tdLog.info('tdSql.checkData(0, 1, 2.850000000)')
        tdSql.checkData(0, 1, 2.850000000)
        tdLog.info('=============== step4')
        tdLog.info("insert into tb values (now+3a, 3.4)")
        tdSql.execute("insert into tb values (now+3a, 3.4)")
        tdLog.info('select * from tb order by ts desc')
        tdSql.query('select * from tb order by ts desc')
        tdLog.info('tdSql.checkRow(2)')
        tdSql.checkRows(2)
        tdLog.info('tdSql.checkData(0, 1, 3.400000000)')
        tdSql.checkData(0, 1, 3.400000000)
        tdLog.info('=============== step5')
        tdLog.info("insert into tb values (now+4a, a2) -x step51")
        tdSql.error("insert into tb values (now+4a, a2)1")
        tdLog.info("insert into tb values (now+4a, 0)")
        tdSql.execute("insert into tb values (now+4a, 0)")
        tdLog.info('select * from tb order by ts desc')
        tdSql.query('select * from tb order by ts desc')
        tdLog.info('tdSql.checkRow(3)')
        tdSql.checkRows(3)
        tdLog.info('tdSql.checkData(0, 1, 0.000000000)')
        tdSql.checkData(0, 1, 0.000000000)
        tdLog.info('=============== step6')
        tdLog.info("insert into tb values (now+5a, 2a) -x step6")
        tdSql.error("insert into tb values (now+5a, 2a)")
        tdLog.info("insert into tb values(now+5a, 2)")
        tdSql.execute("insert into tb values(now+5a, 2)")
        tdLog.info('select * from tb order by ts desc')
        tdSql.query('select * from tb order by ts desc')
        tdLog.info('tdSql.checkRow(4)')
        tdSql.checkRows(4)
        tdLog.info('tdSql.checkData(0, 1, 2.000000000)')
        tdSql.checkData(0, 1, 2.000000000)
        tdLog.info('=============== step7')
        tdLog.info("insert into tb values (now+6a, 2a'1) -x step7")
        tdSql.error("insert into tb values (now+6a, 2a'1)")
        tdLog.info("insert into tb values(now+6a, 2)")
        tdSql.execute("insert into tb values(now+6a, 2)")
        tdLog.info('select * from tb order by ts desc')
        tdSql.query('select * from tb order by ts desc')
        tdLog.info('tdSql.checkRow(5)')
        tdSql.checkRows(5)
        tdLog.info('tdSql.checkData(0, 1, 2.000000000)')
        tdSql.checkData(0, 1, 2.000000000)
        tdLog.info('drop database db')
        tdSql.execute('drop database db')
# convert end

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
