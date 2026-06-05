# -*- coding: utf-8 -*-

import sys
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        tdLog.info('=============== step1')
        tdLog.info('=============== step2')
        tdLog.info('create table tb (ts timestamp, speed int)')
        tdSql.execute('create table tb (ts timestamp, speed int)')
        tdLog.info('=============== step3')
        tdLog.info("insert into tb values (now, 1)")
        tdSql.execute("insert into tb values (now, 1)")
        tdLog.info('=============== step4')
        tdLog.info('select * from tb')
        tdSql.query('select * from tb')
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        tdLog.info('=============== step5')
        tdLog.info('describe tb')
        tdSql.query('describe tb')
        tdLog.info('=============== step6')
        tdLog.info('drop database db')
        tdSql.execute('drop database db')
        tdLog.info('select * from information_schema.ins_databases')
        tdSql.query('select * from information_schema.ins_databases')
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
# convert end

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
