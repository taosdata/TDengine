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
        tdLog.info('create table tb (ts timestamp, speed bigint)')
        tdSql.execute('create table tb (ts timestamp, speed bigint)')
        tdLog.info("insert into tb values (now, -9223372036854770000)")
        tdSql.execute("insert into tb values (now, -9223372036854770000)")
        tdLog.info('select * from tb')
        tdSql.query('select * from tb')
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        tdLog.info('tdSql.checkData(0, 1, -9223372036854770000)')
        tdSql.checkData(0, 1, -9223372036854770000)
        tdLog.info('=============== step2')
        tdLog.info("insert into tb values (now+1a, -9223372036854770000)")
        tdSql.execute("insert into tb values (now+1a, -9223372036854770000)")
        tdLog.info('select * from tb')
        tdSql.query('select * from tb')
        tdLog.info('tdSql.checkRow(2)')
        tdSql.checkRows(2)
        tdLog.info('tdSql.checkData(0, 1, -9223372036854770000)')
        tdSql.checkData(0, 1, -9223372036854770000)
        tdLog.info('=============== step3')
        tdLog.info("insert into tb values (now+2a, 9223372036854770000)")
        tdSql.execute("insert into tb values (now+2a, 9223372036854770000)")
        tdLog.info('select * from tb order by ts desc')
        tdSql.query('select * from tb order by ts desc')
        tdLog.info('tdSql.checkRow(3)')
        tdSql.checkRows(3)
        tdLog.info('tdSql.checkData(0, 1, 9223372036854770000)')
        tdSql.checkData(0, 1, 9223372036854770000)
        tdLog.info('=============== step4')
        tdLog.info("insert into tb values (now+3a, 9223372036854770000)")
        tdSql.execute("insert into tb values (now+3a, 9223372036854770000)")
        tdLog.info('select * from tb order by ts desc')
        tdSql.query('select * from tb order by ts desc')
        tdLog.info('tdSql.checkRow(4)')
        tdSql.checkRows(4)
        tdLog.info('tdSql.checkData(0, 1, 9223372036854770000)')
        tdSql.checkData(0, 1, 9223372036854770000)
        tdLog.info('drop database db')
        tdSql.execute('drop database db')
# convert end

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
