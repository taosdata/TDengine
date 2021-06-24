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

        tdLog.info('======================== dnode1 start')
        tbPrefix = "ta_fl_tb"
        mtPrefix = "ta_fl_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200
        tdLog.info('=============== step1')
        i = 0
        mt = "%s%d" % (mtPrefix, i)
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol float, tgTs timestamp, tgcol2 int)' %(mt))
        i = 0
        ts = 1605045600000
        tsStr = "2020-11-11 06:00:00"
        while (i < 5):
            tb = "%s%d" % (tbPrefix, i)
            tdLog.info('create table %s using %s tags(%d, %d, %d)' % (tb, mt, i, ts + i, i))
            tdSql.execute('create table %s using %s tags(%d, %d, %d)' % (tb, mt, i, ts + i, i))
            x = 0
            while (x < rowNum):
                ms = x * 60000
                #tdLog.info(
                #    "insert into %s values (%d, %d)" %
                #    (tb, 1605045600000 + ms, x))
                tdSql.execute(
                    "insert into %s values (%d, %d)" %
                    (tb, 1605045600000 + ms, x))
                x = x + 1
            i = i + 1
        tdLog.info('=============== step2')
        tdSql.query('select * from %s' % (mt))
        tdSql.checkRows(5 * rowNum)
        
        tdSql.query('select * from %s where tgTs = %ld and tgcol2 = 0' % (mt, ts))
        tdSql.checkRows(rowNum)

        tdSql.query('select * from %s where tgTs = \"%s\" and tgcol2 = 0' % (mt, tsStr))
        tdSql.checkRows(rowNum)

        tdLog.info('=============== step3')
        i = 0 
        while (i < 5):
            tb = "%s%d" % (tbPrefix, i + 100)
            tdLog.info('create table %s using %s tags(%d, \"%s\", %d)' % (tb, mt, i + 100, tsStr, i + 100))
            tdSql.execute('create table %s using %s tags(%d, \"%s\", %d)' % (tb, mt, i + 100, tsStr, i + 100))
            x = 0
            while (x < rowNum):
                ms = x * 60000
                #tdLog.info(
                #    "insert into %s values (%d, %d)" %
                #    (tb, 1605045600000 + ms, x))
                tdSql.execute(
                    "insert into %s values (%d, %d)" %
                    (tb, 1605045600000 + ms, x))
                x = x + 1
            i = i + 1

        tdSql.query('select * from %s where tgTs = %ld and tgcol2 = 100' % (mt, ts))
        tdSql.checkRows(rowNum)

        tdSql.query('select * from %s where tgTs = \"%s\" and tgcol2 = 100' % (mt, tsStr))
        tdSql.checkRows(rowNum)

        tdLog.info('=============== step4')

        i = 0
        tb = "%s%d"%(tbPrefix, i + 1000) 
        tdSql.execute('insert into  %s using %s tags(%d, \"%s\", %d) values(now, 10)' % (tb, mt, i + 100, tsStr, i + 1000))
        tdSql.execute('insert into  %s using %s tags(%d, \"%s\", %d) values(now+2s, 10)' % (tb, mt, i + 100, tsStr, i + 1000))
        tdSql.execute('insert into  %s using %s tags(%d, \"%s\", %d) values(now+3s, 10)' % (tb, mt, i + 100, tsStr, i + 1000))
        tdSql.query('select * from %s where tgTs = \"%s\" and tgcol2 = 1000' % (mt, tsStr))
        tdSql.checkRows(3)

        i = 0
        tb = "%s%d"%(tbPrefix, i + 10000) 
        tdSql.execute('create table %s using %s tags(%d, now, %d)' % (tb, mt, i + 10000,i + 10000))
        tdSql.checkRows(3)
         
         
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
