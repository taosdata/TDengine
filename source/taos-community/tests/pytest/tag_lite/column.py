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

        # TSIM: system sh/stop_dnodes.sh
        # TSIM:
        # TSIM:
        # TSIM: system sh/deploy.sh -n dnode1 -i 1
        # TSIM: system sh/cfg.sh -n dnode1 -c walLevel -v 0
        # TSIM: system sh/exec.sh -n dnode1 -s start
        # TSIM:
        # TSIM: sleep 3000
        # TSIM: sql connect
        # TSIM:
        # TSIM: print ======================== dnode1 start
        tdLog.info('======================== dnode1 start')
        # TSIM:
        # TSIM: $dbPrefix = ta_co_db
        # TSIM: $tbPrefix = ta_co_tb
        tbPrefix = "ta_co_tb"
        # TSIM: $mtPrefix = ta_co_mt
        mtPrefix = "ta_co_mt"
        # TSIM: $tbNum = 10
        tbNum = 10
        # TSIM: $rowNum = 20
        rowNum = 20
        # TSIM: $totalNum = 200
        totalNum = 200
        # TSIM:
        # TSIM: print =============== step1
        tdLog.info('=============== step1')
        # TSIM: $i = 0
        i = 0
        # TSIM: $db = $dbPrefix . $i
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM:
        # TSIM: sql create database $db
        # TSIM: sql use $db
        # TSIM:
        # TSIM: $i = 0
        i = 0
        # TSIM: sql create table $mt (ts timestamp, tbcol int, tbcol2
        # binary(10)) TAGS(tgcol int, tgcol2 binary(10))
        tdLog.info(
            'create table %s (ts timestamp, tbcol int, tbcol2 binary(10)) TAGS(tgcol int, tgcol2 binary(10))' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int, tbcol2 binary(10)) TAGS(tgcol int, tgcol2 binary(10))' %
            (mt))
        # TSIM:
        # TSIM: print =============== step2
        tdLog.info('=============== step2')
        # TSIM:
        # TSIM: $i = 0
        i = 0
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $tb using $mt tags(  0,  '0' )
        tdLog.info('create table %s using %s tags(  0,  "0" )' % (tb, mt))
        tdSql.execute('create table %s using %s tags(  0,  "0" )' % (tb, mt))
        # TSIM:
        # TSIM: $i = 1
        i = 1
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $tb using $mt tags(  1,   1  )
        tdLog.info('create table %s using %s tags(  1,   1  )' % (tb, mt))
        tdSql.execute('create table %s using %s tags(  1,   1  )' % (tb, mt))
        # TSIM:
        # TSIM: $i = 2
        i = 2
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $tb using $mt tags( '2', '2' )
        tdLog.info('create table %s using %s tags( "2", "2" )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( "2", "2" )' % (tb, mt))
        # TSIM:
        # TSIM: $i = 3
        i = 3
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $tb using $mt tags( '3',  3  )
        tdLog.info('create table %s using %s tags( "3",  3  )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( "3",  3  )' % (tb, mt))
        # TSIM:
        # TSIM: sql show tables
        tdLog.info('show tables')
        tdSql.query('show tables')
        # TSIM: if $rows != 4 then
        tdLog.info('tdSql.checkRow(4)')
        tdSql.checkRows(4)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step3
        tdLog.info('=============== step3')
        # TSIM:
        # TSIM: $i = 0
        i = 0
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql insert into $tb values(now,  0,  '0')
        tdLog.info('insert into %s values(now,  0,  "0")' % (tb))
        tdSql.execute('insert into %s values(now,  0,  "0")' % (tb))
        # TSIM:
        # TSIM: $i = 1
        i = 1
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql insert into $tb values(now,  1,   1  )
        tdLog.info('insert into %s values(now,  1,   1  )' % (tb))
        tdSql.execute('insert into %s values(now,  1,   1  )' % (tb))
        # TSIM:
        # TSIM: $i = 2
        i = 2
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql insert into $tb values(now, '2', '2')
        tdLog.info('insert into %s values(now, "2", "2")' % (tb))
        tdSql.execute('insert into %s values(now, "2", "2")' % (tb))
        # TSIM:
        # TSIM: $i = 3
        i = 3
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql insert into $tb values(now, '3',  3)
        tdLog.info('insert into %s values(now, "3",  3)' % (tb))
        tdSql.execute('insert into %s values(now, "3",  3)' % (tb))
        # TSIM:
        # TSIM: print =============== step4
        tdLog.info('=============== step4')
        # TSIM: sql select * from $mt where tgcol2 = '1'
        tdLog.info('select * from %s where tgcol2 = "1"' % (mt))
        tdSql.query('select * from %s where tgcol2 = "1"' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step5
        tdLog.info('=============== step5')
        # TSIM: sql select * from $mt
        tdLog.info('select * from %s' % (mt))
        tdSql.query('select * from %s' % (mt))
        # TSIM: if $rows != 4 then
        tdLog.info('tdSql.checkRow(4)')
        tdSql.checkRows(4)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol = 1
        tdLog.info('select * from %s where tgcol = 1' % (mt))
        tdSql.query('select * from %s where tgcol = 1' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== clear
        tdLog.info('=============== clear')
        # TSIM: sql drop database $db
        tdLog.info('drop database db')
        tdSql.execute('drop database db')
        # TSIM: sql select * from information_schema.ins_databases
        tdLog.info('select * from information_schema.ins_databases')
        tdSql.query('select * from information_schema.ins_databases')
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: system sh/exec.sh -n dnode1 -s stop -x SIGINT
# convert end

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
