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
        # TSIM: $dbPrefix = ta_cr_db
        # TSIM: $tbPrefix = ta_cr_tb
        tbPrefix = "ta_cr_tb"
        # TSIM: $mtPrefix = ta_cr_mt
        mtPrefix = "ta_cr_mt"
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
        # TSIM:
        # TSIM: sql create database $db
        # TSIM: sql use $db
        # TSIM:
        # TSIM: print =============== step2
        tdLog.info('=============== step2')
        # TSIM: $i = 2
        i = 2
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol bool)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bool)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bool)' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1 )
        tdLog.info('create table %s using %s tags( 1 )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1 )' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol = 1
        tdLog.info('select * from %s where tgcol = 1' % (mt))
        tdSql.query('select * from %s where tgcol = 1' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol = 0
        tdLog.info('select * from %s where tgcol = 0' % (mt))
        tdSql.query('select * from %s where tgcol = 0' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step3
        tdLog.info('=============== step3')
        # TSIM: $i = 3
        i = 3
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol
        # smallint)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol smallint)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol smallint)' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1 )
        tdLog.info('create table %s using %s tags( 1 )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1 )' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol = 1
        tdLog.info('select * from %s where tgcol = 1' % (mt))
        tdSql.query('select * from %s where tgcol = 1' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol = 0
        tdLog.info('select * from %s where tgcol = 0' % (mt))
        tdSql.query('select * from %s where tgcol = 0' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step4
        tdLog.info('=============== step4')
        # TSIM: $i = 4
        i = 4
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol
        # tinyint)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol tinyint)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol tinyint)' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1 )
        tdLog.info('create table %s using %s tags( 1 )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1 )' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol = 1
        tdLog.info('select * from %s where tgcol = 1' % (mt))
        tdSql.query('select * from %s where tgcol = 1' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol = 0
        tdLog.info('select * from %s where tgcol = 0' % (mt))
        tdSql.query('select * from %s where tgcol = 0' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step5
        tdLog.info('=============== step5')
        # TSIM: $i = 5
        i = 5
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol int)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol int)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol int)' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1 )
        tdLog.info('create table %s using %s tags( 1 )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1 )' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol = 1
        tdLog.info('select * from %s where tgcol = 1' % (mt))
        tdSql.query('select * from %s where tgcol = 1' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol = 0
        tdLog.info('select * from %s where tgcol = 0' % (mt))
        tdSql.query('select * from %s where tgcol = 0' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step6
        tdLog.info('=============== step6')
        # TSIM: $i = 6
        i = 6
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol
        # bigint)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bigint)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bigint)' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1 )
        tdLog.info('create table %s using %s tags( 1 )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1 )' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol = 1
        tdLog.info('select * from %s where tgcol = 1' % (mt))
        tdSql.query('select * from %s where tgcol = 1' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol = 0
        tdLog.info('select * from %s where tgcol = 0' % (mt))
        tdSql.query('select * from %s where tgcol = 0' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step7
        tdLog.info('=============== step7')
        # TSIM: $i = 7
        i = 7
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol
        # float)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol float)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol float)' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1 )
        tdLog.info('create table %s using %s tags( 1 )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1 )' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol = 1
        tdLog.info('select * from %s where tgcol = 1' % (mt))
        tdSql.query('select * from %s where tgcol = 1' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol = 0
        tdLog.info('select * from %s where tgcol = 0' % (mt))
        tdSql.query('select * from %s where tgcol = 0' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: print expect 0, actual: $rows
        tdLog.info('expect 0, actual: $rows')
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step8
        tdLog.info('=============== step8')
        # TSIM: $i = 8
        i = 8
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol
        # double)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol double)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol double)' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1 )
        tdLog.info('create table %s using %s tags( 1 )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1 )' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol = 1
        tdLog.info('select * from %s where tgcol = 1' % (mt))
        tdSql.query('select * from %s where tgcol = 1' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol = 0
        tdLog.info('select * from %s where tgcol = 0' % (mt))
        tdSql.query('select * from %s where tgcol = 0' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step9
        tdLog.info('=============== step9')
        # TSIM: $i = 9
        i = 9
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol
        # binary(10))
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol binary(10))' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol binary(10))' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( '1')
        tdLog.info('create table %s using %s tags( "1")' % (tb, mt))
        tdSql.execute('create table %s using %s tags( "1")' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol = '1'
        tdLog.info('select * from %s where tgcol = "1"' % (mt))
        tdSql.query('select * from %s where tgcol = "1"' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol = '0'
        tdLog.info('select * from %s where tgcol = "0"' % (mt))
        tdSql.query('select * from %s where tgcol = "0"' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step10
        tdLog.info('=============== step10')
        # TSIM: $i = 10
        i = 10
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol bool,
        # tgcol2 bool)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 bool)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 bool)' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1, 2 )
        tdLog.info('create table %s using %s tags( 1, 2 )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1, 2 )' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol2 = 2
        tdLog.info('select * from %s where tgcol2 = 2' % (mt))
        tdSql.query('select * from %s where tgcol2 = 2' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: print expect 1, actual: $rows
        tdLog.info('expect 1, actual: $rows')
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol2 = 0
        tdLog.info('select * from %s where tgcol2 = 0' % (mt))
        tdSql.query('select * from %s where tgcol2 = 0' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step11
        tdLog.info('=============== step11')
        # TSIM: $i = 11
        i = 11
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol bool,
        # tgcol2 smallint)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 smallint)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 smallint)' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1, 2 )
        tdLog.info('create table %s using %s tags( 1, 2 )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1, 2 )' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol2 = 2
        tdLog.info('select * from %s where tgcol2 = 2' % (mt))
        tdSql.query('select * from %s where tgcol2 = 2' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol2 = 0
        tdLog.info('select * from %s where tgcol2 = 0' % (mt))
        tdSql.query('select * from %s where tgcol2 = 0' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step12
        tdLog.info('=============== step12')
        # TSIM: $i = 12
        i = 12
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol bool,
        # tgcol2 tinyint)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 tinyint)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 tinyint)' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1, 2 )
        tdLog.info('create table %s using %s tags( 1, 2 )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1, 2 )' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol2 = 2
        tdLog.info('select * from %s where tgcol2 = 2' % (mt))
        tdSql.query('select * from %s where tgcol2 = 2' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol2 = 0
        tdLog.info('select * from %s where tgcol2 = 0' % (mt))
        tdSql.query('select * from %s where tgcol2 = 0' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step13
        tdLog.info('=============== step13')
        # TSIM: $i = 13
        i = 13
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol bool,
        # tgcol2 int)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 int)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 int)' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1, 2 )
        tdLog.info('create table %s using %s tags( 1, 2 )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1, 2 )' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol2 = 2
        tdLog.info('select * from %s where tgcol2 = 2' % (mt))
        tdSql.query('select * from %s where tgcol2 = 2' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol2 = 0
        tdLog.info('select * from %s where tgcol2 = 0' % (mt))
        tdSql.query('select * from %s where tgcol2 = 0' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step14
        tdLog.info('=============== step14')
        # TSIM: $i = 14
        i = 14
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol bool,
        # tgcol2 bigint)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 bigint)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 bigint)' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1, 2 )
        tdLog.info('create table %s using %s tags( 1, 2 )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1, 2 )' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol2 = 2
        tdLog.info('select * from %s where tgcol2 = 2' % (mt))
        tdSql.query('select * from %s where tgcol2 = 2' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol2 = 0
        tdLog.info('select * from %s where tgcol2 = 0' % (mt))
        tdSql.query('select * from %s where tgcol2 = 0' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: print =============== step15
        tdLog.info('=============== step15')
        # TSIM: $i = 15
        i = 15
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol bool,
        # tgcol2 float)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 float)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 float)' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1, 2 )
        tdLog.info('create table %s using %s tags( 1, 2 )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1, 2 )' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol2 = 2
        tdLog.info('select * from %s where tgcol2 = 2' % (mt))
        tdSql.query('select * from %s where tgcol2 = 2' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol2 = 0
        tdLog.info('select * from %s where tgcol2 = 0' % (mt))
        tdSql.query('select * from %s where tgcol2 = 0' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step16
        tdLog.info('=============== step16')
        # TSIM: $i = 16
        i = 16
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol bool,
        # tgcol2 double)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 double)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 double)' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1, 2 )
        tdLog.info('create table %s using %s tags( 1, 2 )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1, 2 )' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol2 = 2
        tdLog.info('select * from %s where tgcol2 = 2' % (mt))
        tdSql.query('select * from %s where tgcol2 = 2' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol2 = 0
        tdLog.info('select * from %s where tgcol2 = 0' % (mt))
        tdSql.query('select * from %s where tgcol2 = 0' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step17
        tdLog.info('=============== step17')
        # TSIM: $i = 17
        i = 17
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol bool,
        # tgcol2 binary(10))
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 binary(10))' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 binary(10))' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1, '2' )
        tdLog.info('create table %s using %s tags( 1, "2" )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1, "2" )' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol = true
        tdLog.info('select * from %s where tgcol = true' % (mt))
        tdSql.query('select * from %s where tgcol = true' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol2 = 0
        tdLog.info('select * from %s where tgcol2 = 0' % (mt))
        tdSql.query('select * from %s where tgcol2 = 0' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step18
        tdLog.info('=============== step18')
        # TSIM: $i = 18
        i = 18
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol
        # smallint, tgcol2 tinyint)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol smallint, tgcol2 tinyint)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol smallint, tgcol2 tinyint)' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1, 2 )
        tdLog.info('create table %s using %s tags( 1, 2 )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1, 2 )' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol2 = 2
        tdLog.info('select * from %s where tgcol2 = 2' % (mt))
        tdSql.query('select * from %s where tgcol2 = 2' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol2 = 0
        tdLog.info('select * from %s where tgcol2 = 0' % (mt))
        tdSql.query('select * from %s where tgcol2 = 0' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step19
        tdLog.info('=============== step19')
        # TSIM: $i = 19
        i = 19
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol
        # tinyint, tgcol2 int)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol tinyint, tgcol2 int)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol tinyint, tgcol2 int)' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1, 2 )
        tdLog.info('create table %s using %s tags( 1, 2 )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1, 2 )' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol2 = 2
        tdLog.info('select * from %s where tgcol2 = 2' % (mt))
        tdSql.query('select * from %s where tgcol2 = 2' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol2 = 0
        tdLog.info('select * from %s where tgcol2 = 0' % (mt))
        tdSql.query('select * from %s where tgcol2 = 0' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step20
        tdLog.info('=============== step20')
        # TSIM: $i = 20
        i = 20
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol int,
        # tgcol2 bigint)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol int, tgcol2 bigint)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol int, tgcol2 bigint)' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1, 2 )
        tdLog.info('create table %s using %s tags( 1, 2 )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1, 2 )' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol2 = 2
        tdLog.info('select * from %s where tgcol2 = 2' % (mt))
        tdSql.query('select * from %s where tgcol2 = 2' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol2 = 0
        tdLog.info('select * from %s where tgcol2 = 0' % (mt))
        tdSql.query('select * from %s where tgcol2 = 0' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step21
        tdLog.info('=============== step21')
        # TSIM: $i = 21
        i = 21
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol
        # bigint, tgcol2 float)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bigint, tgcol2 float)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bigint, tgcol2 float)' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1, 2 )
        tdLog.info('create table %s using %s tags( 1, 2 )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1, 2 )' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol2 = 2
        tdLog.info('select * from %s where tgcol2 = 2' % (mt))
        tdSql.query('select * from %s where tgcol2 = 2' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol2 = 0
        tdLog.info('select * from %s where tgcol2 = 0' % (mt))
        tdSql.query('select * from %s where tgcol2 = 0' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step22
        tdLog.info('=============== step22')
        # TSIM: $i = 22
        i = 22
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol
        # float, tgcol2 double)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol float, tgcol2 double)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol float, tgcol2 double)' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1, 2 )
        tdLog.info('create table %s using %s tags( 1, 2 )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1, 2 )' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol2 = 2
        tdLog.info('select * from %s where tgcol2 = 2' % (mt))
        tdSql.query('select * from %s where tgcol2 = 2' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol2 = 0
        tdLog.info('select * from %s where tgcol2 = 0' % (mt))
        tdSql.query('select * from %s where tgcol2 = 0' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step23
        tdLog.info('=============== step23')
        # TSIM: $i = 23
        i = 23
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol
        # double, tgcol2 binary(10))
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol double, tgcol2 binary(10))' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol double, tgcol2 binary(10))' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1, '2' )
        tdLog.info('create table %s using %s tags( 1, "2" )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1, "2" )' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol2 = '2'
        tdLog.info('select * from %s where tgcol2 = "2"' % (mt))
        tdSql.query('select * from %s where tgcol2 = "2"' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol2 = 0
        tdLog.info('select * from %s where tgcol2 = 0' % (mt))
        tdSql.query('select * from %s where tgcol2 = 0' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step24
        tdLog.info('=============== step24')
        # TSIM: $i = 24
        i = 24
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol1
        # bool, tgcol2 bool, tgcol3 int, tgcol4 float, tgcol5 double, tgcol6
        # binary(10))
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 bool, tgcol3 int, tgcol4 float, tgcol5 double, tgcol6 binary(10))' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 bool, tgcol3 int, tgcol4 float, tgcol5 double, tgcol6 binary(10))' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1, 2, 3, 4, 5, '6' )
        tdLog.info(
            'create table %s using %s tags( 1, 2, 3, 4, 5, "6" )' %
            (tb, mt))
        tdSql.execute(
            'create table %s using %s tags( 1, 2, 3, 4, 5, "6" )' %
            (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol1 = 1
        tdLog.info('select * from %s where tgcol1 = 1' % (mt))
        tdSql.query('select * from %s where tgcol1 = 1' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol2 = 2
        tdLog.info('select * from %s where tgcol2 = 2' % (mt))
        tdSql.query('select * from %s where tgcol2 = 2' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol3 = 3
        tdLog.info('select * from %s where tgcol3 = 3' % (mt))
        tdSql.query('select * from %s where tgcol3 = 3' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol4 = 4
        tdLog.info('select * from %s where tgcol4 = 4' % (mt))
        tdSql.query('select * from %s where tgcol4 = 4' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol5 = 5
        tdLog.info('select * from %s where tgcol5 = 5' % (mt))
        tdSql.query('select * from %s where tgcol5 = 5' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol6 = '6'
        tdLog.info('select * from %s where tgcol6 = "6"' % (mt))
        tdSql.query('select * from %s where tgcol6 = "6"' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol6 = '0'
        tdLog.info('select * from %s where tgcol6 = "0"' % (mt))
        tdSql.query('select * from %s where tgcol6 = "0"' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step25
        tdLog.info('=============== step25')
        # TSIM: $i = 25
        i = 25
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol bool,
        # tgcol2 int, tgcol3 float, tgcol4 double, tgcol5 binary(10), tgcol6
        # binary(10))
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 int, tgcol3 float, tgcol4 double, tgcol5 binary(10), tgcol6 binary(10))' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 int, tgcol3 float, tgcol4 double, tgcol5 binary(10), tgcol6 binary(10))' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1, 2, 3, 4, '5', '6' )
        tdLog.info(
            'create table %s using %s tags( 1, 2, 3, 4, "5", "6" )' %
            (tb, mt))
        tdSql.execute(
            'create table %s using %s tags( 1, 2, 3, 4, "5", "6" )' %
            (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol6 = '6'
        tdLog.info('select * from %s where tgcol6 = "6"' % (mt))
        tdSql.query('select * from %s where tgcol6 = "6"' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol6 = '0'
        tdLog.info('select * from %s where tgcol6 = "0"' % (mt))
        tdSql.query('select * from %s where tgcol6 = "0"' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step26
        tdLog.info('=============== step26')
        # TSIM: $i = 26
        i = 26
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol
        # binary(10), tgcol2 binary(10), tgcol3 binary(10), tgcol4 binary(10),
        # tgcol5 binary(10), tgcol6 binary(10))
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol binary(10), tgcol2 binary(10), tgcol3 binary(10), tgcol4 binary(10), tgcol5 binary(10), tgcol6 binary(10))' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol binary(10), tgcol2 binary(10), tgcol3 binary(10), tgcol4 binary(10), tgcol5 binary(10), tgcol6 binary(10))' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( '1', '2', '3', '4', '5',
        # '6' )
        tdLog.info(
            'create table %s using %s tags( "1", "2", "3", "4", "5", "6" )' %
            (tb, mt))
        tdSql.execute(
            'create table %s using %s tags( "1", "2", "3", "4", "5", "6" )' %
            (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol3 = '3'
        tdLog.info('select * from %s where tgcol3 = "3"' % (mt))
        tdSql.query('select * from %s where tgcol3 = "3"' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol3 = '0'
        tdLog.info('select * from %s where tgcol3 = "0"' % (mt))
        tdSql.query('select * from %s where tgcol3 = "0"' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step27
        tdLog.info('=============== step27')
        # TSIM: $i = 27
        i = 27
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol bool,
        # tgcol2 bool, tgcol3 int, tgcol4 float, tgcol5 double, tgcol6
        # binary(10), tgcol7) -x step27
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 bool, tgcol3 int, tgcol4 float, tgcol5 double, tgcol6 binary(10), tgcol7) -x step27' %
            (mt))
        tdSql.error(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 bool, tgcol3 int, tgcol4 float, tgcol5 double, tgcol6 binary(10), tgcol7)7' %
            (mt))
        # TSIM: return -1
        # TSIM: step27:
        # TSIM:
        # TSIM: print =============== step28
        tdLog.info('=============== step28')
        # TSIM: $i = 28
        i = 28
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol
        # binary(250), tgcol2 binary(250))
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol binary(250), tgcol2 binary(250))' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol binary(250), tgcol2 binary(250))' %
            (mt))
        # TSIM: sql create table $tb using $mt tags('1', '1')
        tdLog.info('create table %s using %s tags("1", "1")' % (tb, mt))
        tdSql.execute('create table %s using %s tags("1", "1")' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol = '1'
        tdLog.info('select * from %s where tgcol = "1"' % (mt))
        tdSql.query('select * from %s where tgcol = "1"' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step29
        tdLog.info('=============== step29')
        # TSIM: $i = 29
        i = 29
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol
        # binary(25), tgcol2 binary(250))
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol binary(25), tgcol2 binary(250))' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol binary(25), tgcol2 binary(250))' %
            (mt))
        # TSIM: sql create table $tb using $mt tags('1', '1')
        tdLog.info('create table %s using %s tags("1", "1")' % (tb, mt))
        tdSql.execute('create table %s using %s tags("1", "1")' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol = '1'
        tdLog.info('select * from %s where tgcol = "1"' % (mt))
        tdSql.query('select * from %s where tgcol = "1"' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info('tdSql.checkData(0, 1, 1)')
        tdSql.checkData(0, 1, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step30
        tdLog.info('=============== step30')
        # TSIM: $i = 30
        i = 30
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol
        # binary(250), tgcol2 binary(250), tgcol3 binary(30)) -x step30
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol binary(250), tgcol2 binary(250), tgcol3 binary(30)) -x step30' %
            (mt))
        tdSql.error(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol binary(250), tgcol2 binary(250), tgcol3 binary(30))0' %
            (mt))
        # TSIM: return -1
        # TSIM: step30:
        # TSIM:
        # TSIM: print =============== step31
        tdLog.info('=============== step31')
        # TSIM: $i = 31
        i = 31
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol
        # binary(5))
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol binary(5))' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol binary(5))' %
            (mt))
        # TSIM: sql_error create table $tb using $mt tags('1234567')
        tdLog.info('create table %s using %s tags("1234567")' % (tb, mt))
        tdSql.error('create table %s using %s tags("1234567")' % (tb, mt))
        # TSIM: sql create table $tb using $mt tags('12345')
        tdLog.info('create table %s using %s tags("12345")' % (tb, mt))
        tdSql.execute('create table %s using %s tags("12345")' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt
        tdLog.info('select * from %s' % (mt))
        tdSql.query('select * from %s' % (mt))
        # TSIM: print sql select * from $mt
        tdLog.info('sql select * from $mt')
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print $data00 $data01 $data02
        tdLog.info('$data00 $data01 $data02')
        # TSIM: if $data02 != 12345 then
        tdLog.info('tdSql.checkData(0, 2, "12345")')
        tdSql.checkData(0, 2, "12345")
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
