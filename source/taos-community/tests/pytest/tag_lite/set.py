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
        # TSIM: $dbPrefix = ta_se_db
        # TSIM: $tbPrefix = ta_se_tb
        tbPrefix = "ta_se_tb"
        # TSIM: $mtPrefix = ta_se_mt
        mtPrefix = "ta_se_mt"
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
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol1
        # bool, tgcol2 int)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int)' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1, 2 )
        tdLog.info('create table %s using %s tags( 1, 2 )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1, 2 )' % (tb, mt))
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
        # TSIM: if $data02 != 1 then
        tdLog.info('tdSql.checkData(0, 2, 1)')
        tdSql.checkData(0, 2, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 2 then
        tdLog.info('tdSql.checkData(0, 3, 2)')
        tdSql.checkData(0, 3, 2)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql alter table $tb set tag tagcx 1 -x step21
        tdLog.info('alter table %s set tag tagcx 1 -x step21' % (tb))
        tdSql.error('alter table %s set tag tagcx 11' % (tb))
        # TSIM: return -1
        # TSIM: step21:
        # TSIM: sql alter table $tb set tag tgcol1=false
        tdLog.info('alter table %s set tag tgcol1=false' % (tb))
        tdSql.execute('alter table %s set tag tgcol1=false' % (tb))
        # TSIM: sql alter table $tb set tag tgcol2=4
        tdLog.info('alter table %s set tag tgcol2=4' % (tb))
        tdSql.execute('alter table %s set tag tgcol2=4' % (tb))
        # TSIM:
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM:
        # TSIM: sql select * from $mt where tgcol1 = false
        tdLog.info('select * from %s where tgcol1 = false' % (mt))
        tdSql.query('select * from %s where tgcol1 = false' % (mt))
        # TSIM: print $data01 $data02 $data03
        tdLog.info('$data01 $data02 $data03')
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
        # TSIM: if $data02 != 0 then
        tdLog.info('tdSql.checkData(0, 2, 0)')
        tdSql.checkData(0, 2, 0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 4 then
        tdLog.info('tdSql.checkData(0, 3, 4)')
        tdSql.checkData(0, 3, 4)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol2 = 4
        tdLog.info('select * from %s where tgcol2 = 4' % (mt))
        tdSql.query('select * from %s where tgcol2 = 4' % (mt))
        # TSIM: print $data01 $data02 $data03
        tdLog.info('$data01 $data02 $data03')
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
        # TSIM: if $data02 != 0 then
        tdLog.info('tdSql.checkData(0, 2, 0)')
        tdSql.checkData(0, 2, 0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 4 then
        tdLog.info('tdSql.checkData(0, 3, 4)')
        tdSql.checkData(0, 3, 4)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql describe $tb
        tdLog.info('describe %s' % (tb))
        tdSql.query('describe %s' % (tb))
        # TSIM: print $data21 $data23 $data32 $data33
        tdLog.info('$data21 $data23 $data32 $data33')
        # TSIM: if $data21 != BOOL then
        tdLog.info('tdSql.checkDataType(2, 1, "BOOL")')
        tdSql.checkDataType(2, 1, "BOOL")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data31 != INT then
        tdLog.info('tdSql.checkDataType(3, 1, "INT")')
        tdSql.checkDataType(3, 1, "INT")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data23 != false then
        tdLog.info('tdSql.checkData(2, 3, "TAG")')
        tdSql.checkData(2, 3, "TAG")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data33 != 4 then
        tdLog.info('tdSql.checkData(3, 3, "TAG")')
        tdSql.checkData(3, 3, "TAG")
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
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol1
        # smallint, tgcol2 tinyint)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 smallint, tgcol2 tinyint)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 smallint, tgcol2 tinyint)' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1, 2 )
        tdLog.info('create table %s using %s tags( 1, 2 )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1, 2 )' % (tb, mt))
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
        # TSIM: if $data02 != 1 then
        tdLog.info('tdSql.checkData(0, 2, 1)')
        tdSql.checkData(0, 2, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 2 then
        tdLog.info('tdSql.checkData(0, 3, 2)')
        tdSql.checkData(0, 3, 2)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql alter table $tb set tag tgcol1=3
        tdLog.info('alter table %s set tag tgcol1=3' % (tb))
        tdSql.execute('alter table %s set tag tgcol1=3' % (tb))
        # TSIM: sql alter table $tb set tag tgcol2=4
        tdLog.info('alter table %s set tag tgcol2=4' % (tb))
        tdSql.execute('alter table %s set tag tgcol2=4' % (tb))
        # TSIM:
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM:
        # TSIM: sql select * from $mt where tgcol1 = 3
        tdLog.info('select * from %s where tgcol1 = 3' % (mt))
        tdSql.query('select * from %s where tgcol1 = 3' % (mt))
        # TSIM: print $data01 $data02 $data03
        tdLog.info('$data01 $data02 $data03')
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
        # TSIM: if $data02 != 3 then
        tdLog.info('tdSql.checkData(0, 2, 3)')
        tdSql.checkData(0, 2, 3)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 4 then
        tdLog.info('tdSql.checkData(0, 3, 4)')
        tdSql.checkData(0, 3, 4)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol2 = 4
        tdLog.info('select * from %s where tgcol2 = 4' % (mt))
        tdSql.query('select * from %s where tgcol2 = 4' % (mt))
        # TSIM: print $data01 $data02 $data03
        tdLog.info('$data01 $data02 $data03')
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
        # TSIM: if $data02 != 3 then
        tdLog.info('tdSql.checkData(0, 2, 3)')
        tdSql.checkData(0, 2, 3)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 4 then
        tdLog.info('tdSql.checkData(0, 3, 4)')
        tdSql.checkData(0, 3, 4)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol2 = 2
        tdLog.info('select * from %s where tgcol2 = 2' % (mt))
        tdSql.query('select * from %s where tgcol2 = 2' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM:
        # TSIM: print =============== step4
        tdLog.info('=============== step4')
        # TSIM: $i = 4
        i = 4
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol1
        # bigint, tgcol2 float)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 bigint, tgcol2 float)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 bigint, tgcol2 float)' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1, 2 )
        tdLog.info('create table %s using %s tags( 1, 2 )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1, 2 )' % (tb, mt))
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
        # TSIM: if $data02 != 1 then
        tdLog.info('tdSql.checkData(0, 2, 1)')
        tdSql.checkData(0, 2, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 2.00000 then
        tdLog.info('tdSql.checkData(0, 3, 2.00000)')
        tdSql.checkData(0, 3, 2.00000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql alter table $tb set tag tgcol1=3
        tdLog.info('alter table %s set tag tgcol1=3' % (tb))
        tdSql.execute('alter table %s set tag tgcol1=3' % (tb))
        # TSIM: sql alter table $tb set tag tgcol2=4
        tdLog.info('alter table %s set tag tgcol2=4' % (tb))
        tdSql.execute('alter table %s set tag tgcol2=4' % (tb))
        # TSIM:
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM:
        # TSIM: sql select * from $mt where tgcol1 = 3
        tdLog.info('select * from %s where tgcol1 = 3' % (mt))
        tdSql.query('select * from %s where tgcol1 = 3' % (mt))
        # TSIM: print $data01 $data02 $data03
        tdLog.info('$data01 $data02 $data03')
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
        # TSIM: if $data02 != 3 then
        tdLog.info('tdSql.checkData(0, 2, 3)')
        tdSql.checkData(0, 2, 3)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 4.00000 then
        tdLog.info('tdSql.checkData(0, 3, 4.00000)')
        tdSql.checkData(0, 3, 4.00000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol2 = 4
        tdLog.info('select * from %s where tgcol2 = 4' % (mt))
        tdSql.query('select * from %s where tgcol2 = 4' % (mt))
        # TSIM: print $data01 $data02 $data03
        tdLog.info('$data01 $data02 $data03')
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
        # TSIM: if $data02 != 3 then
        tdLog.info('tdSql.checkData(0, 2, 3)')
        tdSql.checkData(0, 2, 3)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 4.00000 then
        tdLog.info('tdSql.checkData(0, 3, 4.00000)')
        tdSql.checkData(0, 3, 4.00000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM:
        # TSIM: print =============== step5
        tdLog.info('=============== step5')
        # TSIM: $i = 5
        i = 5
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol1
        # double, tgcol2 binary(10))
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 double, tgcol2 binary(10))' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 double, tgcol2 binary(10))' %
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
        # TSIM: if $data02 != 1.000000000 then
        tdLog.info('tdSql.checkData(0, 2, 1.000000000)')
        tdSql.checkData(0, 2, 1.000000000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 2 then
        tdLog.info('tdSql.checkData(0, 3, "2")')
        tdSql.checkData(0, 3, "2")
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql alter table $tb set tag tgcol1=3
        tdLog.info('alter table %s set tag tgcol1=3' % (tb))
        tdSql.execute('alter table %s set tag tgcol1=3' % (tb))
        # TSIM: sql alter table $tb set tag tgcol2='4'
        tdLog.info('alter table %s set tag tgcol2="4"' % (tb))
        tdSql.execute('alter table %s set tag tgcol2="4"' % (tb))
        # TSIM:
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM:
        # TSIM: sql select * from $mt where tgcol1 = 3
        tdLog.info('select * from %s where tgcol1 = 3' % (mt))
        tdSql.query('select * from %s where tgcol1 = 3' % (mt))
        # TSIM: print $data01 $data02 $data03
        tdLog.info('$data01 $data02 $data03')
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
        # TSIM: if $data02 != 3.000000000 then
        tdLog.info('tdSql.checkData(0, 2, 3.000000000)')
        tdSql.checkData(0, 2, 3.000000000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 4 then
        tdLog.info('tdSql.checkData(0, 3, "4")')
        tdSql.checkData(0, 3, "4")
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol2 = '4'
        tdLog.info('select * from %s where tgcol2 = "4"' % (mt))
        tdSql.query('select * from %s where tgcol2 = "4"' % (mt))
        # TSIM: print $data01 $data02 $data03
        tdLog.info('$data01 $data02 $data03')
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
        # TSIM: if $data02 != 3.000000000 then
        tdLog.info('tdSql.checkData(0, 2, 3.000000000)')
        tdSql.checkData(0, 2, 3.000000000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 4 then
        tdLog.info('tdSql.checkData(0, 3, "4")')
        tdSql.checkData(0, 3, "4")
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
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol1
        # binary(10), tgcol2 int, tgcol3 smallint, tgcol4 binary(11), tgcol5
        # double, tgcol6 binary(20))
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 binary(10), tgcol2 int, tgcol3 smallint, tgcol4 binary(11), tgcol5 double, tgcol6 binary(20))' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 binary(10), tgcol2 int, tgcol3 smallint, tgcol4 binary(11), tgcol5 double, tgcol6 binary(20))' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( '1', 2, 3, '4', 5, '6' )
        tdLog.info(
            'create table %s using %s tags( "1", 2, 3, "4", 5, "6" )' %
            (tb, mt))
        tdSql.execute(
            'create table %s using %s tags( "1", 2, 3, "4", 5, "6" )' %
            (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol1 = '1'
        tdLog.info('select * from %s where tgcol1 = "1"' % (mt))
        tdSql.query('select * from %s where tgcol1 = "1"' % (mt))
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
        # TSIM: if $data02 != 1 then
        tdLog.info('tdSql.checkData(0, 2, "1")')
        tdSql.checkData(0, 2, "1")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 2 then
        tdLog.info('tdSql.checkData(0, 3, 2)')
        tdSql.checkData(0, 3, 2)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data04 != 3 then
        tdLog.info('tdSql.checkData(0, 4, 3)')
        tdSql.checkData(0, 4, 3)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data05 != 4 then
        tdLog.info('tdSql.checkData(0, 5, "4")')
        tdSql.checkData(0, 5, "4")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data06 != 5.000000000 then
        tdLog.info('tdSql.checkData(0, 6, 5.000000000)')
        tdSql.checkData(0, 6, 5.000000000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data07 != 6 then
        tdLog.info('tdSql.checkData(0, 7, "6")')
        tdSql.checkData(0, 7, "6")
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql alter table $mt drop tag tgcol3
        tdLog.info('alter table %s drop tag tgcol3' % (mt))
        tdSql.execute('alter table %s drop tag tgcol3' % (mt))
        # TSIM: sql alter table $tb set tag tgcol1='7'
        tdLog.info('alter table %s set tag tgcol1="7"' % (tb))
        tdSql.execute('alter table %s set tag tgcol1="7"' % (tb))
        # TSIM: sql alter table $tb set tag tgcol2=8
        tdLog.info('alter table %s set tag tgcol2=8' % (tb))
        tdSql.execute('alter table %s set tag tgcol2=8' % (tb))
        # TSIM: sql alter table $tb set tag tgcol4='9'
        tdLog.info('alter table %s set tag tgcol4="9"' % (tb))
        tdSql.execute('alter table %s set tag tgcol4="9"' % (tb))
        # TSIM: sql alter table $tb set tag tgcol5=10
        tdLog.info('alter table %s set tag tgcol5=10' % (tb))
        tdSql.execute('alter table %s set tag tgcol5=10' % (tb))
        # TSIM: sql alter table $tb set tag tgcol6='11'
        tdLog.info('alter table %s set tag tgcol6="11"' % (tb))
        tdSql.execute('alter table %s set tag tgcol6="11"' % (tb))
        # TSIM:
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM:
        # TSIM: sql select * from $mt where tgcol1 = '7'
        tdLog.info('select * from %s where tgcol1 = "7"' % (mt))
        tdSql.query('select * from %s where tgcol1 = "7"' % (mt))
        # TSIM: print $data01 $data02 $data03
        tdLog.info('$data01 $data02 $data03')
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
        # TSIM: if $data02 != 7 then
        tdLog.info('tdSql.checkData(0, 2, "7")')
        tdSql.checkData(0, 2, "7")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 8 then
        tdLog.info('tdSql.checkData(0, 3, 8)')
        tdSql.checkData(0, 3, 8)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data04 != 9 then
        tdLog.info('tdSql.checkData(0, 4, "9")')
        tdSql.checkData(0, 4, "9")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data05 != 10.000000000 then
        tdLog.info('tdSql.checkData(0, 5, 10.000000000)')
        tdSql.checkData(0, 5, 10.000000000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data06 != 11 then
        tdLog.info('tdSql.checkData(0, 6, "11")')
        tdSql.checkData(0, 6, "11")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data07 != NULL then
        tdLog.info('tdSql.checkData(0, 7, NULL)')
        try:
            tdSql.checkData(0, 7, None)
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("out of range")
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol2 = 8
        tdLog.info('select * from %s where tgcol2 = 8' % (mt))
        tdSql.query('select * from %s where tgcol2 = 8' % (mt))
        # TSIM: print $data01 $data02 $data03
        tdLog.info('$data01 $data02 $data03')
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
        # TSIM: if $data02 != 7 then
        tdLog.info('tdSql.checkData(0, 2, "7")')
        tdSql.checkData(0, 2, "7")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 8 then
        tdLog.info('tdSql.checkData(0, 3, 8)')
        tdSql.checkData(0, 3, 8)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data04 != 9 then
        tdLog.info('tdSql.checkData(0, 4, "9")')
        tdSql.checkData(0, 4, "9")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data05 != 10.000000000 then
        tdLog.info('tdSql.checkData(0, 5, 10.000000000)')
        tdSql.checkData(0, 5, 10.000000000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data06 != 11 then
        tdLog.info('tdSql.checkData(0, 6, "11")')
        tdSql.checkData(0, 6, "11")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data07 != NULL then
        tdLog.info('tdSql.checkData(0, 7, NULL)')
        try:
            tdSql.checkData(0, 7, None)
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("out of range")
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol4 = '9'
        tdLog.info('select * from %s where tgcol4 = "9"' % (mt))
        tdSql.query('select * from %s where tgcol4 = "9"' % (mt))
        # TSIM: print $data01 $data02 $data03
        tdLog.info('$data01 $data02 $data03')
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
        # TSIM: if $data02 != 7 then
        tdLog.info('tdSql.checkData(0, 2, "7")')
        tdSql.checkData(0, 2, "7")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 8 then
        tdLog.info('tdSql.checkData(0, 3, 8)')
        tdSql.checkData(0, 3, 8)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data04 != 9 then
        tdLog.info('tdSql.checkData(0, 4, "9")')
        tdSql.checkData(0, 4, "9")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data05 != 10.000000000 then
        tdLog.info('tdSql.checkData(0, 5, 10.000000000)')
        tdSql.checkData(0, 5, 10.000000000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data06 != 11 then
        tdLog.info('tdSql.checkData(0, 6, "11")')
        tdSql.checkData(0, 6, "11")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data07 != NULL then
        tdLog.info('tdSql.checkData(0, 7, NULL)')
        try:
            tdSql.checkData(0, 7, None)
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("out of range")
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol5 = 10
        tdLog.info('select * from %s where tgcol5 = 10' % (mt))
        tdSql.query('select * from %s where tgcol5 = 10' % (mt))
        # TSIM: print $data01 $data02 $data03
        tdLog.info('$data01 $data02 $data03')
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
        # TSIM: if $data02 != 7 then
        tdLog.info('tdSql.checkData(0, 2, "7")')
        tdSql.checkData(0, 2, "7")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 8 then
        tdLog.info('tdSql.checkData(0, 3, 8)')
        tdSql.checkData(0, 3, 8)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data04 != 9 then
        tdLog.info('tdSql.checkData(0, 4, "9")')
        tdSql.checkData(0, 4, "9")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data05 != 10.000000000 then
        tdLog.info('tdSql.checkData(0, 5, 10.000000000)')
        tdSql.checkData(0, 5, 10.000000000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data06 != 11 then
        tdLog.info('tdSql.checkData(0, 6, "11")')
        tdSql.checkData(0, 6, "11")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data07 != NULL then
        tdLog.info('tdSql.checkData(0, 7, NULL)')
        try:
            tdSql.checkData(0, 7, None)
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("out of range")
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol6 = '11'
        tdLog.info('select * from %s where tgcol6 = "11"' % (mt))
        tdSql.query('select * from %s where tgcol6 = "11"' % (mt))
        # TSIM: print $data01 $data02 $data03
        tdLog.info('$data01 $data02 $data03')
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
        # TSIM: if $data02 != 7 then
        tdLog.info('tdSql.checkData(0, 2, "7")')
        tdSql.checkData(0, 2, "7")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 8 then
        tdLog.info('tdSql.checkData(0, 3, 8)')
        tdSql.checkData(0, 3, 8)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data04 != 9 then
        tdLog.info('tdSql.checkData(0, 4, "9")')
        tdSql.checkData(0, 4, "9")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data05 != 10.000000000 then
        tdLog.info('tdSql.checkData(0, 5, 10.000000000)')
        tdSql.checkData(0, 5, 10.000000000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data06 != 11 then
        tdLog.info('tdSql.checkData(0, 6, "11")')
        tdSql.checkData(0, 6, "11")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data07 != NULL then
        tdLog.info('tdSql.checkData(0, 7, NULL)')
        try:
            tdSql.checkData(0, 7, None)
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("out of range")
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
