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
        # TSIM: $dbPrefix = ta_ad_db
        # TSIM: $tbPrefix = ta_ad_tb
        tbPrefix = "ta_ad_tb"
        # TSIM: $mtPrefix = ta_ad_mt
        mtPrefix = "ta_ad_mt"
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
        # TSIM: sql alter table $mt drop tag tgcol2
        tdLog.info('alter table %s drop tag tgcol2' % (mt))
        tdSql.execute('alter table %s drop tag tgcol2' % (mt))
        # TSIM: sql alter table $mt add tag tgcol4 int
        tdLog.info('alter table %s add tag tgcol4 int' % (mt))
        tdSql.execute('alter table %s add tag tgcol4 int' % (mt))
        tdLog.info('select * from %s where tgcol4=6' % (mt))
        tdSql.query('select * from %s where tgcol4=6' % (mt))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM: sql alter table $tb set tag tgcol4 =4
        tdLog.info('alter table %s set tag tgcol4 =4' % (tb))
        tdSql.execute('alter table %s set tag tgcol4 =4' % (tb))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM:
        # TSIM: sql select * from $mt where tgcol4 = 4
        tdLog.info('select * from %s where tgcol4 = 4' % (mt))
        tdSql.query('select * from %s where tgcol4 = 4' % (mt))
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
        # TSIM: if $data02 != 1 then
        tdLog.info('tdSql.checkData(0, 2, 1)')
        tdSql.checkData(0, 2, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 4 then
        tdLog.info('tdSql.checkData(0, 3, 4)')
        tdSql.checkData(0, 3, 4)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol2 = 1 -x step2
        tdLog.info('select * from %s where tgcol2 = 1 -x step2' % (mt))
        tdSql.error('select * from %s where tgcol2 = 1' % (mt))
        tdLog.info('=============== step2-1')
        # TSIM: $i = 2
        i = 21
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol1
        # bool, tgcol2 int)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int unsigned)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int unsigned)' %
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
        # TSIM: sql alter table $mt drop tag tgcol2
        tdLog.info('alter table %s drop tag tgcol2' % (mt))
        tdSql.execute('alter table %s drop tag tgcol2' % (mt))
        # TSIM: sql alter table $mt add tag tgcol4 int
        tdLog.info('alter table %s add tag tgcol4 int unsigned' % (mt))
        tdSql.execute('alter table %s add tag tgcol4 int unsigned' % (mt))
        tdLog.info('select * from %s where tgcol4=6' % (mt))
        tdSql.query('select * from %s where tgcol4=6' % (mt))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM: sql alter table $tb set tag tgcol4 =4
        tdLog.info('alter table %s set tag tgcol4 =4' % (tb))
        tdSql.execute('alter table %s set tag tgcol4 =4' % (tb))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM:
        # TSIM: sql select * from $mt where tgcol4 = 4
        tdLog.info('select * from %s where tgcol4 = 4' % (mt))
        tdSql.query('select * from %s where tgcol4 = 4' % (mt))
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
        # TSIM: if $data02 != 1 then
        tdLog.info('tdSql.checkData(0, 2, 1)')
        tdSql.checkData(0, 2, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 4 then
        tdLog.info('tdSql.checkData(0, 3, 4)')
        tdSql.checkData(0, 3, 4)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol2 = 1 -x step2
        tdLog.info('select * from %s where tgcol2 = 1 -x step2' % (mt))
        tdSql.error('select * from %s where tgcol2 = 1' % (mt))
        # TSIM: return -1
        # TSIM: step2:
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
        # TSIM: sql alter table $mt drop tag tgcol2
        tdLog.info('alter table %s drop tag tgcol2' % (mt))
        tdSql.execute('alter table %s drop tag tgcol2' % (mt))
        # TSIM: sql alter table $mt add tag tgcol4 tinyint
        tdLog.info('alter table %s add tag tgcol4 tinyint' % (mt))
        tdSql.execute('alter table %s add tag tgcol4 tinyint' % (mt))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM: sql alter table $tb set tag tgcol4=4
        tdLog.info('alter table %s set tag tgcol4=4' % (tb))
        tdSql.execute('alter table %s set tag tgcol4=4' % (tb))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM:
        # TSIM: sql select * from $mt where tgcol4 = 4
        tdLog.info('select * from %s where tgcol4 = 4' % (mt))
        tdSql.query('select * from %s where tgcol4 = 4' % (mt))
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
        # TSIM: if $data02 != 1 then
        tdLog.info('tdSql.checkData(0, 2, 1)')
        tdSql.checkData(0, 2, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 4 then
        tdLog.info('tdSql.checkData(0, 3, 4)')
        tdSql.checkData(0, 3, 4)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol2 = 1 -x step3
        tdLog.info('select * from %s where tgcol2 = 1 -x step3' % (mt))
        tdSql.error('select * from %s where tgcol2 = 1' % (mt))
        tdLog.info('=============== step3-1')
        # TSIM: $i = 3
        i = 31
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol1
        # smallint, tgcol2 tinyint)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 smallint unsigned, tgcol2 tinyint unsigned)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 smallint unsigned, tgcol2 tinyint unsigned)' %
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
        # TSIM: sql alter table $mt drop tag tgcol2
        tdLog.info('alter table %s drop tag tgcol2' % (mt))
        tdSql.execute('alter table %s drop tag tgcol2' % (mt))
        # TSIM: sql alter table $mt add tag tgcol4 tinyint
        tdLog.info('alter table %s add tag tgcol4 tinyint unsigned' % (mt))
        tdSql.execute('alter table %s add tag tgcol4 tinyint unsigned' % (mt))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM: sql alter table $tb set tag tgcol4=4
        tdLog.info('alter table %s set tag tgcol4=4' % (tb))
        tdSql.execute('alter table %s set tag tgcol4=4' % (tb))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM:
        # TSIM: sql select * from $mt where tgcol4 = 4
        tdLog.info('select * from %s where tgcol4 = 4' % (mt))
        tdSql.query('select * from %s where tgcol4 = 4' % (mt))
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
        # TSIM: if $data02 != 1 then
        tdLog.info('tdSql.checkData(0, 2, 1)')
        tdSql.checkData(0, 2, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 4 then
        tdLog.info('tdSql.checkData(0, 3, 4)')
        tdSql.checkData(0, 3, 4)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol2 = 1 -x step3
        tdLog.info('select * from %s where tgcol2 = 1 -x step3' % (mt))
        tdSql.error('select * from %s where tgcol2 = 1' % (mt))
        # TSIM: return -1
        # TSIM: step3:
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
        # TSIM: sql describe $tb
        tdLog.info('describe %s' % (tb))
        tdSql.query('describe %s' % (tb))
        # TSIM: if $data21 != BIGINT then
        tdLog.info('tdSql.checkDataType(2, 1, "BIGINT")')
        tdSql.checkDataType(2, 1, "BIGINT")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data31 != FLOAT then
        tdLog.info('tdSql.checkDataType(3, 1, "FLOAT")')
        tdSql.checkDataType(3, 1, "FLOAT")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data23 != 1 then
        tdLog.info('tdSql.checkData(2, 3, TAG)')
        tdSql.checkData(2, 3, "TAG")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data33 != 2.000000 then
        tdLog.info('tdSql.checkData(3, 3, 2.000000)')
        tdSql.checkData(3, 3, "TAG")
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql alter table $mt drop tag tgcol2
        tdLog.info('alter table %s drop tag tgcol2' % (mt))
        tdSql.execute('alter table %s drop tag tgcol2' % (mt))
        # TSIM: sql alter table $mt add tag tgcol4 float
        tdLog.info('alter table %s add tag tgcol4 float' % (mt))
        tdSql.execute('alter table %s add tag tgcol4 float' % (mt))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM: sql alter table $tb set tag tgcol4=4
        tdLog.info('alter table %s set tag tgcol4=4' % (tb))
        tdSql.execute('alter table %s set tag tgcol4=4' % (tb))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM:
        # TSIM: sql select * from $mt where tgcol4 = 4
        tdLog.info('select * from %s where tgcol4 = 4' % (mt))
        tdSql.query('select * from %s where tgcol4 = 4' % (mt))
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
        # TSIM: if $data02 != 1 then
        tdLog.info('tdSql.checkData(0, 2, 1)')
        tdSql.checkData(0, 2, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 4.00000 then
        tdLog.info('tdSql.checkData(0, 3, 4.00000)')
        tdSql.checkData(0, 3, 4.00000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol2 = 1 -x step4
        tdLog.info('select * from %s where tgcol2 = 1 -x step4' % (mt))
        tdSql.error('select * from %s where tgcol2 = 1' % (mt))
        tdLog.info('=============== step4-1')
        # TSIM: $i = 4
        i = 41
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol1
        # bigint, tgcol2 float)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 bigint unsigned, tgcol2 float)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 bigint unsigned, tgcol2 float)' %
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
        # TSIM: sql describe $tb
        tdLog.info('describe %s' % (tb))
        tdSql.query('describe %s' % (tb))
        # TSIM: if $data21 != BIGINT then
        tdLog.info('tdSql.checkDataType(2, 1, "BIGINT UNSIGNED")')
        tdSql.checkDataType(2, 1, "BIGINT")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data31 != FLOAT then
        tdLog.info('tdSql.checkDataType(3, 1, "FLOAT")')
        tdSql.checkDataType(3, 1, "FLOAT")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data23 != 1 then
        tdLog.info('tdSql.checkData(2, 3, TAG)')
        tdSql.checkData(2, 3, "TAG")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data33 != 2.000000 then
        tdLog.info('tdSql.checkData(3, 3, 2.000000)')
        tdSql.checkData(3, 3, "TAG")
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql alter table $mt drop tag tgcol2
        tdLog.info('alter table %s drop tag tgcol2' % (mt))
        tdSql.execute('alter table %s drop tag tgcol2' % (mt))
        # TSIM: sql alter table $mt add tag tgcol4 float
        tdLog.info('alter table %s add tag tgcol4 float' % (mt))
        tdSql.execute('alter table %s add tag tgcol4 float' % (mt))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM: sql alter table $tb set tag tgcol4=4
        tdLog.info('alter table %s set tag tgcol4=4' % (tb))
        tdSql.execute('alter table %s set tag tgcol4=4' % (tb))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM:
        # TSIM: sql select * from $mt where tgcol4 = 4
        tdLog.info('select * from %s where tgcol4 = 4' % (mt))
        tdSql.query('select * from %s where tgcol4 = 4' % (mt))
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
        # TSIM: if $data02 != 1 then
        tdLog.info('tdSql.checkData(0, 2, 1)')
        tdSql.checkData(0, 2, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 4.00000 then
        tdLog.info('tdSql.checkData(0, 3, 4.00000)')
        tdSql.checkData(0, 3, 4.00000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol2 = 1 -x step4
        tdLog.info('select * from %s where tgcol2 = 1 -x step4' % (mt))
        tdSql.error('select * from %s where tgcol2 = 1' % (mt))
        # TSIM: return -1
        # TSIM: step4:
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
        tdLog.info('tdSql.checkData(0, 3, 2)')
        tdSql.checkData(0, 3, "2")
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql alter table $mt drop tag tgcol2
        tdLog.info('alter table %s drop tag tgcol2' % (mt))
        tdSql.execute('alter table %s drop tag tgcol2' % (mt))
        # TSIM: sql alter table $mt add tag tgcol4 smallint
        tdLog.info('alter table %s add tag tgcol4 smallint' % (mt))
        tdSql.execute('alter table %s add tag tgcol4 smallint' % (mt))
        # TSIM: sql alter table $mt add tag tgcol5 smallint unsigned
        tdLog.info('alter table %s add tag tgcol5 smallint unsigned' % (mt))
        tdSql.execute('alter table %s add tag tgcol5 smallint unsigned' % (mt))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM: sql alter table $tb set tag tgcol4=4
        tdLog.info('alter table %s set tag tgcol4=4' % (tb))
        tdSql.execute('alter table %s set tag tgcol4=4' % (tb))
        # TSIM: sql alter table $tb set tag tgcol5=5
        tdLog.info('alter table %s set tag tgcol5=5' % (tb))
        tdSql.execute('alter table %s set tag tgcol5=5' % (tb))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM:
        # TSIM: sql select * from $mt where tgcol4 = 4
        tdLog.info('select * from %s where tgcol4 = 4' % (mt))
        tdSql.query('select * from %s where tgcol4 = 4' % (mt))
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
        # TSIM: if $data02 != 1.000000000 then
        tdLog.info('tdSql.checkData(0, 2, 1.000000000)')
        tdSql.checkData(0, 2, 1.000000000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 4 then
        tdLog.info('tdSql.checkData(0, 3, 4)')
        tdSql.checkData(0, 3, 4)
        tdSql.query('select * from %s where tgcol5 = 5' % (mt))
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1.000000000)
        tdSql.checkData(0, 4, 5)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol3 = '1' -x step5
        tdLog.info('select * from %s where tgcol3 = "1" -x step5' % (mt))
        tdSql.error('select * from %s where tgcol3 = "1"' % (mt))
        # TSIM: return -1
        # TSIM: step5:
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
        # bool, tgcol2 int, tgcol3 tinyint)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int, tgcol3 tinyint)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int, tgcol3 tinyint)' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1, 2, 3 )
        tdLog.info('create table %s using %s tags( 1, 2, 3 )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1, 2, 3 )' % (tb, mt))
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
        # TSIM: if $data04 != 3 then
        tdLog.info('tdSql.checkData(0, 4, 3)')
        tdSql.checkData(0, 4, 3)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql alter table $mt change tag tgcol1 tgcol4
        tdLog.info('alter table %s change tag tgcol1 tgcol4' % (mt))
        tdSql.execute('alter table %s change tag tgcol1 tgcol4' % (mt))
        # TSIM: sql alter table $mt drop tag tgcol2
        tdLog.info('alter table %s drop tag tgcol2' % (mt))
        tdSql.execute('alter table %s drop tag tgcol2' % (mt))
        # TSIM: sql alter table $mt drop tag tgcol3
        tdLog.info('alter table %s drop tag tgcol3' % (mt))
        tdSql.execute('alter table %s drop tag tgcol3' % (mt))
        # TSIM: sql alter table $mt add tag tgcol5 binary(10)
        tdLog.info('alter table %s add tag tgcol5 binary(10)' % (mt))
        tdSql.execute('alter table %s add tag tgcol5 binary(10)' % (mt))
        # TSIM: sql alter table $mt add tag tgcol6 binary(10)
        tdLog.info('alter table %s add tag tgcol6 binary(10)' % (mt))
        tdSql.execute('alter table %s add tag tgcol6 binary(10)' % (mt))
        # TSIM:
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM: sql alter table $tb set tag tgcol4=false
        tdLog.info('alter table %s set tag tgcol4=false' % (tb))
        tdSql.execute('alter table %s set tag tgcol4=false' % (tb))
        # TSIM: sql alter table $tb set tag tgcol5=5
        tdLog.info('alter table %s set tag tgcol5=5' % (tb))
        tdSql.execute('alter table %s set tag tgcol5=5' % (tb))
        # TSIM: sql alter table $tb set tag tgcol6=6
        tdLog.info('alter table %s set tag tgcol6=6' % (tb))
        tdSql.execute('alter table %s set tag tgcol6=6' % (tb))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM:
        # TSIM: sql select * from $mt where tgcol5 = '5'
        tdLog.info('select * from %s where tgcol5 = "5"' % (mt))
        tdSql.query('select * from %s where tgcol5 = "5"' % (mt))
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
        # TSIM: if $data03 != 5 then
        tdLog.info('tdSql.checkData(0, 3, 5)')
        tdSql.checkData(0, 3, "5")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data04 != 6 then
        tdLog.info('tdSql.checkData(0, 4, 6)')
        tdSql.checkData(0, 4, "6")
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol6 = '6'
        tdLog.info('select * from %s where tgcol6 = "6"' % (mt))
        tdSql.query('select * from %s where tgcol6 = "6"' % (mt))
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
        # TSIM: if $data03 != 5 then
        tdLog.info('tdSql.checkData(0, 3, 5)')
        tdSql.checkData(0, 3, "5")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data04 != 6 then
        tdLog.info('tdSql.checkData(0, 4, 6)')
        tdSql.checkData(0, 4, "6")
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol4 = 1
        tdLog.info('select * from %s where tgcol4 = 1' % (mt))
        tdSql.query('select * from %s where tgcol4 = 1' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol3 = 1 -x step52
        tdLog.info('select * from %s where tgcol3 = 1 -x step52' % (mt))
        tdSql.error('select * from %s where tgcol3 = 12' % (mt))
        # TSIM: return -1
        # TSIM: step52:
        # TSIM:
        # TSIM: print =============== step7
        tdLog.info('=============== step7')
        # TSIM: $i = 7
        i = 7
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol1
        # smallint, tgcol2 tinyint, tgcol3 binary(10))
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 smallint, tgcol2 tinyint, tgcol3 binary(10))' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 smallint, tgcol2 tinyint, tgcol3 binary(10))' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1, 2, '3' )
        tdLog.info('create table %s using %s tags( 1, 2, "3" )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1, 2, "3" )' % (tb, mt))
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
        # TSIM: if $data04 != 3 then
        tdLog.info('tdSql.checkData(0, 4, 3)')
        tdSql.checkData(0, 4, "3")
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql alter table $mt change tag tgcol1 tgcol4
        tdLog.info('alter table %s change tag tgcol1 tgcol4' % (mt))
        tdSql.execute('alter table %s change tag tgcol1 tgcol4' % (mt))
        # TSIM: sql alter table $mt drop tag tgcol2
        tdLog.info('alter table %s drop tag tgcol2' % (mt))
        tdSql.execute('alter table %s drop tag tgcol2' % (mt))
        # TSIM: sql alter table $mt drop tag tgcol3
        tdLog.info('alter table %s drop tag tgcol3' % (mt))
        tdSql.execute('alter table %s drop tag tgcol3' % (mt))
        # TSIM: sql alter table $mt add tag tgcol5 bigint
        tdLog.info('alter table %s add tag tgcol5 bigint' % (mt))
        tdSql.execute('alter table %s add tag tgcol5 bigint' % (mt))
        # TSIM: sql alter table $mt add tag tgcol6 tinyint
        tdLog.info('alter table %s add tag tgcol6 tinyint' % (mt))
        tdSql.execute('alter table %s add tag tgcol6 tinyint' % (mt))
        # TSIM:
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM: sql alter table $tb set tag tgcol4=4
        tdLog.info('alter table %s set tag tgcol4=4' % (tb))
        tdSql.execute('alter table %s set tag tgcol4=4' % (tb))
        # TSIM: sql alter table $tb set tag tgcol5=5
        tdLog.info('alter table %s set tag tgcol5=5' % (tb))
        tdSql.execute('alter table %s set tag tgcol5=5' % (tb))
        # TSIM: sql alter table $tb set tag tgcol6=6
        tdLog.info('alter table %s set tag tgcol6=6' % (tb))
        tdSql.execute('alter table %s set tag tgcol6=6' % (tb))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM:
        # TSIM: sql select * from $mt where tgcol6 = 6
        tdLog.info('select * from %s where tgcol6 = 6' % (mt))
        tdSql.query('select * from %s where tgcol6 = 6' % (mt))
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
        # TSIM: if $data02 != 4 then
        tdLog.info('tdSql.checkData(0, 2, 4)')
        tdSql.checkData(0, 2, 4)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 5 then
        tdLog.info('tdSql.checkData(0, 3, 5)')
        tdSql.checkData(0, 3, 5)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data04 != 6 then
        tdLog.info('tdSql.checkData(0, 4, 6)')
        tdSql.checkData(0, 4, 6)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol2 = 1 -x step71
        tdLog.info('select * from %s where tgcol2 = 1 -x step71' % (mt))
        tdSql.error('select * from %s where tgcol2 = 11' % (mt))
        # TSIM: return -1
        # TSIM: step71:
        # TSIM: sql select * from $mt where tgcol3 = 1 -x step72
        tdLog.info('select * from %s where tgcol3 = 1 -x step72' % (mt))
        tdSql.error('select * from %s where tgcol3 = 12' % (mt))
        # TSIM: return -1
        # TSIM: step72:
        # TSIM:
        # TSIM: print =============== step8
        tdLog.info('=============== step8')
        # TSIM: $i = 8
        i = 8
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol1
        # bigint, tgcol2 float, tgcol3 binary(10))
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 bigint, tgcol2 float, tgcol3 binary(10))' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 bigint, tgcol2 float, tgcol3 binary(10))' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1, 2, '3' )
        tdLog.info('create table %s using %s tags( 1, 2, "3" )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1, 2, "3" )' % (tb, mt))
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
        # TSIM: if $data04 != 3 then
        tdLog.info('tdSql.checkData(0, 4, 3)')
        tdSql.checkData(0, 4, "3")
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql alter table $mt change tag tgcol1 tgcol4
        tdLog.info('alter table %s change tag tgcol1 tgcol4' % (mt))
        tdSql.execute('alter table %s change tag tgcol1 tgcol4' % (mt))
        # TSIM: sql alter table $mt drop tag tgcol2
        tdLog.info('alter table %s drop tag tgcol2' % (mt))
        tdSql.execute('alter table %s drop tag tgcol2' % (mt))
        # TSIM: sql alter table $mt drop tag tgcol3
        tdLog.info('alter table %s drop tag tgcol3' % (mt))
        tdSql.execute('alter table %s drop tag tgcol3' % (mt))
        # TSIM: sql alter table $mt add tag tgcol5 binary(17)
        tdLog.info('alter table %s add tag tgcol5 binary(17)' % (mt))
        tdSql.execute('alter table %s add tag tgcol5 binary(17)' % (mt))
        # TSIM: sql alter table $mt add tag tgcol6 bool
        tdLog.info('alter table %s add tag tgcol6 bool' % (mt))
        tdSql.execute('alter table %s add tag tgcol6 bool' % (mt))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM: sql alter table $tb set tag tgcol4=4
        tdLog.info('alter table %s set tag tgcol4=4' % (tb))
        tdSql.execute('alter table %s set tag tgcol4=4' % (tb))
        # TSIM: sql alter table $tb set tag tgcol5=5
        tdLog.info('alter table %s set tag tgcol5=5' % (tb))
        tdSql.execute('alter table %s set tag tgcol5=5' % (tb))
        # TSIM: sql alter table $tb set tag tgcol6=1
        tdLog.info('alter table %s set tag tgcol6=1' % (tb))
        tdSql.execute('alter table %s set tag tgcol6=1' % (tb))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM:
        # TSIM: sql select * from $mt where tgcol5 = '5'
        tdLog.info('select * from %s where tgcol5 = "5"' % (mt))
        tdSql.query('select * from %s where tgcol5 = "5"' % (mt))
        # TSIM: print select * from $mt where tgcol5 = 5
        tdLog.info('select * from $mt where tgcol5 = 5')
        # TSIM: print $data01 $data02 $data03 $data04
        tdLog.info('$data01 $data02 $data03 $data04')
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
        # TSIM: if $data02 != 4 then
        tdLog.info('tdSql.checkData(0, 2, 4)')
        tdSql.checkData(0, 2, 4)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 5 then
        tdLog.info('tdSql.checkData(0, 3, 5)')
        tdSql.checkData(0, 3, "5")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data04 != 1 then
        tdLog.info('tdSql.checkData(0, 4, 1)')
        tdSql.checkData(0, 4, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol2 = 1 -x step81
        tdLog.info('select * from %s where tgcol2 = 1 -x step81' % (mt))
        tdSql.error('select * from %s where tgcol2 = 11' % (mt))
        # TSIM: return -1
        # TSIM: step81:
        # TSIM: sql select * from $mt where tgcol3 = 1 -x step82
        tdLog.info('select * from %s where tgcol3 = 1 -x step82' % (mt))
        tdSql.error('select * from %s where tgcol3 = 12' % (mt))
        # TSIM: return -1
        # TSIM: step82:
        # TSIM:
        # TSIM: print =============== step9
        tdLog.info('=============== step9')
        # TSIM: $i = 9
        i = 9
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol1
        # double, tgcol2 binary(10), tgcol3 binary(10))
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 double, tgcol2 binary(10), tgcol3 binary(10))' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 double, tgcol2 binary(10), tgcol3 binary(10))' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1, 2, '3' )
        tdLog.info('create table %s using %s tags( 1, 2, "3" )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1, 2, "3" )' % (tb, mt))
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
        tdLog.info('tdSql.checkData(0, 3, 2)')
        tdSql.checkData(0, 3, "2")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data04 != 3 then
        tdLog.info('tdSql.checkData(0, 4, 3)')
        tdSql.checkData(0, 4, "3")
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql alter table $mt change tag tgcol1 tgcol4
        tdLog.info('alter table %s change tag tgcol1 tgcol4' % (mt))
        tdSql.execute('alter table %s change tag tgcol1 tgcol4' % (mt))
        # TSIM: sql alter table $mt drop tag tgcol2
        tdLog.info('alter table %s drop tag tgcol2' % (mt))
        tdSql.execute('alter table %s drop tag tgcol2' % (mt))
        # TSIM: sql alter table $mt drop tag tgcol3
        tdLog.info('alter table %s drop tag tgcol3' % (mt))
        tdSql.execute('alter table %s drop tag tgcol3' % (mt))
        # TSIM: sql alter table $mt add tag tgcol5 bool
        tdLog.info('alter table %s add tag tgcol5 bool' % (mt))
        tdSql.execute('alter table %s add tag tgcol5 bool' % (mt))
        # TSIM: sql alter table $mt add tag tgcol6 float
        tdLog.info('alter table %s add tag tgcol6 float' % (mt))
        tdSql.execute('alter table %s add tag tgcol6 float' % (mt))
        # TSIM:
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM: sql alter table $tb set tag tgcol4=4
        tdLog.info('alter table %s set tag tgcol4=4' % (tb))
        tdSql.execute('alter table %s set tag tgcol4=4' % (tb))
        # TSIM: sql alter table $tb set tag tgcol5=1
        tdLog.info('alter table %s set tag tgcol5=1' % (tb))
        tdSql.execute('alter table %s set tag tgcol5=1' % (tb))
        # TSIM: sql alter table $tb set tag tgcol6=6
        tdLog.info('alter table %s set tag tgcol6=6' % (tb))
        tdSql.execute('alter table %s set tag tgcol6=6' % (tb))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM:
        # TSIM: sql select * from $mt where tgcol5 = 1
        tdLog.info('select * from %s where tgcol5 = 1' % (mt))
        tdSql.query('select * from %s where tgcol5 = 1' % (mt))
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
        # TSIM: if $data02 != 4.000000000 then
        tdLog.info('tdSql.checkData(0, 2, 4.000000000)')
        tdSql.checkData(0, 2, 4.000000000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 1 then
        tdLog.info('tdSql.checkData(0, 3, 1)')
        tdSql.checkData(0, 3, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data04 != 6.00000 then
        tdLog.info('tdSql.checkData(0, 4, 6.00000)')
        tdSql.checkData(0, 4, 6.00000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol3 = 1 -x step91
        tdLog.info('select * from %s where tgcol3 = 1 -x step91' % (mt))
        tdSql.error('select * from %s where tgcol3 = 11' % (mt))
        # TSIM: return -1
        # TSIM: step91:
        # TSIM: sql select * from $mt where tgcol2 = 1 -x step92
        tdLog.info('select * from %s where tgcol2 = 1 -x step92' % (mt))
        tdSql.error('select * from %s where tgcol2 = 12' % (mt))
        # TSIM: return -1
        # TSIM: step92:
        # TSIM:
        # TSIM: print =============== step10
        tdLog.info('=============== step10')
        # TSIM: $i = 10
        i = 10
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol1
        # binary(10), tgcol2 binary(10), tgcol3 binary(10), tgcol4 binary(10))
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 binary(10), tgcol2 binary(10), tgcol3 binary(10), tgcol4 binary(10))' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 binary(10), tgcol2 binary(10), tgcol3 binary(10), tgcol4 binary(10))' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( '1', '2', '3', '4' )
        tdLog.info(
            'create table %s using %s tags( "1", "2", "3", "4" )' %
            (tb, mt))
        tdSql.execute(
            'create table %s using %s tags( "1", "2", "3", "4" )' %
            (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM: sql select * from $mt where tgcol4 = '4'
        tdLog.info('select * from %s where tgcol4 = "4"' % (mt))
        tdSql.query('select * from %s where tgcol4 = "4"' % (mt))
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
        tdSql.checkData(0, 2, "1")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 2 then
        tdLog.info('tdSql.checkData(0, 3, 2)')
        tdSql.checkData(0, 3, "2")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data04 != 3 then
        tdLog.info('tdSql.checkData(0, 4, 3)')
        tdSql.checkData(0, 4, "3")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data05 != 4 then
        tdLog.info('tdSql.checkData(0, 5, 4)')
        tdSql.checkData(0, 5, "4")
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql alter table $mt change tag tgcol1 tgcol4 -x step103
        tdLog.info('alter table %s change tag tgcol1 tgcol4 -x step103' % (mt))
        tdSql.error('alter table %s change tag tgcol1 tgcol4' % (mt))
        # TSIM: return -1
        # TSIM: step103:
        # TSIM:
        # TSIM: sql alter table $mt drop tag tgcol2
        tdLog.info('alter table %s drop tag tgcol2' % (mt))
        tdSql.execute('alter table %s drop tag tgcol2' % (mt))
        # TSIM: sql alter table $mt drop tag tgcol3
        tdLog.info('alter table %s drop tag tgcol3' % (mt))
        tdSql.execute('alter table %s drop tag tgcol3' % (mt))
        # TSIM: sql alter table $mt drop tag tgcol4
        tdLog.info('alter table %s drop tag tgcol4' % (mt))
        tdSql.execute('alter table %s drop tag tgcol4' % (mt))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM: sql alter table $mt add tag tgcol4 binary(10)
        tdLog.info('alter table %s add tag tgcol4 binary(10)' % (mt))
        tdSql.execute('alter table %s add tag tgcol4 binary(10)' % (mt))
        # TSIM: sql alter table $mt add tag tgcol5 bool
        tdLog.info('alter table %s add tag tgcol5 bool' % (mt))
        tdSql.execute('alter table %s add tag tgcol5 bool' % (mt))
        # TSIM:
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM: sql alter table $tb set tag tgcol4=4
        tdLog.info('alter table %s set tag tgcol4=4' % (tb))
        tdSql.execute('alter table %s set tag tgcol4=4' % (tb))
        # TSIM: sql alter table $tb set tag tgcol5=false
        tdLog.info('alter table %s set tag tgcol5=false' % (tb))
        tdSql.execute('alter table %s set tag tgcol5=false' % (tb))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM:
        # TSIM: sql select * from $mt where tgcol4 = '4'
        tdLog.info('select * from %s where tgcol4 = "4"' % (mt))
        tdSql.query('select * from %s where tgcol4 = "4"' % (mt))
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
        # TSIM: if $data02 != 1 then
        tdLog.info('tdSql.checkData(0, 2, 1)')
        tdSql.checkData(0, 2, "1")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 4 then
        tdLog.info('tdSql.checkData(0, 3, 4)')
        tdSql.checkData(0, 3, "4")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data04 != 0 then
        tdLog.info('tdSql.checkData(0, 4, 0)')
        tdSql.checkData(0, 4, 0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data05 != NULL then
       #tdLog.info('tdSql.checkData(0, 5, NULL)')
       # tdSql.checkData(0, 5, None)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol2 = 1 -x step101
        tdLog.info('select * from %s where tgcol2 = 1 -x step101' % (mt))
        tdSql.error('select * from %s where tgcol2 = 101' % (mt))
        # TSIM: return -1
        # TSIM: step101:
        # TSIM: sql select * from $mt where tgcol3 = 1 -x step102
        tdLog.info('select * from %s where tgcol3 = 1 -x step102' % (mt))
        tdSql.error('select * from %s where tgcol3 = 102' % (mt))
        # TSIM: return -1
        # TSIM: step102:
        # TSIM:
        # TSIM: print =============== step11
        tdLog.info('=============== step11')
        # TSIM: $i = 11
        i = 11
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol1
        # bool, tgcol2 int, tgcol3 smallint, tgcol4 float, tgcol5 binary(10))
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int, tgcol3 smallint, tgcol4 float, tgcol5 binary(10))' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int, tgcol3 smallint, tgcol4 float, tgcol5 binary(10))' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1, 2, 3, 4, '5' )
        tdLog.info(
            'create table %s using %s tags( 1, 2, 3, 4, "5" )' %
            (tb, mt))
        tdSql.execute(
            'create table %s using %s tags( 1, 2, 3, 4, "5" )' %
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
        # TSIM: if $data04 != 3 then
        tdLog.info('tdSql.checkData(0, 4, 3)')
        tdSql.checkData(0, 4, 3)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data05 != 4.00000 then
        tdLog.info('tdSql.checkData(0, 5, 4.00000)')
        tdSql.checkData(0, 5, 4.00000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data06 != 5 then
        tdLog.info('tdSql.checkData(0, 6, 5)')
        tdSql.checkData(0, 6, "5")
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql alter table $mt change tag tgcol1 tgcol4 -x step114
        tdLog.info('alter table %s change tag tgcol1 tgcol4 -x step114' % (mt))
        tdSql.error('alter table %s change tag tgcol1 tgcol4' % (mt))
        # TSIM: return -1
        # TSIM: step114:
        # TSIM:
        # TSIM: sql alter table $mt drop tag tgcol2
        tdLog.info('alter table %s drop tag tgcol2' % (mt))
        tdSql.execute('alter table %s drop tag tgcol2' % (mt))
        # TSIM: sql alter table $mt drop tag tgcol3
        tdLog.info('alter table %s drop tag tgcol3' % (mt))
        tdSql.execute('alter table %s drop tag tgcol3' % (mt))
        # TSIM: sql alter table $mt drop tag tgcol4
        tdLog.info('alter table %s drop tag tgcol4' % (mt))
        tdSql.execute('alter table %s drop tag tgcol4' % (mt))
        # TSIM: sql alter table $mt drop tag tgcol5
        tdLog.info('alter table %s drop tag tgcol5' % (mt))
        tdSql.execute('alter table %s drop tag tgcol5' % (mt))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM: sql alter table $mt add tag tgcol4 binary(10)
        tdLog.info('alter table %s add tag tgcol4 binary(10)' % (mt))
        tdSql.execute('alter table %s add tag tgcol4 binary(10)' % (mt))
        # TSIM: sql alter table $mt add tag tgcol5 int
        tdLog.info('alter table %s add tag tgcol5 int' % (mt))
        tdSql.execute('alter table %s add tag tgcol5 int' % (mt))
        # TSIM: sql alter table $mt add tag tgcol6 binary(10)
        tdLog.info('alter table %s add tag tgcol6 binary(10)' % (mt))
        tdSql.execute('alter table %s add tag tgcol6 binary(10)' % (mt))
        # TSIM: sql alter table $mt add tag tgcol7 bigint
        tdLog.info('alter table %s add tag tgcol7 bigint' % (mt))
        tdSql.execute('alter table %s add tag tgcol7 bigint' % (mt))
        # TSIM: sql alter table $mt add tag tgcol8 smallint
        tdLog.info('alter table %s add tag tgcol8 smallint' % (mt))
        tdSql.execute('alter table %s add tag tgcol8 smallint' % (mt))
        # TSIM:
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM: sql alter table $tb set tag tgcol4=4
        tdLog.info('alter table %s set tag tgcol4=4' % (tb))
        tdSql.execute('alter table %s set tag tgcol4=4' % (tb))
        # TSIM: sql alter table $tb set tag tgcol5=5
        tdLog.info('alter table %s set tag tgcol5=5' % (tb))
        tdSql.execute('alter table %s set tag tgcol5=5' % (tb))
        # TSIM: sql alter table $tb set tag tgcol6=6
        tdLog.info('alter table %s set tag tgcol6=6' % (tb))
        tdSql.execute('alter table %s set tag tgcol6=6' % (tb))
        # TSIM: sql alter table $tb set tag tgcol7=7
        tdLog.info('alter table %s set tag tgcol7=7' % (tb))
        tdSql.execute('alter table %s set tag tgcol7=7' % (tb))
        # TSIM: sql alter table $tb set tag tgcol8=8
        tdLog.info('alter table %s set tag tgcol8=8' % (tb))
        tdSql.execute('alter table %s set tag tgcol8=8' % (tb))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM:
        # TSIM: sql select * from $mt where tgcol5 =5
        tdLog.info('select * from %s where tgcol5 =5' % (mt))
        tdSql.query('select * from %s where tgcol5 =5' % (mt))
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
        # TSIM: if $data02 != 1 then
        tdLog.info('tdSql.checkData(0, 2, 1)')
        tdSql.checkData(0, 2, 1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 4 then
        tdLog.info('tdSql.checkData(0, 3, 4)')
        tdSql.checkData(0, 3, "4")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data04 != 5 then
        tdLog.info('tdSql.checkData(0, 4, 5)')
        tdSql.checkData(0, 4, 5)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data05 != 6 then
        tdLog.info('tdSql.checkData(0, 5, 6)')
        tdSql.checkData(0, 5, "6")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data06 != 7 then
        tdLog.info('tdSql.checkData(0, 6, 7)')
        tdSql.checkData(0, 6, 7)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data07 != 8 then
        tdLog.info('tdSql.checkData(0, 7, 8)')
        tdSql.checkData(0, 7, 8)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol2 = 1 -x step111
        tdLog.info('select * from %s where tgcol2 = 1 -x step111' % (mt))
        tdSql.error('select * from %s where tgcol2 = 111' % (mt))
        # TSIM: return -1
        # TSIM: step111:
        # TSIM: sql select * from $mt where tgcol3 = 1 -x step112
        tdLog.info('select * from %s where tgcol3 = 1 -x step112' % (mt))
        tdSql.error('select * from %s where tgcol3 = 112' % (mt))
        # TSIM: return -1
        # TSIM: step112:
        # TSIM: sql select * from $mt where tgcol9 = 1 -x step113
        tdLog.info('select * from %s where tgcol9 = 1 -x step113' % (mt))
        tdSql.error('select * from %s where tgcol9 = 113' % (mt))
        # TSIM: return -1
        # TSIM: step113:
        # TSIM:
        # TSIM: print =============== step12
        tdLog.info('=============== step12')
        # TSIM: $i = 12
        i = 12
        # TSIM: $mt = $mtPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM: $tb = $tbPrefix . $i
        tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol1
        # bool, tgcol2 smallint, tgcol3 float, tgcol4 double, tgcol5
        # binary(10), tgcol6 binary(20))
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 smallint, tgcol3 float, tgcol4 double, tgcol5 binary(10), tgcol6 binary(20))' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 smallint, tgcol3 float, tgcol4 double, tgcol5 binary(10), tgcol6 binary(20))' %
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
        # TSIM: if $data04 != 3.00000 then
        tdLog.info('tdSql.checkData(0, 4, 3.00000)')
        tdSql.checkData(0, 4, 3.00000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data05 != 4.000000000 then
        tdLog.info('tdSql.checkData(0, 5, 4.000000000)')
        tdSql.checkData(0, 5, 4.000000000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data06 != 5 then
        tdLog.info('tdSql.checkData(0, 6, 5)')
        tdSql.checkData(0, 6, "5")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data07 != 6 then
        tdLog.info('tdSql.checkData(0, 7, 6)')
        tdSql.checkData(0, 7, "6")
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql alter table $mt drop tag tgcol2
        tdLog.info('alter table %s drop tag tgcol2' % (mt))
        tdSql.execute('alter table %s drop tag tgcol2' % (mt))
        # TSIM: sql alter table $mt drop tag tgcol3
        tdLog.info('alter table %s drop tag tgcol3' % (mt))
        tdSql.execute('alter table %s drop tag tgcol3' % (mt))
        # TSIM: sql alter table $mt drop tag tgcol4
        tdLog.info('alter table %s drop tag tgcol4' % (mt))
        tdSql.execute('alter table %s drop tag tgcol4' % (mt))
        # TSIM: sql alter table $mt drop tag tgcol5
        tdLog.info('alter table %s drop tag tgcol5' % (mt))
        tdSql.execute('alter table %s drop tag tgcol5' % (mt))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM: sql alter table $mt add tag tgcol2 binary(10)
        tdLog.info('alter table %s add tag tgcol2 binary(10)' % (mt))
        tdSql.execute('alter table %s add tag tgcol2 binary(10)' % (mt))
        # TSIM: sql alter table $mt add tag tgcol3 int
        tdLog.info('alter table %s add tag tgcol3 int' % (mt))
        tdSql.execute('alter table %s add tag tgcol3 int' % (mt))
        # TSIM: sql alter table $mt add tag tgcol4 binary(10)
        tdLog.info('alter table %s add tag tgcol4 binary(10)' % (mt))
        tdSql.execute('alter table %s add tag tgcol4 binary(10)' % (mt))
        # TSIM: sql alter table $mt add tag tgcol5 bigint
        tdLog.info('alter table %s add tag tgcol5 bigint' % (mt))
        tdSql.execute('alter table %s add tag tgcol5 bigint' % (mt))
        # TSIM:
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM: sql alter table $tb set tag tgcol1=false
        tdLog.info('alter table %s set tag tgcol1=false' % (tb))
        tdSql.execute('alter table %s set tag tgcol1=false' % (tb))
        # TSIM: sql alter table $tb set tag tgcol2=5
        tdLog.info('alter table %s set tag tgcol2=5' % (tb))
        tdSql.execute('alter table %s set tag tgcol2=5' % (tb))
        # TSIM: sql alter table $tb set tag tgcol3=4
        tdLog.info('alter table %s set tag tgcol3=4' % (tb))
        tdSql.execute('alter table %s set tag tgcol3=4' % (tb))
        # TSIM: sql alter table $tb set tag tgcol4=3
        tdLog.info('alter table %s set tag tgcol4=3' % (tb))
        tdSql.execute('alter table %s set tag tgcol4=3' % (tb))
        # TSIM: sql alter table $tb set tag tgcol5=2
        tdLog.info('alter table %s set tag tgcol5=2' % (tb))
        tdSql.execute('alter table %s set tag tgcol5=2' % (tb))
        # TSIM: sql alter table $tb set tag tgcol6=1
        tdLog.info('alter table %s set tag tgcol6=1' % (tb))
        tdSql.execute('alter table %s set tag tgcol6=1' % (tb))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM:
        # TSIM: sql select * from $mt where tgcol4 = '3'
        tdLog.info('select * from %s where tgcol4 = "3"' % (mt))
        tdSql.query('select * from %s where tgcol4 = "3"' % (mt))
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
        # TSIM: if $data03 != 1 then
        tdLog.info('tdSql.checkData(0, 3, 1)')
        tdSql.checkData(0, 3, "1")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data04 != 5 then
        tdLog.info('tdSql.checkData(0, 4, 5)')
        tdSql.checkData(0, 4, "5")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data05 != 4 then
        tdLog.info('tdSql.checkData(0, 5, 4)')
        tdSql.checkData(0, 5, 4)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data06 != 3 then
        tdLog.info('tdSql.checkData(0, 6, 3)')
        tdSql.checkData(0, 6, "3")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data07 != 2 then
        tdLog.info('tdSql.checkData(0, 7, 2)')
        tdSql.checkData(0, 7, 2)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol2 = '5'
        tdLog.info('select * from %s where tgcol2 = "5"' % (mt))
        tdSql.query('select * from %s where tgcol2 = "5"' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol3 = 4
        tdLog.info('select * from %s where tgcol3 = 4' % (mt))
        tdSql.query('select * from %s where tgcol3 = 4' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol5 = 2
        tdLog.info('select * from %s where tgcol5 = 2' % (mt))
        tdSql.query('select * from %s where tgcol5 = 2' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol6 = '1'
        tdLog.info('select * from %s where tgcol6 = "1"' % (mt))
        tdSql.query('select * from %s where tgcol6 = "1"' % (mt))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
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
        tdLog.info('tdSql.checkData(0, 2, 1)')
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
        tdLog.info('tdSql.checkData(0, 5, 4)')
        tdSql.checkData(0, 5, "4")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data06 != 5.000000000 then
        tdLog.info('tdSql.checkData(0, 6, 5.000000000)')
        tdSql.checkData(0, 6, 5.000000000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data07 != 6 then
        tdLog.info('tdSql.checkData(0, 7, 6)')
        tdSql.checkData(0, 7, "6")
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql alter table $mt drop tag tgcol2
        tdLog.info('alter table %s drop tag tgcol2' % (mt))
        tdSql.execute('alter table %s drop tag tgcol2' % (mt))
        # TSIM: sql alter table $mt drop tag tgcol4
        tdLog.info('alter table %s drop tag tgcol4' % (mt))
        tdSql.execute('alter table %s drop tag tgcol4' % (mt))
        # TSIM: sql alter table $mt drop tag tgcol6
        tdLog.info('alter table %s drop tag tgcol6' % (mt))
        tdSql.execute('alter table %s drop tag tgcol6' % (mt))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM: sql alter table $mt add tag tgcol2 binary(10)
        tdLog.info('alter table %s add tag tgcol2 binary(10)' % (mt))
        tdSql.execute('alter table %s add tag tgcol2 binary(10)' % (mt))
        # TSIM: sql alter table $mt add tag tgcol4 int
        tdLog.info('alter table %s add tag tgcol4 int' % (mt))
        tdSql.execute('alter table %s add tag tgcol4 int' % (mt))
        # TSIM: sql alter table $mt add tag tgcol6 bigint
        tdLog.info('alter table %s add tag tgcol6 bigint' % (mt))
        tdSql.execute('alter table %s add tag tgcol6 bigint' % (mt))
        # TSIM:
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM: sql alter table $tb set tag tgcol1=7
        tdLog.info('alter table %s set tag tgcol1=7' % (tb))
        tdSql.execute('alter table %s set tag tgcol1=7' % (tb))
        # TSIM: sql alter table $tb set tag tgcol2=8
        tdLog.info('alter table %s set tag tgcol2=8' % (tb))
        tdSql.execute('alter table %s set tag tgcol2=8' % (tb))
        # TSIM: sql alter table $tb set tag tgcol3=9
        tdLog.info('alter table %s set tag tgcol3=9' % (tb))
        tdSql.execute('alter table %s set tag tgcol3=9' % (tb))
        # TSIM: sql alter table $tb set tag tgcol4=10
        tdLog.info('alter table %s set tag tgcol4=10' % (tb))
        tdSql.execute('alter table %s set tag tgcol4=10' % (tb))
        # TSIM: sql alter table $tb set tag tgcol5=11
        tdLog.info('alter table %s set tag tgcol5=11' % (tb))
        tdSql.execute('alter table %s set tag tgcol5=11' % (tb))
        # TSIM: sql alter table $tb set tag tgcol6=12
        tdLog.info('alter table %s set tag tgcol6=12' % (tb))
        tdSql.execute('alter table %s set tag tgcol6=12' % (tb))
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM:
        # TSIM: sql select * from $mt where tgcol2 = '8'
        tdLog.info('select * from %s where tgcol2 = "8"' % (mt))
        tdSql.query('select * from %s where tgcol2 = "8"' % (mt))
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
        tdLog.info('tdSql.checkData(0, 2, 7)')
        tdSql.checkData(0, 2, "7")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 9 then
        tdLog.info('tdSql.checkData(0, 3, 9)')
        tdSql.checkData(0, 3, 9)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data04 != 11.000000000 then
        tdLog.info('tdSql.checkData(0, 4, 11.000000000)')
        tdSql.checkData(0, 4, 11.000000000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data05 != 8 then
        tdLog.info('tdSql.checkData(0, 5, 8)')
        tdSql.checkData(0, 5, "8")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data06 != 10 then
        tdLog.info('tdSql.checkData(0, 6, 10)')
        tdSql.checkData(0, 6, 10)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data07 != 12 then
        tdLog.info('tdSql.checkData(0, 7, 12)')
        tdSql.checkData(0, 7, 12)
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
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol1
        # bool, tgcol2 bigint)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 bigint)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 bigint)' %
            (mt))
        # TSIM: sql create table $tb using $mt tags( 1, 1 )
        tdLog.info('create table %s using %s tags( 1, 1 )' % (tb, mt))
        tdSql.execute('create table %s using %s tags( 1, 1 )' % (tb, mt))
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info('insert into %s values(now, 1)' % (tb))
        tdSql.execute('insert into %s values(now, 1)' % (tb))
        # TSIM:
        # TSIM: sql alter table $mt add tag tgcol3 binary(10)
        tdLog.info('alter table %s add tag tgcol3 binary(10)' % (mt))
        tdSql.execute('alter table %s add tag tgcol3 binary(10)' % (mt))
        # TSIM: sql alter table $mt add tag tgcol4 int
        tdLog.info('alter table %s add tag tgcol4 int' % (mt))
        tdSql.execute('alter table %s add tag tgcol4 int' % (mt))
        # TSIM: sql alter table $mt add tag tgcol5 bigint
        tdLog.info('alter table %s add tag tgcol5 bigint' % (mt))
        tdSql.execute('alter table %s add tag tgcol5 bigint' % (mt))
        # TSIM: sql alter table $mt add tag tgcol6 bigint
        tdLog.info('alter table %s add tag tgcol6 bigint' % (mt))
        tdSql.execute('alter table %s add tag tgcol6 bigint' % (mt))
        # TSIM:
        # TSIM: return
        # TSIM: sql alter table $mt add tag tgcol7 bigint -x step141
        tdLog.info('alter table %s add tag tgcol7 bigint -x step141' % (mt))
        tdSql.error('alter table %s add tag tgcol7 bigint41' % (mt))
        # TSIM: return -1
        # TSIM: step141:
        # TSIM: sql reset query cache
        tdLog.info('reset query cache')
        tdSql.execute('reset query cache')
        # TSIM: sql alter table $mt drop tag tgcol6
        tdLog.info('alter table %s drop tag tgcol6' % (mt))
        tdSql.execute('alter table %s drop tag tgcol6' % (mt))
        # TSIM: sql alter table $mt add tag tgcol7 bigint
        tdLog.info('alter table %s add tag tgcol7 bigint' % (mt))
        tdSql.execute('alter table %s add tag tgcol7 bigint' % (mt))
        # TSIM: sql alter table $mt add tag tgcol8 bigint -x step142
        tdLog.info('alter table %s add tag tgcol8 bigint -x step142' % (mt))
        tdSql.error('alter table %s add tag tgcol8 bigint42' % (mt))
        # TSIM: return -1
        # TSIM: step142:
        # TSIM:
        # TSIM: print =============== clear
        tdLog.info('=============== clear')
        # TSIM: sql drop database $db
        tdLog.info('sql drop database db')
        tdSql.execute('drop database db')
        # TSIM: sql show databases
        tdLog.info('show databases')
        tdSql.query('show databases')
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
