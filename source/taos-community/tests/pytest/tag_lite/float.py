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
        # TSIM: $dbPrefix = ta_fl_db
        # TSIM: $tbPrefix = ta_fl_tb
        tbPrefix = "ta_fl_tb"
        # TSIM: $mtPrefix = ta_fl_mt
        mtPrefix = "ta_fl_mt"
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
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol
        # float)
        tdLog.info(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol float)' %
            (mt))
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol float)' %
            (mt))
        # TSIM:
        # TSIM: $i = 0
        i = 0
        # TSIM: while $i < 5
        while (i < 5):
            # TSIM: $tb = $tbPrefix . $i
            tb = "%s%d" % (tbPrefix, i)
            # TSIM: sql create table $tb using $mt tags( 0 )
            tdLog.info('create table %s using %s tags( 0 )' % (tb, mt))
            tdSql.execute('create table %s using %s tags( 0 )' % (tb, mt))
            # TSIM: $x = 0
            x = 0
            # TSIM: while $x < $rowNum
            while (x < rowNum):
                # TSIM: $ms = $x . m
                ms = x * 60000
                tdLog.info(
                    "insert into %s values (%d, %d)" %
                    (tb, 1605045600000 + ms, x))
                tdSql.execute(
                    "insert into %s values (%d, %d)" %
                    (tb, 1605045600000 + ms, x))
                # TSIM: $x = $x + 1
                x = x + 1
                # TSIM: endw
            # TSIM: $i = $i + 1
            i = i + 1
            # TSIM: endw
        # TSIM: while $i < 10
        while (i < 10):
            # TSIM: $tb = $tbPrefix . $i
            tb = "%s%d" % (tbPrefix, i)
            # TSIM: sql create table $tb using $mt tags( 1 )
            tdLog.info('create table %s using %s tags( 1 )' % (tb, mt))
            tdSql.execute('create table %s using %s tags( 1 )' % (tb, mt))
            # TSIM: $x = 0
            x = 0
            # TSIM: while $x < $rowNum
            while (x < rowNum):
                # TSIM: $ms = $x . m
                ms = x * 60000
                tdLog.info(
                    "insert into %s values (%d, %d)" %
                    (tb, 1605045600000 + ms, x))
                tdSql.execute(
                    "insert into %s values (%d, %d)" %
                    (tb, 1605045600000 + ms, x))
                # TSIM: $x = $x + 1
                x = x + 1
                # TSIM: endw
            # TSIM: $i = $i + 1
            i = i + 1
            # TSIM: endw
        # TSIM:
        # TSIM: print =============== step2
        tdLog.info('=============== step2')
        # TSIM: sleep 100
        # TSIM: sql select * from $tb
        tdLog.info('select * from %s' % (tb))
        tdSql.query('select * from %s' % (tb))
        # TSIM: if $rows != $rowNum then
        tdLog.info('tdSql.checkRow($rowNum)')
        tdSql.checkRows(rowNum)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $tb where ts < 1605045600000 + 240001
        tdLog.info('select * from %s where ts < 1605045600000 + 240001' % (tb))
        tdSql.query('select * from %s where ts < 1605045600000 + 240001' % (tb))
        # TSIM: if $rows != 5 then
        tdLog.info('tdSql.checkRow(5)')
        tdSql.checkRows(5)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $tb where ts <= 1605045600000 + 240001
        tdLog.info('select * from %s where ts <= 1605045600000 + 240001' % (tb))
        tdSql.query('select * from %s where ts <= 1605045600000 + 240001' % (tb))
        # TSIM: if $rows != 5 then
        tdLog.info('tdSql.checkRow(5)')
        tdSql.checkRows(5)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $tb where ts > 1605045600000 + 240001
        tdLog.info('select * from %s where ts > 1605045600000 + 240001' % (tb))
        tdSql.query('select * from %s where ts > 1605045600000 + 240001' % (tb))
        # TSIM: if $rows != 15 then
        tdLog.info('tdSql.checkRow(15)')
        tdSql.checkRows(15)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $tb where ts >= 1605045600000 + 240001
        tdLog.info('select * from %s where ts >= 1605045600000 + 240001' % (tb))
        tdSql.query('select * from %s where ts >= 1605045600000 + 240001' % (tb))
        # TSIM: if $rows != 15 then
        tdLog.info('tdSql.checkRow(15)')
        tdSql.checkRows(15)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $tb where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001' %
            (tb))
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001' %
            (tb))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $tb where ts < 1605045600000 + 240001 and ts > 1605045600000 + 300001
        tdLog.info(
            'select * from %s where ts < 1605045600000 + 240001 and ts > 1605045600000 + 300001' %
            (tb))
        tdSql.query(
            'select * from %s where ts < 1605045600000 + 240001 and ts > 1605045600000 + 300001' %
            (tb))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $tb where ts > 100000 and ts < 100000
        tdLog.info('select * from %s where ts > 100000 and ts < 100000' % (tb))
        tdSql.query(
            'select * from %s where ts > 100000 and ts < 100000' %
            (tb))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $tb where ts > 1605045600000 + 240001 and ts < 1605045600000 + 180001
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 180001' %
            (tb))
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 180001' %
            (tb))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $tb where ts > 1605045600000 + 240001 and ts > 1605045600000 + 300001 and
        # ts < 1605045600000 + 360001
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and ts > 1605045600000 + 300001 and ts < 1605045600000 + 360001' %
            (tb))
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and ts > 1605045600000 + 300001 and ts < 1605045600000 + 360001' %
            (tb))
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step3
        tdLog.info('=============== step3')
        # TSIM: sql select * from $mt
        tdLog.info('select * from %s' % (mt))
        tdSql.query('select * from %s' % (mt))
        # TSIM: if $rows != $totalNum then
        tdLog.info('tdSql.checkRow($totalNum)')
        tdSql.checkRows(totalNum)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where ts < 1605045600000 + 240001
        tdLog.info('select * from %s where ts < 1605045600000 + 240001' % (mt))
        tdSql.query('select * from %s where ts < 1605045600000 + 240001' % (mt))
        # TSIM: if $rows != 50 then
        tdLog.info('tdSql.checkRow(50)')
        tdSql.checkRows(50)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where ts > 1605045600000 + 240001
        tdLog.info('select * from %s where ts > 1605045600000 + 240001' % (mt))
        tdSql.query('select * from %s where ts > 1605045600000 + 240001' % (mt))
        # TSIM: if $rows != 150 then
        tdLog.info('tdSql.checkRow(150)')
        tdSql.checkRows(150)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where ts = 1605045600000 + 240001
        tdLog.info('select * from %s where ts = 1605045600000 + 240001' % (mt))
        tdSql.query('select * from %s where ts = 1605045600000 + 240001' % (mt))
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001' %
            (mt))
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001' %
            (mt))
        # TSIM: if $rows != 10 then
        tdLog.info('tdSql.checkRow(10)')
        tdSql.checkRows(10)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step4
        tdLog.info('=============== step4')
        # TSIM: sql select * from $mt where tgcol = 0
        tdLog.info('select * from %s where tgcol = 0' % (mt))
        tdSql.query('select * from %s where tgcol = 0' % (mt))
        # TSIM: if $rows != 100 then
        tdLog.info('tdSql.checkRow(100)')
        tdSql.checkRows(100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol <> 0
        tdLog.info('select * from %s where tgcol <> 0' % (mt))
        tdSql.query('select * from %s where tgcol <> 0' % (mt))
        # TSIM: if $rows != 100 then
        tdLog.info('tdSql.checkRow(100)')
        tdSql.checkRows(100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol = 1
        tdLog.info('select * from %s where tgcol = 1' % (mt))
        tdSql.query('select * from %s where tgcol = 1' % (mt))
        # TSIM: if $rows != 100 then
        tdLog.info('tdSql.checkRow(100)')
        tdSql.checkRows(100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol <> 1
        tdLog.info('select * from %s where tgcol <> 1' % (mt))
        tdSql.query('select * from %s where tgcol <> 1' % (mt))
        # TSIM: if $rows != 100 then
        tdLog.info('tdSql.checkRow(100)')
        tdSql.checkRows(100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol = 1
        tdLog.info('select * from %s where tgcol = 1' % (mt))
        tdSql.query('select * from %s where tgcol = 1' % (mt))
        # TSIM: if $rows != 100 then
        tdLog.info('tdSql.checkRow(100)')
        tdSql.checkRows(100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol <> 1
        tdLog.info('select * from %s where tgcol <> 1' % (mt))
        tdSql.query('select * from %s where tgcol <> 1' % (mt))
        # TSIM: if $rows != 100 then
        tdLog.info('tdSql.checkRow(100)')
        tdSql.checkRows(100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol = 0
        tdLog.info('select * from %s where tgcol = 0' % (mt))
        tdSql.query('select * from %s where tgcol = 0' % (mt))
        # TSIM: if $rows != 100 then
        tdLog.info('tdSql.checkRow(100)')
        tdSql.checkRows(100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where tgcol <> 0
        tdLog.info('select * from %s where tgcol <> 0' % (mt))
        tdSql.query('select * from %s where tgcol <> 0' % (mt))
        # TSIM: if $rows != 100 then
        tdLog.info('tdSql.checkRow(100)')
        tdSql.checkRows(100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step5
        tdLog.info('=============== step5')
        # TSIM: sql select * from $mt where ts > 1605045600000 + 240001 and tgcol = 1
        tdLog.info('select * from %s where ts > 1605045600000 + 240001 and tgcol = 1' % (mt))
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol = 1' %
            (mt))
        # TSIM: if $rows != 75 then
        tdLog.info('tdSql.checkRow(75)')
        tdSql.checkRows(75)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where ts > 1605045600000 + 240001 and tgcol <> 1
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol <> 1' %
            (mt))
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol <> 1' %
            (mt))
        # TSIM: if $rows != 75 then
        tdLog.info('tdSql.checkRow(75)')
        tdSql.checkRows(75)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where ts < 1605045600000 + 240001 and tgcol = 0
        tdLog.info('select * from %s where ts < 1605045600000 + 240001 and tgcol = 0' % (mt))
        tdSql.query(
            'select * from %s where ts < 1605045600000 + 240001 and tgcol = 0' %
            (mt))
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where ts < 1605045600000 + 240001 and tgcol <> 0
        tdLog.info(
            'select * from %s where ts < 1605045600000 + 240001 and tgcol <> 0' %
            (mt))
        tdSql.query(
            'select * from %s where ts < 1605045600000 + 240001 and tgcol <> 0' %
            (mt))
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where ts <= 1605045600000 + 240001 and tgcol = 0
        tdLog.info(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol = 0' %
            (mt))
        tdSql.query(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol = 0' %
            (mt))
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where ts <= 1605045600000 + 240001 and tgcol <> 0
        tdLog.info(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol <> 0' %
            (mt))
        tdSql.query(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol <> 0' %
            (mt))
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001 and
        # tgcol <> 0
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001 and tgcol <> 0' %
            (mt))
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001 and tgcol <> 0' %
            (mt))
        # TSIM: if $rows != 5 then
        tdLog.info('tdSql.checkRow(5)')
        tdSql.checkRows(5)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from $mt where ts > 1605045600000 + 240001 and tgcol <> 0 and ts
        # < 1605045600000 + 300001
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol <> 0 and ts < 1605045600000 + 300001' %
            (mt))
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol <> 0 and ts < 1605045600000 + 300001' %
            (mt))
        # TSIM: if $rows != 5 then
        tdLog.info('tdSql.checkRow(5)')
        tdSql.checkRows(5)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step6
        tdLog.info('=============== step6')
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from $mt
        tdLog.info(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s' %
            (mt))
        tdSql.query(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s' %
            (mt))
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data00 != 200 then
        tdLog.info('tdSql.checkData(0, 0, 200)')
        tdSql.checkData(0, 0, 200)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step7
        tdLog.info('=============== step7')
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from $mt where tgcol = 1
        tdLog.info(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tgcol = 1' %
            (mt))
        tdSql.query(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tgcol = 1' %
            (mt))
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data00 != 100 then
        tdLog.info('tdSql.checkData(0, 0, 100)')
        tdSql.checkData(0, 0, 100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step8
        tdLog.info('=============== step8')
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from $mt where ts < 1605045600000 + 240001
        tdLog.info(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001' %
            (mt))
        tdSql.query(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001' %
            (mt))
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data00 != 50 then
        tdLog.info('tdSql.checkData(0, 0, 50)')
        tdSql.checkData(0, 0, 50)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step9
        tdLog.info('=============== step9')
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from $mt group by tgcol
        tdLog.info(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s group by tgcol' %
            (mt))
        tdSql.query(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s group by tgcol' %
            (mt))
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data00 != 100 then
        tdLog.info('tdSql.checkData(0, 0, 100)')
        tdSql.checkData(0, 0, 100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step10
        tdLog.info('=============== step10')
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from $mt where tgcol = 1 group
        # by tgcol
        tdLog.info(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tgcol = 1 group by tgcol' %
            (mt))
        tdSql.query(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tgcol = 1 group by tgcol' %
            (mt))
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data00 != 100 then
        tdLog.info('tdSql.checkData(0, 0, 100)')
        tdSql.checkData(0, 0, 100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step11
        tdLog.info('=============== step11')
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from $mt where ts < 1605045600000 + 240001
        # group by tgcol
        tdLog.info(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001 group by tgcol' %
            (mt))
        tdSql.query(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001 group by tgcol' %
            (mt))
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data00 != 25 then
        tdLog.info('tdSql.checkData(0, 0, 25)')
        tdSql.checkData(0, 0, 25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM:
        # TSIM: print =============== step12
        tdLog.info('=============== step12')
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from $mt interval(1d) group by
        # tgcol
        tdLog.info(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s interval(1d) group by tgcol' %
            (mt))
        tdSql.query(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s interval(1d) group by tgcol' %
            (mt))
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data01 != 100 then
        tdLog.info('tdSql.checkData(0, 1, 100)')
        tdSql.checkData(0, 1, 100)
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
