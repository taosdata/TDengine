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
        dbPrefix = "ta_3_db"
        tbPrefix = "ta_3_tb"
        mtPrefix = "ta_3_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200
        # TSIM:
        # TSIM: print =============== step1
        tdLog.info('=============== step1')
        i = 0
        db = "%s%d" % (dbPrefix, i)
        mt = "%s%d" % (mtPrefix, i)
        # TSIM:
        # TSIM: sql create database $db
        # TSIM: sql use $db
        # TSIM: sql create table %s (ts timestamp, tbcol int) TAGS(tgcol1 bool,
        # tgcol2 int, tgcol3 float)
        tdLog.info(
            "create table %s (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int, tgcol3 float)" %
            mt)
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int, tgcol3 float)' %
            mt)
        # TSIM:
        i = 0
        while (i < 5):
            tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $tb using %s tags( 0, 0, 0 )
            tdLog.info("create table %s using %s tags( 0, 0, 0 )" % (tb, mt))
            tdSql.execute(
                'create table %s using %s tags( 0, 0, 0 )' %
                (tb, mt))

            x = 0
            while (x < rowNum):
                ms = x * 60000
        # TSIM: sql insert into $tb values (1605045600000 + $ms , $x )
                tdLog.info(
                    "insert into %s values (%d, %d)" %
                    (tb, 1605045600000 + ms, x))
                tdSql.execute(
                    "insert into %s values (%d, %d)" %
                    (tb, 1605045600000 + ms, x))
                x = x + 1
        # TSIM: endw
            i = i + 1
        # TSIM: endw
        while (i < 10):
            tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $tb using %s tags( 1, 1, 1 )
            tdLog.info("create table %s using %s tags( 1, 1, 1 )" % (tb, mt))
            tdSql.execute(
                'create table %s using %s tags( 1, 1, 1 )' %
                (tb, mt))
            x = 0
            while (x < rowNum):
                ms = x * 60000
                tdLog.info(
                    "insert into %s values (%d, %d)" %
                    (tb, 1605045600000 + ms, x))
                tdSql.execute(
                    "insert into %s values (%d, %d)" %
                    (tb, 1605045600000 + ms, x))
                x = x + 1
        # TSIM: endw
            i = i + 1
        # TSIM: endw
        # TSIM:
        # TSIM: print =============== step2
        tdLog.info('=============== step2')
        # TSIM: sql select * from %s
        tdLog.info('select * from %s' % mt)
        tdSql.query('select * from %s' % mt)
        # TSIM: if $rows != $totalNum then
        tdLog.info('tdSql.checkRow(%d)' % totalNum)
        tdSql.checkRows(totalNum)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from %s where ts < 1605045600000 + 240001
        tdLog.info('select * from %s where ts < 1605045600000 + 240001' % mt)
        tdSql.query('select * from %s where ts < 1605045600000 + 240001' % mt)
        # TSIM: if $rows != 50 then
        tdLog.info('tdSql.checkRow(50)')
        tdSql.checkRows(50)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001
        tdLog.info('select * from %s where ts > 1605045600000 + 240001' % mt)
        tdSql.query('select * from %s where ts > 1605045600000 + 240001' % mt)
        # TSIM: if $rows != 150 then
        tdLog.info('tdSql.checkRow(150)')
        tdSql.checkRows(150)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts = 1605045600000 + 240001
        tdLog.info('select * from %s where ts = 1605045600000 + 240001' % mt)
        tdSql.query('select * from %s where ts = 1605045600000 + 240001' % mt)
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001' %
            mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001' %
            mt)
        tdLog.info('tdSql.checkRow(10)')
        tdSql.checkRows(10)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step3
        tdLog.info('=============== step3')
        # TSIM: sql select * from %s where tgcol1 = 0
        tdLog.info('select * from %s where tgcol1 = 0' % mt)
        tdSql.query('select * from %s where tgcol1 = 0' % mt)
        # TSIM: if $rows != 100 then
        tdLog.info('tdSql.checkRow(100)')
        tdSql.checkRows(100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where tgcol1 <> 0
        tdLog.info('select * from %s where tgcol1 <> 0' % mt)
        tdSql.query('select * from %s where tgcol1 <> 0' % mt)
        tdLog.info('tdSql.checkRow(100)')
        tdSql.checkRows(100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where tgcol1 = 1
        tdLog.info('select * from %s where tgcol1 = 1' % mt)
        tdSql.query('select * from %s where tgcol1 = 1' % mt)
        # TSIM: if $rows != 100 then
        tdLog.info('tdSql.checkRow(100)')
        tdSql.checkRows(100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where tgcol1 <> 1
        tdLog.info('select * from %s where tgcol1 <> 1' % mt)
        tdSql.query('select * from %s where tgcol1 <> 1' % mt)
        tdLog.info('tdSql.checkRow(100)')
        tdSql.checkRows(100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where tgcol1 = true
        tdLog.info('select * from %s where tgcol1 = true' % mt)
        tdSql.query('select * from %s where tgcol1 = true' % mt)
        tdLog.info('tdSql.checkRow(100)')
        tdSql.checkRows(100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where tgcol1 <> true
        tdLog.info('select * from %s where tgcol1 <> true' % mt)
        tdSql.query('select * from %s where tgcol1 <> true' % mt)
        tdLog.info('tdSql.checkRow(100)')
        tdSql.checkRows(100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where tgcol1 = false
        tdLog.info('select * from %s where tgcol1 = false' % mt)
        tdSql.query('select * from %s where tgcol1 = false' % mt)
        tdLog.info('tdSql.checkRow(100)')
        tdSql.checkRows(100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where tgcol1 <> false
        tdLog.info('select * from %s where tgcol1 <> false' % mt)
        tdSql.query('select * from %s where tgcol1 <> false' % mt)
        # TSIM: if $rows != 100 then
        tdLog.info('tdSql.checkRow(100)')
        tdSql.checkRows(100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step4
        tdLog.info('=============== step4')
        # TSIM: sql select * from %s where tgcol2 = 0
        tdLog.info('select * from %s where tgcol2 = 0' % mt)
        tdSql.query('select * from %s where tgcol2 = 0' % mt)
        tdLog.info('tdSql.checkRow(100)')
        tdSql.checkRows(100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where tgcol2 <> 0
        tdLog.info('select * from %s where tgcol2 <> 0' % mt)
        tdSql.query('select * from %s where tgcol2 <> 0' % mt)
        tdLog.info('tdSql.checkRow(100)')
        tdSql.checkRows(100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where tgcol2 = 1
        tdLog.info('select * from %s where tgcol2 = 1' % mt)
        tdSql.query('select * from %s where tgcol2 = 1' % mt)
        tdLog.info('tdSql.checkRow(100)')
        tdSql.checkRows(100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where tgcol2 <> 1
        tdLog.info('select * from %s where tgcol2 <> 1' % mt)
        tdSql.query('select * from %s where tgcol2 <> 1' % mt)
        tdLog.info('tdSql.checkRow(100)')
        tdSql.checkRows(100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step5
        tdLog.info('=============== step5')
        # TSIM: sql select * from %s where tgcol3 = 0
        tdLog.info('select * from %s where tgcol3 = 0' % mt)
        tdSql.query('select * from %s where tgcol3 = 0' % mt)
        # TSIM: if $rows != 100 then
        tdLog.info('tdSql.checkRow(100)')
        tdSql.checkRows(100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where tgcol3 <> 0
        tdLog.info('select * from %s where tgcol3 <> 0' % mt)
        tdSql.query('select * from %s where tgcol3 <> 0' % mt)
        tdLog.info('tdSql.checkRow(100)')
        tdSql.checkRows(100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where tgcol3 = 1
        tdLog.info('select * from %s where tgcol3 = 1' % mt)
        tdSql.query('select * from %s where tgcol3 = 1' % mt)
        tdLog.info('tdSql.checkRow(100)')
        tdSql.checkRows(100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where tgcol3 <> 1
        tdLog.info('select * from %s where tgcol3 <> 1' % mt)
        tdSql.query('select * from %s where tgcol3 <> 1' % mt)
        tdLog.info('tdSql.checkRow(100)')
        tdSql.checkRows(100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step6
        tdLog.info('=============== step6')
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and tgcol1 = true
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol1 = true' %
            mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol1 = true' %
            mt)
        tdLog.info('tdSql.checkRow(75)')
        tdSql.checkRows(75)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and tgcol1 <> true
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol1 <> true' %
            mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol1 <> true' %
            mt)
        tdLog.info('tdSql.checkRow(75)')
        tdSql.checkRows(75)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts < 1605045600000 + 240001 and tgcol1 = false
        tdLog.info(
            'select * from %s where ts < 1605045600000 + 240001 and tgcol1 = false' %
            mt)
        tdSql.query(
            'select * from %s where ts < 1605045600000 + 240001 and tgcol1 = false' %
            mt)
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts < 1605045600000 + 240001 and tgcol1 <> false
        tdLog.info(
            'select * from %s where ts < 1605045600000 + 240001 and tgcol1 <> false' %
            mt)
        tdSql.query(
            'select * from %s where ts < 1605045600000 + 240001 and tgcol1 <> false' %
            mt)
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts <= 1605045600000 + 240001 and tgcol1 = false
        tdLog.info(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol1 = false' %
            mt)
        tdSql.query(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol1 = false' %
            mt)
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts <= 1605045600000 + 240001 and tgcol1 <> false
        tdLog.info(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol1 <> false' %
            mt)
        tdSql.query(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol1 <> false' %
            mt)
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001 and
        # tgcol1 <> false
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001 and tgcol1 <> false' %
            mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001 and tgcol1 <> false' %
            mt)
        tdLog.info('tdSql.checkRow(5)')
        tdSql.checkRows(5)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and tgcol1 <> false
        # and ts < 1605045600000 + 300001
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol1 <> false and ts < 1605045600000 + 300001' %
            mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol1 <> false and ts < 1605045600000 + 300001' %
            mt)
        # TSIM: if $rows != 5 then
        tdLog.info('tdSql.checkRow(5)')
        tdSql.checkRows(5)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step7
        tdLog.info('=============== step7')
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and tgcol2 = 1
        tdLog.info('select * from %s where ts > 1605045600000 + 240001 and tgcol2 = 1' % mt)
        tdSql.query('select * from %s where ts > 1605045600000 + 240001 and tgcol2 = 1' % mt)
        # TSIM: if $rows != 75 then
        tdLog.info('tdSql.checkRow(75)')
        tdSql.checkRows(75)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and tgcol2 <> 1
        tdLog.info('select * from %s where ts > 1605045600000 + 240001 and tgcol2 <> 1' % mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol2 <> 1' %
            mt)
        # TSIM: if $rows != 75 then
        tdLog.info('tdSql.checkRow(75)')
        tdSql.checkRows(75)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts < 1605045600000 + 240001 and tgcol2 = 0
        tdLog.info('select * from %s where ts < 1605045600000 + 240001 and tgcol2 = 0' % mt)
        tdSql.query('select * from %s where ts < 1605045600000 + 240001 and tgcol2 = 0' % mt)
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts < 1605045600000 + 240001 and tgcol2 <> 0
        tdLog.info('select * from %s where ts < 1605045600000 + 240001 and tgcol2 <> 0' % mt)
        tdSql.query(
            'select * from %s where ts < 1605045600000 + 240001 and tgcol2 <> 0' %
            mt)
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts <= 1605045600000 + 240001 and tgcol2 = 0
        tdLog.info('select * from %s where ts <= 1605045600000 + 240001 and tgcol2 = 0' % mt)
        tdSql.query(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol2 = 0' %
            mt)
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts <= 1605045600000 + 240001 and tgcol2 <> 0
        tdLog.info(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol2 <> 0' %
            mt)
        tdSql.query(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol2 <> 0' %
            mt)
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001 and
        # tgcol2 <> 0
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001 and tgcol2 <> 0' %
            mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001 and tgcol2 <> 0' %
            mt)
        # TSIM: if $rows != 5 then
        tdLog.info('tdSql.checkRow(5)')
        tdSql.checkRows(5)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and tgcol2 <> 0 and ts
        # < 1605045600000 + 300001
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol2 <> 0 and ts < 1605045600000 + 300001' %
            mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol2 <> 0 and ts < 1605045600000 + 300001' %
            mt)
        # TSIM: if $rows != 5 then
        tdLog.info('tdSql.checkRow(5)')
        tdSql.checkRows(5)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step8
        tdLog.info('=============== step8')
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and tgcol3 = 1
        tdLog.info('select * from %s where ts > 1605045600000 + 240001 and tgcol3 = 1' % mt)
        tdSql.query('select * from %s where ts > 1605045600000 + 240001 and tgcol3 = 1' % mt)
        # TSIM: if $rows != 75 then
        tdLog.info('tdSql.checkRow(75)')
        tdSql.checkRows(75)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and tgcol3 <> 1
        tdLog.info('select * from %s where ts > 1605045600000 + 240001 and tgcol3 <> 1' % mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol3 <> 1' %
            mt)
        # TSIM: if $rows != 75 then
        tdLog.info('tdSql.checkRow(75)')
        tdSql.checkRows(75)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts < 1605045600000 + 240001 and tgcol3 = 0
        tdLog.info('select * from %s where ts < 1605045600000 + 240001 and tgcol3 = 0' % mt)
        tdSql.query('select * from %s where ts < 1605045600000 + 240001 and tgcol3 = 0' % mt)
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts < 1605045600000 + 240001 and tgcol3 <> 0
        tdLog.info('select * from %s where ts < 1605045600000 + 240001 and tgcol3 <> 0' % mt)
        tdSql.query(
            'select * from %s where ts < 1605045600000 + 240001 and tgcol3 <> 0' %
            mt)
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts <= 1605045600000 + 240001 and tgcol3 = 0
        tdLog.info('select * from %s where ts <= 1605045600000 + 240001 and tgcol3 = 0' % mt)
        tdSql.query(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol3 = 0' %
            mt)
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts <= 1605045600000 + 240001 and tgcol3 <> 0
        tdLog.info(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol3 <> 0' %
            mt)
        tdSql.query(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol3 <> 0' %
            mt)
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001 and
        # tgcol3 <> 0
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001 and tgcol3 <> 0' %
            mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001 and tgcol3 <> 0' %
            mt)
        # TSIM: if $rows != 5 then
        tdLog.info('tdSql.checkRow(5)')
        tdSql.checkRows(5)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and tgcol3 <> 0 and ts
        # < 1605045600000 + 300001
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol3 <> 0 and ts < 1605045600000 + 300001' %
            mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol3 <> 0 and ts < 1605045600000 + 300001' %
            mt)
        # TSIM: if $rows != 5 then
        tdLog.info('tdSql.checkRow(5)')
        tdSql.checkRows(5)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step9
        tdLog.info('=============== step9')
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and tgcol2 = 1 and
        # tgcol1 = true
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol2 = 1 and tgcol1 = true' %
            mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol2 = 1 and tgcol1 = true' %
            mt)
        # TSIM: if $rows != 75 then
        tdLog.info('tdSql.checkRow(75)')
        tdSql.checkRows(75)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and tgcol2 <> 1 and
        # tgcol1 <> true
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol2 <> 1 and tgcol1 <> true' %
            mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol2 <> 1 and tgcol1 <> true' %
            mt)
        # TSIM: if $rows != 75 then
        tdLog.info('tdSql.checkRow(75)')
        tdSql.checkRows(75)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts < 1605045600000 + 240001 and tgcol2 = 0 and
        # tgcol1 = false
        tdLog.info(
            'select * from %s where ts < 1605045600000 + 240001 and tgcol2 = 0 and tgcol1 = false' %
            mt)
        tdSql.query(
            'select * from %s where ts < 1605045600000 + 240001 and tgcol2 = 0 and tgcol1 = false' %
            mt)
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts < 1605045600000 + 240001 and tgcol2 <> 0 and
        # tgcol1 <> false
        tdLog.info(
            'select * from %s where ts < 1605045600000 + 240001 and tgcol2 <> 0 and tgcol1 <> false' %
            mt)
        tdSql.query(
            'select * from %s where ts < 1605045600000 + 240001 and tgcol2 <> 0 and tgcol1 <> false' %
            mt)
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts <= 1605045600000 + 240001 and tgcol2 = 0 and
        # tgcol1 = false
        tdLog.info(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol2 = 0 and tgcol1 = false' %
            mt)
        tdSql.query(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol2 = 0 and tgcol1 = false' %
            mt)
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts <= 1605045600000 + 240001 and tgcol2 <> 0 and
        # tgcol1 <> false
        tdLog.info(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol2 <> 0 and tgcol1 <> false' %
            mt)
        tdSql.query(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol2 <> 0 and tgcol1 <> false' %
            mt)
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001 and
        # tgcol2 <> 0 and tgcol1 <> false
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001 and tgcol2 <> 0 and tgcol1 <> false' %
            mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001 and tgcol2 <> 0 and tgcol1 <> false' %
            mt)
        # TSIM: if $rows != 5 then
        tdLog.info('tdSql.checkRow(5)')
        tdSql.checkRows(5)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and tgcol2 <> 0 and ts
        # < 1605045600000 + 300001 and ts < 1605045600000 + 300001 and tgcol1 <> false
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol2 <> 0 and ts < 1605045600000 + 300001 and ts < 1605045600000 + 300001 and tgcol1 <> false' %
            mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol2 <> 0 and ts < 1605045600000 + 300001 and ts < 1605045600000 + 300001 and tgcol1 <> false' %
            mt)
        # TSIM: if $rows != 5 then
        tdLog.info('tdSql.checkRow(5)')
        tdSql.checkRows(5)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step10
        tdLog.info('=============== step10')
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and tgcol3 = 1 and
        # tgcol1 = true
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol3 = 1 and tgcol1 = true' %
            mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol3 = 1 and tgcol1 = true' %
            mt)
        # TSIM: if $rows != 75 then
        tdLog.info('tdSql.checkRow(75)')
        tdSql.checkRows(75)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and tgcol3 <> 1 and
        # tgcol1 <> true
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol3 <> 1 and tgcol1 <> true' %
            mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol3 <> 1 and tgcol1 <> true' %
            mt)
        # TSIM: if $rows != 75 then
        tdLog.info('tdSql.checkRow(75)')
        tdSql.checkRows(75)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts < 1605045600000 + 240001 and tgcol3 = 0 and
        # tgcol1 = false
        tdLog.info(
            'select * from %s where ts < 1605045600000 + 240001 and tgcol3 = 0 and tgcol1 = false' %
            mt)
        tdSql.query(
            'select * from %s where ts < 1605045600000 + 240001 and tgcol3 = 0 and tgcol1 = false' %
            mt)
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts < 1605045600000 + 240001 and tgcol3 <> 0 and
        # tgcol1 <> false
        tdLog.info(
            'select * from %s where ts < 1605045600000 + 240001 and tgcol3 <> 0 and tgcol1 <> false' %
            mt)
        tdSql.query(
            'select * from %s where ts < 1605045600000 + 240001 and tgcol3 <> 0 and tgcol1 <> false' %
            mt)
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts <= 1605045600000 + 240001 and tgcol3 = 0 and
        # tgcol1 = false
        tdLog.info(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol3 = 0 and tgcol1 = false' %
            mt)
        tdSql.query(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol3 = 0 and tgcol1 = false' %
            mt)
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts <= 1605045600000 + 240001 and tgcol3 <> 0 and
        # tgcol1 <> false
        tdLog.info(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol3 <> 0 and tgcol1 <> false' %
            mt)
        tdSql.query(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol3 <> 0 and tgcol1 <> false' %
            mt)
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001 and
        # tgcol3 <> 0 and tgcol1 <> false
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001 and tgcol3 <> 0 and tgcol1 <> false' %
            mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001 and tgcol3 <> 0 and tgcol1 <> false' %
            mt)
        # TSIM: if $rows != 5 then
        tdLog.info('tdSql.checkRow(5)')
        tdSql.checkRows(5)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and tgcol3 <> 0 and ts
        # < 1605045600000 + 300001 and ts < 1605045600000 + 300001 and tgcol1 <> false
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol3 <> 0 and ts < 1605045600000 + 300001 and ts < 1605045600000 + 300001 and tgcol1 <> false' %
            mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol3 <> 0 and ts < 1605045600000 + 300001 and ts < 1605045600000 + 300001 and tgcol1 <> false' %
            mt)
        # TSIM: if $rows != 5 then
        tdLog.info('tdSql.checkRow(5)')
        tdSql.checkRows(5)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step11
        tdLog.info('=============== step11')
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and tgcol3 = 1 and
        # tgcol2 = 1
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol3 = 1 and tgcol2 = 1' %
            mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol3 = 1 and tgcol2 = 1' %
            mt)
        # TSIM: if $rows != 75 then
        tdLog.info('tdSql.checkRow(75)')
        tdSql.checkRows(75)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and tgcol3 <> 1 and
        # tgcol2 <> 1
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol3 <> 1 and tgcol2 <> 1' %
            mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol3 <> 1 and tgcol2 <> 1' %
            mt)
        # TSIM: if $rows != 75 then
        tdLog.info('tdSql.checkRow(75)')
        tdSql.checkRows(75)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts < 1605045600000 + 240001 and tgcol3 = 0 and
        # tgcol2 = 0
        tdLog.info(
            'select * from %s where ts < 1605045600000 + 240001 and tgcol3 = 0 and tgcol2 = 0' %
            mt)
        tdSql.query(
            'select * from %s where ts < 1605045600000 + 240001 and tgcol3 = 0 and tgcol2 = 0' %
            mt)
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts < 1605045600000 + 240001 and tgcol3 <> 0 and
        # tgcol2 <> 0
        tdLog.info(
            'select * from %s where ts < 1605045600000 + 240001 and tgcol3 <> 0 and tgcol2 <> 0' %
            mt)
        tdSql.query(
            'select * from %s where ts < 1605045600000 + 240001 and tgcol3 <> 0 and tgcol2 <> 0' %
            mt)
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts <= 1605045600000 + 240001 and tgcol3 = 0 and
        # tgcol2 = 0
        tdLog.info(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol3 = 0 and tgcol2 = 0' %
            mt)
        tdSql.query(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol3 = 0 and tgcol2 = 0' %
            mt)
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts <= 1605045600000 + 240001 and tgcol3 <> 0 and
        # tgcol2 <> 0
        tdLog.info(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol3 <> 0 and tgcol2 <> 0' %
            mt)
        tdSql.query(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol3 <> 0 and tgcol2 <> 0' %
            mt)
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001 and
        # tgcol3 <> 0 and tgcol2 <> 0
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001 and tgcol3 <> 0 and tgcol2 <> 0' %
            mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001 and tgcol3 <> 0 and tgcol2 <> 0' %
            mt)
        # TSIM: if $rows != 5 then
        tdLog.info('tdSql.checkRow(5)')
        tdSql.checkRows(5)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and tgcol3 <> 0 and ts
        # < 1605045600000 + 300001 and ts < 1605045600000 + 300001 and tgcol2 <> 0
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol3 <> 0 and ts < 1605045600000 + 300001 and ts < 1605045600000 + 300001 and tgcol2 <> 0' %
            mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol3 <> 0 and ts < 1605045600000 + 300001 and ts < 1605045600000 + 300001 and tgcol2 <> 0' %
            mt)
        # TSIM: if $rows != 5 then
        tdLog.info('tdSql.checkRow(5)')
        tdSql.checkRows(5)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step12
        tdLog.info('=============== step12')
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and tgcol1 = 1 and
        # tgcol2 = 1 and tgcol3 = 1
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1' %
            mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol1 = 1 and tgcol2 = 1 and tgcol3 = 1' %
            mt)
        # TSIM: if $rows != 75 then
        tdLog.info('tdSql.checkRow(75)')
        tdSql.checkRows(75)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and tgcol1 <> 1 and
        # tgcol2 <> 1  and tgcol3 <> 1
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol1 <> 1 and tgcol2 <> 1  and tgcol3 <> 1' %
            mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol1 <> 1 and tgcol2 <> 1  and tgcol3 <> 1' %
            mt)
        # TSIM: if $rows != 75 then
        tdLog.info('tdSql.checkRow(75)')
        tdSql.checkRows(75)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts < 1605045600000 + 240001 and tgcol1 = 0 and
        # tgcol2 = 0 and tgcol3 = 0
        tdLog.info(
            'select * from %s where ts < 1605045600000 + 240001 and tgcol1 = 0 and tgcol2 = 0 and tgcol3 = 0' %
            mt)
        tdSql.query(
            'select * from %s where ts < 1605045600000 + 240001 and tgcol1 = 0 and tgcol2 = 0 and tgcol3 = 0' %
            mt)
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts < 1605045600000 + 240001 and tgcol1 <> 0 and
        # tgcol2 <> 0 and tgcol3 <> 0
        tdLog.info(
            'select * from %s where ts < 1605045600000 + 240001 and tgcol1 <> 0 and tgcol2 <> 0 and tgcol3 <> 0' %
            mt)
        tdSql.query(
            'select * from %s where ts < 1605045600000 + 240001 and tgcol1 <> 0 and tgcol2 <> 0 and tgcol3 <> 0' %
            mt)
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts <= 1605045600000 + 240001 and tgcol1 = 0 and
        # tgcol2 = 0 and tgcol3 = 0
        tdLog.info(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol1 = 0 and tgcol2 = 0 and tgcol3 = 0' %
            mt)
        tdSql.query(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol1 = 0 and tgcol2 = 0 and tgcol3 = 0' %
            mt)
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts <= 1605045600000 + 240001 and tgcol1 <> 0 and
        # tgcol2 <> 0 and tgcol3 <> 0
        tdLog.info(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol1 <> 0 and tgcol2 <> 0 and tgcol3 <> 0' %
            mt)
        tdSql.query(
            'select * from %s where ts <= 1605045600000 + 240001 and tgcol1 <> 0 and tgcol2 <> 0 and tgcol3 <> 0' %
            mt)
        # TSIM: if $rows != 25 then
        tdLog.info('tdSql.checkRow(25)')
        tdSql.checkRows(25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001 and
        # tgcol1 <> 0 and tgcol2 <> 0  and tgcol3 <> 0
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001 and tgcol1 <> 0 and tgcol2 <> 0  and tgcol3 <> 0' %
            mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and ts < 1605045600000 + 300001 and tgcol1 <> 0 and tgcol2 <> 0  and tgcol3 <> 0' %
            mt)
        # TSIM: if $rows != 5 then
        tdLog.info('tdSql.checkRow(5)')
        tdSql.checkRows(5)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: sql select * from %s where ts > 1605045600000 + 240001 and tgcol1 <> 0 and ts
        # < 1605045600000 + 300001 and ts < 1605045600000 + 300001 and tgcol2 <> 0  and tgcol3 <> 0
        tdLog.info(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol1 <> 0 and ts < 1605045600000 + 300001 and ts < 1605045600000 + 300001 and tgcol2 <> 0  and tgcol3 <> 0' %
            mt)
        tdSql.query(
            'select * from %s where ts > 1605045600000 + 240001 and tgcol1 <> 0 and ts < 1605045600000 + 300001 and ts < 1605045600000 + 300001 and tgcol2 <> 0  and tgcol3 <> 0' %
            mt)
        # TSIM: if $rows != 5 then
        tdLog.info('tdSql.checkRow(5)')
        tdSql.checkRows(5)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step13
        tdLog.info('=============== step13')
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from %s
        tdLog.info(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s' %
            mt)
        tdSql.query(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s' %
            mt)
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data00 != 200 then
        tdLog.info('tdSql.checkData(0, 0, 200)')
        tdSql.checkData(0, 0, 200)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step14
        tdLog.info('=============== step14')
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true
        tdLog.info(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true' %
            mt)
        tdSql.query(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true' %
            mt)
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data00 != 100 then
        tdLog.info('tdSql.checkData(0, 0, 100)')
        tdSql.checkData(0, 0, 100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true and
        # tgcol2 = 1
        tdLog.info(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true and tgcol2 = 1' %
            mt)
        tdSql.query(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true and tgcol2 = 1' %
            mt)
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data00 != 100 then
        tdLog.info('tdSql.checkData(0, 0, 100)')
        tdSql.checkData(0, 0, 100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true and
        # tgcol2 = 1 and tgcol3 = 1
        tdLog.info(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true and tgcol2 = 1 and tgcol3 = 1' %
            mt)
        tdSql.query(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true and tgcol2 = 1 and tgcol3 = 1' %
            mt)
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data00 != 100 then
        tdLog.info('tdSql.checkData(0, 0, 100)')
        tdSql.checkData(0, 0, 100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step15
        tdLog.info('=============== step15')
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001
        tdLog.info(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001' %
            mt)
        tdSql.query(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001' %
            mt)
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data00 != 50 then
        tdLog.info('tdSql.checkData(0, 0, 50)')
        tdSql.checkData(0, 0, 50)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001 and
        # tgcol1 = true
        tdLog.info(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001 and tgcol1 = true' %
            mt)
        tdSql.query(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001 and tgcol1 = true' %
            mt)
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data00 != 25 then
        tdLog.info('tdSql.checkData(0, 0, 25)')
        tdSql.checkData(0, 0, 25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001 and
        # tgcol1 = true and tgcol2 = 1
        tdLog.info(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001 and tgcol1 = true and tgcol2 = 1' %
            mt)
        tdSql.query(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001 and tgcol1 = true and tgcol2 = 1' %
            mt)
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data00 != 25 then
        tdLog.info('tdSql.checkData(0, 0, 25)')
        tdSql.checkData(0, 0, 25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001 and
        # tgcol1 = true and tgcol2 = 1 and tgcol3 = 1
        tdLog.info('select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001 and tgcol1 = true and tgcol2 = 1 and tgcol3 = 1' % mt)
        tdSql.query(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001 and tgcol1 = true and tgcol2 = 1 and tgcol3 = 1' %
            mt)
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data00 != 25 then
        tdLog.info('tdSql.checkData(0, 0, 25)')
        tdSql.checkData(0, 0, 25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step16
        tdLog.info('=============== step16')
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from %s group by tgcol1
        tdLog.info(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s group by tgcol1' %
            mt)
        tdSql.query(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s group by tgcol1' %
            mt)
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data00 != 100 then
        tdLog.info('tdSql.checkData(0, 0, 100)')
        tdSql.checkData(0, 0, 100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from %s group by tgcol2
        tdLog.info(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s group by tgcol2' %
            mt)
        tdSql.query(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s group by tgcol2' %
            mt)
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data00 != 100 then
        tdLog.info('tdSql.checkData(0, 0, 100)')
        tdSql.checkData(0, 0, 100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from %s group by tgcol3
        tdLog.info(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s group by tgcol3' %
            mt)
        tdSql.query(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s group by tgcol3' %
            mt)
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data00 != 100 then
        tdLog.info('tdSql.checkData(0, 0, 100)')
        tdSql.checkData(0, 0, 100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step17
        tdLog.info('=============== step17')
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true
        # group by tgcol1
        tdLog.info(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true group by tgcol1' %
            mt)
        tdSql.query(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true group by tgcol1' %
            mt)
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data00 != 100 then
        tdLog.info('tdSql.checkData(0, 0, 100)')
        tdSql.checkData(0, 0, 100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true and
        # tgcol2 = 1  group by tgcol1
        tdLog.info(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true and tgcol2 = 1  group by tgcol1' %
            mt)
        tdSql.query(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true and tgcol2 = 1  group by tgcol1' %
            mt)
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data00 != 100 then
        tdLog.info('tdSql.checkData(0, 0, 100)')
        tdSql.checkData(0, 0, 100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true and
        # tgcol2 = 1 and tgcol3 = 1 group by tgcol1
        tdLog.info(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true and tgcol2 = 1 and tgcol3 = 1 group by tgcol1' %
            mt)
        tdSql.query(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true and tgcol2 = 1 and tgcol3 = 1 group by tgcol1' %
            mt)
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data00 != 100 then
        tdLog.info('tdSql.checkData(0, 0, 100)')
        tdSql.checkData(0, 0, 100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step18
        tdLog.info('=============== step18')
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001
        # group by tgcol2
        tdLog.info(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001 group by tgcol2' %
            mt)
        tdSql.query(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001 group by tgcol2' %
            mt)
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data00 != 25 then
        tdLog.info('tdSql.checkData(0, 0, 25)')
        tdSql.checkData(0, 0, 25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001 and
        # tgcol1 = true group by tgcol2
        tdLog.info(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001 and tgcol1 = true group by tgcol2' %
            mt)
        tdSql.query(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001 and tgcol1 = true group by tgcol2' %
            mt)
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data00 != 25 then
        tdLog.info('tdSql.checkData(0, 0, 25)')
        tdSql.checkData(0, 0, 25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001 and
        # tgcol1 = true and tgcol2 = 1  group by tgcol2
        tdLog.info('select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001 and tgcol1 = true and tgcol2 = 1  group by tgcol2' % mt)
        tdSql.query(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001 and tgcol1 = true and tgcol2 = 1  group by tgcol2' %
            mt)
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data00 != 25 then
        tdLog.info('tdSql.checkData(0, 0, 25)')
        tdSql.checkData(0, 0, 25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001 and
        # tgcol1 = true and tgcol2 = 1 and tgcol3 = 1 group by tgcol2
        tdLog.info('select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001 and tgcol1 = true and tgcol2 = 1 and tgcol3 = 1 group by tgcol2' % mt)
        tdSql.query('select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where ts < 1605045600000 + 240001 and tgcol1 = true and tgcol2 = 1 and tgcol3 = 1 group by tgcol2' % mt)
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data00 != 25 then
        tdLog.info('tdSql.checkData(0, 0, 25)')
        tdSql.checkData(0, 0, 25)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step19
        tdLog.info('=============== step19')
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true and
        # tgcol2 = 1 and tgcol3 = 1 interval(1d) group by tgcol1
        tdLog.info('select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true and tgcol2 = 1 and tgcol3 = 1 interval(1d) group by tgcol1' % mt)
        tdSql.query('select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true and tgcol2 = 1 and tgcol3 = 1 interval(1d) group by tgcol1' % mt)
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data01 != 100 then
        tdLog.info('tdSql.checkData(0, 1, 100)')
        tdSql.checkData(0, 1, 100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true and
        # tgcol2 = 1 and tgcol3 = 1 interval(1d) group by tgcol2
        tdLog.info('select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true and tgcol2 = 1 and tgcol3 = 1 interval(1d) group by tgcol2' % mt)
        tdSql.query('select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true and tgcol2 = 1 and tgcol3 = 1 interval(1d) group by tgcol2' % mt)
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info('$data00 $data01 $data02 $data03 $data04 $data05 $data06')
        # TSIM: if $data01 != 100 then
        tdLog.info('tdSql.checkData(0, 1, 100)')
        tdSql.checkData(0, 1, 100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true and
        # tgcol2 = 1 and tgcol3 = 1 interval(1d) group by tgcol3
        tdLog.info('select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true and tgcol2 = 1 and tgcol3 = 1 interval(1d) group by tgcol3' % mt)
        tdSql.query('select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tgcol1 = true and tgcol2 = 1 and tgcol3 = 1 interval(1d) group by tgcol3' % mt)
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
