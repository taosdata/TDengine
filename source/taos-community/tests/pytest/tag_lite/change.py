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
        # TSIM: $dbPrefix = ta_ch_db
        # TSIM: $tbPrefix = ta_ch_tb
        # TSIM: $mtPrefix = ta_ch_mt
        # TSIM: $tbNum = 10
        # TSIM: $rowNum = 20
        # TSIM: $totalNum = 200
        # TSIM:
        # TSIM: print =============== step1
        tdLog.info('=============== step1')
        # TSIM: $i = 0
        # TSIM: $db = $dbPrefix . $i
        # TSIM:
        # TSIM: sql create database $db
        # TSIM: sql use $db
        # TSIM:
        # TSIM: print =============== step2
        tdLog.info('=============== step2')
        # TSIM: $i = 2
        # TSIM: $mt = $mtPrefix . $i
        # TSIM: $tb = $tbPrefix . $i
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol1
        # bool, tgcol2 int)
        tdLog.info(
            "create table ta_ch_mt2 (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int)")
        tdSql.execute(
            'create table ta_ch_mt2 (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 int)')
        # TSIM: sql create table $tb using $mt tags( 1, 2 )
        tdLog.info("create table tb2 using ta_ch_mt2 tags( 1, 2 )")
        tdSql.execute('create table tb2 using ta_ch_mt2 tags( 1, 2 )')
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info("insert into tb2 values(now, 1)")
        tdSql.execute("insert into tb2 values(now, 1)")
        # TSIM: sql select * from $mt where tgcol1 = 1
        tdLog.info('select * from ta_ch_mt2 where tgcol1 = 1')
        tdSql.query('select * from ta_ch_mt2 where tgcol1 = 1')
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
        # TSIM: sql alter table $mt change tag tagcx tgcol3 -x step21
        tdLog.info("alter table ta_ch_mt2 change tag tagcx tgcol3 -x step21")
        tdSql.error("alter table ta_ch_mt2 change tag tagcx tgcol3")
        # TSIM: return -1
        # TSIM: step21:
        # TSIM: sql alter table $mt change tag tgcol1 tgcol2 -x step22
        tdLog.info("alter table ta_ch_mt2 change tag tgcol1 tgcol2 -x step22")
        tdSql.error("alter table ta_ch_mt2 change tag tgcol1 tgcol2")
        # TSIM: return -1
        # TSIM: step22:
        # TSIM: sql alter table $mt change tag tgcol1
        # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx -x
        # step20
        tdLog.info(
            "alter table ta_ch_mt2 change tag tgcol1 xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx -x step20")
        tdSql.error(
            "alter table ta_ch_mt2 change tag tgcol1 xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx -x step20")
        # TSIM: return -1
        # TSIM: step20:
        # TSIM:
        # TSIM: sql alter table $mt change tag tgcol1 tgcol3
        tdLog.info("alter table ta_ch_mt2 change tag tgcol1 tgcol3")
        tdSql.execute("alter table ta_ch_mt2 change tag tgcol1 tgcol3")
        # TSIM: sql alter table $mt change tag tgcol2 tgcol4
        tdLog.info("alter table ta_ch_mt2 change tag tgcol2 tgcol4")
        tdSql.execute("alter table ta_ch_mt2 change tag tgcol2 tgcol4")
        # TSIM: sql alter table $mt change tag tgcol4 tgcol3 -x step23
        tdLog.info("alter table ta_ch_mt2 change tag tgcol4 tgcol3 -x step23")
        tdSql.error("alter table ta_ch_mt2 change tag tgcol4 tgcol3")
        # TSIM: return -1
        # TSIM: step23:
        # TSIM:
        # TSIM: print =============== step3
        tdLog.info('=============== step3')
        # TSIM: $i = 3
        # TSIM: $mt = $mtPrefix . $i
        # TSIM: $tb = $tbPrefix . $i
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol1
        # smallint, tgcol2 tinyint)
        tdLog.info(
            "create table ta_ch_mt3 (ts timestamp, tbcol int) TAGS(tgcol1 smallint, tgcol2 tinyint)")
        tdSql.execute(
            'create table ta_ch_mt3 (ts timestamp, tbcol int) TAGS(tgcol1 smallint, tgcol2 tinyint)')
        # TSIM: sql create table $tb using $mt tags( 1, 2 )
        tdLog.info("create table tb3 using ta_ch_mt3 tags( 1, 2 )")
        tdSql.execute('create table tb3 using ta_ch_mt3 tags( 1, 2 )')
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info("insert into tb3 values(now, 1)")
        tdSql.execute("insert into tb3 values(now, 1)")
        # TSIM: sql select * from $mt where tgcol1 = 1
        tdLog.info('select * from ta_ch_mt3 where tgcol1 = 1')
        tdSql.query('select * from ta_ch_mt3 where tgcol1 = 1')
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
        # TSIM: sql alter table $mt change tag tgcol1 tgcol3
        tdLog.info("alter table ta_ch_mt3 change tag tgcol1 tgcol3")
        tdSql.execute("alter table ta_ch_mt3 change tag tgcol1 tgcol3")
        # TSIM: sql alter table $mt change tag tgcol2 tgcol4
        tdLog.info("alter table ta_ch_mt3 change tag tgcol2 tgcol4")
        tdSql.execute("alter table ta_ch_mt3 change tag tgcol2 tgcol4")
        # TSIM:
        # TSIM: print =============== step4
        tdLog.info('=============== step4')
        # TSIM: $i = 4
        # TSIM: $mt = $mtPrefix . $i
        # TSIM: $tb = $tbPrefix . $i
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol1
        # bigint, tgcol2 float)
        tdLog.info(
            "create table ta_ch_mt4 (ts timestamp, tbcol int) TAGS(tgcol1 bigint, tgcol2 float)")
        tdSql.execute(
            'create table ta_ch_mt4 (ts timestamp, tbcol int) TAGS(tgcol1 bigint, tgcol2 float)')
        # TSIM: sql create table $tb using $mt tags( 1, 2 )
        tdLog.info("create table tb4 using ta_ch_mt4 tags( 1, 2 )")
        tdSql.execute('create table tb4 using ta_ch_mt4 tags( 1, 2 )')
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info("insert into tb4 values(now, 1)")
        tdSql.execute("insert into tb4 values(now, 1)")
        # TSIM: sql select * from $mt where tgcol1 = 1
        tdLog.info('select * from ta_ch_mt4 where tgcol1 = 1')
        tdSql.query('select * from ta_ch_mt4 where tgcol1 = 1')
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
        # TSIM: sql alter table $mt change tag tgcol1 tgcol3
        tdLog.info("alter table ta_ch_mt4 change tag tgcol1 tgcol3")
        tdSql.execute("alter table ta_ch_mt4 change tag tgcol1 tgcol3")
        # TSIM: sql alter table $mt change tag tgcol2 tgcol4
        tdLog.info("alter table ta_ch_mt4 change tag tgcol2 tgcol4")
        tdSql.execute("alter table ta_ch_mt4 change tag tgcol2 tgcol4")
        # TSIM:
        # TSIM: print =============== step5
        tdLog.info('=============== step5')
        # TSIM: $i = 5
        # TSIM: $mt = $mtPrefix . $i
        # TSIM: $tb = $tbPrefix . $i
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol1
        # double, tgcol2 binary(10))
        tdLog.info(
            "create table ta_ch_mt5 (ts timestamp, tbcol int) TAGS(tgcol1 double, tgcol2 binary(10))")
        tdSql.execute(
            'create table ta_ch_mt5 (ts timestamp, tbcol int) TAGS(tgcol1 double, tgcol2 binary(10))')
        # TSIM: sql create table $tb using $mt tags( 1, '2' )
        tdLog.info("create table tb5 using ta_ch_mt5 tags( 1, '2' )")
        tdSql.execute("create table tb5 using ta_ch_mt5 tags( 1, '2' )")
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info("insert into tb5 values(now, 1)")
        tdSql.execute("insert into tb5 values(now, 1)")
        # TSIM: sql select * from $mt where tgcol2 = '2'
        tdLog.info("select * from ta_ch_mt5 where tgcol2 = '2'")
        tdSql.query("select * from ta_ch_mt5 where tgcol2 = '2'")
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
        # TSIM: sql alter table $mt change tag tgcol1 tgcol3
        tdLog.info("alter table ta_ch_mt5 change tag tgcol1 tgcol3")
        tdSql.execute("alter table ta_ch_mt5 change tag tgcol1 tgcol3")
        # TSIM: sql alter table $mt change tag tgcol2 tgcol4
        tdLog.info("alter table ta_ch_mt5 change tag tgcol2 tgcol4")
        tdSql.execute("alter table ta_ch_mt5 change tag tgcol2 tgcol4")
        # TSIM:
        # TSIM: print =============== step6
        tdLog.info('=============== step6')
        # TSIM: $i = 6
        # TSIM: $mt = $mtPrefix . $i
        # TSIM: $tb = $tbPrefix . $i
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol1
        # binary(10), tgcol2 int, tgcol3 smallint, tgcol4 binary(11), tgcol5
        # double, tgcol6 binary(20))
        tdLog.info("create table ta_ch_mt6 (ts timestamp, tbcol int) TAGS(tgcol1 binary(10), tgcol2 int, tgcol3 smallint, tgcol4 binary(11), tgcol5 double, tgcol6 binary(20))")
        tdSql.execute(
            'create table ta_ch_mt6 (ts timestamp, tbcol int) TAGS(tgcol1 binary(10), tgcol2 int, tgcol3 smallint, tgcol4 binary(11), tgcol5 double, tgcol6 binary(20))')
        # TSIM: sql create table $tb using $mt tags( '1', 2, 3, '4', 5, '6' )
        tdLog.info(
            "create table tb6 using ta_ch_mt6 tags( '1', 2, 3, '4', 5, '6' )")
        tdSql.execute(
            "create table tb6 using ta_ch_mt6 tags( '1', 2, 3, '4', 5, '6' )")
        # TSIM: sql insert into $tb values(now, 1)
        tdLog.info("insert into tb6 values(now, 1)")
        tdSql.execute("insert into tb6 values(now, 1)")
        # TSIM: sql select * from $mt where tgcol1 = '1'
        tdLog.info("select * from ta_ch_mt6 where tgcol1 = '1'")
        tdSql.query("select * from ta_ch_mt6 where tgcol1 = '1'")
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data01 != 1 then
        tdLog.info("tdSql.checkData(0, 1, 1)")
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
        tdLog.info('tdSql.checkData(0, 5, 4)')
        tdSql.checkData(0, 5, '4')
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data06 != 5.000000000 then
        tdLog.info('tdSql.checkData(0, 6, 5.000000000)')
        tdSql.checkData(0, 6, 5.000000000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data07 != 6 then
        tdLog.info('tdSql.checkData(0, 7, 6)')
        tdSql.checkData(0, 7, '6')
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql alter table $mt drop tag tgcol3
        tdLog.info("alter table ta_ch_mt6 drop tag tgcol3")
        tdSql.execute("alter table ta_ch_mt6 drop tag tgcol3")
        # TSIM: sql reset query cache
        tdLog.info("reset query cache")
        tdSql.execute("reset query cache")
        # TSIM: sql alter table $mt change tag tgcol4 tgcol3
        tdLog.info("alter table ta_ch_mt6 change tag tgcol4 tgcol3")
        tdSql.execute("alter table ta_ch_mt6 change tag tgcol4 tgcol3")
        # TSIM: sql alter table $mt change tag tgcol1 tgcol7
        tdLog.info("alter table ta_ch_mt6 change tag tgcol1 tgcol7")
        tdSql.execute("alter table ta_ch_mt6 change tag tgcol1 tgcol7")
        # TSIM: sql alter table $mt change tag tgcol2 tgcol8
        tdLog.info("alter table ta_ch_mt6 change tag tgcol2 tgcol8")
        tdSql.execute("alter table ta_ch_mt6 change tag tgcol2 tgcol8")
        # TSIM: sql reset query cache
        tdLog.info("reset query cache")
        tdSql.execute("reset query cache")
        # TSIM: sql alter table $mt change tag tgcol3 tgcol9
        tdLog.info("alter table ta_ch_mt6 change tag tgcol3 tgcol9")
        tdSql.execute("alter table ta_ch_mt6 change tag tgcol3 tgcol9")
        # TSIM: sql alter table $mt change tag tgcol5 tgcol10
        tdLog.info("alter table ta_ch_mt6 change tag tgcol5 tgcol10")
        tdSql.execute("alter table ta_ch_mt6 change tag tgcol5 tgcol10")
        # TSIM: sql alter table $mt change tag tgcol6 tgcol11
        tdLog.info("alter table ta_ch_mt6 change tag tgcol6 tgcol11")
        tdSql.execute("alter table ta_ch_mt6 change tag tgcol6 tgcol11")
        # TSIM:
        # TSIM: sleep 5000
        # TSIM: sql reset query cache
        tdLog.info("reset query cache")
        tdSql.execute("reset query cache")
        # TSIM:
        # TSIM: print =============== step2
        tdLog.info('=============== step2')
        # TSIM: $i = 2
        # TSIM: $mt = $mtPrefix . $i
        # TSIM: $tb = $tbPrefix . $i
        # TSIM:
        # TSIM: sql select * from $mt where tgcol1 = 1 -x step24
        tdLog.info('select * from ta_ch_mt2 where tgcol1 = 1 -x step24')
        tdSql.error("select * from ta_ch_mt2 where tgcol1 = 1")
        # TSIM: return -1
        # TSIM: step24:
        # TSIM: sql select * from $mt where tgcol2 = 1 -x step25
        tdLog.info('select * from ta_ch_mt2 where tgcol2 = 1 -x step25')
        tdSql.error('select * from ta_ch_mt2 where tgcol2 = 1')
        # TSIM: return -1
        # TSIM: step25:
        # TSIM:
        # TSIM: sql select * from $mt where tgcol3 = 1
        tdLog.info('select * from ta_ch_mt2 where tgcol3 = 1')
        tdSql.query('select * from ta_ch_mt2 where tgcol3 = 1')
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
        # TSIM: if $data03 != 2 then
        tdLog.info('tdSql.checkData(0, 3, 2)')
        tdSql.checkData(0, 3, 2)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol4 = 2
        tdLog.info('select * from ta_ch_mt2 where tgcol4 = 2')
        tdSql.query('select * from ta_ch_mt2 where tgcol4 = 2')
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
        # TSIM: if $data03 != 2 then
        tdLog.info('tdSql.checkData(0, 3, 2)')
        tdSql.checkData(0, 3, 2)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step3
        tdLog.info('=============== step3')
        # TSIM: $i = 3
        # TSIM: $mt = $mtPrefix . $i
        # TSIM: $tb = $tbPrefix . $i
        # TSIM:
        # TSIM: sql select * from $mt where tgcol1 = 1 -x step31
        tdLog.info('select * from ta_ch_mt3 where tgcol1 = 1 -x step31')
        tdSql.error('select * from ta_ch_mt3 where tgcol1 = 1')
        # TSIM: return -1
        # TSIM: step31:
        # TSIM: sql select * from $mt where tgcol2 = 1 -x step32
        tdLog.info('select * from ta_ch_mt3 where tgcol2 = 1 -x step32')
        tdSql.error('select * from ta_ch_mt3 where tgcol2 = 1')
        # TSIM: return -1
        # TSIM: step32:
        # TSIM:
        # TSIM: sql select * from $mt where tgcol3 = 1
        tdLog.info('select * from ta_ch_mt3 where tgcol3 = 1')
        tdSql.query('select * from ta_ch_mt3 where tgcol3 = 1')
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
        # TSIM: if $data03 != 2 then
        tdLog.info('tdSql.checkData(0, 3, 2)')
        tdSql.checkData(0, 3, 2)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol4 = 2
        tdLog.info('select * from ta_ch_mt3 where tgcol4 = 2')
        tdSql.query('select * from ta_ch_mt3 where tgcol4 = 2')
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
        # TSIM: if $data03 != 2 then
        tdLog.info('tdSql.checkData(0, 3, 2)')
        tdSql.checkData(0, 3, 2)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step4
        tdLog.info('=============== step4')
        # TSIM: $i = 4
        # TSIM: $mt = $mtPrefix . $i
        # TSIM: $tb = $tbPrefix . $i
        # TSIM:
        # TSIM: sql select * from $mt where tgcol1 = 1 -x step41
        tdLog.info('select * from ta_ch_mt4 where tgcol1 = 1 -x step41')
        tdSql.error('select * from ta_ch_mt4 where tgcol1 = 11')
        # TSIM: return -1
        # TSIM: step41:
        # TSIM: sql select * from $mt where tgcol2 = 1 -x step42
        tdLog.info('select * from ta_ch_mt4 where tgcol2 = 1 -x step42')
        tdSql.error('select * from ta_ch_mt4 where tgcol2 = 12')
        # TSIM: return -1
        # TSIM: step42:
        # TSIM:
        # TSIM: sql select * from $mt where tgcol3 = 1
        tdLog.info('select * from ta_ch_mt4 where tgcol3 = 1')
        tdSql.query('select * from ta_ch_mt4 where tgcol3 = 1')
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
        # TSIM: if $data03 != 2.00000 then
        tdLog.info('tdSql.checkData(0, 3, 2.00000)')
        tdSql.checkData(0, 3, 2.00000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol4 = 2
        tdLog.info('select * from ta_ch_mt4 where tgcol4 = 2')
        tdSql.query('select * from ta_ch_mt4 where tgcol4 = 2')
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
        # TSIM: if $data03 != 2.00000 then
        tdLog.info('tdSql.checkData(0, 3, 2.00000)')
        tdSql.checkData(0, 3, 2.00000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step5
        tdLog.info('=============== step5')
        # TSIM: $i = 5
        # TSIM: $mt = $mtPrefix . $i
        # TSIM: $tb = $tbPrefix . $i
        # TSIM:
        # TSIM: sql select * from $mt where tgcol1 = 1 -x step51
        tdLog.info('select * from ta_ch_mt5 where tgcol1 = 1 -x step51')
        tdSql.error('select * from ta_ch_mt5 where tgcol1 = 11')
        # TSIM: return -1
        # TSIM: step51:
        # TSIM: sql select * from $mt where tgcol2 = 1 -x step52
        tdLog.info('select * from ta_ch_mt5 where tgcol2 = 1 -x step52')
        tdSql.error('select * from ta_ch_mt5 where tgcol2 = 12')
        # TSIM: return -1
        # TSIM: step52:
        # TSIM:
        # TSIM: sql select * from $mt where tgcol3 = 1
        tdLog.info('select * from ta_ch_mt5 where tgcol3 = 1')
        tdSql.query('select * from ta_ch_mt5 where tgcol3 = 1')
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
        # TSIM: if $data03 != 2 then
        tdLog.info('tdSql.checkData(0, 3, 2)')
        tdSql.checkData(0, 3, "2")
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select * from $mt where tgcol4 = '2'
        tdLog.info("select * from ta_ch_mt5 where tgcol4 = '2'")
        tdSql.query("select * from ta_ch_mt5 where tgcol4 = '2'")
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
        # TSIM: if $data03 != 2 then
        tdLog.info('tdSql.checkData(0, 3, 2)')
        tdSql.checkData(0, 3, "2")
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step6
        tdLog.info('=============== step6')
        # TSIM: $i = 6
        # TSIM: $mt = $mtPrefix . $i
        # TSIM: $tb = $tbPrefix . $i
        # TSIM:
        # TSIM: sql select * from $mt where tgcol1 = 1 -x step61
        tdLog.info('select * from ta_ch_mt6 where tgcol1 = 1 -x step61')
        tdSql.error('select * from ta_ch_mt6 where tgcol1 = 1')
        # TSIM: return -1
        # TSIM: step61:
        # TSIM: sql select * from $mt where tgcol2 = 1 -x step62
        tdLog.info('select * from ta_ch_mt6 where tgcol2 = 1 -x step62')
        tdSql.error('select * from ta_ch_mt6 where tgcol2 = 1')
        # TSIM: return -1
        # TSIM: step62:
        # TSIM: sql select * from $mt where tgcol3 = 1 -x step63
        tdLog.info('select * from ta_ch_mt6 where tgcol3 = 1 -x step63')
        tdSql.error('select * from ta_ch_mt6 where tgcol3 = 1')
        # TSIM: return -1
        # TSIM: step63:
        # TSIM: sql select * from $mt where tgcol4 = 1 -x step64
        tdLog.info('select * from ta_ch_mt6 where tgcol4 = 1 -x step64')
        tdSql.error('select * from ta_ch_mt6 where tgcol4 = 1')
        # TSIM: return -1
        # TSIM: step64:
        # TSIM: sql select * from $mt where tgcol5 = 1 -x step65
        tdLog.info('select * from ta_ch_mt6 where tgcol5 = 1 -x step65')
        tdSql.error('select * from ta_ch_mt6 where tgcol5 = 1')
        # TSIM: return -1
        # TSIM: step65:
        # TSIM: sql select * from $mt where tgcol6 = 1 -x step66
        tdLog.info('select * from ta_ch_mt6 where tgcol6 = 1 -x step66')
        tdSql.error('select * from ta_ch_mt6 where tgcol6 = 1')
        # TSIM: return -1
        # TSIM: step66:
        # TSIM:
        # TSIM: sql select * from $mt where tgcol7 = '1'
        tdLog.info("select * from ta_ch_mt6 where tgcol7 = '1'")
        tdSql.query("select * from ta_ch_mt6 where tgcol7 = '1'")
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
        tdLog.info('tdSql.checkData(0, 2, "1")')
        tdSql.checkData(0, 2, "1")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 2 then
        tdLog.info('tdSql.checkData(0, 3, 2)')
        tdSql.checkData(0, 3, 2)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data04 != 4 then
        tdLog.info('tdSql.checkData(0, 4, "4")')
        tdSql.checkData(0, 4, "4")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data05 != 5.000000000 then
        tdLog.info('tdSql.checkData(0, 5, 5.000000000)')
        tdSql.checkData(0, 5, 5.000000000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data06 != 6 then
        tdLog.info('tdSql.checkData(0, 6, "6")')
        tdSql.checkData(0, 6, "6")
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
        # TSIM: sql select * from $mt where tgcol8 = 2
        tdLog.info('select * from ta_ch_mt6 where tgcol8 = 2')
        tdSql.query('select * from ta_ch_mt6 where tgcol8 = 2')
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
        tdLog.info('tdSql.checkData(0, 2, "1")')
        tdSql.checkData(0, 2, "1")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 2 then
        tdLog.info('tdSql.checkData(0, 3, 2)')
        tdSql.checkData(0, 3, 2)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data04 != 4 then
        tdLog.info('tdSql.checkData(0, 4, "4"")')
        tdSql.checkData(0, 4, "4")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data05 != 5.000000000 then
        tdLog.info('tdSql.checkData(0, 5, 5.000000000)')
        tdSql.checkData(0, 5, 5.000000000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data06 != 6 then
        tdLog.info('tdSql.checkData(0, 6, "6")')
        tdSql.checkData(0, 6, "6")
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
        # TSIM: sql select * from $mt where tgcol9 = '4'
        tdLog.info("select * from ta_ch_mt6 where tgcol9 = '4'")
        tdSql.query("select * from ta_ch_mt6 where tgcol9 = '4'")
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
        tdLog.info('tdSql.checkData(0, 2, "1")')
        tdSql.checkData(0, 2, "1")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 2 then
        tdLog.info('tdSql.checkData(0, 3, 2)')
        tdSql.checkData(0, 3, 2)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data04 != 4 then
        tdLog.info('tdSql.checkData(0, 4, "4")')
        tdSql.checkData(0, 4, "4")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data05 != 5.000000000 then
        tdLog.info('tdSql.checkData(0, 5, 5.000000000)')
        tdSql.checkData(0, 5, 5.000000000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data06 != 6 then
        tdLog.info('tdSql.checkData(0, 6, "6")')
        tdSql.checkData(0, 6, "6")
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
        # TSIM: sql select * from $mt where tgcol10 = 5
        tdLog.info('select * from ta_ch_mt6 where tgcol10 = 5')
        tdSql.query('select * from ta_ch_mt6 where tgcol10 = 5')
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
        tdLog.info('tdSql.checkData(0, 2, "1")')
        tdSql.checkData(0, 2, "1")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 2 then
        tdLog.info('tdSql.checkData(0, 3, 2)')
        tdSql.checkData(0, 3, 2)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data04 != 4 then
        tdLog.info('tdSql.checkData(0, 4, "4")')
        tdSql.checkData(0, 4, "4")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data05 != 5.000000000 then
        tdLog.info('tdSql.checkData(0, 5, 5.000000000)')
        tdSql.checkData(0, 5, 5.000000000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data06 != 6 then
        tdLog.info('tdSql.checkData(0, 6, "6")')
        tdSql.checkData(0, 6, "6")
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
        # TSIM: sql select * from $mt where tgcol11 = '6'
        tdLog.info("select * from ta_ch_mt6 where tgcol11 = '6'")
        tdSql.query("select * from ta_ch_mt6 where tgcol11 = '6'")
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
        tdLog.info('tdSql.checkData(0, 2, "1")')
        tdSql.checkData(0, 2, "1")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data03 != 2 then
        tdLog.info('tdSql.checkData(0, 3, 2)')
        tdSql.checkData(0, 3, 2)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data04 != 4 then
        tdLog.info('tdSql.checkData(0, 4, "4")')
        tdSql.checkData(0, 4, "4")
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data05 != 5.000000000 then
        tdLog.info('tdSql.checkData(0, 5, 5.000000000)')
        tdSql.checkData(0, 5, 5.000000000)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data06 != 6 then
        tdLog.info('tdSql.checkData(0, 6, "6")')
        tdSql.checkData(0, 6, "6")
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
