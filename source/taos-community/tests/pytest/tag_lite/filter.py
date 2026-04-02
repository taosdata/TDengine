###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

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
        # TSIM: system sh/exec.sh -n dnode1 -s start
        # TSIM:
        # TSIM: sleep 3000
        # TSIM: sql connect
        # TSIM:
        # TSIM: print ======================== dnode1 start
        tdLog.info('======================== dnode1 start')
        # TSIM:
        dbPrefix = "ta_fi_db"
        tbPrefix = "ta_fi_tb"
        mtPrefix = "ta_fi_mt"
        # TSIM: $tbNum = 10
        rowNum = 20
        # TSIM: $totalNum = 200
        # TSIM:
        # TSIM: print =============== step1
        tdLog.info('=============== step1')
        i = 0
        # TSIM: $db = $dbPrefix . $i
        mt = "%s%d" % (mtPrefix, i)
        # TSIM:
        # TSIM: sql create database $db
        # TSIM: sql use $db
        # TSIM: sql create table $mt (ts timestamp, tbcol int) TAGS(tgcol
        # binary(10))
        tdLog.info(
            "create table %s (ts timestamp, tbcol int) TAGS(tgcol binary(10))" %
            mt)
        tdSql.execute(
            'create table %s (ts timestamp, tbcol int) TAGS(tgcol binary(10))' %
            mt)
        # TSIM:
        i = 0
        while (i < 5):
            tb = "tbPrefix%d" % i
            tdLog.info("create table %s using %s tags( '0' )" % (tb, mt))
            tdSql.execute("create table %s using %s tags( '0' )" % (tb, mt))

            x = 0
            while (x < rowNum):
                ms = "%dm" % x
                tdLog.info(
                    "insert into %s values (now + %s , %d)" %
                    (tb, ms, x))
                tdSql.execute(
                    "insert into %s values (now + %s , %d)" %
                    (tb, ms, x))
                x = x + 1
            i = i + 1

        while (i < 10):
            tb = "%s%d" % (tbPrefix, i)
        # TSIM: sql create table $tb using $mt tags( '1' )
            tdLog.info("create table %s using %s tags( '1' )" % (tb, mt))
            tdSql.execute("create table %s using %s tags( '1' )" % (tb, mt))
            x = 0
            while (x < rowNum):
                ms = "%dm" % x
        # TSIM: sql insert into $tb values (now + $ms , $x )
                tdLog.info(
                    "insert into %s values (now + %s, %d )" %
                    (tb, ms, x))
                tdSql.execute(
                    "insert into %s values (now + %s, %d )" %
                    (tb, ms, x))
                x = x + 1
            i = i + 1
        # TSIM:
        # TSIM: print =============== step2
        tdLog.info('=============== step2')
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from $mt where tgcol = '1'
        tdLog.info(
            "select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tgcol = '1'" %
            mt)
        tdSql.query(
            "select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tgcol = '1'" %
            mt)
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info(
            "%s %s %s %s %s %s %s" %
            (tdSql.getData(
                0, 0), tdSql.getData(
                0, 1), tdSql.getData(
                0, 2), tdSql.getData(
                    0, 3), tdSql.getData(
                        0, 4), tdSql.getData(
                            0, 5), tdSql.getData(
                                0, 6)))
        # TSIM: if $data00 != 100 then
        tdLog.info('tdSql.checkData(0, 0, 100)')
        tdSql.checkData(0, 0, 100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from $mt where tg = '1' -x
        # step2
        tdLog.info(
            "select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tg = '1' -x step2" %
            mt)
        tdSql.error(
            "select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tg = '1'" %
            mt)
        # TSIM: return -1
        # TSIM: step2:
        # TSIM:
        # TSIM: print =============== step3
        tdLog.info('=============== step3')
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from $mt where noexist = '1' -x
        # step3
        tdLog.info(
            "select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where noexist = '1' -x step3" %
            mt)
        tdSql.error(
            "select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where noexist = '1'" %
            mt)
        # TSIM: return -1
        # TSIM: step3:
        # TSIM:
        # TSIM: print =============== step4
        tdLog.info('=============== step4')
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from $mt where tbcol = '1'
        tdLog.info(
            "select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tbcol = '1'" %
            mt)
        tdSql.query(
            "select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s where tbcol = '1'" %
            mt)
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data00 != 10 then
        tdLog.info('tdSql.checkData(0, 0, 10)')
        tdSql.checkData(0, 0, 10)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step5
        tdLog.info('=============== step5')
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from $mt
        tdLog.info(
            "select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s" %
            mt)
        tdSql.query(
            "select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s" %
            mt)
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
        tdLog.info(
            "%s %s %s %s %s %s %s" %
            (tdSql.getData(
                0, 0), tdSql.getData(
                0, 1), tdSql.getData(
                0, 2), tdSql.getData(
                    0, 3), tdSql.getData(
                        0, 4), tdSql.getData(
                            0, 5), tdSql.getData(
                                0, 6)))
        # TSIM: if $data00 != 200 then
        tdLog.info('tdSql.checkData(0, 0, 200)')
        tdSql.checkData(0, 0, 200)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step6
        tdLog.info('=============== step6')
        # TSIM: sql select count(tbcol), avg(cc), sum(xx), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from $mt -x step6
        tdLog.info(
            "select count(tbcol), avg(cc), sum(xx), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s -x step6" %
            mt)
        tdSql.error(
            "select count(tbcol), avg(cc), sum(xx), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s" %
            mt)
        # TSIM: return -1
        # TSIM: step6:
        # TSIM:
        # TSIM: print =============== step7
        tdLog.info('=============== step7')
        # TSIM: sql select count(tgcol), avg(tgcol), sum(tgcol), min(tgcol),
        # max(tgcol), first(tgcol), last(tgcol) from $mt -x step7
        tdLog.info(
            "select count(tgcol), avg(tgcol), sum(tgcol), min(tgcol), max(tgcol), first(tgcol), last(tgcol) from %s -x step7" %
            mt)
        tdSql.error(
            "select count(tgcol), avg(tgcol), sum(tgcol), min(tgcol), max(tgcol), first(tgcol), last(tgcol) from %s" %
            mt)
        # TSIM: return -1
        # TSIM: step7:
        # TSIM:
        # TSIM: print =============== step8
        tdLog.info('=============== step8')
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from $mt group by tbcol
        tdLog.info(
            "select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s by tbcol" %
            mt)
        tdSql.query(
            "select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s group by tbcol" %
            mt)
        # TSIM:
        # TSIM: print =============== step9
        tdLog.info('=============== step9')
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from $mt group by noexist  -x
        # step9
        tdLog.info(
            "select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s group by noexist  -x step9" %
            mt)
        tdSql.error(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s group by noexist ' %
            mt)
        # TSIM: return -1
        # TSIM: step9:
        # TSIM:
        # TSIM: print =============== step10
        tdLog.info('=============== step10')
        # TSIM: sql select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol),
        # max(tbcol), first(tbcol), last(tbcol) from $mt group by tgcol
        tdLog.info(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s group by tgcol' %
            mt)
        tdSql.query(
            'select count(tbcol), avg(tbcol), sum(tbcol), min(tbcol), max(tbcol), first(tbcol), last(tbcol) from %s group by tgcol' %
            mt)
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
        # TSIM: sql select count(tbcol) as c from $mt group by tbcol
        tdLog.info('select count(tbcol) as c from %s group by tbcol' % mt)
        tdSql.query('select count(tbcol) as c from %s group by tbcol' % mt)
        # TSIM:
        # TSIM: print =============== step12
        tdLog.info('=============== step12')
        # TSIM: sql select count(tbcol) as c from $mt group by noexist -x
        # step12
        tdLog.info(
            'select count(tbcol) as c from %s group by noexist -x step12' %
            mt)
        tdSql.error('select count(tbcol) as c from %s group by noexist2' % mt)
        # TSIM: return -1
        # TSIM: step12:
        # TSIM:
        # TSIM: print =============== step13
        tdLog.info('=============== step13')
        # TSIM: sql select count(tbcol) as c from $mt group by tgcol
        tdLog.info('select count(tbcol) as c from %s group by tgcol' % mt)
        tdSql.query('select count(tbcol) as c from %s group by tgcol' % mt)
        # TSIM: print $data00
        tdLog.info('$data00')
        # TSIM: if $data00 != 100 then
        tdLog.info('tdSql.checkData(0, 0, 100)')
        tdSql.checkData(0, 0, 100)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step14
        tdLog.info('=============== step14')
        # TSIM: sql select count(tbcol) as c from $mt where ts > 1000 group by
        # tgcol
        tdLog.info(
            'select count(tbcol) as c from %s where ts > 1000 group by tgcol' %
            mt)
        tdSql.query(
            'select count(tbcol) as c from %s where ts > 1000 group by tgcol' %
            mt)
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
#        tdLog.info("%s %s %s %s %s %s %s" % (tdSql.getData(0, 0), tdSql.getData(0, 1), tdSql.getData(0, 2), tdSql.getData(0, 3), tdSql.getData(0, 4), tdSql.getData(0, 5), tdSql.getData(0, 6)))
        # TSIM: if $data00 != 100 then
        tdLog.info('tdSql.checkData(0, 0, 100)')
        tdSql.checkData(0, 0, 100)
        # TSIM: print expect 100, actual $data00
        tdLog.info('expect 100, actual $data00')
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step15
        tdLog.info('=============== step15')
        # TSIM: sql select count(tbcol) as c from $mt where noexist < 1  group
        # by tgcol -x step15
        tdLog.info(
            'select count(tbcol) as c from %s where noexist < 1  group by tgcol -x step15' %
            mt)
        tdSql.error(
            'select count(tbcol) as c from %s where noexist < 1  group by tgcol5' %
            mt)
        # TSIM: return -1
        # TSIM: step15:
        # TSIM:
        # TSIM: print =============== step16
        tdLog.info('=============== step16')
        # TSIM: sql select count(tbcol) as c from $mt where tgcol = '1' group
        # by tgcol
        tdLog.info(
            "select count(tbcol) as c from %s where tgcol = '1' group by tgcol" %
            mt)
        tdSql.query(
            "select count(tbcol) as c from %s where tgcol = '1' group by tgcol" %
            mt)
        # TSIM: print $data00 $data01 $data02 $data03 $data04 $data05 $data06
#        tdLog.info("%s %s %s %s %s %s %s" % (tdSql.getData(0, 0), tdSql.getData(0, 1), tdSql.getData(0, 2), tdSql.getData(0, 3), tdSql.getData(0, 4), tdSql.getData(0, 5), tdSql.getData(0, 6)))
        # TSIM: if $data00 != 100 then
        tdLog.info('tdSql.checkData(0, 0, 100)')
        tdSql.checkData(0, 0, 100)
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
