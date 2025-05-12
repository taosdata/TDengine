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

        # TSIM: system sh/stop_dnodes.sh
        # TSIM:
        # TSIM: system sh/ip.sh -i 1 -s up
        # TSIM: system sh/deploy.sh -n dnode1 -m 192.168.0.1 -i 192.168.0.1
        # TSIM: system sh/cfg.sh -n dnode1 -c walLevel -v 0
        # TSIM: system sh/exec.sh -n dnode1 -s start
        # TSIM:
        # TSIM: sleep 3000
        # TSIM: sql connect
        # TSIM:
        # TSIM: $i = 0
        # TSIM: $dbPrefix = lm_da_db
        # TSIM: $tbPrefix = lm_da_tb
        # TSIM: $db = $dbPrefix . $i
        # TSIM: $tb = $tbPrefix . $i
        # TSIM:
        # TSIM: print =============== step1
        tdLog.info('=============== step1')
        # TSIM: sql create database $db
        # TSIM: sql use $db
        # TSIM:
        # TSIM: sql create table $tb (ts timestamp, speed int)
        tdLog.info("create table tb0 (ts timestamp, speed int)")
        tdSql.execute('create table tb0 (ts timestamp, speed int)')
        # TSIM: sql insert into $tb values ('2017-01-01 08:00:00.001', 1)
        tdLog.info("insert into tb0 values ('2017-01-01 08:00:00.001', 1)")
        tdSql.execute("insert into tb0 values ('2017-01-01 08:00:00.001', 1)")
        # TSIM: sql select ts from $tb
        tdLog.info('select ts from tb0')
        tdSql.query('select ts from tb0')
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM: if $data00 != @17-01-01 08:00:00.001@ then
        tdLog.info("tdSql.checkData(0, 0, 17-01-01 08:00:00.001)")
        expectedData = datetime.datetime.strptime(
            "17-01-01 08:00:00.001", "%y-%m-%d %H:%M:%S.%f")
        tdSql.checkData(0, 0, expectedData)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step2
        tdLog.info('=============== step2')
        # TSIM: sql insert into $tb values ('2017-08-28 00:23:46.429+ 1a', 2)
        tdLog.info("insert into tb0 values ('2017-08-28 00:23:46.429+ 1a', 2)")
        tdSql.execute(
            "insert into tb0 values ('2017-08-28 00:23:46.429+ 1a', 2)")
        # TSIM: #sql insert into $tb values ('2017-08-28 00:23:46cd .429', 2)
        # TSIM: sql select ts from $tb
        tdLog.info('select ts from tb0')
        tdSql.query('select ts from tb0')
        # TSIM: if $rows != 2 then
        tdLog.info('tdSql.checkRow(2)')
        tdSql.checkRows(2)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step3
        tdLog.info('=============== step3')
        # TSIM: #sql insert into $tb values ('1970-01-01 08:00:00.000', 3)
        # TSIM: #sql insert into $tb values ('1970-01-01 08:00:00.000', 3)
        # TSIM: sql select ts from $tb
        tdLog.info('select ts from tb0')
        tdSql.query('select ts from tb0')
        # TSIM: if $rows != 2 then
        tdLog.info('tdSql.checkRow(2)')
        tdSql.checkRows(2)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step4
        tdLog.info('=============== step4')
        # TSIM: sql insert into $tb values(now, 4);
        tdLog.info("insert into tb0 values(now, 4);")
        tdSql.execute("insert into tb0 values(now, 4);")
        # TSIM: sql insert into $tb values(now+1a, 5);
        tdLog.info("insert into tb0 values(now+1a, 5);")
        tdSql.execute("insert into tb0 values(now+1a, 5);")
        # TSIM: sql insert into $tb values(now+1s, 6);
        tdLog.info("insert into tb0 values(now+1s, 6);")
        tdSql.execute("insert into tb0 values(now+1s, 6);")
        # TSIM: sql insert into $tb values(now+1m, 7);
        tdLog.info("insert into tb0 values(now+1m, 7);")
        tdSql.execute("insert into tb0 values(now+1m, 7);")
        # TSIM: sql insert into $tb values(now+1h, 8);
        tdLog.info("insert into tb0 values(now+1h, 8);")
        tdSql.execute("insert into tb0 values(now+1h, 8);")
        # TSIM: sql insert into $tb values(now+1d, 9);
        tdLog.info("insert into tb0 values(now+1d, 9);")
        tdSql.execute("insert into tb0 values(now+1d, 9);")
        # TSIM: sql_error insert into $tb values(now+3w, 10);
        tdLog.info("insert into tb0 values(now+3w, 10);")
        tdSql.error("insert into tb0 values(now+3w, 10);")
        # TSIM: sql_error insert into $tb values(now+1n, 11);
        tdLog.info("insert into tb0 values(now+1n, 11);")
        tdSql.error("insert into tb0 values(now+1n, 11);")
        # TSIM: sql_error insert into $tb values(now+1y, 12);
        tdLog.info("insert into tb0 values(now+1y, 12);")
        tdSql.error("insert into tb0 values(now+1y, 12);")
        # TSIM:
        # TSIM: print =============== step5
        tdLog.info('=============== step5')
        # TSIM: sql_error insert into $tb values ('9999-12-31 213:59:59.999',
        # 13)
        tdLog.info("insert into tb0 values ('9999-12-31 213:59:59.999', 13)")
        tdSql.error("insert into tb0 values ('9999-12-31 213:59:59.999', 13)")
        # TSIM: sql select ts from $tb
        tdLog.info('select ts from tb0')
        tdSql.query('select ts from tb0')
        # TSIM: print $rows
        tdLog.info('$rows')
        # TSIM: if $rows != 8 then
        tdLog.info('tdSql.checkRow(8)')
        tdSql.checkRows(8)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step6
        tdLog.info('=============== step6')
        # TSIM: sql_error insert into $tb values ('9999-12-99 23:59:59.999',
        # 13)
        tdLog.info("insert into tb0 values ('9999-12-99 23:59:59.999', 13)")
        tdSql.error("insert into tb0 values ('9999-12-99 23:59:59.999', 13)")
        # TSIM:
        # TSIM: sql select ts from $tb
        tdLog.info('select ts from tb0')
        tdSql.query('select ts from tb0')
        # TSIM: if $rows != 8 then
        tdLog.info('tdSql.checkRow(8)')
        tdSql.checkRows(8)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step7
        tdLog.info('=============== step7')
        # TSIM: $i = 1
        # TSIM: $tb = $tbPrefix . $i
        # TSIM: sql create table $tb (ts timestamp, ts2 timestamp)
        tdLog.info("create table tb1 (ts timestamp, ts2 timestamp)")
        tdSql.execute('create table tb1 (ts timestamp, ts2 timestamp)')
        # TSIM:
        # TSIM: print =============== step8
        tdLog.info('=============== step8')
        # TSIM: sql insert into $tb values (now, now)
        tdLog.info("insert into tb1 values (now, now)")
        tdSql.execute("insert into tb1 values (now, now)")
        # TSIM: sql select * from $tb
        tdLog.info('select * from tb1')
        tdSql.query('select * from tb1')
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
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
# convert end

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
