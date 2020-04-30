# -*- coding: utf-8 -*-

import sys
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def run(self):
        tdSql.prepare()

        # TSIM: system sh/stop_dnodes.sh
        # TSIM:
        # TSIM: system sh/ip.sh -i 1 -s up
        # TSIM: system sh/deploy.sh -n dnode1 -m 192.168.0.1 -i 192.168.0.1
        # TSIM: system sh/cfg.sh -n dnode1 -c commitLog -v 0
        # TSIM: system sh/exec.sh -n dnode1 -s start
        # TSIM:
        # TSIM: sleep 3000
        # TSIM: sql connect
        # TSIM:
        # TSIM: $i = 0
        # TSIM: $dbPrefix = lm_cm_db
        # TSIM: $tbPrefix = lm_cm_tb
        # TSIM: $db = $dbPrefix . $i
        # TSIM: $tb = $tbPrefix . $i
        # TSIM:
        # TSIM: print =============== step1
        tdLog.info('=============== step1')
        # TSIM: sql create database $db
        # TSIM: sql use $db
        # TSIM:
        # TSIM: sql drop table dd -x step0
        tdLog.info('drop table dd -x step0')
        tdSql.error('drop table dd')
        # TSIM: return -1
        # TSIM: step0:
        # TSIM:
        # TSIM: sql create table $tb(ts timestamp, int) -x step1
        tdLog.info('create table tb(ts timestamp, int) -x step1')
        tdSql.error('create table tb(ts timestamp, int)')
        # TSIM: return -1
        # TSIM: step1:
        # TSIM:
        # TSIM: sql show tables
        tdLog.info('show tables')
        tdSql.query('show tables')
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step2
        tdLog.info('=============== step2')
        # TSIM: sql create table $tb (ts timestamp, s int)
        tdLog.info('create table tb (ts timestamp, s int)')
        tdSql.execute('create table tb (ts timestamp, s int)')
        # TSIM: sql show tables
        tdLog.info('show tables')
        tdSql.query('show tables')
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql drop table $tb
        tdLog.info('drop table tb')
        tdSql.execute('drop table tb')
        # TSIM: sql show tables
        tdLog.info('show tables')
        tdSql.query('show tables')
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step3
        tdLog.info('=============== step3')
        # TSIM: sql create table $tb (ts timestamp, a0123456789 int)
        tdLog.info('create table tb (ts timestamp, a0123456789 int)')
        tdSql.execute('create table tb (ts timestamp, a0123456789 int)')
        # TSIM: sql show tables
        tdLog.info('show tables')
        tdSql.query('show tables')
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql drop table $tb
        tdLog.info('drop table tb')
        tdSql.execute('drop table tb')
        # TSIM: sql show tables
        tdLog.info('show tables')
        tdSql.query('show tables')
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step4
        tdLog.info('=============== step4')
        # TSIM: sql create table $tb (ts timestamp,
        # a0123456789012345678901234567890123456789 int)
        tdLog.info(
            'create table tb (ts timestamp, a0123456789012345678901234567890123456789 int)')
        tdSql.execute(
            'create table tb (ts timestamp, a0123456789012345678901234567890123456789 int)')
        # TSIM: sql drop table $tb
        tdLog.info('drop table tb')
        tdSql.execute('drop table tb')
        # TSIM:
        # TSIM: sql show tables
        tdLog.info('show tables')
        tdSql.query('show tables')
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: print =============== step5
        tdLog.info('=============== step5')
        # TSIM: sql create table $tb (ts timestamp, a0123456789 int)
        tdLog.info('create table tb (ts timestamp, a0123456789 int)')
        tdSql.execute('create table tb (ts timestamp, a0123456789 int)')
        # TSIM: sql show tables
        tdLog.info('show tables')
        tdSql.query('show tables')
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql insert into $tb values (now , 1)
        tdLog.info("insert into tb values (now , 1)")
        tdSql.execute("insert into tb values (now , 1)")
        # TSIM: sql select * from $tb
        tdLog.info('select * from tb')
        tdSql.query('select * from tb')
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql drop database $db
        tdLog.info('drop database db')
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
        # TSIM:
        # TSIM:
        # TSIM:
# convert end

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
