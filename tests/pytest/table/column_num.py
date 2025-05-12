# -*- coding: utf-8 -*-

import sys
import subprocess
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        tdLog.info('=============== step1')
        tdLog.info('create table tb() -x step1')
        tdSql.error('create table tb()')
        tdLog.info('show tables')
        tdSql.query('show tables')
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        tdLog.info('=============== step2')
        # TSIM: sql create table $tb (ts timestamp)  -x step2
        tdLog.info('create table tb (ts timestamp)  -x step2')
        tdSql.error('create table tb (ts timestamp) ')
        tdLog.info('show tables')
        tdSql.query('show tables')
        # TSIM: if $rows != 0 then
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        tdLog.info('=============== step3')
        tdLog.info('create table tb (ts int)  -x step3')
        tdSql.error('create table tb (ts int) ')
        tdLog.info('show tables')
        tdSql.query('show tables')
        tdLog.info('tdSql.checkRow(0)')
        tdSql.checkRows(0)
        tdLog.info('=============== step4')
        tdLog.info('create table tb (ts timestamp, a1 int, a2 int, a3 int, a4 int, a5 int, a6 int, a7 int, a8 int, a9 int, a10 int, a11 int, a12 int, a13 int, a14 int, a15 int, a16 int, a17 int, a18 int, a19 int, a20 int, a21 int, a22 int, a23 int, a24 int, a25 int, a26 int, a27 int, a28 int,a29 int,a30 int,a31 int,a32 int, b1 int, b2 int, b3 int, b4 int, b5 int, b6 int, b7 int, b8 int, b9 int, b10 int, b11 int, b12 int, b13 int, b14 int, b15 int, b16 int, b17 int, b18 int, b19 int, b20 int, b21 int, b22 int, b23 int, b24 int, b25 int, b26 int, b27 int, b28 int,b29 int,b30 int,b31 int,b32 int)')
        tdSql.execute('create table tb (ts timestamp, a1 int, a2 int, a3 int, a4 int, a5 int, a6 int, a7 int, a8 int, a9 int, a10 int, a11 int, a12 int, a13 int, a14 int, a15 int, a16 int, a17 int, a18 int, a19 int, a20 int, a21 int, a22 int, a23 int, a24 int, a25 int, a26 int, a27 int, a28 int,a29 int,a30 int,a31 int,a32 int, b1 int, b2 int, b3 int, b4 int, b5 int, b6 int, b7 int, b8 int, b9 int, b10 int, b11 int, b12 int, b13 int, b14 int, b15 int, b16 int, b17 int, b18 int, b19 int, b20 int, b21 int, b22 int, b23 int, b24 int, b25 int, b26 int, b27 int, b28 int,b29 int,b30 int,b31 int,b32 int)')
        # TSIM:
        # TSIM: sql show tables
        tdLog.info('show tables')
        tdSql.query('show tables')
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)

        tdLog.info('=============== step5')

        getMaxColumnNum = "grep -w '#define TSDB_MAX_COLUMNS' ../../src/inc/taosdef.h|awk '{print $3}'"
        boundary = int(subprocess.check_output(getMaxColumnNum, shell=True))
        tdLog.info("get max column number is %d" % boundary)

        columnSeq = "ts timestamp"
        for x in range(0, boundary):
            columnSeq = columnSeq + ", col%d int" % x

        tdLog.info("create table tb1 (%s)" % columnSeq)
        tdSql.error('create table tb1 (%s)' % columnSeq)

        columnSeq = "ts timestamp"
        for x in range(0, boundary - 1):
            columnSeq = columnSeq + ", col%d int" % x

        tdLog.info("create table tb1 (%s)" % columnSeq)
        tdSql.execute('create table tb1 (%s)' % columnSeq)

        tdLog.info('show tables')
        tdSql.query('show tables')
        # TSIM: if $rows != 2 then
        tdLog.info('tdSql.checkRow(2)')
        tdSql.checkRows(2)

        data = "now"
        for x in range(0, boundary - 1):
            data = data + ", %d" % x
        tdLog.info("insert into tb1 values (%s)" % data)
        tdSql.execute("insert into tb1 values (%s)" % data)
        # TSIM: sql select * from $tb
        tdLog.info('select * from tb1')
        tdSql.query('select * from tb1')
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
        # TSIM: return -1
        # TSIM: endi
        # TSIM:
        # TSIM: sql drop table $tb
        tdLog.info('drop table tb1')
        tdSql.execute('drop table tb1')
        # TSIM: sql show tables
        tdLog.info('show tables')
        tdSql.query('show tables')
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
