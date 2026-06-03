# -*- coding: utf-8 -*-

import sys
import string
import random
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
        tdLog.info('drop table dd -x step0')
        tdSql.error('drop table dd')
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
        # TSIM: sql create table $tb (ts timestamp, a0123456789012345678901234567890123456789 int)
        getMaxColNum = "grep -w '#define TSDB_COL_NAME_LEN' ../../src/inc/taosdef.h|awk '{print $3}'"
        boundary = int(subprocess.check_output(getMaxColNum, shell=True)) - 1
        tdLog.info("get max column name length is %d" % boundary)
        chars = string.ascii_uppercase + string.ascii_lowercase

#        col_name = ''.join(random.choices(chars, k=boundary+1))
#        tdLog.info(
#            'create table tb (ts timestamp, %s int), col_name length is %d' % (col_name, len(col_name)))
#        tdSql.error(
#            'create table tb (ts timestamp, %s int)' % col_name)

        col_name = ''.join(random.choices(chars, k=boundary))
        tdLog.info(
            'create table tb (ts timestamp, %s int), col_name length is %d' %
            (col_name, len(col_name)))
        tdSql.execute(
            'create table tb (ts timestamp, %s int)' % col_name)

        # TSIM: sql insert into $tb values (now , 1)
        tdLog.info("insert into tb values (now , 1)")
        tdSql.execute("insert into tb values (now , 1)")
        # TSIM: sql select * from $tb
        tdLog.info('select * from tb')
        tdSql.query('select * from tb')
        # TSIM: if $rows != 1 then
        tdLog.info('tdSql.checkRow(1)')
        tdSql.checkRows(1)
# convert end

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
