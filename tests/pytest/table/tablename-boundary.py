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

        getTableNameLen = "grep -w '#define TSDB_TABLE_NAME_LEN' ../../src/inc/taosdef.h|awk '{print $3}'"
        tableNameMaxLen = int( subprocess.check_output(getTableNameLen, shell=True)) - 1
        tdLog.info("table name max length is %d" % tableNameMaxLen)
        chars = string.ascii_uppercase + string.ascii_lowercase
        tb_name = ''.join(random.choices(chars, k=tableNameMaxLen))
        tdLog.info('tb_name length %d' % len(tb_name))
        tdLog.info('create table %s (ts timestamp, value int)' % tb_name)
        tdSql.error(
            'create table %s (ts timestamp, speed binary(4089))' %
            tb_name)

        tb_name = ''.join(random.choices(chars, k=191))
        tdLog.info('tb_name length %d' % len(tb_name))
        tdLog.info('create table %s (ts timestamp, value int)' % tb_name)
        tdSql.execute(
            'create table %s (ts timestamp, speed binary(4089))' %
            tb_name)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
