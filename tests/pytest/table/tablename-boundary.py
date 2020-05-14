# -*- coding: utf-8 -*-

import sys
import string
import random
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def run(self):
        tdSql.prepare()

        chars = string.ascii_uppercase+string.ascii_lowercase
        tb_name = ''.join(random.choices(chars, k=192))
        tdLog.info('tb_name length %d' % len(tb_name))
        tdLog.info('create table %s (ts timestamp, value int)' % tb_name)
        tdSql.error('create table %s (ts timestamp, speed binary(4089))' % tb_name)

        tb_name = ''.join(random.choices(chars, k=191))
        tdLog.info('tb_name length %d' % len(tb_name))
        tdLog.info('create table %s (ts timestamp, value int)' % tb_name)
        tdSql.execute('create table %s (ts timestamp, speed binary(4089))' % tb_name)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
