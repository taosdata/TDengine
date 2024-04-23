# -*- coding: utf-8 -*-

from util.log import *
from util.cases import *
from util.sql import *
import time


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self.dbname = "test"
        self.stbname = "stb"
        self.ctbname = "ctb"
        self.keep_value = "2d,2d,2d"
        self.duration_value = "16h"
        self.offset_time = 5
        self.sleep_time = self.offset_time*2

    def run(self):
        tdSql.execute(f'create database if not exists {self.dbname} duration {self.duration_value} keep {self.keep_value};')
        tdSql.execute(f'create table {self.dbname}.{self.stbname} (ts timestamp, c11 int) TAGS(t11 int, t12 int );')
        tdSql.execute(f'create table {self.dbname}.{self.ctbname} using {self.dbname}.{self.stbname} TAGS (1, 1);')
        expired_row_ts = f'now-{int(self.keep_value.split(",")[0].replace("d", "")) * 86400 - self.offset_time}s'
        tdSql.execute(f'insert into {self.dbname}.{self.ctbname} values ({expired_row_ts}, 1);')
        tdSql.query(f'select * from {self.dbname}.{self.ctbname}')
        tdSql.checkEqual(tdSql.queryRows, 1)
        time.sleep(self.offset_time * 2)
        tdSql.query(f'select * from {self.dbname}.{self.ctbname}')
        tdSql.checkEqual(tdSql.queryRows, 0)
        tdSql.execute(f'TRIM DATABASE {self.dbname}')

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
