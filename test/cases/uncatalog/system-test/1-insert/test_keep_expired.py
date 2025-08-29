# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql
import os
import time

class TestKeepExpired:
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        #tdSql.init(conn.cursor(), logSql), logSql)
        cls.dbname = "test"
        cls.stbname = "stb"
        cls.ctbname = "ctb"
        cls.keep_value = "2d,2d,2d"
        cls.duration_value = "16h"
        cls.offset_time = 5
        cls.sleep_time = cls.offset_time*2

    def test_keep_expired(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
        - xxx:xxx

        History:
        - xxx
        - xxx

        """
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
        
        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
