# -*- coding: utf-8 -*-

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *


class TDTestCase(TBase):
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        sqls = [
            "drop database if exists ts_5101",
            "create database ts_5101 cachemodel 'both';",
            "use ts_5101;",
            "CREATE STABLE meters (ts timestamp, current float) TAGS (location binary(64), groupId int);",
            "CREATE TABLE d1001 USING meters TAGS ('California.B', 2);",
            "CREATE TABLE d1002 USING meters TAGS ('California.S', 3);",
            "INSERT INTO d1001 VALUES ('2024-07-03 10:00:00.000', 10);",
            "INSERT INTO d1002 VALUES ('2024-07-03 13:00:00.000', 13);",
        ]
        tdSql.executes(sqls)

        # 执行多次，有些时候last_row(ts)会返回错误的值，详见TS-5105
        for i in range(1, 10):
            sql = "select last(ts), last_row(ts) from meters;"
            tdLog.debug(f"{i}th execute sql: {sql}")
            tdSql.query(sql)
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, "2024-07-03 13:00:00.000")
            tdSql.checkData(0, 1, "2024-07-03 13:00:00.000")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
