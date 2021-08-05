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
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.init(conn.cursor(), logSql)

    def insertnow(self):

        # timestamp list:
        #   0 -> "1970-01-01 08:00:00" | -28800000 -> "1970-01-01 00:00:00" | -946800000000 -> "1940-01-01 00:00:00"
        #   -631180800000 -> "1950-01-01 00:00:00"

        tsp1 = 0
        tsp2 = -28800000
        tsp3 = -946800000000
        tsp4 = "1969-01-01 00:00:00.000"

        tdSql.execute("insert into tcq1 values (now-11d, 5)")
        tdSql.execute(f"insert into tcq1 values ({tsp1}, 4)")
        tdSql.execute(f"insert into tcq1 values ({tsp2}, 3)")
        tdSql.execute(f"insert into tcq1 values ('{tsp4}', 2)")
        tdSql.execute(f"insert into tcq1 values ({tsp3}, 1)")

    def waitedQuery(self, sql, expectRows, timeout):
        tdLog.info(f"sql: {sql}, try to retrieve {expectRows} rows in {timeout} seconds")
        try:
            for i in range(timeout):
                tdSql.cursor.execute(sql)
                self.queryResult = tdSql.cursor.fetchall()
                self.queryRows = len(self.queryResult)
                self.queryCols = len(tdSql.cursor.description)
                # tdLog.info("sql: %s, try to retrieve %d rows,get %d rows" % (sql, expectRows, self.queryRows))
                if self.queryRows >= expectRows:
                    return (self.queryRows, i)
                time.sleep(1)
        except Exception as e:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            tdLog.notice(f"{caller.filename}({caller.lineno}) failed: sql:{sql}, {repr(e)}")
            raise Exception(repr(e))
        return (self.queryRows, timeout)

    def showstream(self):
        tdSql.execute(
            "create table cq1 as select avg(c1) from tcq1 interval(10d) sliding(1d)"
        )
        sql = "show streams"
        timeout = 30
        exception = "ValueError('year -292275055 is out of range')"
        try:
            for i in range(timeout):
                tdSql.cursor.execute(sql)
                self.queryResult = tdSql.cursor.fetchall()
                self.queryRows = len(self.queryResult)
                self.queryCols = len(tdSql.cursor.description)
                # tdLog.info("sql: %s, try to retrieve %d rows,get %d rows" % (sql, expectRows, self.queryRows))
                if self.queryRows >= 1:
                    tdSql.query(sql)
                    tdSql.checkData(0, 5, None)
                    return (self.queryRows, i)
                time.sleep(1)
        except Exception as e:
            tdLog.exit(f"sql: {sql} except raise {exception}, actually raise {repr(e)} ")
        # else:
            # tdLog.exit(f"sql: {sql} except raise {exception}, actually not")

    def run(self):
        tdSql.execute("drop database if exists dbcq")
        tdSql.execute("create database  if not exists dbcq keep 36500")
        tdSql.execute("use dbcq")

        tdSql.execute("create table stbcq (ts timestamp, c1 int ) TAGS(t1 int)")
        tdSql.execute("create table tcq1 using stbcq tags(1)")

        self.insertnow()
        self.showstream()


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())