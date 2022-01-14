###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import sys
import taos
from util.log import *
from util.cases import *
from util.sql import *

class ElapsedCase:
    def __init__(self, restart = False):
        self.restart = restart

    def selectTest(self):
        tdSql.execute("use wxy_db")

        tdSql.query("select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.checkRows(1)
        tdSql.checkCols(1)

        tdSql.query("select elapsed(ts, 1m) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.checkEqual(int(tdSql.getData(0, 0)), 999)

        tdSql.query("select elapsed(ts), elapsed(ts, 1m), elapsed(ts, 10m) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.checkEqual(int(tdSql.getData(0, 1)), 999)
        tdSql.checkEqual(int(tdSql.getData(0, 2)), 99)

        tdSql.query("select elapsed(ts), count(*), avg(f), twa(f), irate(f), sum(f), stddev(f), leastsquares(f, 1, 1), "
                    "min(f), max(f), first(f), last(f), percentile(i, 20), apercentile(i, 30), last_row(i), spread(i) "
                    "from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.checkRows(1)
        tdSql.checkCols(16)
        tdSql.checkEqual(int(tdSql.getData(0, 1)), 1000)

        tdSql.query("select elapsed(ts) + 10, elapsed(ts) - 20, elapsed(ts) * 0, elapsed(ts) / 10, elapsed(ts) / elapsed(ts, 1m) from t1 "
                    "where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.checkRows(1)
        tdSql.checkCols(5)
        tdSql.checkEqual(int(tdSql.getData(0, 2)), 0)

        tdSql.query("select elapsed(ts) from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' group by tbname")
        tdSql.checkRows(2)
        tdSql.checkCols(2) # append tbname

        tdSql.query("select elapsed(ts, 10m) from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' group by tbname")
        tdSql.checkEqual(int(tdSql.getData(0, 0)), 99)
        tdSql.checkEqual(int(tdSql.getData(1, 0)), 99)

        tdSql.query("select elapsed(ts), elapsed(ts, 10m), elapsed(ts, 100m) from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' group by tbname")
        tdSql.checkEqual(int(tdSql.getData(0, 1)), 99)
        tdSql.checkEqual(int(tdSql.getData(0, 2)), 9)
        # stddev(f),
        tdSql.query("select elapsed(ts), count(*), avg(f), twa(f), irate(f), sum(f), min(f), max(f), first(f), last(f), apercentile(i, 30), last_row(i), spread(i) "
                    "from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' group by tbname")
        tdSql.checkRows(2)
        tdSql.checkCols(14) # append tbname
        tdSql.checkEqual(int(tdSql.getData(0, 1)), 500)

        tdSql.query("select elapsed(ts) + 10, elapsed(ts) - 20, elapsed(ts) * 0, elapsed(ts) / 10, elapsed(ts) / elapsed(ts, 1m) "
                    "from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' group by tbname")
        tdSql.checkRows(2)
        tdSql.checkCols(6) # append tbname
        tdSql.checkEqual(int(tdSql.getData(0, 2)), 0)

        tdSql.query("select elapsed(ts), tbname from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' group by tbname")
        tdSql.checkRows(2)
        tdSql.checkCols(3) # additional append tbname

        tdSql.execute("use wxy_db_ns")
        tdSql.query("select elapsed(ts, 1b), elapsed(ts, 1u) from t1")
        tdSql.checkRows(1)
        tdSql.checkCols(2)

        self.selectIllegalTest()

    # It has little to do with the elapsed function, so just simple test.
    def whereTest(self):
        tdSql.execute("use wxy_db")

        tdSql.query("select elapsed(ts) from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' and id = 1 group by tbname")
        tdSql.checkRows(1)
        tdSql.checkCols(2) # append tbname

    # It has little to do with the elapsed function, so just simple test.
    def sessionTest(self):
        tdSql.execute("use wxy_db")

        tdSql.query("select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' session(ts, 10s)")
        tdSql.checkRows(1000)

        tdSql.query("select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' session(ts, 70s)")
        tdSql.checkRows(1)

    # It has little to do with the elapsed function, so just simple test.
    def stateWindowTest(self):
        tdSql.execute("use wxy_db")

        tdSql.query("select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' state_window(i)")
        tdSql.checkRows(1000)

        tdSql.query("select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' state_window(b)")
        tdSql.checkRows(2)

    def intervalTest(self):
        tdSql.execute("use wxy_db")

        tdSql.query("select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(1m)")
        tdSql.checkRows(1000)

        # The first window has 9 records, and the last window has 1 record.
        tdSql.query("select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(10m)")
        tdSql.checkRows(101)
        tdSql.checkEqual(int(tdSql.getData(0, 1)), 9 * 60 * 1000)
        tdSql.checkEqual(int(tdSql.getData(100, 1)), 0)

        # Skip windows without data.
        tdSql.query("select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(35s)")
        tdSql.checkRows(1000)

        tdSql.query("select elapsed(ts), count(*), avg(f), twa(f), irate(f), sum(f), stddev(f), leastsquares(f, 1, 1), "
                    "min(f), max(f), first(f), last(f), percentile(i, 20), apercentile(i, 30), last_row(i), spread(i) "
                    "from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(20m)")
        tdSql.checkRows(51) # ceil(1000/50) + 1(last point), window is half-open interval.
        tdSql.checkCols(17) # front push timestamp

        tdSql.query("select elapsed(ts) from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(40s) group by tbname")
        tdSql.checkRows(1000)

        tdSql.query("select elapsed(ts) + 10, elapsed(ts) - 20, elapsed(ts) * 0, elapsed(ts) / 10, elapsed(ts) / elapsed(ts, 1m) "
                    "from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(30m) group by tbname")
        tdSql.checkRows(68) # ceil(1000/30)
        tdSql.checkCols(7) # front push timestamp and append tbname

    # It has little to do with the elapsed function, so just simple test.
    def fillTest(self):
        tdSql.execute("use wxy_db")

        tdSql.query("select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(30s) fill(value, 1000)")
        tdSql.checkRows(2880) # The range of window conditions is 24 hours.
        tdSql.checkEqual(int(tdSql.getData(0, 1)), 1000)

        tdSql.query("select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(30s) fill(prev)")
        tdSql.checkRows(2880) # The range of window conditions is 24 hours.
        tdSql.checkData(0, 1, None)

        tdSql.query("select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(30s) fill(null)")
        tdSql.checkRows(2880) # The range of window conditions is 24 hours.
        tdSql.checkData(0, 1, None)

        tdSql.query("select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(30s) fill(linear)")
        tdSql.checkRows(2880) # The range of window conditions is 24 hours.

        tdSql.query("select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(30s) fill(next)")
        tdSql.checkRows(2880) # The range of window conditions is 24 hours.

    # Elapsed only support group by tbname. Supported tests have been done in selectTest().
    def groupbyTest(self):
        tdSql.execute("use wxy_db")

        tdSql.error("select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' group by i")
        tdSql.error("select elapsed(ts) from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' group by i")

    def orderbyCheck(self, sql, elapsedCol):
        resultAsc = tdSql.getResult(sql)
        resultdesc = tdSql.getResult(sql + " order by ts desc")
        resultRows = len(resultAsc)
        for i in range(resultRows):
            tdSql.checkEqual(resultAsc[i][elapsedCol], resultdesc[resultRows - i - 1][elapsedCol])

    def splitStableResult(self, sql, elapsedCol, tbnameCol):
        subtable = {}
        result = tdSql.getResult(sql)
        for i in range(len(result)):
            if None == subtable.get(result[i][tbnameCol]):
                subtable[result[i][tbnameCol]] = [result[i][elapsedCol]]
            else:
                subtable[result[i][tbnameCol]].append(result[i][elapsedCol])
        return subtable

    def doOrderbyCheck(self, resultAsc, resultdesc):
        resultRows = len(resultAsc)
        for i in range(resultRows):
            tdSql.checkEqual(resultAsc[i], resultdesc[resultRows - i - 1])

    def orderbyForStableCheck(self, sql, elapsedCol, tbnameCol):
        subtableAsc = self.splitStableResult(sql, elapsedCol, tbnameCol)
        subtableDesc = self.splitStableResult(sql + " order by ts desc", elapsedCol, tbnameCol)
        for kv in subtableAsc.items():
            descValue = subtableDesc.get(kv[0])
            if None == descValue:
                tdLog.exit("%s failed: subtable %s not exists" % (sql))
            else:
                self.doOrderbyCheck(kv[1], descValue)

    # Orderby clause only changes the output order and has no effect on the calculation results.
    def orderbyTest(self):
        tdSql.execute("use wxy_db")

        self.orderbyCheck("select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'", 0)
        self.orderbyCheck("select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(40s)", 1)
        self.orderbyCheck("select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(1m)", 1)
        self.orderbyCheck("select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(10m)", 1)
        self.orderbyCheck("select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(150m)", 1)
        self.orderbyCheck("select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(222m)", 1)
        self.orderbyCheck("select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(1000m)", 1)

        self.orderbyForStableCheck("select elapsed(ts) from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' group by tbname", 0, 1)
        self.orderbyForStableCheck("select elapsed(ts) from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(40s) group by tbname", 1, 2)
        self.orderbyForStableCheck("select elapsed(ts) from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(1m) group by tbname", 1, 2)
        self.orderbyForStableCheck("select elapsed(ts) from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(10m) group by tbname", 1, 2)
        self.orderbyForStableCheck("select elapsed(ts) from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(150m) group by tbname", 1, 2)
        self.orderbyForStableCheck("select elapsed(ts) from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(222m) group by tbname", 1, 2)
        self.orderbyForStableCheck("select elapsed(ts) from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(1000m) group by tbname", 1, 2)

        #nested query
        resAsc = tdSql.getResult("select elapsed(ts) from (select csum(i) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00')")
        resDesc = tdSql.getResult("select elapsed(ts) from (select csum(i) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' order by ts desc)")
        resRows = len(resAsc)
        for i in range(resRows):
            tdSql.checkEqual(resAsc[i][0], resDesc[resRows - i - 1][0])

    def slimitCheck(self, sql):
        tdSql.checkEqual(tdSql.query(sql + " slimit 0"), 0)
        tdSql.checkEqual(tdSql.query(sql + " slimit 1 soffset 0"), tdSql.query(sql + " slimit 0, 1"))
        tdSql.checkEqual(tdSql.query(sql + " slimit 1, 1"), tdSql.query(sql) / 2)
        tdSql.checkEqual(tdSql.query(sql + " slimit 10"), tdSql.query(sql))

    # It has little to do with the elapsed function, so just simple test.
    def slimitTest(self):
        tdSql.execute("use wxy_db")

        self.slimitCheck("select elapsed(ts) from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' group by tbname")
        self.slimitCheck("select elapsed(ts) from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(40s) group by tbname")

    def limitCheck(self, sql, groupby = 0):
        rows = tdSql.query(sql)
        if rows > 0:
            tdSql.checkEqual(tdSql.query(sql + " limit 0"), 0)
            if 1 == groupby:
                tdSql.checkEqual(tdSql.query(sql + " limit 1"), 2)
                tdSql.checkEqual(tdSql.query(sql + " limit %d offset %d" % (rows / 2, rows / 3)), tdSql.query(sql + " limit %d, %d" % (rows / 3, rows / 2)))
                tdSql.checkEqual(tdSql.query(sql + " limit %d" % (rows / 2)), rows)
            else:
                tdSql.checkEqual(tdSql.query(sql + " limit 1"), 1)
                tdSql.checkEqual(tdSql.query(sql + " limit %d offset %d" % (rows / 2, rows / 3)), tdSql.query(sql + " limit %d, %d" % (rows / 3, rows / 2)))
                tdSql.checkEqual(tdSql.query(sql + " limit %d" % (rows + 1)), rows)

    # It has little to do with the elapsed function, so just simple test.
    def limitTest(self):
        tdSql.execute("use wxy_db")

        self.limitCheck("select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        self.limitCheck("select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(40s)")

        self.limitCheck("select elapsed(ts) from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' group by tbname", 1)
        self.limitCheck("select elapsed(ts) from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(40s) group by tbname", 1)

    def fromCheck(self, sqlTemplate, table):
        #tdSql.checkEqual(tdSql.getResult(sqlTemplate % table), tdSql.getResult(sqlTemplate % ("(select * from %s)" % table)))
        tdSql.query(sqlTemplate % ("(select last(ts) from %s interval(10s))" % table))
        tdSql.query(sqlTemplate % ("(select elapsed(ts) from %s interval(10s))" % table))

    # It has little to do with the elapsed function, so just simple test.
    def fromTest(self):
        tdSql.execute("use wxy_db")

        self.fromCheck("select elapsed(ts) from %s where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'", "t1")
        self.fromCheck("select elapsed(ts) from %s where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(40s)", "t1")
        tdSql.query("select * from (select elapsed(ts) from t1 interval(10s)) where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.query("select * from (select elapsed(ts) from t1)")
        # empty table test
        tdSql.checkEqual(tdSql.query("select elapsed(ts) from t2"), 0)
        tdSql.checkEqual(tdSql.query("select elapsed(ts) from st2 group by tbname"), 0)
        tdSql.checkEqual(tdSql.query("select elapsed(ts) from st3 group by tbname"), 0)
        # Tags not allowed for table query, so there is no need to test super table.
        tdSql.error("select elapsed(ts) from (select * from st1)")

    def joinCheck(self, sqlTemplate, rtable):
        tdSql.checkEqual(tdSql.getResult(sqlTemplate % (rtable, "")), tdSql.getResult(sqlTemplate % ("t1, %s t2" % rtable, "t1.ts = t2.ts and ")))

    # It has little to do with the elapsed function, so just simple test.
    def joinTest(self):
        tdSql.execute("use wxy_db")

        # st1s1 is a subset of t1.
        self.joinCheck("select elapsed(ts) from %s where %s ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'", "st1s1")
        self.joinCheck("select elapsed(ts) from %s where %s ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(150m)", "st1s1")
        # join query does not support group by, so there is no need to test super table.

    def unionAllCheck(self, sql1, sql2):
        rows1 = tdSql.query(sql1)
        rows2 = tdSql.query(sql2)
        tdSql.checkEqual(tdSql.query(sql1 + " union all " + sql2), rows1 + rows2)

    # It has little to do with the elapsed function, so just simple test.
    def unionAllTest(self):
        tdSql.execute("use wxy_db")

        self.unionAllCheck("select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'",
                           "select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-22 01:00:00'")
        self.unionAllCheck("select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(40s)",
                           "select elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(150m)")
        self.unionAllCheck("select elapsed(ts) from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' group by tbname",
                           "select elapsed(ts) from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-22 02:00:00' group by tbname")
        self.unionAllCheck("select elapsed(ts) from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(1m) group by tbname",
                           "select elapsed(ts) from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' interval(222m) group by tbname")

    # It has little to do with the elapsed function, so just simple test.
    def continuousQueryTest(self):
        tdSql.execute("use wxy_db")

        if (self.restart):
            tdSql.execute("drop table elapsed_t")
            tdSql.execute("drop table elapsed_st")
        tdSql.error("create table elapsed_t as select elapsed(ts) from t1 interval(1m) sliding(30s)")
        tdSql.error("create table elapsed_st as select elapsed(ts) from st1 interval(1m) sliding(30s) group by tbname")

    def selectIllegalTest(self):
        tdSql.execute("use wxy_db")
        tdSql.error("select elapsed() from t1")
        tdSql.error("select elapsed(,) from t1")
        tdSql.error("select elapsed(1) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed('2021-11-18 00:00:10') from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(now) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(i) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(b) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(f) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(d) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(bin) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(s) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(t) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(bl) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(n) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(ts1) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(*) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(ts, '1s') from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(ts, i) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(ts, now) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(ts, now-7d+2h-3m+2s) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(ts, 7d+2h+now+3m+2s) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(ts, ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(ts + 1) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(ts, 1b) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(ts, 1u) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(max(ts)) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select distinct elapsed(ts) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(ts) from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select distinct elapsed(ts) from st1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00' group by tbname")
        tdSql.error("select elapsed(ts), i from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(ts), ts from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(ts), _c0 from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(ts), top(i, 1) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(ts), bottom(i, 1) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(ts), inerp(i) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(ts), diff(i) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(ts), derivative(i, 1s, 0) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(ts), ceil(i) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(ts), floor(i) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")
        tdSql.error("select elapsed(ts), round(i) from t1 where ts > '2021-11-22 00:00:00' and ts < '2021-11-23 00:00:00'")

    def run(self):
        self.selectTest()
        self.whereTest()
        self.sessionTest()
        self.stateWindowTest()
        self.intervalTest()
        self.fillTest()
        self.groupbyTest()
        self.orderbyTest()
        self.slimitTest()
        self.limitTest()
        self.fromTest()
        self.joinTest()
        self.unionAllTest()
        self.continuousQueryTest()
