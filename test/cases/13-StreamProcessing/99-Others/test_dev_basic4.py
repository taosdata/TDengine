import time
import math
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdStream


class TestStreamDevBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_dev_basic(self):
        """basic test

        Verification testing during the development process.

        Catalog:
            - Streams:Others

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-26 Simon Guan Created

        """

        self.createSnode()
        self.createDatabase()
        self.prepareQueryData()
        self.prepareTriggerTable()
        self.createStream()
        self.writeTriggerData()
        self.checkResults()

    def createSnode(self):
        tdLog.info("create snode")
        tdStream.createSnode(1)

    def createDatabase(self):
        tdLog.info(f"create database")

        tdSql.prepare(dbname="qdb", vgroups=1)
        tdSql.prepare(dbname="tdb", vgroups=1)
        tdSql.prepare("rdb", vgroups=1)
        # tdSql.prepare("qdb2", vgroups=1)
        clusterComCheck.checkDbReady("qdb")
        clusterComCheck.checkDbReady("tdb")
        clusterComCheck.checkDbReady("rdb")
        # clusterComCheck.checkDbReady("qdb2")

    def prepareQueryData(self):
        tdLog.info("prepare child tables for query")
        tdStream.prepareChildTables(tbBatch=1, tbPerBatch=100)

        tdLog.info("prepare normal tables for query")
        tdStream.prepareNormalTables(tables=10)

        tdLog.info("prepare virtual tables for query")
        tdStream.prepareVirtualTables(tables=10)

        tdLog.info("prepare json tag tables for query, include None")
        tdStream.prepareJsonTables(tbBatch=1, tbPerBatch=10)

    def prepareTriggerTable(self):
        tdLog.info("prepare child tables for trigger")
        tdSql.execute(
            "create table tdb.triggers (ts timestamp, c1 int, c2 int) tags(id int);"
        )
        tdSql.execute("create table tdb.t1 using tdb.triggers tags(1)")
        tdSql.execute("create table tdb.t2 using tdb.triggers tags(2)")
        tdSql.execute("create table tdb.t3 using tdb.triggers tags(3)")

    def writeTriggerData(self):
        tdLog.info("write data to trigger table")
        sqls = [
            "insert into tdb.t1 values ('2025-01-01 00:00:00', 0, 0), ('2025-01-01 00:05:00', 1, 1), ('2025-01-01 00:10:00', 2, 2)",
            "insert into tdb.t1 values ('2025-01-01 00:11:00', 0, 0), ('2025-01-01 00:12:00', 1, 1), ('2025-01-01 00:15:00', 2, 2)",
            "insert into tdb.t1 values ('2025-01-01 00:21:00', 0, 0)",
        ]
        tdSql.executes(sqls)

    def createStream(self):
        tdLog.info("create stream")
        sql = "create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers into rdb.st1  as select _twstart ts, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twend;"
        tdSql.execute(sql)

    def checkResults(self):
        tdLog.info("check stream result")

        tdSql.checkResultsByFunc("show rdb.tables", func=lambda: tdSql.getRows() == 1)

        res_query = "select ts, c1, c2 from rdb.st1"
        tdSql.checkResultsByFunc(
            sql=res_query,
            func=lambda: tdSql.getRows() >= 4
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 2, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 3, "2025-01-01 00:00:00.000")
            and tdSql.compareData(1, 0, 1000)
            and tdSql.compareData(1, 1, 1000)
            and tdSql.compareData(1, 2, 1000)
            and tdSql.compareData(1, 3, 1000)
            and tdSql.compareData(2, 0, 4.5)
            and tdSql.compareData(2, 1, 14.5)
            and tdSql.compareData(2, 2, 24.5)
            and tdSql.compareData(2, 3, 34.5),
        )

        # exp_query = "select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters interval(5m)"

        # exp_result = tdSql.getResult(exp_query)
        # tdSql.checkResultsByArray(res_query, exp_result, retry=10)
