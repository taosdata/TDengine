import time
import math
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


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
        self.createStreams()
        self.writeTriggerData()
        self.checkStreamStatus()
        self.checkResults()

    def createSnode(self):
        tdLog.info("create snode")
        tdStream.createSnode(1)

    def createDatabase(self):
        tdLog.info(f"create database")

        tdSql.prepare(dbname="qdb", vgroups=1)
        tdSql.prepare(dbname="tdb", vgroups=1)
        tdSql.prepare(dbname="rdb", vgroups=1)
        clusterComCheck.checkDbReady("qdb")
        clusterComCheck.checkDbReady("tdb")
        clusterComCheck.checkDbReady("rdb")

    def prepareQueryData(self):
        tdLog.info("prepare child tables for query")
        tdStream.prepareChildTables(tbBatch=1, rowBatch=1, rowsPerBatch=400)

        tdLog.info("prepare normal tables for query")
        tdStream.prepareNormalTables(tables=10, rowBatch=1)

        tdLog.info("prepare virtual tables for query")
        tdStream.prepareVirtualTables(tables=10)

        tdLog.info("prepare json tag tables for query, include None and primary key")
        tdStream.prepareJsonTables(tbBatch=1, tbPerBatch=10)

        tdLog.info("prepare view")
        tdStream.prepareViews(views=5)

    def prepareTriggerTable(self):
        tdLog.info("prepare tables for trigger")

        stb = "create table tdb.triggers (ts timestamp, c1 int, c2 int) tags(id int);"
        ctb = "create table tdb.t1 using tdb.triggers tags(1) tdb.t2 using tdb.triggers tags(2) tdb.t3 using tdb.triggers tags(3)"
        tdSql.execute(stb)
        tdSql.execute(ctb)

        ntb = "create table tdb.n1 (ts timestamp, c1 int, c2 int)"
        tdSql.execute(ntb)

        vstb = "create stable tdb.vtriggers (ts timestamp, c1 int, c2 int) tags(id int) VIRTUAL 1"
        vctb = "create vtable tdb.v1 (tdb.t1.c1, tdb.t2.c2) using tdb.vtriggers tags(1)"
        tdSql.execute(vstb)
        tdSql.execute(vctb)

    def writeTriggerData(self):
        tdLog.info("write data to trigger table")
        sqls = [
            "insert into tdb.t1 values ('2025-01-01 00:00:00', 0, 0), ('2025-01-01 00:05:00', 5, 50), ('2025-01-01 00:10:00', 10, 100)",
            "insert into tdb.t2 values ('2025-01-01 00:11:00', 11, 110), ('2025-01-01 00:12:00', 12, 20), ('2025-01-01 00:15:00', 15, 150)",
            "insert into tdb.t3 values ('2025-01-01 00:21:00', 21, 210)",
            "insert into tdb.n1 values ('2025-01-01 01:00:00', 100, 1000), ('2025-01-01 01:05:00', 105, 1050), ('2025-01-01 01:10:00', 110, 1100)",
            "insert into tdb.t1 values ('2025-01-01 02:00:00', 200, 2000), ('2025-01-01 02:05:00', 205, 2050), ('2025-01-01 02:10:00', 210, 3100)",
            "insert into tdb.n1 values ('2025-01-01 03:00:00', 300, 3000) ('2025-01-01 03:10:00', 310, 3100)",
        ]
        tdSql.executes(sqls)

    def checkStreamStatus(self):
        tdLog.info(f"wait total:{len(self.streams)} streams run finish")
        tdStream.checkStreamStatus()

    def checkResults(self):
        tdLog.info(f"check total:{len(self.streams)} streams result")
        for stream in self.streams:
            stream.checkResults(print=True)

    def createStreams(self):
        self.streams = []

        stream = StreamItem(
            id=0,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select _twstart ts, _twend te, _twduration td, _twrownum tw, _tgrpid tg, _tlocaltime tl, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twend and _twduration is not null and _twrownum is not null and _tgrpid is not null and _tlocaltime is not null;",
            res_query="select ts, te, td, tw, tg, c1, c2 from rdb.r0;",
            exp_query="select _wstart ts, _wend te, _wduration td, count(cts) tw, 0 as tg, count(cint) c1, avg(cint) c2 from qdb.meters where (cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:25:00') or (cts >= '2025-01-01 02:00:00' and cts < '2025-01-01 02:15:00') interval(5m);",
            check_func=self.check0,
        )
        self.streams.append(stream)

        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            stream.createStream()


    def check0(self):
        tdSql.checkResultsByFunc(
            "desc rdb.r0",
            func=lambda: tdSql.getRows() >= 4
            and tdSql.compareData(0, 0, "ts")
            and tdSql.compareData(0, 1, "te")
            and tdSql.compareData(0, 2, "td")
            and tdSql.compareData(0, 3, "tw")
            and tdSql.compareData(0, 4, "tg")
            and tdSql.compareData(0, 5, "tl")
            and tdSql.compareData(0, 6, "c1")
            and tdSql.compareData(0, 7, "c2")
            and tdSql.compareData(1, 0, "TIMESTAMP")
            and tdSql.compareData(1, 1, "TIMESTAMP")
            and tdSql.compareData(1, 2, "TIMESTAMP")
            and tdSql.compareData(1, 3, "BIGINT")
            and tdSql.compareData(1, 4, "BIGINT")
            and tdSql.compareData(1, 5, "TIMESTAMP")
            and tdSql.compareData(1, 6, "BIGINT")
            and tdSql.compareData(1, 7, "DOUBLE")
            and tdSql.compareData(3, 0, "")
            and tdSql.compareData(3, 1, "")
            and tdSql.compareData(3, 2, "")
            and tdSql.compareData(3, 3, "")
            and tdSql.compareData(3, 4, "")
            and tdSql.compareData(3, 5, "")
            and tdSql.compareData(3, 6, "")
            and tdSql.compareData(3, 7, ""),
        )
