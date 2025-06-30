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
        self.checkStreamStatus()
        self.writeTriggerData()
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

        stb = "create table tdb.triggers (ts timestamp, c1 int, c2 int) tags(id int, name varchar(16));"
        ctb = "create table tdb.t1 using tdb.triggers tags(1, '1') tdb.t2 using tdb.triggers tags(2, '2') tdb.t3 using tdb.triggers tags(3, '3')"
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
            "insert into tdb.t1 values ('2025-01-01 00:00:00', 0,  0  ) ('2025-01-01 00:05:00', 5,  50 ) ('2025-01-01 00:10:00', 10, 100)",
            "insert into tdb.t2 values ('2025-01-01 00:11:00', 11, 110) ('2025-01-01 00:12:00', 12, 120) ('2025-01-01 00:15:00', 15, 150)",
            "insert into tdb.t3 values ('2025-01-01 00:21:00', 21, 210)",
            "insert into tdb.n1 values ('2025-01-01 00:25:00', 25, 250) ('2025-01-01 00:26:00', 26, 260) ('2025-01-01 00:27:00', 27, 270)",
            "insert into tdb.t1 values ('2025-01-01 00:30:00', 30, 300) ('2025-01-01 00:32:00', 32, 320) ('2025-01-01 00:36:00', 36, 360)",
            "insert into tdb.n1 values ('2025-01-01 00:40:00', 40, 400) ('2025-01-01 00:42:00', 42, 420)",
        ]
        tdSql.executes(sqls)

    def checkStreamStatus(self):
        tdLog.info(f"wait total:{len(self.streams)} streams run finish")
        tdStream.checkStreamStatus()

    def checkResults(self):
        tdLog.info(f"check total:{len(self.streams)} streams result")
        for stream in self.streams:
            stream.checkResults()

    def createStreams(self):
        self.streams = []

        stream = StreamItem(
            id=74,
            stream="create stream rdb.s74 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r74 as select first(cts), _twstart, %%tbname, cint, avg(cfloat), count(cdouble), sum(cfloat), twa(cdouble) from qdb.meters where cts >= _twstart and cts < _twend and tbname = %%1 group by cint having count(cdouble) == 1 order by first(cts);",
            res_query="select `first(cts)`, cint, `avg(cfloat)`, `count(cdouble)`, `sum(cfloat)`, `twa(cdouble)` from rdb.r74 where tag_tbname='t1'",
            exp_query="select first(cts), cint, avg(cfloat), count(cdouble), sum(cfloat), twa(cdouble) from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:35:00.000' and tbname = 't1' group by cint having count(cdouble) == 1 order by first(cts);",
        )
        self.streams.append(stream)

        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            stream.createStream()

    def check0(self):
        tdSql.checkTableType(
            dbname="rdb", tbname="r0", typename="NORMAL_TABLE", columns=3
        )
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r0",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["c1", "BIGINT", 8, ""],
                ["c2", "DOUBLE", 8, ""],
            ],
        )
