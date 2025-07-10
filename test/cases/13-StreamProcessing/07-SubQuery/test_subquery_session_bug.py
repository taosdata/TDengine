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
            "insert into tdb.t1 values ('2025-01-01 00:00:00', 0,  0  ) ('2025-01-01 00:01:00', 0,  10 ) ('2025-01-01 00:05:00', 10, 0)",
            "insert into tdb.t2 values ('2025-01-01 00:15:00', 11, 110) ('2025-01-01 00:16:00', 11, 120) ('2025-01-01 00:20:00', 21, 210)",
            "insert into tdb.t3 values ('2025-01-01 00:20:00', 20, 210)",
            "insert into tdb.n1 values ('2025-01-01 00:25:00', 25, 0  ) ('2025-01-01 00:26:00', 25, 10 ) ('2025-01-01 00:30:00', 30, 0)",
            "insert into tdb.t1 values ('2025-01-01 00:06:00', 10, 10 ) ('2025-01-01 00:10:00', 20, 0  ) ('2025-01-01 00:11:00', 20, 10 ) ('2025-01-01 00:30:00', 30, 0) ('2025-01-01 00:31:00', 30, 10) ('2025-01-01 00:35:00', 40, 0) ('2025-01-01 00:36:00', 40, 10)",
            "insert into tdb.n1 values ('2025-01-01 00:31:00', 30, 10 ) ('2025-01-01 00:40:00', 40, 0  )",   
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
            id=1,
            stream="create stream rdb.s1 session(ts, 2m) from tdb.triggers into rdb.r1 as select _twstart ts, _twstart + 5m te, _twduration td, _twrownum tw, _tgrpid tg, cast(_tlocaltime as bigint) tl, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twstart + 5m and _twduration is not null and _twrownum is not null and _tgrpid is not null and _tlocaltime is not null;",
            res_query="select ts, te, td, tg, c1, c2 from rdb.r1 where ts >= '2025-01-01 00:00:00' and ts < '2025-01-01 00:15:00';",
            exp_query="select _wstart ts, _wend te, 60000, 0 tg, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:15:00' interval(5m);",
            check_func=self.check1,
        )
        self.streams.append(stream)

        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            stream.createStream()

    def check1(self):
        tdSql.checkTableType(
            dbname="rdb", tbname="r1", typename="NORMAL_TABLE", columns=8
        )
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r1",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["te", "TIMESTAMP", 8, ""],
                ["td", "BIGINT", 8, ""],
                ["tw", "BIGINT", 8, ""],
                ["tg", "BIGINT", 8, ""],
                ["tl", "BIGINT", 8, ""],
                ["c1", "BIGINT", 8, ""],
                ["c2", "DOUBLE", 8, ""],
            ],
        )

        # tdSql.checkResultsBySql(
        #     sql="select ts, tw from rdb.r1;",
        #     exp_sql="select _wstart, count(*) from tdb.triggers where ts >= '2025-01-01 00:00:00' and ts < '2025-01-01 00:35:00' interval(5m) fill(value, 0);",
        # )