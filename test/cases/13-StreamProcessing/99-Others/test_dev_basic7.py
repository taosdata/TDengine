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
            id=0,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _twstart ts, count(c1), avg(c2), from %%tbname where ts >= _twstart and ts < _twend and %%tbname = 't1'",
            # stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _twstart ts, count(c1), avg(c2), %%tbname from %%tbname where ts >= _twstart and ts < _twend and %%tbname = 't1'",
            # stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _twstart ts, count(c1), avg(c2) from %%tbname where ts >= _twstart and ts < _twend",
            res_query="",
            check_func=self.check0,
        )
        self.streams.append(stream)

        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            stream.createStream()

    def check0(self):
        tdSql.checkTableType(
            dbname="rdb", stbname="r0", tags = 1, columns=3
        )

        tdSql.checkResultsByFunc(
            sql="select *, tag_tbname from rdb.r0 where tag_tbname='t1'",
            func=lambda: tdSql.getRows() == 7
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.001")
            and tdSql.compareData(1, 0, "2025-01-01 00:05:00.000")
            and tdSql.compareData(2, 0, "2025-01-01 00:10:00.000")
            and tdSql.compareData(3, 0, "2025-01-01 00:15:00.000")
            and tdSql.compareData(4, 0, "2025-01-01 00:20:00.000")
            and tdSql.compareData(5, 0, "2025-01-01 00:25:00.000")
            and tdSql.compareData(6, 0, "2025-01-01 00:30:00.000")
            and tdSql.compareData(0, 1, 1)
            and tdSql.compareData(1, 1, 1)
            and tdSql.compareData(2, 1, 1)
            and tdSql.compareData(3, 1, 0)
            and tdSql.compareData(4, 1, 0)
            and tdSql.compareData(5, 1, 0)
            and tdSql.compareData(6, 1, 2)
            and tdSql.compareData(0, 2, 0)
            and tdSql.compareData(1, 2, 50)
            and tdSql.compareData(2, 2, 100)
            and tdSql.compareData(3, 2, None)
            and tdSql.compareData(4, 2, None)
            and tdSql.compareData(5, 2, None)
            and tdSql.compareData(6, 2, 310)
        )

        tdSql.checkResultsByFunc(
            sql="select *, tag_tbname from rdb.r0 where tag_tbname='t2'",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2025-01-01 00:10:00.000")
            and tdSql.compareData(0, 1, 2)
            and tdSql.compareData(0, 2, 115),
        )
