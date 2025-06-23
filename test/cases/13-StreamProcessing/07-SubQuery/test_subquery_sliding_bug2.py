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

        # stream = StreamItem(
        #     id=19,
        #     stream="create stream rdb.s19 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r19 as select _twend tw, sum(c1) sumcnt from (select tbname, count(*) c1 from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000' and tint=%%1 partition by tbname)",
        #     res_query="select tw, sumcnt from rdb.r19 where id = 1",
        #     exp_query="select _wend, count(*) from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:35:00.000' and tint=1 interval(5m)",
        # )
        
        # stream = StreamItem(
        #     id=20,
        #     stream="create stream rdb.s20 interval(5m) sliding(5m) from tdb.triggers partition by id, name into rdb.r20 as select _twend tw, count(*) c1, _tgrpid tg, _tlocaltime tl from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000' and tint=%%1;",
        #     res_query="select tw, c1, id, name from rdb.r20 where id=1",
        #     exp_query="select _wend, count(*) cnt, 1, '1' from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:35:00.000' and tint=1 interval(5m)",
        # )
        
        # stream = StreamItem(
        #     id=21,
        #     stream="create stream rdb.s21 sliding(5m) from tdb.n1 into rdb.r21 as select _tprev_ts tw, count(*) c1, _tgrpid tg, _tlocaltime tl from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000'",
        #     res_query="select tw, c1, tg from rdb.r21",
        #     exp_query="select _wend, count(*) cnt, 0 from qdb.meters where cts >= '2025-01-01 00:20:00.000' and cts < '2025-01-01 00:35:00.000' interval(5m);",
        # )
        
        # stream = StreamItem(
        #     id=22,
        #     stream="create stream rdb.s22 interval(5m) sliding(5m) from tdb.triggers into rdb.r22 as select _twend tw, sum(cint) c1, _tgrpid tg, _tlocaltime tl from qdb.meters where cts >= _twstart and cts < _twend",
        #     res_query="select tw, c1 from rdb.r22;",
        #     exp_query="select _wend, sum(cint) from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:35:00.000' interval(5m);",
        # )     
        
        # stream = StreamItem(
        #     id=23,
        #     stream="create stream rdb.s23 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r23 as select _twend tw, sum(cint) c1, _tgrpid tg, _tlocaltime tl from qdb.meters where cts >= _twstart and cts < _twend and tbname=%%tbname partition by tbname",
        #     res_query="select tw, c1, tag_tbname from rdb.r23 where tag_tbname='t1';",
        #     exp_query="select _wend, sum(cint), tbname from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:35:00.000' and tbname='t1' partition by tbname interval(5m);",
        # )
        
        # stream = StreamItem(
        #     id=24,
        #     stream="create stream rdb.s24 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r24 as select _twend tw, sum(cint) c1, %%1 c2 from qdb.meters where cts >= _twstart and cts < _twend and tint=%%1 partition by tint",
        #     res_query="select tw, c1, c2, id from rdb.r24 where id=1;",
        #     exp_query="select _wend, sum(cint), tint, tint from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:35:00.000' and tint=1 partition by tint interval(5m);",
        # )
        
        # stream = StreamItem(
        #     id=26,
        #     stream="create stream rdb.s26 interval(5m) sliding(5m) from tdb.triggers into rdb.r26 as select _twstart tw, sum(cint) c1, tbname from qdb.meters where cts >= _twstart and cts < _twend and tbname='t18' partition by tbname",
        #     res_query="select * from rdb.r26",
        #     exp_query="select _wstart, sum(cint), tbname from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:35:00.000' and tbname='t18' partition by tbname interval(5m);",
        # )
        
        stream = StreamItem(
            id=205,
            stream="create stream rdb.s205 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r205 as select _twstart, sum(cint), FIRST(cint) from qdb.meters interval(2m)",
            res_query="select * from rdb.r205",
            exp_query="select sum(cint) cnt from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000'",
        )
        self.streams.append(stream)

        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            stream.createStream()

    def check11(self):
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r11",
            schema=[
                ["tp", "TIMESTAMP", 8, ""],
                ["tc", "TIMESTAMP", 8, ""],
                ["tn", "TIMESTAMP", 8, ""],
                ["tg", "BIGINT", 8, ""],
                ["tl", "TIMESTAMP", 8, ""],
                ["c1", "BIGINT", 8, ""],
                ["c2", "DOUBLE", 8, ""],
            ],
        )
