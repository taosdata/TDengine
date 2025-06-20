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
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select _twstart ts, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, c1, c2 from rdb.r0;",
            exp_query="select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check0,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=1,
            stream="create stream rdb.s1 interval(5m) sliding(5m) from tdb.triggers into rdb.r1 as select _twstart ts, _twend te, _twduration td, _twrownum tw, _tgrpid tg, cast(_tlocaltime as bigint) tl, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twend and _twduration is not null and _twrownum is not null and _tgrpid is not null and _tlocaltime is not null;",
            res_query="select ts, te, td, tg, c1, c2 from rdb.r1;",
            exp_query="select _wstart ts, _wend te, _wduration td, 0 tg, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check1,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=2,
            stream="create stream rdb.s2 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r2 as select _twstart ts, _twend te, _twduration td, _twrownum tw, _tgrpid tg, _tlocaltime tl, tbname tb, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twend and _twduration is not null and _twrownum is not null and _tgrpid is not null and _tlocaltime is not null partition by tbname",
            res_query="select ts, te, td, c1, tag_tbname from rdb.r2 where tag_tbname='t1';",
            exp_query="select _wstart ts, _wend te, _wduration td, count(cint) c1, 't1' from qdb.t1 where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check2,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=3,
            stream="create stream rdb.s3 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r3 as select _twstart ts, _twend te, _twduration td, _twrownum tw, _tgrpid tg, _tlocaltime tl, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twend and _twduration is not null and _twrownum is not null and _tgrpid is not null and _tlocaltime is not null;",
            res_query="select ts, te, td, c1, tag_tbname from rdb.r3 where tag_tbname='t1';",
            exp_query="select _wstart ts, _wend te, _wduration td, count(cint) c1, 't1' from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check3,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=4,
            stream="create stream rdb.s4 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r4 as select _twstart ts, _twend te, _twduration td, _twrownum tw, _tgrpid tg, _tlocaltime tl, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twend and _twduration is not null and _twrownum is not null and _tgrpid is not null and _tlocaltime is not null and tbname=%%tbname",
            res_query="select ts, te, td, c1, tag_tbname from rdb.r4 where tag_tbname='t1';",
            exp_query="select _wstart ts, _wend te, _wduration td, count(cint) c1, 't1' from qdb.t1 where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check4,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=5,
            stream="create stream rdb.s5 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r5 as select _twstart ts, _twend te, _twduration td, _twrownum tw, %%tbname as tb, count(c1) c1, avg(c2) c2 from %%tbname where ts >= _twstart and ts < _twend",
            res_query="select ts, te, td, tw, tb, c1, c2, tag_tbname from rdb.r5 where tag_tbname='t1';",
            exp_query="select _wstart, _wend, _wduration, count(c1), 't1', count(c1), avg(c2), 't1' from tdb.t1 where ts >= '2025-01-01 00:00:00' and ts < '2025-01-01 00:35:00' interval(5m) fill(value, 0, 0, null);",
            check_func=self.check5,
        )
        # self.streams.append(stream) basic4

        stream = StreamItem(
            id=6,
            stream="create stream rdb.s6 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r6 as select _twstart ts, count(c1), avg(c2) from %%trows where ts >= _twstart and ts < _twend partition by tbname",
            res_query="select *, tag_tbname from rdb.r6 where tag_tbname='t1'",
            exp_query="select _wstart, count(c1), avg(c2), 't1', 't1' from tdb.t1 where ts >= '2025-01-01 00:00:00' and ts < '2025-01-01 00:35:00' interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=7,
            stream="create stream rdb.s7 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r7 as select _twstart ts, count(c1), avg(c2) from %%tbname where ts >= _twstart and ts < _twend and %%tbname = tbname",
            res_query="select *, tag_tbname from rdb.r7 where tag_tbname='t1'",
            exp_query="select _wstart, count(c1), avg(c2), 't1', 't1' from tdb.t1 where ts >= '2025-01-01 00:00:00' and ts < '2025-01-01 00:35:00' interval(5m) fill(value, 0, null);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=8,
            stream="create stream rdb.s8 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r8 as select _twstart ts, count(c1), avg(c2) from %%trows where ts >= _twstart and ts < _twend partition by %%1",
            res_query="select *, tag_tbname from rdb.r8 where tag_tbname='t1'",
            exp_query="select _wstart, count(c1), avg(c2), 't1', 't1' from tdb.t1 where ts >= '2025-01-01 00:00:00' and ts < '2025-01-01 00:35:00' interval(5m);",
        )
        self.streams.append(stream)
        
        stream = StreamItem(
            id=9,
            stream="create stream rdb.s9 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r9 tags(gid bigint as _tgrpid, id int as %%1) as select _twstart ts, _twend te, _twduration td, _twrownum tw, _tgrpid tg, _tlocaltime tl, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twend and _twduration is not null and _twrownum is not null and _tgrpid is not null and _tlocaltime is not null;",
            res_query="select ts, te, c1, c2, id from rdb.r9 where id=1;",
            exp_query="select _wstart, _wend, count(cint) c1, avg(cint) c2, 1 from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
        )
        self.streams.append(stream)
        
        stream = StreamItem(
            id=11,
            stream="create stream rdb.s11 sliding(5m) from tdb.n1 into rdb.r11 as select _tprev_ts tp, _tcurrent_ts tc, _tnext_ts tn, _tgrpid tg, _tlocaltime tl, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _tprev_ts and cts < _tcurrent_ts and _tgrpid is not null and _tlocaltime is not null and tbname != 't1';",
            res_query="select tp, tc, tn, tg, c1, c2 from rdb.r11;",
            exp_query="select _wstart, _wend, _wend + 5m, 0, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= '2025-01-01 00:25:00.000' and cts < '2025-01-01 00:40:00.000' and tbname != 't1' interval(5m);",
            check_func=self.check11,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=12,
            stream="create stream rdb.s12 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r12 as select _twstart ts, %%tbname tb, %%1, count(*) v1, avg(c1) v2, first(c1) v3, last(c1) v4 from %%trows where c2 > 0;",
            res_query="select ts, tb, `%%1`, v2, v3, v4, tag_tbname from rdb.r12 where tb='t1'",
            exp_query="select _wstart, 't1', 't1', avg(c1) v2, first(c1) v3, last(c1) v4, 't1' from tdb.t1 where ts >= '2025-01-01 00:00:00' and ts < '2025-01-01 00:35:00' interval(5m) fill(NULL);",
            check_func=self.check12,
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

        tdSql.checkResultsBySql(
            sql="select ts, tw from rdb.r1;",
            exp_sql="select _wstart, count(*) from tdb.triggers where ts >= '2025-01-01 00:00:00' and ts < '2025-01-01 00:35:00' interval(5m) fill(value, 0);",
        )

    def check2(self):
        tdSql.checkTableType(
            dbname="rdb",
            stbname="r2",
            columns=9,
            tags=1,
        )
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r2",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["te", "TIMESTAMP", 8, ""],
                ["td", "BIGINT", 8, ""],
                ["tw", "BIGINT", 8, ""],
                ["tg", "BIGINT", 8, ""],
                ["tl", "TIMESTAMP", 8, ""],
                ["tb", "VARCHAR", 270, ""],
                ["c1", "BIGINT", 8, ""],
                ["c2", "DOUBLE", 8, ""],
                ["tag_tbname", "VARCHAR", 270, "TAG"],
            ],
        )
        tdSql.checkResultsByFunc(
            sql="select * from information_schema.ins_tags where db_name='rdb' and stable_name='r2' and tag_name='tag_tbname' and (tag_value='t1' or tag_value='t2');",
            func=lambda: tdSql.getRows() == 2,
        )
        tdSql.checkResultsByFunc(
            sql="select ts, te, td, c1, tag_tbname from rdb.r2 where tag_tbname='t2'",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2025-01-01 00:10:00.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:15:00.000")
            and tdSql.compareData(0, 2, 300000)
            and tdSql.compareData(0, 3, 10)
            and tdSql.compareData(0, 4, "t2"),
        )

    def check3(self):
        tdSql.checkTableType(
            dbname="rdb",
            stbname="r3",
            columns=8,
            tags=1,
        )
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r3",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["te", "TIMESTAMP", 8, ""],
                ["td", "BIGINT", 8, ""],
                ["tw", "BIGINT", 8, ""],
                ["tg", "BIGINT", 8, ""],
                ["tl", "TIMESTAMP", 8, ""],
                ["c1", "BIGINT", 8, ""],
                ["c2", "DOUBLE", 8, ""],
                ["tag_tbname", "VARCHAR", 270, "TAG"],
            ],
        )
        tdSql.checkResultsByFunc(
            sql="select * from information_schema.ins_tags where db_name='rdb' and stable_name='r3' and tag_name='tag_tbname';",
            func=lambda: tdSql.getRows() == 2,
        )
        tdSql.checkResultsByFunc(
            sql="select ts, te, td, c1, tag_tbname from rdb.r3 where tag_tbname='t2'",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2025-01-01 00:10:00.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:15:00.000")
            and tdSql.compareData(0, 2, 300000)
            and tdSql.compareData(0, 3, 1000)
            and tdSql.compareData(0, 4, "t2"),
        )

    def check4(self):
        tdSql.checkTableType(
            dbname="rdb",
            stbname="r4",
            columns=8,
            tags=1,
        )
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r4",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["te", "TIMESTAMP", 8, ""],
                ["td", "BIGINT", 8, ""],
                ["tw", "BIGINT", 8, ""],
                ["tg", "BIGINT", 8, ""],
                ["tl", "TIMESTAMP", 8, ""],
                ["c1", "BIGINT", 8, ""],
                ["c2", "DOUBLE", 8, ""],
                ["tag_tbname", "VARCHAR", 270, "TAG"],
            ],
        )
        tdSql.checkResultsByFunc(
            sql="select * from information_schema.ins_tags where db_name='rdb' and stable_name='r4' and tag_name='tag_tbname';",
            func=lambda: tdSql.getRows() == 2,
        )
        tdSql.checkResultsByFunc(
            sql="select ts, te, td, c1, tag_tbname from rdb.r4 where tag_tbname='t2'",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2025-01-01 00:10:00.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:15:00.000")
            and tdSql.compareData(0, 2, 300000)
            and tdSql.compareData(0, 3, 10)
            and tdSql.compareData(0, 4, "t2"),
        )

    def check5(self):
        tdSql.checkResultsByFunc(
            sql="select * from information_schema.ins_tags where db_name='rdb' and stable_name='r5' and tag_name='tag_tbname';",
            func=lambda: tdSql.getRows() == 2,
        )
        tdSql.checkResultsByFunc(
            sql="select ts, te, td, c1, tag_tbname from rdb.r5 where tag_tbname='t2'",
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, "2025-01-01 00:10:00.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:15:00.000")
            and tdSql.compareData(0, 2, 300000)
            and tdSql.compareData(0, 3, 2)
            and tdSql.compareData(0, 4, "t2"),
        )

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
        
    def check12(self):
        tdSql.checkTableType(
            dbname="rdb",
            stbname="r12",
            columns=7,
            tags=1,
        )
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r12",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["tb", "VARCHAR", 270, ""],
                ["%%1", "VARCHAR", 270, ""],
                ["v1", "BIGINT", 8, ""],
                ["v2", "DOUBLE", 8, ""],
                ["v3", "INT", 4, ""],
                ["v4", "INT", 4, ""],
                ["tag_tbname", "VARCHAR", 270, "TAG"],
            ],
        )
