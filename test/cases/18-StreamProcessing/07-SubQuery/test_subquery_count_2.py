import time
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamSubqueryCount:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_subquery_count_2(self):
        """Subquery: count(2)

        1. Use count trigger mode

        2. Output results include 4 dimensions:
            No grouping
            Group by table name
            Group by tags
            Group by ordinary columns

        3. Generate 100 SQL statements using the following syntax combinations:
            Tables: system tables, super tables, child tables, normal tables, virtual super tables, virtual child tables
            Functions:
                Single-row functions (math/string/conversion/time functions)
                Aggregate functions
                Selection functions
                Time-series-specific functions
                Geometry functions
                System functions
            Queries: projection queries, nested queries, join queries, window queries (time/event/count/session/state), SHOW commands, GROUP BY, PARTITION BY, ORDER BY, LIMIT, SLIMIT, UNION, etc.
            Filters: time comparisons, ordinary column comparisons, tag column comparisons
            Operators: arithmetic, string, bitwise, comparison, logical, JSON operators
            Others:
                Queries on databases/tables same as/different from the trigger table
                View queries

        4. Include the following combinations in step 3 query results:
            Use all data types: numeric, binary, string, geometry, json, etc.
            Use all pseudo-columns: _qstart, _qend, _wstart, _wend, _wduration, _c0, _rowts, irowts, _irowtsorigin, tbname, etc.
            Include data columns and tag columns
            Randomly include None and NULL in result sets
            Result set sizes: 1 row, n rows
            Include duplicate timestamp in result sets

        5. Test placeholder usage in step 3's queries, including:
            Placeholders in various positions like FROM, SELECT, WHERE
            Each placeholder: _twstart, _twend, _twduration, _twrownum, _tcurrent_ts, _tgrpid, _tlocaltime, %%n, %%tbname, %%tbrows

        6. Validation checks:
            Verify table structures and table counts
            Validate correctness of calculation results
            Validate the accuracy of placeholder data, such as %%trows

        Catalog:
            - Streams:SubQuery

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-30 Simon Guan Create Case

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
        vctb1 = (
            "create vtable tdb.v1 (tdb.t1.c1, tdb.t1.c2) using tdb.vtriggers tags(1)"
        )
        vctb2 = (
            "create vtable tdb.v2 (tdb.t1.c1, tdb.t2.c2) using tdb.vtriggers tags(2)"
        )
        tdSql.execute(vstb)
        tdSql.execute(vctb1)
        tdSql.execute(vctb2)

    def writeTriggerData(self):
        tdLog.info("write data to trigger table")
        sqls = [
            "insert into tdb.t1 values ('2025-01-01 00:00:00', 0,  0  ) ('2025-01-01 00:01:00', 0,  10 ) ('2025-01-01 00:05:00', 10, 0)",
            "insert into tdb.t2 values ('2025-01-01 00:15:00', 11, 0  ) ('2025-01-01 00:16:00', 11, 10 ) ('2025-01-01 00:20:00', 21, 210)",
            "insert into tdb.t3 values ('2025-01-01 00:20:00', 20, 210)",
            "insert into tdb.n1 values ('2025-01-01 00:25:00', 25, 0  ) ('2025-01-01 00:26:00', 25, 10 ) ('2025-01-01 00:30:00', 30, 0)",
            "insert into tdb.t1 values ('2025-01-01 00:06:00', 10, 10 ) ('2025-01-01 00:10:00', 20, 0  ) ('2025-01-01 00:11:00', 20, 10 ) ('2025-01-01 00:30:00', 30, 0) ('2025-01-01 00:31:00', 30, 10) ('2025-01-01 00:35:00', 40, 0)",
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
        tdLog.info(f"check total:{len(self.streams)} streams result successfully")

    def createStreams(self):
        self.streams = []

        stream = StreamItem(
            id=0,
            stream="create stream rdb.s0 count_window(2, c1) from tdb.v1 into rdb.r0 as select _twstart ts, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twstart + 5m;",
            res_query="select ts, c1, c2 from rdb.r0 limit 3;",
            exp_query="select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:15:00' interval(5m);",
            check_func=self.check0,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=1,
            stream="create stream rdb.s1 count_window(2, c1) from tdb.v1 partition by tbname into rdb.r1 as select _twstart ts, _twstart + 5m te, _twduration td, _twrownum tw, _tgrpid tg, cast(_tlocaltime as bigint) tl, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twstart + 5m and _twduration is not null and _twrownum is not null and _tgrpid is not null and _tlocaltime is not null;",
            res_query="select ts, te, td, c1, c2 from rdb.r1 limit 3;",
            exp_query="select _wstart ts, _wend te, _wduration / 5 td, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:15:00' interval(5m);",
            check_func=self.check1,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=2,
            stream="create stream rdb.s2 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r2 as select _twstart ts, _twstart + 5m te, _twduration td, _twrownum tw, _tgrpid tg, _tlocaltime tl, tbname tb, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twstart + 5m and _twduration is not null and _twrownum is not null and _tgrpid is not null and _tlocaltime is not null partition by tbname",
            res_query="select ts, te, td, c1, tag_tbname from rdb.r2 where tag_tbname='t1' limit 3;",
            exp_query="select _wstart ts, _wend te, 60000, count(cint) c1, 't1' from qdb.t1 where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:15:00' interval(5m);",
            check_func=self.check2,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=3,
            stream="create stream rdb.s3 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r3 as select _twstart ts, _twstart + 5m te, _twduration td, _twrownum tw, _tgrpid tg, _tlocaltime tl, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twstart + 5m and _twduration is not null and _twrownum is not null and _tgrpid is not null and _tlocaltime is not null;",
            res_query="select ts, te, td, c1, tag_tbname from rdb.r3 where tag_tbname='t1' limit 3;",
            exp_query="select _wstart ts, _wend te, _wduration / 5 td , count(cint) c1, 't1' from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:15:00' interval(5m);",
            check_func=self.check3,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=4,
            stream="create stream rdb.s4 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r4 as select _twstart ts, _twstart + 5m te, _twduration td, _twrownum tw, _tgrpid tg, _tlocaltime tl, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twstart + 5m and _twduration is not null and _twrownum is not null and _tgrpid is not null and _tlocaltime is not null and tbname=%%tbname",
            res_query="select ts, te, td, c1, tag_tbname from rdb.r4 where tag_tbname='t1' limit 3;",
            exp_query="select _wstart ts, _wend te, _wduration / 5 td, count(cint) c1, 't1' from qdb.t1 where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:15:00' interval(5m);",
            check_func=self.check4,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=5,
            stream="create stream rdb.s5 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r5 as select _twstart ts, _twstart + 5m te, _twduration td, _twrownum tw, %%tbname as tb, count(c1) c1, avg(c2) c2 from %%tbname where ts >= _twstart and ts < _twstart + 5m",
            res_query="select ts, te, td, tw, tb, c1, c2, tag_tbname from rdb.r5 where tag_tbname='t1' limit 3;",
            exp_query="select _wstart, _wend, _wduration / 5, count(c1), 't1', count(c1), avg(c2), 't1' from tdb.t1 where ts >= '2025-01-01 00:00:00' and ts < '2025-01-01 00:15:00' interval(5m) fill(value, 0, 0, null);",
            check_func=self.check5,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=6,
            stream="create stream rdb.s6 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r6 as select _twstart ts, count(c1), avg(c2) from %%trows partition by tbname",
            res_query="select *, tag_tbname from rdb.r6 where tag_tbname='t1'",
            exp_query="select _wstart, count(c1), avg(c2), 't1', 't1' from tdb.t1 where ts >= '2025-01-01 00:00:00' and ts < '2025-01-01 00:35:00' interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=7,
            stream="create stream rdb.s7 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r7 as select _twstart ts, count(c1), avg(c2) from %%tbname where ts >= _twstart and ts <= _twend and %%tbname = tbname",
            res_query="select *, tag_tbname from rdb.r7 where tag_tbname='t1' limit 3",
            exp_query="select _wstart, count(c1), avg(c2), 't1', 't1' from tdb.t1 where ts >= '2025-01-01 00:00:00' and ts < '2025-01-01 00:15:00' interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=8,
            stream="create stream rdb.s8 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r8 as select _twstart ts, count(c1), avg(c2) from %%trows",
            res_query="select *, tag_tbname from rdb.r8 where tag_tbname='t1' and `count(c1)` != 0;",
            exp_query="select _wstart, count(c1), avg(c2), 't1', 't1' from tdb.t1 where ts >= '2025-01-01 00:00:00' and ts < '2025-01-01 00:35:00' interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=9,
            stream="create stream rdb.s9 count_window(2, c1) from tdb.triggers partition by id, tbname into rdb.r9 tags(gid bigint as _tgrpid, id int as %%1) as select _twstart ts, _twstart + 5m te, _twduration td, _twrownum tw, _tgrpid tg, _tlocaltime tl, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twstart + 5m and _twduration is not null and _twrownum is not null and _tgrpid is not null and _tlocaltime is not null;",
            res_query="select ts, te, c1, c2, id from rdb.r9 where id=1 limit 3;",
            exp_query="select _wstart, _wend, count(cint) c1, avg(cint) c2, 1 from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:15:00' interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=10,
            stream="create stream rdb.s10 count_window(2, c1) from tdb.triggers partition by id, tbname into rdb.r10 as select _twstart ts, _twstart + 5m te, _twduration td, _twrownum tw, _tgrpid tg, cast(_tlocaltime % 1000000 as timestamp) tl, %%1 t1_data, %%2 t2_data, %%tbname tb_data, count(cint) c1_data, avg(cint) c2_data from qdb.meters where cts >= _twstart and cts < _twstart + 5m and _twduration is not null and _twrownum is not null and _tgrpid is not null and _tlocaltime is not null and tbname = %%2;",
            res_query="select ts, t1_data, t2_data, tb_data, c1_data, c2_data, id, tag_tbname from rdb.r10 where id=1 limit 3;",
            exp_query="select _wstart, 1, 't1', 't1', count(cint) c1, avg(cint) c2, 1, 't1' from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:15:00' and tbname='t1' interval(5m);",
            check_func=self.check10,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=11,
            stream="create stream rdb.s11 count_window(2, c1) from tdb.n1 into rdb.r11 as select _twstart tc, _twstart - 5m tp, _twstart + 5m tn, case when _tgrpid != 0 then 1 else 0 end tg, _tlocaltime tl, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twstart + 5m and _tgrpid is not null and _tlocaltime is not null and tbname != 't1';",
            res_query="select tc, tp, tn, tg, c1, c2 from rdb.r11 limit 2;",
            exp_query="select _wstart, _wstart - 5m, _wend, 0, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= '2025-01-01 00:25:00.000' and cts < '2025-01-01 00:35:00.000' and tbname != 't1' interval(5m);",
            check_func=self.check11,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=12,
            stream="create stream rdb.s12 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r12 as select _twstart ts, %%tbname tb, %%1, count(*) v1, avg(c1) v2, first(c1) v3, last(c1) v4 from %%trows;",
            res_query="select ts, tb, `%%1`, v2, v3, v4, tag_tbname from rdb.r12 where tb='t1'",
            exp_query="select _wstart, 't1', 't1', avg(c1) v2, first(c1) v3, last(c1) v4, 't1' from tdb.t1 where ts >= '2025-01-01 00:00:00' and ts < '2025-01-01 00:35:00' and c2 > 0 interval(5m);",
            check_func=self.check12,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=13,
            stream="create stream rdb.s13 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r13 as select _twstart ts, _twstart + 5m te, _twduration td, _twrownum tw, _tgrpid tg, _tlocaltime tl, %%1 t1 , %%tbname t2, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twstart + 5m and _twduration is not null and _twrownum is not null and _tgrpid is not null and _tlocaltime is not null and %%1 = tbname and %%tbname = tbname;",
            res_query="select ts, t1, t2, c1, c2 from rdb.r13 where tag_tbname = 't1' limit 3",
            exp_query="select _wstart ts, 't1', 't1', count(cint) c1, avg(cint) c2 from qdb.meters where cts >='2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' and tbname = 't1' interval(5m)",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=14,
            stream="create stream rdb.s14 count_window(2, c1) from tdb.triggers partition by id, tbname into rdb.r14 as select _twstart ts, %%tbname t1, %%1 t2, count(*) c1, avg(c1) c2, first(c1) c3, last(c1) c4 from %%trows;",
            res_query="select ts, t1, t2, c1, c2, c3, c4 from rdb.r14 where id = 1 limit 3",
            exp_query="select _wstart ts, 't1', 1, count(*) c1, avg(c1) c2, first(c1) c3, last(c1) c4 from tdb.t1 where ts >='2025-01-01 00:00:00.000' and ts < '2025-01-01 00:15:00.000' interval(5m) fill(value, 0, null, null, null);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=15,
            stream="create stream rdb.s15 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r15 as select _wstart, sum(cint), FIRST(cint) from qdb.meters where cts >= _twstart and cts < _twstart + 5m interval(150s)",
            res_query="select * from rdb.r15 where tag_tbname='t1' limit 6;",
            exp_query="select _wstart, sum(cint), FIRST(cint), 't1' from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' interval(150s);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=16,
            stream="create stream rdb.s16 count_window(2, c1) from tdb.n1 into rdb.r16 as select _twstart ts, count(*) c1, avg(cint) c2, _twstart + 1 as ts2, case when _tgrpid != 0 then 1 else 0 end from qdb.meters where tbname = 't1' partition by tbname;",
            res_query="select * from rdb.r16 limit 1;",
            exp_query="select cast('2025-01-01 00:25:00.000' as timestamp), count(*), avg(cint), cast('2025-01-01 00:25:00.001' as timestamp), 0 from qdb.meters where tbname = 't1'",
            check_func=self.check16,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=17,
            stream="create stream rdb.s17 count_window(2, c1) from tdb.v1 into rdb.r17 as select _wstart, sum(cint), FIRST(cint) from qdb.meters interval(5m)",
            res_query="select * from rdb.r17",
            exp_query="select _wstart, sum(cint), FIRST(cint) from qdb.meters interval(5m)",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=18,
            stream="create stream rdb.s18 count_window(2, c1) from tdb.triggers partition by id, tbname into rdb.r18 tags(t1 int as %%1, t2 int as cast(%%2 as int)) as select _twstart c1, first(tw) c2, last(te) c3, count(tb) c4, sum(cnt) c5 from (select _wstart tw, _wend te, tbname tb, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twstart + 5m and tint=%%1 partition by tbname count_window(1000))",
            res_query="select * from rdb.r18 where t1=1 limit 1",
            exp_query="select first(tw), first(tw), last(te), count(tb), sum(cnt), 1, 0 from (select _wstart tw, _wend te, tbname tb, count(*) cnt from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000' and tint=1 partition by tbname count_window(1000));",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=19,
            stream="create stream rdb.s19 count_window(2, c1) from tdb.triggers partition by id, tbname into rdb.r19 as select _twend + 4m tw, sum(c1) sumcnt from (select tbname, count(*) c1 from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000' and tint=%%1 partition by tbname)",
            res_query="select tw, sumcnt from rdb.r19 where id = 1 limit 3",
            exp_query="select _wend, count(*) from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' and tint=1 interval(5m)",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=20,
            stream="create stream rdb.s20 count_window(2, c1) from tdb.triggers partition by id, name, tbname into rdb.r20 as select _twend + 4m tw, count(*) c1, _tgrpid tg, _tlocaltime tl from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000' and tint=%%1;",
            res_query="select tw, c1, id, name from rdb.r20 where id=1 limit 3",
            exp_query="select _wend, count(*) cnt, 1, '1' from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' and tint=1 interval(5m)",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=21,
            stream="create stream rdb.s21 count_window(2, c1) from tdb.n1 into rdb.r21 as select _twstart tw, count(*) c1, case when _tgrpid != 0 then 1 else 0 end tg, _tlocaltime tl from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000'",
            res_query="select tw, c1, tg from rdb.r21",
            exp_query="select _wstart, count(*) cnt, 0 from qdb.meters where cts >= '2025-01-01 00:25:00.000' and cts < '2025-01-01 00:35:00.000' interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=22,
            stream="create stream rdb.s22 count_window(2, c1) from tdb.v1 into rdb.r22 as select _twend + 4m tw, sum(cint) c1, _tgrpid tg, _tlocaltime tl from qdb.meters where cts >= _twstart and cts < _twstart + 5m",
            res_query="select tw, c1 from rdb.r22 limit 3;",
            exp_query="select _wend, sum(cint) from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=23,
            stream="create stream rdb.s23 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r23 as select _twend + 4m tw, sum(cint) c1, _tgrpid tg, _tlocaltime tl from qdb.meters where cts >= _twstart and cts < _twstart + 5m and tbname=%%tbname partition by tbname",
            res_query="select tw, c1, tag_tbname from rdb.r23 where tag_tbname='t1' limit 3;",
            exp_query="select _wend, sum(cint), tbname from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' and tbname='t1' partition by tbname interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=24,
            stream="create stream rdb.s24 count_window(2, c1) from tdb.triggers partition by id, tbname into rdb.r24 as select _twend + 4m tw, sum(cint) c1, %%1 c2 from qdb.meters where cts >= _twstart and cts < _twstart + 5m and tint=%%1 partition by tint",
            res_query="select tw, c1, c2, id from rdb.r24 where id=1 limit 3;",
            exp_query="select _wend, sum(cint), tint, tint from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' and tint=1 partition by tint interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=25,
            stream="create stream rdb.s25 count_window(2, c1) from tdb.triggers partition by id, name, tbname into rdb.r25 as select _twstart tw, sum(cint) c1, %%1 c2, %%2 c3 from qdb.meters where cts >= _twstart and cts < _twstart + 5m and tint=%%1",
            res_query="select tw, c1, c2, c3, id, name from rdb.r25 where id=1 limit 3;",
            exp_query="select _wstart, sum(cint), tint, cast(tint as varchar(8)), tint, cast(tint as varchar(8)) from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' and tint=1 partition by tint interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=26,
            stream="create stream rdb.s26 count_window(2, c1) from tdb.v1 into rdb.r26 as select _twstart tw, sum(cint) c1, tbname from qdb.meters where cts >= _twstart and cts < _twstart + 5m and tbname='t18' partition by tbname",
            res_query="select * from rdb.r26 limit 3",
            exp_query="select _wstart, sum(cint), tbname from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' and tbname='t18' partition by tbname interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=27,
            stream="create stream rdb.s27 count_window(2, c1) from tdb.v2 partition by tbname into rdb.r27 as select _twstart tw, sum(cint) c1, count(cint) c2 from qdb.vmeters where cts >= _twstart and cts < _twstart + 5m and tbname=%%1",
            res_query="select * from rdb.r27 where tag_tbname='v2' limit 3",
            exp_query="select _wstart, sum(cint), count(cint), tbname from qdb.vmeters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' and tbname='v2' partition by tbname interval(5m);",
            check_func=self.check27,
        )
        # self.streams.append(stream) to report

        stream = StreamItem(
            id=28,
            stream="create stream rdb.s28 count_window(2, c1) from tdb.triggers partition by id, tbname into rdb.r28 as select _twstart ts, `name` as cname, `super` csuper, create_time cctime, %%1 from information_schema.ins_users",
            res_query="select ts, cname, csuper, 1000 from rdb.r28 where id = 1 limit 3",
            exp_query="select _wstart ts, 'root',  1, count(cint) from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:15:00' interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=29,
            stream="create stream rdb.s29 count_window(2, c1) from tdb.t1 into rdb.r29 as select _wstart ts, sum(cint), avg(cint) from qdb.meters where cts >= _twstart and cts < _twstart + 5m interval(1m);",
            res_query="select * from rdb.r29 where ts >= '2025-01-01 00:00:00.000' and ts < '2025-01-01 00:15:00.000'",
            exp_query="select _wstart ts, sum(cint), avg(cint) from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' interval(1m)",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=30,
            stream="create stream rdb.s30 count_window(2, c1) from tdb.n1 into rdb.r30 as select _wstart, sum(cint), FIRST(cint) from qdb.meters interval(2m)",
            res_query="select * from rdb.r30",
            exp_query="select _wstart, sum(cint), FIRST(cint) from qdb.meters interval(2m)",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=31,
            stream="create stream rdb.s31 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r31 as select _twstart tp, _twstart + 5m tc, _twstart + 10m tn, _tgrpid tg, cast(_tlocaltime as bigint) tl, %%1 tg1, %%tbname tb, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twstart + 5m and _twstart - 5m > '2024-12-30' and _twstart > '2024-12-30' and _twstart + 5m > '2024-12-30' and _tgrpid is not null and _tlocaltime is not null and %%1 != '1' and %%tbname != '1';",
            res_query="select tp, tc, tn, tg1, tb, c1, c2, tag_tbname from rdb.r31 where tag_tbname = 't1' limit 3",
            exp_query="select _wstart, _wstart + 5m, _wstart + 10m, 't1', 't1', count(cint) c1, avg(cint) c2, 't1' from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=32,
            stream="create stream rdb.s32 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r32 as select _wstart ts, sum(cint), avg(cint) from qdb.meters where tbname != %%tbname and cts >= _twstart and cts < _twstart + 5m interval(1m);",
            res_query="select * from rdb.r32 where tag_tbname='t1' and ts >= '2025-01-01 00:00:00.000' and ts < '2025-01-01 00:15:00.000'",
            exp_query="select _wstart ts, sum(cint) c1, avg(cint) c2, 't1' from qdb.meters where tbname != 't1' and cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' interval(1m)",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=33,
            stream="create stream rdb.s33 count_window(2, c1) from tdb.triggers partition by id, tbname into rdb.r33 tags(tbn varchar(128) as cast(%%1 as varchar)) as select _twstart, sum(cint), avg(cint) from qdb.meters where tint=%%1 and cts >= _twstart and cts < _twstart + 5m interval(1m);",
            res_query="select `sum(cint)`, `avg(cint)` from rdb.r33 where tbn = 1 limit 1",
            exp_query="select sum(cint), avg(cint) from qdb.meters where tint=1 and cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:35:00.000' interval(1m) limit 1 offset 4",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=34,
            stream="create stream rdb.s34 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r34 as select _twstart - 5m tc, TIMEDIFF(_twstart - 5m, _twstart + 5m) tx, %%tbname tb, %%1 tg1, sum(c1) c1, avg(c2) c2, first(c1) c3, last(c2) c4 from %%trows;",
            res_query="select * from rdb.r34 where tag_tbname = 't1';",
            exp_query="select _wstart - 5m, TIMEDIFF(_wstart, _wend) * 2, 't1', 't1', sum(c1) c1, avg(c2) c2, first(c1) c3, last(c2) c4 , 't1' from tdb.t1 where ts >= '2025-01-01 00:00:00.000' and ts < '2025-01-01 00:35:00.000' interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=35,
            stream="create stream rdb.s35 count_window(2, c1) from tdb.triggers partition by id, name, tbname into rdb.r35 as select _wstart ts, count(c1) c1, sum(c2) c2, %%1 c3, %%2 c4, cast(_tlocaltime / 1000000 as timestamp) c5, _tgrpid c6 from %%trows count_window(2);",
            res_query="select ts, c1, c2, c3, c4, id, name from rdb.r35 where id = 1",
            exp_query="select _wstart ts, count(c1) c1, sum(c2) c2, 1, '1', 1, '1' from tdb.t1 where ts >= '2025-01-01 00:00:00.000' and ts < '2025-01-01 00:35:00.000' interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=36,
            stream="create stream rdb.s36 count_window(2, c1) from tdb.triggers partition by id, name, tbname into rdb.r36 as select _twstart - 5m t1, _twstart t2, _twstart + 5m + 1 as t3, TIMEDIFF(_twstart, _twstart - 5m) tx1, TIMEDIFF(_twstart + 5m, _twstart) tx2, sum(cint) c1, avg(cuint) c2, _tgrpid from qdb.meters where tint=%%1 and cts >= _twstart and cts < _twstart + 5m;",
            res_query="select t1, t2, t3, tx1, tx2, c1, c2 from rdb.r36 where id = 1 limit 3",
            exp_query="select _wstart - 5m, _wstart, _wstart + 5m + 1, TIMEDIFF(_wend, _wstart), TIMEDIFF(_wend, _wstart), sum(cint), avg(cuint) from qdb.meters where tint=1 and cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' interval(5m)",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=37,
            stream="create stream rdb.s37 count_window(2, c1) from tdb.triggers partition by id, name, tbname into rdb.r37 as select _twstart ts, _twend, count(c1), sum(c2) from %%trows",
            res_query="select * from rdb.r37 where id == 1 limit 3",
            exp_query="select _wstart, _wstart + 1m, count(c1), sum(c2), 1, '1', 't1' from tdb.triggers where ts >= '2025-01-01 00:00:00.000' and ts < '2025-01-01 00:15:00.000' and id = 1 interval(5m) fill(value, 0, NULL);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=38,
            stream="create stream rdb.s38 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r38 as select _twstart ts, _irowts, _isfilled, interp(c1), _twend, _twduration, _twrownum from %%trows RANGE(_twend) FILL(prev);",
            res_query="select `_irowts`, `_isfilled`, `interp(c1)`, `_twrownum` from rdb.r38 where tag_tbname='t1' limit 1",
            exp_query="select _irowts , _isfilled , interp(c1), 2 from tdb.t1 where _c0 >= '2025-01-01 00:00:00.000' and _c0 <= '2025-01-01 00:01:00.000' RANGE('2025-01-01 00:01:00.000') FILL(prev);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=39,
            stream="create stream rdb.s39 count_window(2, c1) from tdb.v1 into rdb.r39 as select _twstart + 5m tn, TIMETRUNCATE(_twstart + 5m, 1d) tnt, sum(cint) c1, case when _tgrpid != 0 then 1 else 0 end tg, TIMETRUNCATE(cast(_tlocaltime /1000000 as timestamp), 1d) tl from qdb.meters where cts >= _twstart and cts < _twstart + 5m and tint = 1 partition by tint",
            res_query="select * from rdb.r39 limit 3",
            exp_query="select _wstart + 5m, timetruncate(_wstart, 1d), sum(cint), 0, timetruncate(now(), 1d) from qdb.meters where tint=1 and cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=40,
            stream="create stream rdb.s40 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r40 as select _twstart, table_name, db_name, stable_name, tag_name, tag_value from information_schema.ins_tags where table_name='j0' and stable_name='jmeters' and tag_name='tjson'",
            res_query="select `_twstart`, `db_name`, `stable_name`, `tag_name`, `tag_value` from rdb.r40 where tag_tbname='t1' limit 1",
            exp_query="select cast('2025-01-01 00:00:00.000' as timestamp) ts, db_name, stable_name, tag_name, tag_value from information_schema.ins_tags where table_name='j0' and stable_name='jmeters' and tag_name='tjson';",
            check_func=self.check40,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=41,
            stream="create stream rdb.s41 count_window(2, c1) from tdb.triggers partition by id, tbname into rdb.r41 as select cts, cint, cbool, cast(tjson->'k1' as varchar(8)) cjson, _twstart from qdb.j0 where cts >= _twstart and cts < _twstart + 5m and cbool = %%1 order by cts limit 2, 3",
            res_query="select * from rdb.r41 where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000' and id = 1",
            exp_query="select cts, cint, cbool, cast(tjson->'k1' as varchar(8)) cjson, cast('2025-01-01 00:00:00.000' as timestamp), 1, 't1' from qdb.j0 where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000' and cbool = 1 order by cts limit 2, 3",
            check_func=self.check41,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=42,
            stream="create stream rdb.s42 count_window(2, c1) from tdb.triggers partition by id, tbname into rdb.r42 as select _twstart ts, cts, %%1 tt, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cdecimal8, cdecimal16 from qdb.t1 where cts >= _twstart and cts < _twstart + 5m order by cts limit 1",
            res_query="select ts, cts, tt, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary from rdb.r42 where id = 1 limit 1 offset 1",
            exp_query="select cts, cts, 1, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary from qdb.t1 where cts = '2025-01-01 00:05:00.000';",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=43,
            stream="create stream rdb.s43 count_window(2, c1) from tdb.triggers partition by id, tbname into rdb.r43 as select _twstart ts, cts, %%1 tt, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary from qdb.v1 where cts >= _twstart and cts < _twstart + 5m order by cts limit 1",
            res_query="select ts, cts, tt, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary from rdb.r43 where id = 1 limit 1 offset 1",
            exp_query="select cts, cts, 1, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary from qdb.v1 where cts = '2025-01-01 00:05:00.000';",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=44,
            stream="create stream rdb.s44 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r44 as select _twstart ts, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary from qdb.v1 where cts >= _twstart and cts < _twend + 4m order by cts limit 1",
            res_query="select * from rdb.r44 where tag_tbname='t1' limit 1 offset 2",
            exp_query="select cts, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, 't1' from qdb.v1 where cts >= '2025-01-01 00:10:00.000' and cts < '2025-01-01 00:15:00.000' order by cts limit 1",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=45,
            stream="create stream rdb.s45 count_window(2, c1) from tdb.triggers partition by id, name, tbname into rdb.r45 as select _twstart ts, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cdecimal8, cdecimal16 from qdb.meters where cts >= _twstart and cts < _twstart + 5m and tbname='t2' order by cts limit 1",
            res_query="select  ts, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cdecimal8, cdecimal16 from rdb.r45 order by ts limit 1",
            exp_query="select cts, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cdecimal8, cdecimal16 from qdb.t2 where cts = '2025-01-01 00:00:00.000';",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=46,
            stream="create stream rdb.s46 count_window(2, c1) from tdb.v1 into rdb.r46 as select _twstart ts, count(c1) ccnt, sum(c2) csum, first(id) cfirst from %%trows",
            res_query="select ts, ccnt, csum, cfirst from rdb.r46 limit 4",
            exp_query="select _wstart, count(*), sum(c2), first(id) from tdb.v1 count_window(2, c1) limit 4",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=47,
            stream="create stream rdb.s47 count_window(2, c1) from tdb.v1 into rdb.r47 as select _twstart ts, sum(`vgroups`) c1, sum(ntables) c2 from information_schema.ins_databases where name != 'information_schema' and name != 'performance_schema'",
            res_query="select ts, c1, c2 >= 100, 1000 from rdb.r47 limit 3",
            exp_query="select _wstart ts, 3, true, count(cint) from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:15:00' interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=48,
            stream="create stream rdb.s48 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r48 as select _twstart ts, ABS(cint), ACOS(cuint), ASIN(cbigint), ATAN(cubigint), CEIL(cfloat), COS(cdouble), DEGREES(csmallint), EXP(cusmallint), FLOOR(ctinyint), LN(cutinyint), LOG(cfloat), MOD(cbigint, cint), PI(), POW(cuint, 2), RADIANS(cbigint), ROUND(cfloat), SIGN(cdouble), SQRT(csmallint), TAN(cfloat), TRUNCATE(cdouble, 1), CRC32(cvarchar) from qdb.meters where cts >=_twstart and cts <= _twend and tbname=%%1 order by cts limit 1;",
            res_query="select * from rdb.r48 where tag_tbname='t1' limit 1 offset 2;",
            exp_query="select cts, ABS(cint), ACOS(cuint), ASIN(cbigint), ATAN(cubigint), CEIL(cfloat), COS(cdouble), DEGREES(csmallint), EXP(cusmallint), FLOOR(ctinyint), LN(cutinyint), LOG(cfloat), MOD(cbigint, cint), PI(), POW(cuint, 2), RADIANS(cbigint), ROUND(cfloat), SIGN(cdouble), SQRT(csmallint), TAN(cfloat), TRUNCATE(cdouble, 1), CRC32(cvarchar), tbname from qdb.meters where cts='2025-01-01 00:10:00.000' and tbname='t1'",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=49,
            stream="create stream rdb.s49 count_window(2, c1) from tdb.v1 into rdb.r49 as select _twstart ts, ABS(cint), ACOS(cuint), ASIN(cbigint), ATAN(cubigint), CEIL(cfloat), COS(cdouble), DEGREES(csmallint), EXP(cusmallint), FLOOR(ctinyint), LN(cutinyint), LOG(cfloat), MOD(cbigint, cint), PI(), POW(cuint, 2), RADIANS(cbigint), ROUND(cfloat), SIGN(cdouble), SQRT(csmallint), TAN(cfloat), TRUNCATE(cdouble, 1), CRC32(cvarchar) from qdb.meters where cts >=_twstart and cts <= _twend and tbname='t1' order by cts limit 1",
            res_query="select * from rdb.r49 limit 1 offset 2;",
            exp_query="select cts, ABS(cint), ACOS(cuint), ASIN(cbigint), ATAN(cubigint), CEIL(cfloat), COS(cdouble), DEGREES(csmallint), EXP(cusmallint), FLOOR(ctinyint), LN(cutinyint), LOG(cfloat), MOD(cbigint, cint), PI(), POW(cuint, 2), RADIANS(cbigint), ROUND(cfloat), SIGN(cdouble), SQRT(csmallint), TAN(cfloat), TRUNCATE(cdouble, 1), CRC32(cvarchar) from qdb.meters where cts='2025-01-01 00:10:00.000' and tbname='t1';",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=50,
            stream="create stream rdb.s50 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r50 as select _twstart ts, ABS(cint), ACOS(cuint), ASIN(cbigint), ATAN(cubigint), CEIL(cfloat), COS(cdouble), DEGREES(csmallint), EXP(cusmallint), FLOOR(ctinyint), LN(cutinyint), LOG(cfloat), MOD(cbigint, cint), PI(), POW(cuint, 2), RADIANS(cbigint), ROUND(cfloat), SIGN(cdouble), SQRT(csmallint), TAN(cfloat), TRUNCATE(cdouble, 1), CRC32(cvarchar) from qdb.meters where cts >=_twstart and cts <= _twstart + 5m and tbname=%%1 order by cts limit 1;",
            res_query="select * from rdb.r50 where tag_tbname='t1' limit 1 offset 2;",
            exp_query="select cts, ABS(cint), ACOS(cuint), ASIN(cbigint), ATAN(cubigint), CEIL(cfloat), COS(cdouble), DEGREES(csmallint), EXP(cusmallint), FLOOR(ctinyint), LN(cutinyint), LOG(cfloat), MOD(cbigint, cint), PI(), POW(cuint, 2), RADIANS(cbigint), ROUND(cfloat), SIGN(cdouble), SQRT(csmallint), TAN(cfloat), TRUNCATE(cdouble, 1), CRC32(cvarchar), tbname from qdb.meters where cts='2025-01-01 00:10:00.000' and tbname='t1'",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=51,
            stream="create stream rdb.s51 count_window(2, c1) from tdb.triggers partition by id, tbname into rdb.r51 as select _twstart ts, ASCII(cvarchar), CHAR(cnchar), CHAR_LENGTH(cast(%%1 as varchar)), CHAR_LENGTH(cvarchar), CONCAT(cvarchar, cnchar), CONCAT_WS('--', cvarchar, cnchar), LENGTH(cnchar), LOWER(cvarchar), LTRIM(cnchar), POSITION('a' in cvarchar), REPEAT(cnchar, 3), REPLACE(cvarchar, 'a', 'b'), RTRIM(cnchar), SUBSTRING(cvarchar, 1), SUBSTR(cvarchar, 1), SUBSTRING_INDEX(cnchar, 'a', 1), TRIM(cvarchar), UPPER(cnchar) from qdb.n1 where cts >=_twstart and cts <= _twend order by cts limit 1;",
            res_query="select * from rdb.r51 where id='1' limit 1 offset 2;",
            exp_query="select cts, ASCII(cvarchar), CHAR(cnchar), CHAR_LENGTH(cast(1 as varchar)), CHAR_LENGTH(cvarchar), CONCAT(cvarchar, cnchar), CONCAT_WS('--', cvarchar, cnchar), LENGTH(cnchar), LOWER(cvarchar), LTRIM(cnchar), POSITION('a' in cvarchar), REPEAT(cnchar, 3), REPLACE(cvarchar, 'a', 'b'), RTRIM(cnchar), SUBSTRING(cvarchar, 1), SUBSTR(cvarchar, 1), SUBSTRING_INDEX(cnchar, 'a', 1), TRIM(cvarchar), UPPER(cnchar), 1, 't1' from qdb.n1 where cts='2025-01-01 00:10:00.000';",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=52,
            stream="create stream rdb.s52 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r52 as select _twstart ts, ASCII(cvarchar), CHAR(cnchar), CHAR_LENGTH(%%tbname), CHAR_LENGTH(cast(%%1 as varchar)), CHAR_LENGTH(cvarchar), CONCAT(cvarchar, cnchar), CONCAT_WS('--', cvarchar, cnchar), LENGTH(cnchar), LOWER(cvarchar), LTRIM(cnchar), POSITION('a' in cvarchar), REPEAT(cnchar, 3), REPLACE(cvarchar, 'a', 'b'), RTRIM(cnchar), SUBSTRING(cvarchar, 1), SUBSTR(cvarchar, 1), SUBSTRING_INDEX(cnchar, 'a', 1), TRIM(cvarchar), UPPER(cnchar) from qdb.n1 where cts >=_twstart and cts <= _twend order by cts limit 1;",
            res_query="select * from rdb.r52 where tag_tbname='t1' limit 1 offset 2;",
            exp_query="select cts, ASCII(cvarchar), CHAR(cnchar), CHAR_LENGTH('t1'), CHAR_LENGTH(cast('t1' as varchar)), CHAR_LENGTH(cvarchar), CONCAT(cvarchar, cnchar), CONCAT_WS('--', cvarchar, cnchar), LENGTH(cnchar), LOWER(cvarchar), LTRIM(cnchar), POSITION('a' in cvarchar), REPEAT(cnchar, 3), REPLACE(cvarchar, 'a', 'b'), RTRIM(cnchar), SUBSTRING(cvarchar, 1), SUBSTR(cvarchar, 1), SUBSTRING_INDEX(cnchar, 'a', 1), TRIM(cvarchar), UPPER(cnchar), 't1' from qdb.n1 where cts='2025-01-01 00:10:00.000';",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=53,
            stream="create stream rdb.s53 count_window(2, c1) from tdb.v1 into rdb.r53 as select _twstart - 5m ts, ASCII(cvarchar), CHAR(cnchar), CHAR_LENGTH(cvarchar), CONCAT(cvarchar, cnchar), CONCAT_WS('--', cvarchar, cnchar), LENGTH(cnchar), LOWER(cvarchar), LTRIM(cnchar), POSITION('a' in cvarchar), REPEAT(cnchar, 3), REPLACE(cvarchar, 'a', 'b'), RTRIM(cnchar), SUBSTRING(cvarchar, 1), SUBSTR(cvarchar, 1), SUBSTRING_INDEX(cnchar, 'a', 1), TRIM(cvarchar), UPPER(cnchar) from qdb.n1 where cts >=_twstart and cts <= _twstart + 5m order by cts limit 1;",
            res_query="select * from rdb.r53 limit 1 offset 2;",
            exp_query="select cast('2025-01-01 00:05:00.000' as timestamp), ASCII(cvarchar), CHAR(cnchar), CHAR_LENGTH(cvarchar), CONCAT(cvarchar, cnchar), CONCAT_WS('--', cvarchar, cnchar), LENGTH(cnchar), LOWER(cvarchar), LTRIM(cnchar), POSITION('a' in cvarchar), REPEAT(cnchar, 3), REPLACE(cvarchar, 'a', 'b'), RTRIM(cnchar), SUBSTRING(cvarchar, 1), SUBSTR(cvarchar, 1), SUBSTRING_INDEX(cnchar, 'a', 1), TRIM(cvarchar), UPPER(cnchar) from qdb.n1 where cts='2025-01-01 00:10:00.000';",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=54,
            stream="create stream rdb.s54 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r54 as select _twstart ts, cint, CAST(cint as varchar(20)) c1, cuint, cts, TO_CHAR(cts, 'yyyy-mm-dd hh24:mi:ss') c2, TO_ISO8601(cts) c3, TO_TIMESTAMP(TO_CHAR(cts, 'yyyy-mm-dd'), 'yyyy-mm-dd') c4, TO_UNIXTIMESTAMP(TO_CHAR(cts, 'yyyy-mm-dd')) c5 from qdb.v1 where cts >= _twstart and cts < _twstart + 5m and _tlocaltime > '2024-12-30' order by cts limit 1",
            res_query="select * from rdb.r54 where tag_tbname='t1' limit 1 offset 2;",
            exp_query="select cts ts, cint, CAST(cint as varchar(20)) c1, cuint, cts, TO_CHAR(cts, 'yyyy-mm-dd hh24:mi:ss') c2, TO_ISO8601(cts) c3, TO_TIMESTAMP(TO_CHAR(cts, 'yyyy-mm-dd'), 'yyyy-mm-dd') c4, TO_UNIXTIMESTAMP(TO_CHAR(cts, 'yyyy-mm-dd')) c5, 't1' from qdb.v1 where cts='2025-01-01 00:10:00.000';",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=55,
            stream="create stream rdb.s55 count_window(2, c1) from tdb.t1 into rdb.r55 as select _irowts , _isfilled , _irowts_origin, interp(cint), interp(cuint), interp(cbigint), interp(cubigint), interp(cfloat), interp(cdouble), interp(csmallint), interp(cusmallint), interp(ctinyint) from qdb.meters where tbname='t1' RANGE(_twstart + 1s) fill(near)",
            res_query="select * from rdb.r55 limit 3",
            exp_query="select _wstart + 1s, true, _wstart, first(cint), first(cuint), first(cbigint), first(cubigint), first(cfloat), first(cdouble), first(csmallint), first(cusmallint), first(ctinyint) from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' and tbname='t1' interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=56,
            stream="create stream rdb.s56 count_window(2, c1) from tdb.v1 into rdb.r56 as select _wstart ws, _wend we, _twstart tws, _twend + 4m twe, first(c1) cf, last(c1) cl, count(c1) cc from %%trows interval(1m)",
            res_query="select * from rdb.r56 where ws >= '2025-01-01 00:00:00.000' and we <= '2025-01-01 00:05:00.000' ",
            exp_query="select _wstart ws, _wend we, cast('2025-01-01 00:00:00.000' as timestamp) tws, cast('2025-01-01 00:05:00.000' as timestamp) twe, first(c1) cf, last(c1) cl, count(c1) cc from tdb.v1 where ts >= '2025-01-01 00:00:00.000' and ts < '2025-01-01 00:05:00.000' interval(1m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=57,
            stream="create stream rdb.s57 count_window(2, c1) from tdb.v1 into rdb.r57 as select _twstart ts, cts, DAYOFWEEK(_twstart) c1, DAYOFWEEK(_twend) c2, DAYOFWEEK(cast(_tlocaltime/1000000 as timestamp)) c3, TIMEDIFF(_twstart, _twend) c4, _twduration c5, DAYOFWEEK(cvarchar) c6, TIMEDIFF(_twstart, cts) c7, TIMETRUNCATE(cts, 1d, 1) c8, WEEK(cts) c9, WEEKDAY(cts) c10, WEEKOFYEAR(cts) c11 from qdb.v5 order by cts desc limit 1",
            res_query="select ts, cts, c1, c2, c4, c5, c7, c8, c9, c10, c11 from rdb.r57 limit 1 offset 2;",
            exp_query="select cast('2025-01-01 00:10:00.000' as timestamp) ts, cts, DAYOFWEEK(cast('2025-01-01 00:10:00.000' as timestamp)) c1, DAYOFWEEK(cast('2025-01-01 00:15:00.000' as timestamp)) c2, -60000 c4, 60000 c5, TIMEDIFF(cast('2025-01-01 00:10:00.000' as timestamp), cts) c7, TIMETRUNCATE(cts, 1d, 1) c8, WEEK(cts) c9, WEEKDAY(cts) c10, WEEKOFYEAR(cts) c11 from qdb.v5 order by cts desc limit 1;",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=58,
            stream="create stream rdb.s58 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r58 as select _irowts, interp(cbigint), _isfilled from qdb.v1 where ctinyint > 0 and cint > 2 RANGE('2025-01-01 00:02:00.000', '2025-01-01 00:08:00.000') EVERY (1m) FILL(linear) limit 50;",
            res_query="select * from rdb.r58 where tag_tbname='t1'",
            exp_query="select _wstart, first(cbigint), false, 't1' from qdb.v1 where cts >= '2025-01-01 00:02:00.000' and cts <= '2025-01-01 00:08:00.000' and tbname='v1' partition by tbname interval(1m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=59,
            stream="create stream rdb.s59 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r59 as select _irowts, interp(csmallint), _isfilled from qdb.meters where ctinyint > 0 and cint > 2 partition by tbname RANGE('2025-01-01 00:02:01.000') EVERY (1m) FILL(next)",
            res_query="select `_irowts`, `interp(csmallint)`, `_isfilled` from rdb.r59 where tag_tbname='t1'",
            exp_query="select timestamp '2025-01-01 00:02:01.000', last(csmallint), true from qdb.meters where cts >= '2025-01-01 00:02:00.000' and cts <= '2025-01-01 00:02:30.000' and tbname='t1' partition by tbname;",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=60,
            stream="create stream rdb.s60 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r60 as select _twstart ts, RAND(), NOW(), TODAY(), TIMEZONE() from qdb.n2 where _tgrpid != 0 order by cts desc limit 1",
            res_query="select ts, `timezone()`, tag_tbname from rdb.r60 where tag_tbname='t1' limit 1 offset 2;",
            exp_query="select cts, timezone(), 't1' from qdb.n2 where cts='2025-01-01 00:10:00.000';",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=61,
            stream="create stream rdb.s61 count_window(2, c1) from tdb.triggers partition by id, tbname into rdb.r61 as select _twstart ts, RAND(), NOW(), TODAY(), TIMEZONE() from qdb.n2 where _tgrpid != 0 order by cts desc limit 1",
            res_query="select ts, `timezone()`, id from rdb.r61 where id=1 limit 1 offset 2;",
            exp_query="select cts, timezone(), 1 from qdb.n2 where cts='2025-01-01 00:10:00.000';",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=62,
            stream="create stream rdb.s62 count_window(2, c1) from tdb.triggers partition by id, name, tbname into rdb.r62 as select _twstart ts, RAND(), NOW(), TODAY(), TIMEZONE() from qdb.n2 where _tgrpid != 0 order by cts desc limit 1",
            res_query="select ts, `timezone()`, id, name from rdb.r62 where id=1 limit 1 offset 2;",
            exp_query="select cts, timezone(), 1, '1' from qdb.n2 where cts='2025-01-01 00:10:00.000';",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=63,
            stream="create stream rdb.s63 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r63 as select _twstart ts, APERCENTILE(cint, 25) c1, AVG(cuint) c2, SUM(cint) c3, COUNT(cbigint) c4, ELAPSED(cts) c5, HYPERLOGLOG(cdouble) c6, LEASTSQUARES(csmallint, 1, 2) c7, SPREAD(ctinyint) c8, STDDEV(cutinyint) c9, STDDEV_POP(cfloat) c10, SUM(cdecimal8) c11, VAR_POP(cbigint) c12 from qdb.meters where tbname=%%tbname and cts >= _twstart and cts < _twstart + 5m;",
            res_query="select ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c12 from rdb.r63 where tag_tbname='t1' limit 3",
            exp_query="select _wstart, APERCENTILE(cint, 25), AVG(cuint), SUM(cint), COUNT(cbigint), 270000, HYPERLOGLOG(cdouble), LEASTSQUARES(csmallint, 1, 2), SPREAD(ctinyint), STDDEV(cutinyint), STDDEV_POP(cfloat), VAR_POP(cbigint) from qdb.meters where tbname='t1' and cts >='2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=64,
            stream="create stream rdb.s64 count_window(2, c1) from tdb.triggers partition by id, tbname into rdb.r64 as select cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary from qdb.view1 where cts >= _twstart and cts < _twstart + 5m limit 1",
            res_query="select cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary from rdb.r64 where id = 1 limit 1",
            exp_query="select cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary from qdb.view1 where cts = '2025-01-01 00:00:00.000';",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=65,
            stream="create stream rdb.s65 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r65 as select _twstart ts, HISTOGRAM(cfloat, 'user_input', '[1, 7]', 1) th from qdb.meters where tbname=%%tbname and cts >= _twstart and cts < _twstart + 5m;",
            res_query="select ts, th from rdb.r65 where tag_tbname='t1' limit 3",
            exp_query="select _wstart, HISTOGRAM(cfloat, 'user_input', '[1, 7]', 1) th from qdb.meters where tbname='t1' and cts >='2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=66,
            stream="create stream rdb.s66 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r66 as select _twstart ts, FIRST(cuint), LAST(cbigint), LAST_ROW(cubigint) from qdb.meters where tbname=%%tbname and cts >= _twstart and cts < _twstart + 5m;",
            res_query="select * from rdb.r66 where tag_tbname='t1' limit 3",
            exp_query="select _wstart, FIRST(cuint), LAST(cbigint), LAST_ROW(cubigint), 't1' from qdb.meters where tbname='t1' and cts >='2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' interval(5m) fill(null);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=67,
            stream="create stream rdb.s67 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r67 as select _twstart ts, MAX(ctinyint), MIN(cutinyint) from qdb.meters where tbname=%%tbname and cts >= _twstart and cts < _twstart + 5m;",
            res_query="select * from rdb.r67 where tag_tbname='t1' limit 3",
            exp_query="select _wstart, MAX(ctinyint), MIN(cutinyint), 't1' from qdb.meters where tbname='t1' and cts >='2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' interval(5m) fill(null);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=68,
            stream="create stream rdb.s68 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r68 as select _twstart ts, mode(cint) from qdb.meters where tbname=%%tbname and cts >= _twstart and cts < _twstart + 5m;",
            res_query="select * from rdb.r68 where tag_tbname='t1' limit 3",
            exp_query="select _wstart, mode(cint), 't1' from qdb.meters where tbname='t1' and cts >='2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' interval(5m) fill(null);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=69,
            stream="create stream rdb.s69 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r69 as select _twstart ts, SAMPLE(cdecimal8, 1) from qdb.meters where tbname=%%tbname and cts >= _twstart and cts < _twstart + 5m;",
            res_query="select count(*) from (select * from rdb.r69 where tag_tbname='t1' and ts >='2025-01-01 00:00:00.000' and ts < '2025-01-01 00:15:00.000')",
            exp_query="select count(*) from (select _wstart, SAMPLE(cdecimal8, 1), 't1' from qdb.meters where tbname='t1' and cts >='2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' interval(5m))",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=70,
            stream="create stream rdb.s70 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r70 as select _twstart ts, BOTTOM(cint, 1) from qdb.meters where tbname=%%tbname and cts >= _twstart and cts < _twstart + 5m;",
            res_query="select * from rdb.r70 where tag_tbname='t1' limit 3",
            exp_query="select _wstart, BOTTOM(cint, 1), 't1' from qdb.meters where tbname='t1' and cts >='2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=71,
            stream="create stream rdb.s71 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r71 as select _twstart ts, TAIL(cbigint, 1)  from qdb.meters where tbname=%%tbname and cts >= _twstart and cts < _twstart + 5m;",
            res_query="select * from rdb.r71 where tag_tbname='t1' limit 1",
            exp_query="select cast('2025-01-01 00:00:00.000' as timestamp), TAIL(cbigint, 1) , 't1' from qdb.meters where tbname='t1' and cts >='2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000'",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=72,
            stream="create stream rdb.s72 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r72 as select _twstart ts, TOP(cint, 1) from qdb.meters where tbname=%%tbname and cts >= _twstart and cts < _twstart + 5m;",
            res_query="select * from rdb.r72 where tag_tbname='t1' limit 3",
            exp_query="select _wstart, TOP(cint, 1), 't1' from qdb.meters where tbname='t1' and cts >='2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=73,
            stream="create stream rdb.s73 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r73 as select _rowts ts, _twstart tws, _twend twe, CSUM(cint) + CSUM(cuint) val from qdb.v1 where cts >= _twstart and cts < _twstart + 5m;",
            res_query="select ts, val from rdb.r73 where ts >= '2025-01-01 00:00:00.000' and ts < '2025-01-01 00:05:00.000'",
            exp_query="select _rowts, CSUM(cint) + CSUM(cuint) from qdb.v1 where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000'",
        )
        self.streams.append(stream)
        
        stream = StreamItem(
            id=74,
            stream="create stream rdb.s74 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r74 as select first(cts), _twstart, %%tbname, cint, avg(cfloat), count(cdouble), sum(cfloat), twa(cdouble) from qdb.meters where cts >= _twstart and cts < _twstart + 5m and tbname = %%1 group by cint having count(cdouble) == 1 order by first(cts);",
            res_query="select `first(cts)`, cint, `avg(cfloat)`, `count(cdouble)`, `sum(cfloat)`, `twa(cdouble)` from rdb.r74 where tag_tbname='t1' and `first(cts)` >= '2025-01-01 00:00:00.000' and `first(cts)` < '2025-01-01 00:15:00.000'",
            exp_query="select first(cts), cint, avg(cfloat), count(cdouble), sum(cfloat), twa(cdouble) from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' and tbname = 't1' group by cint having count(cdouble) == 1 order by first(cts);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=75,
            stream="create stream rdb.s75 count_window(2, c1) from tdb.vtriggers partition by id, tbname into rdb.r75 as select _wstart, twa(c1), avg(c1), count(c1) from tdb.vtriggers where id=%%1 and ts>=_twstart and ts < _twend + 4m interval(1m) fill(prev)",
            res_query="select * from rdb.r75 where id=1 limit 5 offset 5",
            exp_query="select _wstart, twa(c1), avg(c1), count(c1), 1, 'v1' from tdb.vtriggers where id=1 and ts >= '2025-01-01 00:05:00.000' and ts < '2025-01-01 00:10:00.000' interval(1m) fill(prev)",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=76,
            stream="create stream rdb.s76 count_window(2, c1) from tdb.triggers partition by id, name into rdb.r76 as select _twstart ts, ST_GeomFromText('POINT (2.000000 2.000000)'), ST_AsText(cgeometry), ST_Contains(cgeometry, cgeometry), ST_ContainsProperly(cgeometry, cgeometry), ST_Covers(cgeometry, cgeometry), ST_Equals(cgeometry, cgeometry), ST_Intersects(cgeometry, cgeometry), ST_Touches(cgeometry, cgeometry) from qdb.meters where tbname='t4' and cts >=_twstart and cts <= _twstart + 5m order by cts limit 1;",
            res_query="select * from rdb.r76 limit 1 offset 3",
            exp_query="select cast('2025-01-01 00:15:00.000' as timestamp), ST_GeomFromText('POINT (2.000000 2.000000)'), ST_AsText(cgeometry), ST_Contains(cgeometry, cgeometry), ST_ContainsProperly(cgeometry, cgeometry), ST_Covers(cgeometry, cgeometry), ST_Equals(cgeometry, cgeometry), ST_Intersects(cgeometry, cgeometry), ST_Touches(cgeometry, cgeometry), 1, '1' from qdb.t4 where cts='2025-01-01 00:15:00.000';",
        )
        # self.streams.append(stream) TD-35766 forbidden

        stream = StreamItem(
            id=77,
            stream="create stream rdb.s77 count_window(2, c1) from tdb.vtriggers partition by id, tbname into rdb.r77 as select _twstart ts, count(*) c1 from information_schema.ins_streams where stream_name='s77'",
            res_query="select ts, c1 > 0, 1000 from rdb.r77 where id='1' limit 3",
            exp_query="select _wstart ts, true, count(cint) from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:15:00' interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=78,
            stream="create stream rdb.s78 count_window(2, c1) from tdb.triggers partition by id, name, tbname into rdb.r78 as select _twstart, _twrownum, count(*), sum(cdecimal8) from qdb.meters where tbname !=%%1 and cts >= _twstart and cts < _twstart + 5m and _twrownum > 0;",
            check_func=self.check78,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=79,
            stream="create stream rdb.s79 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r79 as select _twstart, sum(cdecimal8), count(*), _twrownum from qdb.meters where tbname=%%1 and cts >= _twstart and cts < _twstart + 5m and _twrownum > 0;",
            res_query="select `_twstart`, `sum(cdecimal8)`, `count(*)` from rdb.r79 where tag_tbname='t1' limit 3",
            exp_query="select _wstart ts, sum(cdecimal8), count(*) from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' and tbname='t1' partition by tbname interval(5m) limit 3",
            check_func=self.check79,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=80,
            stream="create stream rdb.s80 count_window(2, c1) from tdb.v1 into rdb.r80 as select _twstart, sum(v1), sum(v2) from (select count(c1) v1, sum(c2) v2 from %%trows)",
            check_func=self.check80,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=81,
            stream="create stream rdb.s81 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r81 as select cts, diff(cint) c3, _twstart, _twend, _twrownum from qdb.meters where tbname=%%1 and cts >= _twstart and cts <= _twend + 4m and cint > 5 and _twrownum > 0 ",
            res_query="select cts, c3 from rdb.r81 where tag_tbname='t1' and cts <= '2025-01-01 00:14:30.000';",
            exp_query="select cts, diff(cint) c3 from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts <= '2025-01-01 00:14:30.000' and tbname='t1' and cint > 5;",
            check_func=self.check81,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=82,
            stream="create stream rdb.s82 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r82 as select cts, diff(cint) c3, _twstart, _twend, _twrownum from qdb.meters where tbname=%%1 and cts >= _twstart and cts <= _twend + 4m and cint > 5",
            res_query="select cts, c3 from rdb.r82 where tag_tbname='t1' and cts >= '2025-01-01 00:00:00.000' and cts <= '2025-01-01 00:15:00.000';",
            exp_query="select cts, diff(cint) c3 from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts <= '2025-01-01 00:15:00.000' and tbname='t1' and cint > 5;",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=83,
            stream="create stream rdb.s83 count_window(2, c1) from tdb.t1 into rdb.r83 as select ts, diff(c1) c3 from %%trows ",
            res_query="select * from rdb.r83 where ts = '2025-01-01 00:31:00.000'",
            exp_query="select ts, diff(c1) from tdb.t1 where ts >= '2025-01-01 00:30:00.000' and ts <= '2025-01-01 00:32:00.000';",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=84,
            stream="create stream rdb.s84 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r84 as select cts, top(cint, 5) from qdb.meters where tbname=%%1 and cts >= _twstart and cts <= _twend + 4m order by cint limit 2 offset 2;",
            res_query="select cts, `top(cint, 5)` from rdb.r84 where tag_tbname='t1' limit 2",
            exp_query="select cts, top(cint, 5) from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts <= '2025-01-01 00:05:00.000' and tbname='t1' order by cint limit 2 offset 2;",
            check_func=self.check84,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=85,
            stream="create stream rdb.s85 count_window(2, c1) from tdb.triggers partition by id, tbname into rdb.r85 as select cts, top(cint, 5) from qdb.meters where tint=%%1 and cts >= _twstart and cts < _twstart + 5m order by cint limit 2 offset 2;",
            res_query="select cts, `top(cint, 5)` from rdb.r85 where id=1 limit 1",
            exp_query="select cts, top(cint, 5) from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000' and tint=1 order by cint limit 1 offset 2;",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=86,
            stream="create stream rdb.s86 count_window(2, c1) from tdb.n1 into rdb.r86 as select ts, top(c1, 5) from %%trows limit 1 offset 1;",
            res_query="select * from rdb.r86 limit 1",
            exp_query="select ts, top(c1, 5) from tdb.n1 where ts >= '2025-01-01 00:25:00.000' and ts <= '2025-01-01 00:27:00.000' limit 1 offset 1;",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=87,
            stream="create stream rdb.s87 count_window(2, c1) from tdb.t1 into rdb.r87 as select last(cts, cint, cuint) from qdb.t1 where cts >= _twstart and cts < _twstart + 5m;",
            res_query="select * from rdb.r87 limit 3",
            exp_query="select last(cts, cint, cuint) from qdb.t1 where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=88,
            stream="create stream rdb.s88 count_window(2, c1) from tdb.v1 into rdb.r88 as select _twstart tw, cbool, cast(avg(cint) as int) from qdb.view1 where cts >= _twstart and cts < _twend + 4m and cbool = true group by cbool having avg(cint) > 0",
            res_query="select * from rdb.r88 limit 1",
            exp_query="select cast('2025-01-01 00:00:00.000' as timestamp) tw, cbool, cast(avg(cint) as int) from qdb.view1 where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000' and cbool = true group by cbool having avg(cint) > 0 order by tw;",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=89,
            stream="create stream rdb.s89 count_window(2, c1) from tdb.v1 into rdb.r89 as select _twstart tw, _c0 ta, _rowts tb, c1, c2, rand() c3 from %%trows order by _c0 limit 1",
            res_query="select tw, ta, tb, c1, c2 from rdb.r89 limit 3",
            exp_query="select _c0,  _c0, _rowts, c1, c2 from tdb.v1 where ts = '2025-01-01 00:00:00.000' or ts = '2025-01-01 00:05:00.000' or ts = '2025-01-01 00:10:00.000' order by _c0 limit 3;",
            check_func=self.check89,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=90,
            stream="create stream rdb.s90 count_window(2, c1) from tdb.triggers partition by id, tbname into rdb.r90 as select _twstart ts, sum(tint), sum(t1), sum(t2) from (select _twstart, tint, count(val) t1, sum(val) t2 from (select _wstart, tint, count(cint) val from qdb.meters where tint=%%1 and (cts between _twstart and _twend + 4m) partition by tint interval(1m) having (count(cint) > 0)) tb partition by tint having count(tint) > 0)",
            res_query="select ts, `sum(tint)`, `sum(t1)`, `sum(t2)` from rdb.r90 where id=1 limit 1",
            exp_query="select cast('2025-01-01 00:00:00.000' as timestamp) ts, sum(tint), sum(t1), sum(t2) from (select cast('2025-01-01 00:00:00.000' as timestamp) ts, tint, count(val) t1, sum(val) t2 from (select _wstart, tint, count(cint) val from qdb.meters where tint=1 and (cts between '2025-01-01 00:00:00.000' and '2025-01-01 00:05:00.000') partition by tint interval(1m) having (count(cint) > 0)) tb partition by tint having count(tint) > 0);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=91,
            stream="create stream rdb.s91 count_window(2, c1) from tdb.triggers partition by id, name, tbname into rdb.r91 as select _twstart, id cid, name cname, sum(c1) amount from %%trows partition by id, name having sum(c1) <= 5;",
            res_query="select * from rdb.r91",
            exp_query="select _wstart, id, name, sum(c1), id, name, tbname from tdb.t1 where ts >= '2025-01-01 00:00:00.000' and ts < '2025-01-01 00:15:00.000' partition by tbname interval(5m) having sum(c1) <= 5;",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=92,
            stream="create stream rdb.s92 count_window(2, c1) from tdb.n1 into rdb.r92 as select _twstart, tint, sum(cint) amount from qdb.meters where cts >= _twstart and cts < _twstart + 5m and tbname='t1' partition by tbname",
            res_query="select * from rdb.r92",
            exp_query="select _wstart, tint, sum(cint) from qdb.meters where cts >= '2025-01-01 00:25:00.000' and cts < '2025-01-01 00:35:00.000' and tbname='t1' partition by tbname interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=93,
            stream="create stream rdb.s93 count_window(2, c1) from tdb.v1 into rdb.r93 as select _twstart, sum(cnt) from (select _twstart ts, count(*) cnt from information_schema.ins_tables where db_name='qdb' union all select _twstart ts, count(*) cnt from information_schema.ins_tables where db_name='tdb')",
            res_query="select `_twstart`, `sum(cnt)`, 10 from rdb.r93 limit 3",
            exp_query="select _wstart, 136, count(cint) from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' and tbname='t1' partition by tbname interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=94,
            stream="create stream rdb.s94 count_window(2, c1) from tdb.v1 into rdb.r94 as select ts, c1, c2 from (select ts, c1, c2 from tdb.t3 union all select ts, c1, c2 from %%trows)",
            res_query="select * from rdb.r94",
            exp_query="select ts, c1, c2 from (select ts, c1, c2 from tdb.t3 union all select ts, c1, c2 from tdb.v1) where ts < '2025-01-01 00:35:00.000' order by ts",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=95,
            stream="create stream rdb.s95 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r95 as select _twstart, _twend + 4m, last(cts, cint, cuint) from qdb.meters where tbname=%%1 and cts >= _twstart and cts < _twstart + 5m",
            res_query="select * from rdb.r95 where tag_tbname='t1' limit 3",
            exp_query="select _wstart, _wend, last(cts, cint, cuint), 't1' from qdb.t1 where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=96,
            stream="create stream rdb.s96 count_window(2, c1) from tdb.t1 into rdb.r96 (ts, ti composite key, te, tx, ty) as select _twstart, tint, _twend + 4m, count(cint), sum(cint) from qdb.meters where cts >= _twstart and cts < _twend + 4m and tint is not NULL partition by tint",
            res_query="select * from rdb.r96 order by ts, ti limit 9",
            exp_query="select * from (select _wstart ts, tint ti, _wend te, count(cint) tx, sum(cint) ty from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' and tint is not NULL partition by tint interval(5m)) order by ts, ti",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=97,
            stream="create stream rdb.s97 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r97 as select _twstart, cts, cint, cuint, cbool from qdb.meters where tbname=%%1 and cint > 1 and cint >= 1 and cint < 10000 and cint  <= 100000 and cint <> 1 and cint != 1 and cint is NOT NULL and cts BETWEEN _twstart AND _twend + 4m order by cts desc limit 1",
            res_query="select * from rdb.r97 where tag_tbname='t1' limit 1",
            exp_query="select cast('2025-01-01 00:00:00.000' as timestamp), cts, cint, cuint, cbool, tbname from qdb.meters where tbname='t1' and cint > 1 and cint >= 1 and cint < 10000 and cint  <= 100000 and cint <> 1 and cint != 1 and cint is NOT NULL and cts BETWEEN '2025-01-01 00:00:00.000' AND '2025-01-01 00:05:00.000' order by cts desc limit 1",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=98,
            stream="create stream rdb.s98 count_window(2, c1) from tdb.triggers partition by id, name, tbname into rdb.r98 as select last(cts, cint, cbool) from qdb.jmeters where cts >= _twstart and cts < _twstart + 5m and tjson->'k1'='v1' partition by tjson->'k1'",
            res_query="select `last(cts)`, `last(cint)`, `last(cbool)` from rdb.r98 where id=1 and `last(cts)` >= '2025-01-01 00:00:00.000' and `last(cts)` < '2025-01-01 00:15:00.000'",
            exp_query="select last(cts, cint, cbool) from qdb.jmeters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' and tjson->'k1'='v1' partition by tjson->'k1' interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=99,
            stream="create stream rdb.s99 count_window(2, c1) from tdb.v1 into rdb.r99 as (select cts, cint from qdb.t1 where cts >= _twstart and cts <= _twend + 4m limit 1 offset 0) union all (select cts, cint from qdb.t2 where cts >= _twstart and cts <= _twend + 4m limit 1 offset 2) union all (select cts, cint from qdb.t3 where cts >= _twstart and cts <= _twend + 4m limit 1 offset 4)",
            res_query="select * from rdb.r99 limit 3",
            exp_query="select * from (select cts, cint from qdb.t1 where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000' limit 1 offset 0) union all (select cts, cint from qdb.t2 where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000' limit 1 offset 2) union all (select cts, cint from qdb.t3 where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000' limit 1 offset 4) order by cts;",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=100,
            stream="create stream rdb.s100 count_window(2, c1) from tdb.v1 into rdb.r100 as select _twstart, count(*) from qdb.jmeters where cts >= _twstart and cts < _twend + 4m and tjson->'k1' match 'v1'",
            res_query="select * from rdb.r100 limit 3",
            exp_query="select _wstart, count(*) from qdb.jmeters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' and tjson->'k1' match 'v1' interval(5m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=101,
            stream="create stream rdb.s101 count_window(2, c1) from tdb.v1 into rdb.r101 as select cts, cint & cuint t1, cbigint | ctinyint t2, csmallint > 0 and (cfloat > 1 or cdouble > 1) t3,  cbool = true and (( cvarbinary is not null) or (cvarchar like 'SanFrancisco')) t4, case when cusmallint > 1 then 0 else 1 end t5 from qdb.v1 where cts >= _twstart and cts < _twend + 4m limit 1 offset 1;",
            res_query="select * from rdb.r101 limit 1",
            exp_query="select cts, cint & cuint t1, cbigint | ctinyint t2, csmallint > 0 and (cfloat > 1 or cdouble > 1) t3,  cbool = true and (( cvarbinary is not null) or (cvarchar like 'SanFrancisco')) t4, case when cusmallint > 1 then 0 else 1 end t5 from qdb.v1 where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000' limit 1 offset 1",
            check_func=self.check101,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=102,
            stream="create stream rdb.s102 count_window(2, c1) from tdb.t1 into rdb.r102 as select last(ts), cols(last(ts), c1, c2), count(1) from %%trows",
            res_query="select * from rdb.r102 limit 3",
            exp_query="select last(ts), cols(last(ts), c1, c2), count(1) from tdb.t1 where ts >= '2025-01-01 00:00:00.000' and ts < '2025-01-01 00:15:00.000' interval(5m);",
            check_func=self.check102,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=103,
            stream="create stream rdb.s103 count_window(2, c1) from tdb.vtriggers partition by tbname into rdb.r103 as select ts, cast(c1 as varchar) like '0' t1, cast(c1 as varchar) not like '0' t2, cast(c1 as varchar) regexp '[0-9]+' t3, cast(c1 as varchar) not regexp '[0-9]+' t4 from tdb.vtriggers where tbname=%%tbname and ts >= _twstart and ts < _twend + 4m limit 1",
            res_query="select * from rdb.r103 where tag_tbname='v1' limit 1 offset 1",
            exp_query="select ts, cast(c1 as varchar) like '0' t1, cast(c1 as varchar) not like '0' t2, cast(c1 as varchar) regexp '[0-9]+' t3, cast(c1 as varchar) not regexp '[0-9]+' t4, tbname from tdb.vtriggers where tbname='v1' and ts >= '2025-01-01 00:05:00.000' and ts < '2025-01-01 00:35:00.000' limit 1",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=104,
            stream="create stream rdb.s104 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r104 as select ts, CASE c1 WHEN 0 THEN 'Running' WHEN 5 THEN 'Warning' ELSE 'Unknown' END t1, c1 & c2 t2, c1 | c2 t3, (c1 != 0 or c2 <> 0) t4  from %%trows",
            res_query="select ts, t1, t2, t3, t4 from rdb.r104 where tag_tbname='t1' limit 6",
            exp_query="select ts, CASE c1 WHEN 0 THEN 'Running' WHEN 5 THEN 'Warning' ELSE 'Unknown' END t1, c1 & c2 t2, c1 | c2 t3, (c1 != 0 or c2 <> 0) t4 from tdb.t1 where ts >= '2025-01-01 00:00:00.000' and ts < '2025-01-01 00:15:00.000';",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=105,
            stream="create stream rdb.s105 count_window(2, c1) from tdb.v1 partition by tbname into rdb.r105 as select t1, c11, c12, t2, c21, c22, _twstart, _twend + 4m from (select cols(last_row(cint), cts as t1, cuint as c11), last_row(cint) c12,  cols(first(cint), cts as t2, cuint c21), first(cint) c22  from qdb.meters where tbname='t1' and cts >= _twstart and cts < _twstart + 5m)",
            check_func=self.check105,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=106,
            stream="create stream rdb.s106 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r106 as select _twstart, cts, concat(cvarchar, cnchar), cint + cuint, ctinyint - cdouble, cfloat * cdouble, cbigint * 12, -ctinyint from qdb.meters where tbname=%%tbname limit 1 offset 1;",
            res_query="select * from rdb.r106 where tag_tbname='t1' limit 1",
            exp_query="select cast('2025-01-01 00:00:00.000' as timestamp) ts, cts, concat(cvarchar, cnchar), cint + cuint, ctinyint - cdouble, cfloat * cdouble, cbigint * 12, -ctinyint, tbname from qdb.t1 limit 1 offset 1;",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=107,
            stream="create stream rdb.s107 count_window(2, c1) from tdb.v1 into rdb.r107 as select _twstart, cts, cvarchar like 'SanFrancisco', cnchar not like 'SanFrancisco', cvarchar regexp '%co', cnchar not regexp '%co' from qdb.t5 limit 1 offset 2;",
            res_query="select * from rdb.r107 limit 1",
            exp_query="select cast('2025-01-01 00:00:00.000' as timestamp) ts, cts, cvarchar like 'SanFrancisco', cnchar not like 'SanFrancisco', cvarchar regexp '%co', cnchar not regexp '%co' from qdb.t5 limit 1 offset 2;",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=108,
            stream="create stream rdb.s108 count_window(2, c1) from tdb.t1 into rdb.r108 as select _twstart, count(cts), first(cuint), last(cuint), sum(cuint), tbname from qdb.meters where tbname in ('t1', 't2', 't3', 't4') and cts >= _twstart and cts < _twstart + 5m partition by tbname slimit 1 soffset 2",
            res_query="select * from rdb.r108 limit 1",
            exp_query="select cast('2025-01-01 00:00:00.000' as timestamp) ts, count(cts), first(cuint), last(cuint), sum(cuint), tbname from qdb.meters where tbname in ('t1', 't2', 't3', 't4') and cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000' partition by tbname slimit 1 soffset 2",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=109,
            stream="create stream rdb.s109 count_window(2, c1) from tdb.v2 into rdb.r109 as select _twstart, count(ta.c1), count(ta.c2), sum(ta.c2), count(tb.c1), count(tb.c2), sum(tb.c2) from tdb.v2 ta join tdb.t2 tb on ta.ts = tb.ts where ta.ts >= _twstart and ta.ts < _twend + 4m group by ta.c2 having sum(tb.c2) > 130;",
            res_query="select * from rdb.r109 limit 1",
            exp_query="select cast('2025-01-01 00:15:00.000' as timestamp) ts, count(ta.c1), count(ta.c2), sum(ta.c2), count(tb.c1), count(tb.c2), sum(tb.c2) from tdb.v2 ta join tdb.t2 tb on ta.ts = tb.ts where ta.ts >= '2025-01-01 00:00:00.000' and ta.ts < '2025-01-01 00:35:00.000' group by ta.c2 having sum(tb.c2) > 130;",
        )
        # self.streams.append(stream) toreport

        stream = StreamItem(
            id=110,
            stream="create stream rdb.s110 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r110 as (select _twstart, count(cts), first(cuint), last(cuint), sum(cuint), tbname from qdb.meters where tbname in ('t1', 't2', 't3', 't4') and cts >= _twstart and cts < _twstart + 5m partition by tbname slimit 1 soffset 2) union (select _twstart + 1m, count(cts), first(cuint), last(cuint), sum(cuint), tbname from qdb.meters where tbname in ('t1', 't2', 't3', 't4') and cts >= _twstart and cts < _twstart + 5m partition by tbname slimit 1 soffset 3)",
            res_query="select `_twstart`, `count(cts)`, `first(cuint)`, `last(cuint)`, `sum(cuint)` from rdb.r110 where tag_tbname='t1' limit 1 offset 1",
            exp_query="select cast('2025-01-01 00:01:00.000' as timestamp) ts, count(cts), first(cuint), last(cuint), sum(cuint) from qdb.meters where tbname in ('t1', 't2', 't3', 't4') and cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000' partition by tbname slimit 1 soffset 3",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=111,
            stream="create stream rdb.s111 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r111 as select _wstart, _wend, _twstart, _twend + 4m, csmallint, count(cts), count(cbool) from qdb.meters where cts >= _twstart and cts < _twstart + 5m partition by tbname state_window(csmallint)",
            res_query="select * from rdb.r111 where `_wstart` >= '2025-01-01 00:00:00.000' and `_wstart` < '2025-01-01 00:05:00.000' and tag_tbname='t1';",
            exp_query="select _wstart, _wend, cast('2025-01-01 00:00:00.000' as timestamp) t1, cast('2025-01-01 00:05:00.000' as timestamp) t2, csmallint, count(cts), count(cbool), tbname from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000' and tbname='t1' partition by tbname STATE_WINDOW(csmallint);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=112,
            stream="create stream rdb.s112 count_window(2, c1) from tdb.vtriggers partition by tbname into rdb.r112 as select _twstart, count(ta.c1), count(ta.c2), sum(ta.c2), count(tb.c1), count(tb.c2), sum(tb.c2) from %%tbname ta join tdb.t2 tb on ta.ts = tb.ts where ta.ts >= _twstart and ta.ts < _twend + 4m group by ta.c2 having sum(tb.c2) > 130;",
            res_query="select * from rdb.r112 where tag_tbname='v2'",
            exp_query="select cast('2025-01-01 00:15:00.000' as timestamp) ts, count(ta.c1), count(ta.c2), sum(ta.c2), count(tb.c1), count(tb.c2), sum(tb.c2), 'v2' from tdb.v2 ta join tdb.t2 tb on ta.ts = tb.ts where ta.ts >= '2025-01-01 00:00:00.000' and ta.ts < '2025-01-01 00:35:00.000' group by ta.c2 having sum(tb.c2) > 130;",
        )
        # self.streams.append(stream) toreport

        stream = StreamItem(
            id=113,
            stream="create stream rdb.s113 count_window(2, c1) from tdb.t1 into rdb.r113 as select _wstart, _wend, _twstart, _twend + 4m, first(csmallint), count(cts), count(cbool) from qdb.t1 where cts >= _twstart and cts < _twstart + 5m and csmallint = 2 session(cts, 40s);",
            res_query="select * from rdb.r113 where `_wstart` >= '2025-01-01 00:00:00.000' and `_wstart` < '2025-01-01 00:05:00.000' ",
            exp_query="select _wstart, _wend, cast('2025-01-01 00:00:00.000' as timestamp) t1, cast('2025-01-01 00:05:00.000' as timestamp) t2, first(csmallint), count(cts), count(cbool) from qdb.t1 where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000' and csmallint = 2 partition by tbname session(cts, 40s);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=114,
            stream="create stream rdb.s114 count_window(2, c1) from tdb.vtriggers partition by tbname into rdb.r114 as select _twstart, count(ta.c1), count(ta.c2), sum(ta.c2), count(tb.c1), count(tb.c2), sum(tb.c2) from tdb.t2 ta join %%tbname tb on ta.ts = tb.ts where ta.ts >= _twstart and ta.ts < _twend + 9m group by ta.c2 having sum(tb.c2) > 1;",
            res_query="select * from rdb.r114 where tag_tbname='v2'",
            exp_query="select cast('2025-01-01 00:10:00.000' as timestamp) ts, count(ta.c1), count(ta.c2), sum(ta.c2), count(tb.c1), count(tb.c2), sum(tb.c2), 'v2' from tdb.t2 ta join tdb.v2 tb on ta.ts = tb.ts where ta.ts >= '2025-01-01 00:10:00.000' and ta.ts < '2025-01-01 00:20:00.000' group by ta.c2 having sum(tb.c2) > 0;",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=115,
            stream="create stream rdb.s115 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r115 as select ta.cts tats, tb.cts tbts, ta.cint tat1, tb.cint tbt1, _twstart, _twend + 4m from qdb.t1 ta left join qdb.t2 tb on ta.cts=tb.cts where ta.cts >= _twstart and ta.cts < _twend + 4m;",
            res_query="select tats, tbts, tat1, tbt1 from rdb.r115 where tag_tbname='t1' limit 30",
            exp_query="select ta.cts tats, tb.cts tbts, ta.cint tat1, tb.cint tbt1 from qdb.t1 ta left join qdb.t2 tb on ta.cts=tb.cts where ta.cts >= '2025-01-01 00:00:00.000' and ta.cts < '2025-01-01 00:15:00.000'",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=116,
            stream="create stream rdb.s116 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r116 as select _wstart, _wend, _twstart, _twend + 4m, _wduration, first(csmallint), count(cts), count(cbool) from qdb.meters where tbname=%%tbname and cts >= _twstart and cts < _twstart + 5m partition by tbname event_window start with csmallint = 1 end with csmallint = 2;",
            res_query="select * from rdb.r116 where tag_tbname='t1' and `_wstart` >= '2025-01-01 00:00:00.000' and `_wstart` < '2025-01-01 00:05:00.000';",
            exp_query="select _wstart, _wend, cast('2025-01-01 00:00:00.000' as timestamp) t1, cast('2025-01-01 00:05:00.000' as timestamp) t2, _wduration, first(csmallint), count(cts), count(cbool), 't1' from qdb.t1 where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000' event_window start with csmallint = 1 end with csmallint = 2;",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=117,
            stream="create stream rdb.s117 count_window(2, c1) from tdb.t1 into rdb.r117 as select _twstart, count(tac1), sum(tbcint) from (select ta.ts tats, tb.cts tbts, ta.c1 tac1, tb.cint tbcint from tdb.t1 ta left asof join qdb.t1 tb on ta.ts < tb.cts jlimit 5 where ta.ts >= _twstart and ta.ts < _twend + 4m);",
            res_query="select * from rdb.r117 limit 1",
            exp_query="select cast('2025-01-01 00:00:00.000' as timestamp) ts, count(tac1), sum(tbcint) from (select ta.ts tats, tb.cts tbts, ta.c1 tac1, tb.cint tbcint from tdb.t1 ta left asof join qdb.t1 tb on ta.ts < tb.cts jlimit 5 where ta.ts >= '2025-01-01 00:00:00.000' and ta.ts < '2025-01-01 00:05:00.000');",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=118,
            stream="create stream rdb.s118 count_window(2, c1) from tdb.vtriggers partition by id, tbname into rdb.r118 as select _twstart ts, count(tac1), sum(tbcint) from (select ta.ts tats, tb.cts tbts, ta.c1 tac1, tb.cint tbcint from qdb.t1 tb right asof join tdb.t1 ta on ta.ts < tb.cts jlimit 10 where ta.ts >= _twstart and ta.ts < _twend + 4m and cos(tb.cint) >= 0 and cos(ta.c1) > 0);",
            res_query="select * from rdb.r118 where id = 1 limit 1",
            exp_query="select cast('2025-01-01 00:00:00.000' as timestamp) ts, count(tac1), sum(tbcint), 1, 'v1' from (select ta.ts tats, tb.cts tbts, ta.c1 tac1, tb.cint tbcint from qdb.t1 tb right asof join tdb.t1 ta on ta.ts < tb.cts jlimit 10 where ta.ts >= '2025-01-01 00:00:00.000' and ta.ts < '2025-01-01 00:05:00.000' and cos(tb.cint) >= 0 and cos(ta.c1) > 0);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=119,
            stream="create stream rdb.s119 count_window(2, c1) from tdb.vtriggers partition by id, tbname into rdb.r119 as select _twstart ts, count(tac1), sum(tbcint) from (select ta.ts tats, tb.cts tbts, ta.c1 tac1, tb.cint tbcint from qdb.meters tb right asof join tdb.t1 ta on ta.ts < tb.cts jlimit 10 where tb.tint=1 and ta.ts >= _twstart and ta.ts < _twend + 4m and cos(tb.cint) >= 0 and cos(ta.c1) > 0);",
            res_query="select * from rdb.r119 where id =2 limit 1",
            exp_query="select cast('2025-01-01 00:00:00.000' as timestamp) ts, count(tac1), sum(tbcint), 2, 'v2' from (select ta.ts tats, tb.cts tbts, ta.c1 tac1, tb.cint tbcint from qdb.meters tb right asof join tdb.t1 ta on ta.ts < tb.cts jlimit 10 where tb.tint=1 and ta.ts >= '2025-01-01 00:00:00.000' and ta.ts < '2025-01-01 00:05:00.000' and cos(tb.cint) >= 0 and cos(ta.c1) > 0);",
        )
        # self.streams.append(stream) toreport

        stream = StreamItem(
            id=120,
            stream="create stream rdb.s120 count_window(2, c1) from tdb.triggers partition by id, tbname into rdb.r120 as select _twstart, count(tac1), sum(tbcint) from (select ta.ts tats, tb.cts tbts, ta.c1 tac1, tb.cint tbcint from tdb.t1 ta left window join qdb.meters tb window_offset(0m, 4m) where tb.tint=%%1 and ta.ts >= _twstart and ta.ts < _twstart + 4m and cos(tb.cint) >= 0 and cos(ta.c1) > 0);",
            res_query="select * from rdb.r120 where id=1 limit 1",
            exp_query="select cast('2025-01-01 00:00:00.000' as timestamp) ts, count(tac1), sum(tbcint), 1, 't1' from (select ta.ts tats, tb.cts tbts, ta.c1 tac1, tb.cint tbcint from tdb.t1 ta left window join qdb.meters tb window_offset(0m, 4m) where tb.tint=1 and ta.ts >= '2025-01-01 00:00:00.000' and ta.ts < '2025-01-01 00:05:00.000' and cos(tb.cint) >= 0 and cos(ta.c1) > 0);",
        )
        # self.streams.append(stream) toreport

        stream = StreamItem(
            id=121,
            stream="create stream rdb.s121 count_window(2, c1) from tdb.triggers partition by id into rdb.r121 as select first(ts2), tbname, sum(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t2.v_binary like '%abc%' or t2.v_binary not like '%abc%') where t2.v_binary like '%abc%' or t2.v_binary not like '%abc%') group by tbname order by tbname;",
            res_query="select * from rdb.r121",
            exp_query="select _wstart, sum(cint), count(cint), tbname from qdb.meters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:35:00.000' and tbname='t1' partition by tbname interval(5m);",
        )
        # self.streams.append(stream) TD-36443

        stream = StreamItem(
            id=122,
            stream="create stream rdb.s122 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r122 as select first(tats), last(tbts), count(tat1), sum(tat1), first(tbt1), last(tbt1) from (select ta.cts tats, tb.cts tbts, ta.cint tat1, tb.cint tbt1 from qdb.t1 ta left join qdb.t2 tb on ta.cts=tb.cts and (ta.cint >= tb.cint) order by ta.cts) where tats >= _twstart and tats < _twend + 4m",
            res_query="select * from rdb.r122 where tag_tbname='t1' limit 1 offset 0;",
            exp_query="select first(tats), last(tbts), count(tat1), sum(tat1), first(tbt1), last(tbt1), 't1' from (select ta.cts tats, tb.cts tbts, ta.cint tat1, tb.cint tbt1 from qdb.t1 ta left join qdb.t2 tb on ta.cts=tb.cts and (ta.cint >= tb.cint) order by ta.cts) where tats >= '2025-01-01 00:00:00.000' and tats < '2025-01-01 00:05:00.000';",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=123,
            stream="create stream rdb.s123 count_window(2, c1) from tdb.triggers partition by id, name, tbname into rdb.r123 as select _wstart tw, _wend te, _twstart, _twend + 4m, count(c1) c1, sum(c2) c2 from %%trows interval(1m)",
            res_query="select tw, te, c1, c2 from rdb.r123 where id=1",
            exp_query="select _wstart, _wend, count(c1), sum(c2) from tdb.t1 where ts >= '2025-01-01 00:00:00.000' and ts < '2025-01-01 00:35:00.000' interval(1m);",
            check_func=self.check123,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=124,
            stream="create stream rdb.s124 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r124 as select _wstart ts, count(c1), sum(c2) from %%trows interval(1m)",
            res_query="select * from rdb.r124 where tag_tbname='t1' limit 15;",
            exp_query="select _wstart, count(c1), sum(c2), 't1' from tdb.t1 where ts >= '2025-01-01 00:00:00.000' and ts < '2025-01-01 00:35:00.000' interval(1m);",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=125,
            stream="create stream rdb.s125 count_window(2, c1) from tdb.v1 into rdb.r125 as select _rowts, _twstart ts, DERIVATIVE(cbigint, 5, 0) from qdb.v1 where cts >= _twstart and cts < _twstart + 5m;",
            res_query="select `_rowts`, `derivative(cbigint, 5, 0)` from rdb.r125 limit 9",
            exp_query="select _rowts, DERIVATIVE(cbigint, 5, 0) from qdb.v1 where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000'",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=126,
            stream="create stream rdb.s126 count_window(2, c1) from tdb.vtriggers partition by id, tbname into rdb.r126 as select _twstart ts, IRATE(cbigint) from qdb.vmeters where tbname=%%tbname;",
            res_query="select * from rdb.r126 where id=1 limit 1 offset 2",
            exp_query="select cast('2025-01-01 00:10:00.000' as timestamp) ts, IRATE(cubigint), 1, 'v1' from qdb.v1 where cts >= '2025-01-01 00:10:00.000' and cts < '2025-01-01 00:15:00.000'",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=127,
            stream="create stream rdb.s127 count_window(2, c1) from tdb.v1 into rdb.r127 as select _rowts, MAVG(c1, 1) from %%trows;",
            res_query="select `_rowts`, `mavg(c1, 1)` from rdb.r127 limit 2 offset 2",
            exp_query="select _rowts, MAVG(c1, 1) from tdb.v1 where ts >= '2025-01-01 00:05:00.000' and ts < '2025-01-01 00:10:00.000'",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=128,
            stream="create stream rdb.s128 count_window(2, c1) from tdb.vtriggers partition by tbname into rdb.r128 as select _rowts, STATECOUNT(cdouble, 'LT', 5) from qdb.vmeters where tbname=%%tbname and cts >= _twstart and cts < _twend + 4m;",
            res_query="select * from rdb.r128 where tag_tbname='v1' and `_rowts` >= '2025-01-01 00:00:00.000' and `_rowts` < '2025-01-01 00:05:00.000'",
            exp_query="select cts, STATECOUNT(cdouble, 'LT', 5), 'v1' from qdb.vmeters where tbname='v1' and cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000'",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=129,
            stream="create stream rdb.s129 count_window(2, c1) from tdb.triggers partition by id, tbname into rdb.r129 as select _c0, STATEDURATION(cusmallint, 'LT', 5, 1m) from qdb.meters where tbname=%%2 and cts >= _twstart and cts < _twend + 4m;",
            res_query="select * from rdb.r129 where id=1 and `_c0` >= '2025-01-01 00:00:00.000' and `_c0` < '2025-01-01 00:05:00.000'",
            exp_query="select cts, STATEDURATION(cusmallint, 'LT', 5, 1m), 1, 't1' from qdb.meters where tbname='t1' and cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:05:00.000'",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=130,
            stream="create stream rdb.s130 count_window(2, c1) from tdb.vtriggers partition by tbname into rdb.r130 as select _twstart ts, _twend + 4m, TWA(ctinyint) from qdb.vmeters where tbname=%%1 and cts >= _twstart and cts < _twend + 4m;",
            res_query="select * from rdb.r130 where tag_tbname='v1' limit 1 offset 2",
            exp_query="select _qstart, _qstart + 5m, TWA(ctinyint), 'v1' from qdb.vmeters where tbname='v1' and cts >= '2025-01-01 00:10:00.000' and cts < '2025-01-01 00:15:00.000'",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=131,
            stream="create stream rdb.s131 count_window(2, c1) from tdb.triggers partition by id, tbname into rdb.r131 as select ta.ts tats, tb.cts tbts, ta.c1 tac1, ta.c2 tac2, tb.cint tbc1, tb.cuint tbc2, _twstart, _twend + 4m from tdb.triggers ta right join qdb.t1 tb on ta.ts=tb.cts where ta.ts >= _twstart and ta.ts < _twend + 4m and ta.id=%%1;",
            res_query="select tats, tbts, tac1, tac2, tbc1, tbc2 from rdb.r131 where id=1",
            exp_query="select ta.ts tats, tb.cts tbts, ta.c1 tac1, ta.c2 tac2, tb.cint tbc1, tb.cuint tbc2 from tdb.t1 ta right join qdb.t1 tb on ta.ts=tb.cts where ta.ts >= '2025-01-01 00:00:00.000' and ta.ts < '2025-01-01 00:35:00.000';",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=132,
            stream="create stream rdb.s132 count_window(2, c1) from tdb.t1 partition by id into rdb.r132 as select tb.cts tbts, ta.ts tats, ta.c1 tac1, ta.c2 tac2, tb.cint tbc1, tb.cuint tbc2, _twstart, _twend from tdb.t1 ta right join qdb.t1 tb on ta.ts=tb.cts where tb.cts >= _twstart and tb.cts < _twend + 4m;",
            res_query="select tbts, tats, tac1, tac2, tbc1, tbc2 from rdb.r132 limit 30",
            exp_query="select tb.cts tbts, ta.ts tats, ta.c1 tac1, ta.c2 tac2, tb.cint tbc1, tb.cuint tbc2 from tdb.t1 ta right join qdb.t1 tb on ta.ts=tb.cts where tb.cts >= '2025-01-01 00:00:00.000' and tb.cts < '2025-01-01 00:15:00.000';",
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=133,
            stream="create stream rdb.s133 count_window(2) from tdb.t1 into rdb.r133 as select * from tdb.t1 where _c0 >= _twend and _c0 >= _twstart and _c0 <= _twend;",
            res_query="select * from rdb.r133",
            exp_query="select cols(last(ts), *) from tdb.t1 count_window(2) limit 4;",
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
                ["tag_tbname", "VARCHAR", 270, "TAG"],
            ],
        )

        tdSql.checkResultsBySql(
            sql="select ts, tw from rdb.r1 limit 3;",
            exp_sql="select _wstart, count(*) from tdb.triggers where ts >= '2025-01-01 00:00:00' and ts < '2025-01-01 00:15:00' interval(5m) fill(value, 0);",
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
            and tdSql.compareData(0, 0, "2025-01-01 00:15:00.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:20:00.000")
            and tdSql.compareData(0, 2, 60000)
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
            and tdSql.compareData(0, 0, "2025-01-01 00:15:00.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:20:00.000")
            and tdSql.compareData(0, 2, 60000)
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
            and tdSql.compareData(0, 0, "2025-01-01 00:15:00.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:20:00.000")
            and tdSql.compareData(0, 2, 60000)
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
            and tdSql.compareData(0, 0, "2025-01-01 00:15:00.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:20:00.000")
            and tdSql.compareData(0, 2, 60000)
            and tdSql.compareData(0, 3, 2)
            and tdSql.compareData(0, 4, "t2"),
        )

    def check10(self):
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r10",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["te", "TIMESTAMP", 8, ""],
                ["td", "BIGINT", 8, ""],
                ["tw", "BIGINT", 8, ""],
                ["tg", "BIGINT", 8, ""],
                ["tl", "TIMESTAMP", 8, ""],
                ["t1_data", "INT", 4, ""],
                ["t2_data", "VARCHAR", 270, ""],
                ["tb_data", "VARCHAR", 270, ""],
                ["c1_data", "BIGINT", 8, ""],
                ["c2_data", "DOUBLE", 8, ""],
                ["id", "INT", 4, "TAG"],
                ["tag_tbname", "VARCHAR", 270, "TAG"],
            ],
        )
        tdSql.checkResultsByFunc(
            sql="select * from information_schema.ins_tags where db_name='rdb' and stable_name='r10' and tag_name='tag_tbname';",
            func=lambda: tdSql.getRows() == 2,
        )
        tdSql.checkResultsByFunc(
            sql="select * from rdb.r10 where tag_tbname='t2'",
            func=lambda: tdSql.getRows() == 1,
        )

    def check11(self):
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r11",
            schema=[
                ["tc", "TIMESTAMP", 8, ""],
                ["tp", "TIMESTAMP", 8, ""],
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

    def check16(self):
        tdSql.checkResultsByFunc(
            sql="select * from rdb.r16",
            func=lambda: tdSql.getRows() == 2,
        )

    def check27(self):
        tdSql.checkResultsByFunc(
            sql="select * from rdb.r27 where tag_tbname='v2'",
            func=lambda: tdSql.getRows() == 6
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 45)
            and tdSql.compareData(0, 2, 10)
            and tdSql.compareData(1, 0, "2025-01-01 00:05:00.000")
            and tdSql.compareData(1, 1, 145)
            and tdSql.compareData(1, 2, 10)
            and tdSql.compareData(2, 0, "2025-01-01 00:10:00.000")
            and tdSql.compareData(2, 1, 245)
            and tdSql.compareData(2, 2, 10)
            and tdSql.compareData(3, 0, "2025-01-01 00:15:00.000")
            and tdSql.compareData(3, 1, 345)
            and tdSql.compareData(3, 2, 10)
            and tdSql.compareData(4, 0, "2025-01-01 00:20:00.000")
            and tdSql.compareData(4, 1, 445)
            and tdSql.compareData(4, 2, 10)
            and tdSql.compareData(5, 0, "2025-01-01 00:31:00.000")
            and tdSql.compareData(5, 1, 645)
            and tdSql.compareData(5, 2, 10),
        )

    def check40(self):
        tdSql.checkResultsByFunc(
            "select * from rdb.r40;", func=lambda: tdSql.getRows() == 5
        )

    def check41(self):
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r41",
            schema=[
                ["cts", "TIMESTAMP", 8, ""],
                ["cint", "INT", 4, ""],
                ["cbool", "BOOL", 1, ""],
                ["cjson", "VARCHAR", 8, ""],
                ["_twstart", "TIMESTAMP", 8, ""],
                ["id", "INT", 4, "TAG"],
                ["tag_tbname", "VARCHAR", 270, "TAG"],
            ],
        )

    def check78(self):
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r78",
            schema=[
                ["_twstart", "TIMESTAMP", 8, ""],
                ["_twrownum", "BIGINT", 8, ""],
                ["count(*)", "BIGINT", 8, ""],
                ["sum(cdecimal8)", "DECIMAL(38, 0)", 16, ""],
                ["id", "INT", 4, "TAG"],
                ["name", "VARCHAR", 16, "TAG"],
                ["tag_tbname", "VARCHAR", 270, "TAG"],
            ],
        )

        tdSql.checkResultsByFunc(
            sql="select count(*) from rdb.r78 where id != 2;",
            func=lambda: tdSql.compareData(0, 0, 4),
        )

    def check79(self):
        tdSql.checkResultsByFunc(
            sql="select `_twstart`, `sum(cdecimal8)`, `count(*)` from rdb.r79 where tag_tbname='t1';",
            func=lambda: tdSql.getRows() == 4
            and tdSql.compareData(3, 0, "2025-01-01 00:30:00.000")
            and tdSql.compareData(3, 1, 56)
            and tdSql.compareData(3, 2, 10),
        )

    def check80(self):
        tdSql.checkResultsByFunc(
            sql="select * from rdb.r80;",
            func=lambda: tdSql.getRows() == 4
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 2)
            and tdSql.compareData(0, 2, 10)
            and tdSql.compareData(1, 0, "2025-01-01 00:05:00.000")
            and tdSql.compareData(1, 1, 2)
            and tdSql.compareData(1, 2, 10)
            and tdSql.compareData(2, 0, "2025-01-01 00:10:00.000")
            and tdSql.compareData(2, 1, 2)
            and tdSql.compareData(2, 2, 10)
            and tdSql.compareData(3, 0, "2025-01-01 00:30:00.000")
            and tdSql.compareData(3, 1, 2)
            and tdSql.compareData(3, 2, 10),
        )

    def check81(self):
        tdSql.checkResultsByFunc(
            sql="select count(*) from rdb.r81 where tag_tbname='t1';",
            func=lambda: tdSql.compareData(0, 0, 34),
        )

    def check84(self):
        tdSql.checkResultsByFunc(
            sql="select * from rdb.r84 where tag_tbname='t1';",
            func=lambda: tdSql.getRows() == 8,
        )

    def check89(self):
        tdSql.checkResultsByFunc(
            sql="select tw, ta, tb, c1, c2 from rdb.r89;",
            func=lambda: tdSql.getRows() == 4
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(1, 0, "2025-01-01 00:05:00.000")
            and tdSql.compareData(2, 0, "2025-01-01 00:10:00.000")
            and tdSql.compareData(3, 0, "2025-01-01 00:30:00.000")
            and tdSql.compareData(3, 3, 30)
            and tdSql.compareData(3, 4, 0),
        )

    def check101(self):
        tdSql.checkResultsByFunc(
            sql="select * from rdb.r101", func=lambda: tdSql.getRows() == 4
        )

    def check102(self):
        tdSql.checkResultsByFunc(
            sql="select * from rdb.r102",
            func=lambda: tdSql.getRows() == 4
            and tdSql.compareData(3, 0, "2025-01-01 00:31:00.000")
            and tdSql.compareData(3, 1, 30)
            and tdSql.compareData(3, 2, 10)
            and tdSql.compareData(3, 3, 2),
        )

    def check105(self):
        tdSql.checkResultsByFunc(
            sql="select t1, c11, c12, t2, c21, c22 from rdb.r105 where tag_tbname='v1';",
            func=lambda: tdSql.getRows() == 4
            and tdSql.compareData(0, 0, "2025-01-01 00:04:30.000")
            and tdSql.compareData(0, 1, 1)
            and tdSql.compareData(0, 2, 9)
            and tdSql.compareData(0, 3, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 4, 0)
            and tdSql.compareData(0, 5, 0)
            and tdSql.compareData(1, 0, "2025-01-01 00:09:30.000")
            and tdSql.compareData(1, 1, 3)
            and tdSql.compareData(1, 2, 19)
            and tdSql.compareData(1, 3, "2025-01-01 00:05:00.000")
            and tdSql.compareData(1, 4, 2)
            and tdSql.compareData(1, 5, 10)
            and tdSql.compareData(2, 0, "2025-01-01 00:14:30.000")
            and tdSql.compareData(2, 1, 1)
            and tdSql.compareData(2, 2, 29)
            and tdSql.compareData(2, 3, "2025-01-01 00:10:00.000")
            and tdSql.compareData(2, 4, 0)
            and tdSql.compareData(2, 5, 20)
            and tdSql.compareData(3, 0, "2025-01-01 00:34:30.000")
            and tdSql.compareData(3, 1, 1)
            and tdSql.compareData(3, 2, 69)
            and tdSql.compareData(3, 3, "2025-01-01 00:30:00.000")
            and tdSql.compareData(3, 4, 0)
            and tdSql.compareData(3, 5, 60),
        )

    def check123(self):
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r123",
            schema=[
                ["tw", "TIMESTAMP", 8, ""],
                ["te", "TIMESTAMP", 8, ""],
                ["_twstart", "TIMESTAMP", 8, ""],
                ["_twend + 4m", "TIMESTAMP", 8, ""],
                ["c1", "BIGINT", 8, ""],
                ["c2", "BIGINT", 8, ""],
                ["id", "INT", 4, "TAG"],
                ["name", "VARCHAR", 16, "TAG"],
                ["tag_tbname", "VARCHAR", 270, "TAG"],
            ],
        )
        tdSql.checkResultsByFunc(
            sql="select * from information_schema.ins_tables where db_name='rdb' and stable_name='r123';",
            func=lambda: tdSql.getRows() == 2,
        )
