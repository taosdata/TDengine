import time
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamSubquerySliding:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_subquery_sliding(self):
        """Subquery in Sliding

        1. Use sliding trigger mode

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

        id = 4
        # 里面和外面都要分组
        # stream="create stream rdb.s2 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r2 as select _twstart ts, _twend te, _twduration td, _twrownum tw, _tgrpid tg, _tlocaltime tl, %%tbname tb, %%1 tg1, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twend and _twduration is not null and _twrownum is not null and _tgrpid is not null and _tlocaltime is not null and %%tbname is not null and %%1 is not null;",
        # from %%tbname partition by %%tbname select
        # from %%1 partition by %%tbname select
        # from %%trows partition by %%tbname select
        # self.streams.append(stream)

        ##############
        stream = StreamItem(
            id=2,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.s2 tags(gid bigint as _tgrpid) as select _twstart ts, _twend te, _twduration td, _twrownum tw, _tgrpid tg, _tlocaltime tl, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twend and _twduration is not null and _twrownum is not null and _tgrpid is not null and _tlocaltime is not null;",
            res_query="select ts, c1, c2 from rdb.r1",
            exp_query="select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters interval(5m)",
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=3,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by c1, c2 into rdb.s2 tags(gid bigint as _tgrpid) as select _twstart ts, _twend te, _twduration td, _twrownum tw, _tgrpid tg, _tlocaltime tl, %%1, %%tbname, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twend and _twduration is not null and _twrownum is not null and _tgrpid is not null and _tlocaltime is not null and %%1 != '1' and %%tbname != '1';",
            res_query="select ts, c1, c2 from rdb.r1",
            exp_query="select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters interval(5m)",
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=4,
            stream="create stream rdb.s0 sliding(5m) from tdb.n1 into rdb.r0 as select _twstart ts, _twend te, _twduration td, _twrownum tw, _tgrpid tg, _tlocaltime tl, %%1, %%tbname, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twend and _twduration is not null and _twrownum is not null and _tgrpid is not null and _tlocaltime is not null and %%1 != '1' and %%tbname != '1';",
            res_query="select ts, c1, c2 from rdb.r1",
            exp_query="select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters interval(5m)",
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=5,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select _twstart ts, _twend te, _twduration td, _twrownum tw, _tgrpid tg, _tlocaltime tl, %%1, %%tbname, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twend and _twduration is not null and _twrownum is not null and _tgrpid is not null and _tlocaltime is not null and %%1 != '1' and %%tbname != '1';",
            res_query="select ts, c1, c2 from rdb.r1",
            exp_query="select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters interval(5m)",
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=6,
            stream="create stream rdb.s6 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r6 as select _twstart ts, %%tbname tb, %%1, count(*) v1, avg(c1) v2, first(c1) v3, last(c1) v4 from %%trows where c2 > 0;",
            res_query="select ts, tb, `%%1`, v2, v3, v4, tag_tbname from rdb.r6 where tb='t1'",
            exp_query="select _wstart, 't1', 't1', avg(c1) v2, first(c1) v3, last(c1) v4, 't1' from tdb.t1 where ts >= '2025-01-01 00:00:00' and ts < '2025-01-01 00:35:00' interval(5m) fill(NULL);",
            check_func=self.check6,
        )
        self.streams.append(stream)

        stream = StreamItem(
            id=7,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select _twstart ts, %%tbname tb, %%1, count(*) c1, avg(v1) c2, first(v1) c3, last(v1) c4 from %%trows where v2 > 0;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=8,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by c1, c2 as select _twstart ts, %%tbname tb, %%1, count(*) c1, avg(v1) c2, first(v1) c3, last(v1) c4 from %%trows where v2 > 0;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=9,
            stream="create stream rdb.s0 sliding(5m) from tdb.n1 into rdb.r0 as select _twstart ts, count(*) c1, avg(v1) c2, _twstart + 1 as ts2, %%tbname, _tgrpid from qdb.meters partition by %%tbname where _twduration > 10",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=10,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select _twstart ts, count(*) c1, avg(v1) c2, _twstart + 1 as ts2, %%tbname, _tgrpid from qdb.meters partition by %%tbname where _twduration > 10",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=11,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _twstart ts, count(*) c1, avg(v1) c2, _twstart + 1 as ts2, %%tbname, _tgrpid from qdb.meters partition by %%tbname where _twduration > 10",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=12,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select _wend tw, count(current) c1, _tgrpid tg, _tlocaltime tl  from qdb.meters where ts >= 1704038400000 and ts < 1704038700000 partition by %%1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=13,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by c1, c2 as select _wend tw, count(current) c1, _tgrpid tg, _tlocaltime tl  from qdb.meters where ts >= 1704038400000 and ts < 1704038700000 partition by %%1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=14,
            stream="create stream rdb.s0 sliding(5m) from tdb.n1 into rdb.r0 as select _wend tw, count(current) c1, _tgrpid tg, _tlocaltime tl  from qdb.meters where ts >= 1704038400000 and ts < 1704038700000 partition by %%1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=15,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select _wend tw, count(current) c1, _tgrpid tg, _tlocaltime tl  from qdb.meters where ts >= 1704038400000 and ts < 1704038700000 partition by %%1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=16,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _wend tw, count(current) c1, _tgrpid tg, _tlocaltime tl  from qdb.meters where ts >= 1704038400000 and ts < 1704038700000 partition by %%1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=17,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select _wend tw, count(current) c1, _tgrpid tg, _tlocaltime tl  from qdb.meters where ts >= 1704038400000 and ts < 1704038700000 partition by %%1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=18,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by c1, c2 as select _wend tw, count(current) c1, _tgrpid tg, _tlocaltime tl  from qdb.meters where ts >= 1704038400000 and ts < 1704038700000 partition by %%1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=19,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select _wend tw, count(current) c1, _tgrpid tg, _tlocaltime tl  from qdb.meters where ts >= 1704038400000 and ts < 1704038700000 partition by %%1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=20,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _wend tw, count(current) c1, _tgrpid tg, _tlocaltime tl  from qdb.meters where ts >= 1704038400000 and ts < 1704038700000 partition by %%1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=21,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select _wtstart ts, name, create_time, %%tbname information_schema.ins_users",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=22,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by c1, c2 as select _wtstart ts, name, create_time, %%tbname information_schema.ins_users",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=23,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select _wtstart ts, name, create_time, %%tbname information_schema.ins_users",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # test_dev_basic5.py
        # self.streams.append(stream)

        stream = StreamItem(
            id=24,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _tcurrent_ts tc, _tprev_ts tp, _tnext_ts tn, _tgrpid tg, _tlocaltime tl, %%1 tg1, %%tbname tb, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twend and _tprev_ts > '2024-12-30' and _tcurrent_ts > '2024-12-30' and _tnext_ts > '2024-12-30' and _tgrpid is not null and _tlocaltime is not null and %%1 != '1' and %%tbname != '1';",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=25,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select _tcurrent_ts tc, _tprev_ts tp, _tnext_ts tn, _tgrpid tg, _tlocaltime tl, %%1 tg1, %%tbname tb, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twend and _tprev_ts > '2024-12-30' and _tcurrent_ts > '2024-12-30' and _tnext_ts > '2024-12-30' and _tgrpid is not null and _tlocaltime is not null and %%1 != '1' and %%tbname != '1';",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=26,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select _tcurrent_ts tc, _tprev_ts tp, _tnext_ts tn, _tgrpid tg, _tlocaltime tl, %%1 tg1, %%tbname tb, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twend and _tprev_ts > '2024-12-30' and _tcurrent_ts > '2024-12-30' and _tnext_ts > '2024-12-30' and _tgrpid is not null and _tlocaltime is not null and %%1 != '1' and %%tbname != '1';",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=27,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _tcurrent_ts tc, _tprev_ts - _tnext_ts tx, %%tbname tb, %%1 tg1, count(*) c1, avg(v1) c2, first(v1) c3, last(v1) c4 from %%trows where v1 > 0;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=28,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select _tcurrent_ts tc, _tprev_ts - _tnext_ts tx, %%tbname tb, %%1 tg1, count(*) c1, avg(v1) c2, first(v1) c3, last(v1) c4 from %%trows where v1 > 0;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=29,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by c1, c2 as select _tcurrent_ts tc,  _tlocaltime - _tcurrent_ts tx1, _tcurrent_ts-_tprev_ts tx2, _tnext_ts-_tcurrent_ts tx3, count(*) c1, avg(v1) c2, _tnext_ts + 1 as ts2, %%trows, _tgrpid from qdb.meters partition by %%tbname;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=30,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select _tcurrent_ts tc,  _tlocaltime - _tcurrent_ts tx1, _tcurrent_ts-_tprev_ts tx2, _tnext_ts-_tcurrent_ts tx3, count(*) c1, avg(v1) c2, _tnext_ts + 1 as ts2, %%trows, _tgrpid from qdb.meters partition by %%tbname;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=31,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by c1, c2 as select _tcurrent_ts tc,  _tlocaltime - _tcurrent_ts tx1, _tcurrent_ts-_tprev_ts tx2, _tnext_ts-_tcurrent_ts tx3, count(*) c1, avg(v1) c2, _tnext_ts + 1 as ts2, %%trows, _tgrpid from qdb.meters partition by %%tbname;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=32,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select _tnext_ts tn, TIMETRUNCATE(_tnext_ts, '1d'), count(current) c1, _tgrpid tg, _tlocaltime tl  from qdb.meters where ts >= 1704038400000 and ts < 1704038700000 partition by %%1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=33,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _tnext_ts tn, TIMETRUNCATE(_tnext_ts, '1d'), count(current) c1, _tgrpid tg, _tlocaltime tl  from qdb.meters where ts >= 1704038400000 and ts < 1704038700000 partition by %%1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=34,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select _tnext_ts tn, TIMETRUNCATE(_tnext_ts, '1d'), count(current) c1, _tgrpid tg, _tlocaltime tl  from qdb.meters where ts >= 1704038400000 and ts < 1704038700000 partition by %%1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=35,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by c1, c2 as select _tcurrent_ts ts, name, create_time, %%tbname, %%1, %%trows, information_schema.ins_users",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=36,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select _tcurrent_ts ts, name, create_time, %%tbname, %%1, %%trows, information_schema.ins_users",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=37,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _tcurrent_ts ts, name, create_time, %%tbname, %%1, %%trows, information_schema.ins_users",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=38,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select _wtstart ts, count(*), sum(`vgroups`), avg(ntables) from information_schema.ins_databases where name != `information_schema` and name != 'performance_schema'",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=39,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _wtstart ts, count(*), sum(`vgroups`), avg(ntables) from information_schema.ins_databases where name != `information_schema` and name != 'performance_schema'",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=40,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select _wtstart ts, count(*), sum(`vgroups`), avg(ntables) from information_schema.ins_databases where name != `information_schema` and name != 'performance_schema'",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=41,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _wtstart ts, ABS(cint), ACOS(cuint), ASIN(cbigint), ATAN(cubigint), CEIL(cfloat), COS(cdouble), DEGREES(csmallint), EXP(cusmallint), FLOOR(ctinyint), LN(cutinyint), LOG(cbool), MOD(cdecimal8, cdecimal16), PI(), POW(cuint, 2), RADIANS(cbigint), ROUND(cfloat), SIGN(cdouble), SQRT(csmallint), TAN(cfloat), TRUNCATE(cdouble, 1), CRC32(cvarchar) from qdb.meters where tbname=%%1 order by cts limit 1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=42,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select _wtstart ts, ABS(cint), ACOS(cuint), ASIN(cbigint), ATAN(cubigint), CEIL(cfloat), COS(cdouble), DEGREES(csmallint), EXP(cusmallint), FLOOR(ctinyint), LN(cutinyint), LOG(cbool), MOD(cdecimal8, cdecimal16), PI(), POW(cuint, 2), RADIANS(cbigint), ROUND(cfloat), SIGN(cdouble), SQRT(csmallint), TAN(cfloat), TRUNCATE(cdouble, 1), CRC32(cvarchar) from qdb.meters where tbname=%%1 order by cts limit 1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=43,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _wtstart ts, ABS(cint), ACOS(cuint), ASIN(cbigint), ATAN(cubigint), CEIL(cfloat), COS(cdouble), DEGREES(csmallint), EXP(cusmallint), FLOOR(ctinyint), LN(cutinyint), LOG(cbool), MOD(cdecimal8, cdecimal16), PI(), POW(cuint, 2), RADIANS(cbigint), ROUND(cfloat), SIGN(cdouble), SQRT(csmallint), TAN(cfloat), TRUNCATE(cdouble, 1), CRC32(cvarchar) from qdb.meters where tbname=%%1 order by cts limit 1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=44,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select _wtstart ts, ASCII(cvarchar), CHAR(cnchar), CHAR_LENGTH(%%tbname), CHAR_LENGTH(%%n), CHAR_LENGTH(cvarchar), CONCAT(cvarchar, cnchar), CONCAT_WS('--', cvarchar, cnchar), LENGTH(cnchar), LOWER(cvarchar), LTRIM(cnchar), POSITION('a', cvarchar), REPEAT(cnchar, 3), REPLACE(cvarchar, 'a', 'b'), RTRIM(cnchar), SUBSTRING(cvarchar, 'a'), SUBSTRG(cvarchar, 'a'), SUBSTRING_INDEX(cnchar, 1, 'a'), TRIM(cvarchar), UPPER(cnchar) from qdb.n1 order by cts limit 1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=45,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _wtstart ts, ASCII(cvarchar), CHAR(cnchar), CHAR_LENGTH(%%tbname), CHAR_LENGTH(%%n), CHAR_LENGTH(cvarchar), CONCAT(cvarchar, cnchar), CONCAT_WS('--', cvarchar, cnchar), LENGTH(cnchar), LOWER(cvarchar), LTRIM(cnchar), POSITION('a', cvarchar), REPEAT(cnchar, 3), REPLACE(cvarchar, 'a', 'b'), RTRIM(cnchar), SUBSTRING(cvarchar, 'a'), SUBSTRG(cvarchar, 'a'), SUBSTRING_INDEX(cnchar, 1, 'a'), TRIM(cvarchar), UPPER(cnchar) from qdb.n1 order by cts limit 1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=46,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select _wtstart ts, ASCII(cvarchar), CHAR(cnchar), CHAR_LENGTH(%%tbname), CHAR_LENGTH(%%n), CHAR_LENGTH(cvarchar), CONCAT(cvarchar, cnchar), CONCAT_WS('--', cvarchar, cnchar), LENGTH(cnchar), LOWER(cvarchar), LTRIM(cnchar), POSITION('a', cvarchar), REPEAT(cnchar, 3), REPLACE(cvarchar, 'a', 'b'), RTRIM(cnchar), SUBSTRING(cvarchar, 'a'), SUBSTRG(cvarchar, 'a'), SUBSTRING_INDEX(cnchar, 1, 'a'), TRIM(cvarchar), UPPER(cnchar) from qdb.n1 order by cts limit 1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=47,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _wtstart ts, CAST(cint as varchar), TO_CHAR(cts, 'yyyy-mm-dd'), TO_ISO8601(cts), TO_TIMESTAMP(TO_CHAR(cts, 'yyyy-mm-dd'), 'yyyy-mm-dd'), TO_UNIXTIMESTAMP(TO_CHAR(cts, 'yyyy-mm-dd')) from qdb.v1 where _tlocaltime > > '2024-12-30' order by cts limit 1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=48,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select _wtstart ts, CAST(cint as varchar), TO_CHAR(cts, 'yyyy-mm-dd'), TO_ISO8601(cts), TO_TIMESTAMP(TO_CHAR(cts, 'yyyy-mm-dd'), 'yyyy-mm-dd'), TO_UNIXTIMESTAMP(TO_CHAR(cts, 'yyyy-mm-dd')) from qdb.v1 where _tlocaltime > > '2024-12-30' order by cts limit 1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=49,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select _wtstart ts, CAST(cint as varchar), TO_CHAR(cts, 'yyyy-mm-dd'), TO_ISO8601(cts), TO_TIMESTAMP(TO_CHAR(cts, 'yyyy-mm-dd'), 'yyyy-mm-dd'), TO_UNIXTIMESTAMP(TO_CHAR(cts, 'yyyy-mm-dd')) from qdb.v1 where _tlocaltime > > '2024-12-30' order by cts limit 1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=50,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select _wtstart ts, DAYOFWEEK(_twstart), DAYOFWEEK(_twend), DAYOFWEEK(_tlocaltime), TIMEDIFF(_twstart, _twend), _wduration, DAYOFWEEK(cvarchar), TIMEDIFF(_wstart, cts), TIMETRUNCATE(cts, '1y'), WEEK(cts), WEEKDAY(cts), WEEKOFYEAR(cts) from qdb.v5 order by cts desc limit 1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=51,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by c1, c2 as select _wtstart ts, DAYOFWEEK(_twstart), DAYOFWEEK(_twend), DAYOFWEEK(_tlocaltime), TIMEDIFF(_twstart, _twend), _wduration, DAYOFWEEK(cvarchar), TIMEDIFF(_wstart, cts), TIMETRUNCATE(cts, '1y'), WEEK(cts), WEEKDAY(cts), WEEKOFYEAR(cts) from qdb.v5 order by cts desc limit 1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=52,
            stream="create stream rdb.s0 sliding(5m) from tdb.n1 into rdb.r0 as select _wtstart ts, DAYOFWEEK(_twstart), DAYOFWEEK(_twend), DAYOFWEEK(_tlocaltime), TIMEDIFF(_twstart, _twend), _wduration, DAYOFWEEK(cvarchar), TIMEDIFF(_wstart, cts), TIMETRUNCATE(cts, '1y'), WEEK(cts), WEEKDAY(cts), WEEKOFYEAR(cts) from qdb.v5 order by cts desc limit 1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=53,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _wtstart ts, RAND(), NOW(), TODAY(), TIMEZONE() from qdb.n2 where _tgrpid != 0 order by cts desc limit 1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=54,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select _wtstart ts, RAND(), NOW(), TODAY(), TIMEZONE() from qdb.n2 where _tgrpid != 0 order by cts desc limit 1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=55,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by c1, c2 as select _wtstart ts, RAND(), NOW(), TODAY(), TIMEZONE() from qdb.n2 where _tgrpid != 0 order by cts desc limit 1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=56,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=57,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _wtstart ts, APERCENTILE(cint), AVG(cuint), SUM(_twrownum), COUNT(_tgrpid), COUNT(cbigint), ELAPSED(cubigint), HISTOGRAM(cfloat, 'user_input', '[1, 3, 5, 7]', 1), HYPERLOGLOG(cdouble), LEASTSQUARES(csmallint, 1, 2), PERCENTILE(cusmallint, 90), SPREAD(ctinyint), STDDEV(cutinyint), STDDEV_POP(cbool), SUM(cdecimal8), VAR_POP(cbigint) from qdb.meters where ts >= _twstart and ts < _twend;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=58,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select _wtstart ts, APERCENTILE(cint), AVG(cuint), SUM(_twrownum), COUNT(_tgrpid), COUNT(cbigint), ELAPSED(cubigint), HISTOGRAM(cfloat, 'user_input', '[1, 3, 5, 7]', 1), HYPERLOGLOG(cdouble), LEASTSQUARES(csmallint, 1, 2), PERCENTILE(cusmallint, 90), SPREAD(ctinyint), STDDEV(cutinyint), STDDEV_POP(cbool), SUM(cdecimal8), VAR_POP(cbigint) from qdb.meters where ts >= _twstart and ts < _twend;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=59,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _wtstart ts, BOTTOM(cint, 1), FIRST(cuint), LAST(cbigint), LAST_ROW(cubigint), GREATEST(cfloat, cdouble), LEAST(cdouble, csmallint), PERCENTILE(cusmallint, 90), MAX(ctinyint), MIN(cutinyint), MODE(cbool), SAMPLE(cdecimal8, 1), TAIL(cbigint, 1), TOP(cbigint, 1) from qdb.n2 where ts >= _twstart and ts < _twend;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=60,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select _wtstart ts, BOTTOM(cint, 1), FIRST(cuint), LAST(cbigint), LAST_ROW(cubigint), GREATEST(cfloat, cdouble), LEAST(cdouble, csmallint), PERCENTILE(cusmallint, 90), MAX(ctinyint), MIN(cutinyint), MODE(cbool), SAMPLE(cdecimal8, 1), TAIL(cbigint, 1), TOP(cbigint, 1) from qdb.n2 where ts >= _twstart and ts < _twend;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=61,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by c1, c2 as select _wtstart ts, BOTTOM(cint, 1), FIRST(cuint), LAST(cbigint), LAST_ROW(cubigint), GREATEST(cfloat, cdouble), LEAST(cdouble, csmallint), PERCENTILE(cusmallint, 90), MAX(ctinyint), MIN(cutinyint), MODE(cbool), SAMPLE(cdecimal8, 1), TAIL(cbigint, 1), TOP(cbigint, 1) from qdb.n2 where ts >= _twstart and ts < _twend;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=62,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select _wtstart ts, CSUM(cint) + CSUM(cuint), DERIVATIVE(cbigint, 5, 0), IRATE(cubigint), MAVG(cfloat, 1), STATECOUNT(cdouble, 'LT', 5), STATEDURATION(cusmallint, , 'LT', 5, '1m'), TWA(ctinyint) from qdb.v3 where ts >= _twstart and ts < _twend;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=63,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _wtstart ts, CSUM(cint) + CSUM(cuint), DERIVATIVE(cbigint, 5, 0), IRATE(cubigint), MAVG(cfloat, 1), STATECOUNT(cdouble, 'LT', 5), STATEDURATION(cusmallint, , 'LT', 5, '1m'), TWA(ctinyint) from qdb.v3 where ts >= _twstart and ts < _twend;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=64,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select _wtstart ts, CSUM(cint) + CSUM(cuint), DERIVATIVE(cbigint, 5, 0), IRATE(cubigint), MAVG(cfloat, 1), STATECOUNT(cdouble, 'LT', 5), STATEDURATION(cusmallint, , 'LT', 5, '1m'), TWA(ctinyint) from qdb.v3 where ts >= _twstart and ts < _twend;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=65,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by c1, c2 as select _wtstart ts, ST_GeomFromText(cgeometry), ST_AsText(cgeometry), ST_Contains(cgeometry, cgeometry), ST_ContainsProperly(cgeometry, cgeometry), ST_Covers(cgeometry, cgeometry), ST_Equals(cgeometry, cgeometry), ST_Intersects(cgeometry, cgeometry), ST_Touches(cgeometry, cgeometry) from qdb.meters where tbname='t4'",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=66,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select _wtstart ts, ST_GeomFromText(cgeometry), ST_AsText(cgeometry), ST_Contains(cgeometry, cgeometry), ST_ContainsProperly(cgeometry, cgeometry), ST_Covers(cgeometry, cgeometry), ST_Equals(cgeometry, cgeometry), ST_Intersects(cgeometry, cgeometry), ST_Touches(cgeometry, cgeometry) from qdb.meters where tbname='t4'",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=67,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _wtstart ts, ST_GeomFromText(cgeometry), ST_AsText(cgeometry), ST_Contains(cgeometry, cgeometry), ST_ContainsProperly(cgeometry, cgeometry), ST_Covers(cgeometry, cgeometry), ST_Equals(cgeometry, cgeometry), ST_Intersects(cgeometry, cgeometry), ST_Touches(cgeometry, cgeometry) from qdb.meters where tbname='t4'",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=68,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id as select CLIENT_VERSION(), CURRENT_USER(), SERVER_STATUS(), SERVER_VERSION(), DATABASE()",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=69,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by c1, c2 as select CLIENT_VERSION(), CURRENT_USER(), SERVER_STATUS(), SERVER_VERSION(), DATABASE()",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=70,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers as select CLIENT_VERSION(), CURRENT_USER(), SERVER_STATUS(), SERVER_VERSION(), DATABASE()",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=71,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by c1, c2 as select cts, tail(cint, 5) from qdb.meters where tbname='%%1' and cts >= _twstart and cts < _twend and _twrownum > 0;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=72,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select cts, tail(cint, 5) from qdb.meters where tbname='%%1' and cts >= _twstart and cts < _twend and _twrownum > 0;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=73,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select cts, tail(cint, 5) from qdb.meters where tbname='%%1' and cts >= _twstart and cts < _twend and _twrownum > 0;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=74,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select cts, diff(cint, 5) c3 from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twenda and c3 > 5 and _twrownum > 1 ",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=75,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by c1, c2 as select cts, diff(cint, 5) c3 from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twenda and c3 > 5 and _twrownum > 1 ",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=76,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select cts, diff(cint, 5) c3 from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twenda and c3 > 5 and _twrownum > 1 ",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=77,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select cts, top(cint, 5) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend limit 2 offset 2;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=78,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select cts, top(cint, 5) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend limit 2 offset 2;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=79,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by c1, c2 as select cts, top(cint, 5) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend limit 2 offset 2;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=80,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select cts, last(cint, 5) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=81,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by c1, c2 as select cts, last(cint, 5) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=82,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select cts, last(cint, 5) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=83,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select cts, last_row(cint, 5) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend ",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=84,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select cts, last_row(cint, 5) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend ",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=85,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.t1 into rdb.r0 as select cts, last_row(cint, 5) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend ",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=86,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select cts, sum(cint), sum(cint) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=87,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select cts, sum(cint), sum(cint) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=88,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select cts, sum(cint), sum(cint) from qdb.meters where tbname='%%1' and ts >= _twstart and ts < _twend;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=89,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.t1 into rdb.r0 as select deviceid, ts, diff(faev) as diff_faev FROM (SELECT deviceid, ts, faev FROM ((SELECT deviceid, ts, faev FROM (SELECT deviceid, _ts AS ts, faev, DIFF(ROUND(faev*1000)/1000) AS diff_faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid) WHERE diff_faev < 0)UNION ALL(SELECT deviceid, ts, faev FROM (SELECT deviceid, ts, faev, DIFF(ROUND(faev*1000)/1000) as diff_faev FROM (SELECT deviceid, _ts as ts , faev FROM demo WHERE deviceid in ('201000008','K201000258')AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' ORDER BY ts desc) PARTITION BY deviceid) WHERE diff_faev > 0)UNION ALL(SELECT deviceid, _wstart AS ts, LAST(faev) AS faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-11-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid INTERVAL(1n))) ORDER BY deviceid, ts) PARTITION by deviceid;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=90,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select deviceid, ts, diff(faev) as diff_faev FROM (SELECT deviceid, ts, faev FROM ((SELECT deviceid, ts, faev FROM (SELECT deviceid, _ts AS ts, faev, DIFF(ROUND(faev*1000)/1000) AS diff_faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid) WHERE diff_faev < 0)UNION ALL(SELECT deviceid, ts, faev FROM (SELECT deviceid, ts, faev, DIFF(ROUND(faev*1000)/1000) as diff_faev FROM (SELECT deviceid, _ts as ts , faev FROM demo WHERE deviceid in ('201000008','K201000258')AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' ORDER BY ts desc) PARTITION BY deviceid) WHERE diff_faev > 0)UNION ALL(SELECT deviceid, _wstart AS ts, LAST(faev) AS faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-11-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid INTERVAL(1n))) ORDER BY deviceid, ts) PARTITION by deviceid;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=91,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.t1 into rdb.r0 as select deviceid, ts, diff(faev) as diff_faev FROM (SELECT deviceid, ts, faev FROM ((SELECT deviceid, ts, faev FROM (SELECT deviceid, _ts AS ts, faev, DIFF(ROUND(faev*1000)/1000) AS diff_faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid) WHERE diff_faev < 0)UNION ALL(SELECT deviceid, ts, faev FROM (SELECT deviceid, ts, faev, DIFF(ROUND(faev*1000)/1000) as diff_faev FROM (SELECT deviceid, _ts as ts , faev FROM demo WHERE deviceid in ('201000008','K201000258')AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' ORDER BY ts desc) PARTITION BY deviceid) WHERE diff_faev > 0)UNION ALL(SELECT deviceid, _wstart AS ts, LAST(faev) AS faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-11-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid INTERVAL(1n))) ORDER BY deviceid, ts) PARTITION by deviceid;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=92,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select ts, c1 from union_tb1 order by ts asc limit 10) union all (select ts, c1 from union_tb0 order by ts desc limit 2) union all (select ts, c1 from union_tb2 order by ts asc limit 10) order by ts",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=93,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select ts, c1 from union_tb1 order by ts asc limit 10) union all (select ts, c1 from union_tb0 order by ts desc limit 2) union all (select ts, c1 from union_tb2 order by ts asc limit 10) order by ts",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=94,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select ts, c1 from union_tb1 order by ts asc limit 10) union all (select ts, c1 from union_tb0 order by ts desc limit 2) union all (select ts, c1 from union_tb2 order by ts asc limit 10) order by ts",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=95,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.t1 into rdb.r0 as select cols(last(ts), ts, c0), count(1) {t1} from {from_table} group by t1 order by t1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=96,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select cols(last(ts), ts, c0), count(1) {t1} from {from_table} group by t1 order by t1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=97,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select cols(last(ts), ts, c0), count(1) {t1} from {from_table} group by t1 order by t1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=98,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select  c11, c21, _rowts from (select cols(last_row(c0), ts as t1, c1 as c11), cols(first(c0), ts as t2, c1 c21), first(c0)  from test.meters where c0 < 4)",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=99,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.t1 into rdb.r0 as select  c11, c21, _rowts from (select cols(last_row(c0), ts as t1, c1 as c11), cols(first(c0), ts as t2, c1 c21), first(c0)  from test.meters where c0 < 4)",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=100,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select  c11, c21, _rowts from (select cols(last_row(c0), ts as t1, c1 as c11), cols(first(c0), ts as t2, c1 c21), first(c0)  from test.meters where c0 < 4)",
            res_query="select ts, c1, c2 from rdb.r1",
            exp_query="select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters interval(5m)",
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=101,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _twstart, count(*), avg(cint) from qdb.meters interval(1m) where tbname != %%tbname and ts >= _twstart and ts < _twend;",
            res_query="select ts, c1, c2 from rdb.r1",
            exp_query="select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters interval(5m)",
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=102,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 tags(tbn varchar(128) as %%tbname) as select _twstart, count(*), avg(cint) from qdb.meters interval(1m) where tbname != %%tbname and ts >= _twstart and ts < _twend;",
            res_query="select ts, c1, c2 from rdb.r1",
            exp_query="select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters interval(5m)",
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=103,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.t1 into rdb.r0 tags(tbn varchar(128) as %%tbname) as select _twstart, count(*), avg(cint) from qdb.meters interval(1m) where tbname != %%tbname and ts >= _twstart and ts < _twend;",
            res_query="select ts, c1, c2 from rdb.r1",
            exp_query="select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters interval(5m)",
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=104,
            stream="create stream rdb.s0 sliding(5m) from tdb.n1 into rdb.r0 as select _twstart, sum(cts), FIRST(cint) from qdb.meters interval(2m)",
            res_query="select ts, c1, c2 from rdb.r1",
            exp_query="select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters interval(5m)",
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=105,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select _twstart, sum(cts), FIRST(cint) from qdb.meters interval(2m)",
            res_query="select ts, c1, c2 from rdb.r1",
            exp_query="select _wstart ts, count(cint) c1, avg(cint) c2 from qdb.meters interval(5m)",
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=106,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _twstart, sum(cts), FIRST(cint) from qdb.meters interval(2m)",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=107,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 tags(c1 int as t1, c2 int as t2) as select _twstart, count(cts) from qdb.meters partition by tbname count(1000) where t1 < xx",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=108,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.t1 into rdb.r0 as select _twstart, count(cts) from qdb.meters partition by tbname count(1000) where t1 < xx",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=109,
            stream="create stream rdb.s0 sliding(5m) from tdb.n1 into rdb.r0 as select _twstart, count(cts) from qdb.meters partition by tbname count(1000) where t1 < xx",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=110,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.v1 into rdb.r0 as select _twstart, count(cts) from qdb.meters partition by tbname state(cint) where t2 = xx",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=111,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _twstart, count(cts) from qdb.meters partition by tbname state(cint) where t2 = xx",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=112,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select _twstart, count(cts) from qdb.meters partition by tbname state(cint) where t2 = xx",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=113,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.t1 into rdb.r0 as select _twstart, count(cts) from qdb.meters partition by tbname session(cbigint)  where ts >= _twstart and ts < _twend;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=114,
            stream="create stream rdb.s0 sliding(5m) from tdb.n1 into rdb.r0 as select _twstart, count(cts) from qdb.meters partition by tbname session(cbigint)  where ts >= _twstart and ts < _twend;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=115,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.v1 into rdb.r0 as select _twstart, count(cts) from qdb.meters partition by tbname session(cbigint)  where ts >= _twstart and ts < _twend;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=116,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _twstart, count(cts) from qdb.meters partition by tbname event(cbigint) _qstart, _qend, _wstart, _wend, _wduration, _c0, _rowts, irowts, _irowtsorigin, tbname where ts >= _twstart and ts < _twend;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=117,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select _twstart, count(cts) from qdb.meters partition by tbname event(cbigint) _qstart, _qend, _wstart, _wend, _wduration, _c0, _rowts, irowts, _irowtsorigin, tbname where ts >= _twstart and ts < _twend;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=118,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.t1 into rdb.r0 as select _twstart, count(cts) from qdb.meters partition by tbname event(cbigint) _qstart, _qend, _wstart, _wend, _wduration, _c0, _rowts, irowts, _irowtsorigin, tbname where ts >= _twstart and ts < _twend;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=119,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.v1 into rdb.r0 as select dictintc from qdb.meters where tbname in(%%tbname)",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=120,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select dictintc from qdb.meters where tbname in(%%tbname)",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=121,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select first(ts2), tbname, sum(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t2.v_binary like '%abc%' or t2.v_binary not like '%abc%') where t2.v_binary like '%abc%' or t2.v_binary not like '%abc%') group by tbname order by tbname;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=122,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.t1 into rdb.r0 as select first(ts2), tbname, sum(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t2.v_binary like '%abc%' or t2.v_binary not like '%abc%') where t2.v_binary like '%abc%' or t2.v_binary not like '%abc%') group by tbname order by tbname;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=123,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.v1 into rdb.r0 as select first(ts2), tbname, sum(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t2.v_binary like '%abc%' or t2.v_binary not like '%abc%') where t2.v_binary like '%abc%' or t2.v_binary not like '%abc%') group by tbname order by tbname;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=124,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bigint >= t2.t_bigint or t1.t_bigint < t2.t_bigint) and t2.t_bigint_empty is null order by t1.ts;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=125,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bigint >= t2.t_bigint or t1.t_bigint < t2.t_bigint) and t2.t_bigint_empty is null order by t1.ts;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=126,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.v1 into rdb.r0 as select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bigint >= t2.t_bigint or t1.t_bigint < t2.t_bigint) and t2.t_bigint_empty is null order by t1.ts;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=127,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _wend, count(*) from (select t1.ts ts1, t2.ts ts2, t1.v_bigint v_bigint1, t2.v_bigint v_bigint2, t1.tbname from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 order by t1.ts) where v_bigint1 + v_bigint2 > 0 and ts1 between '2024-01-01 12:00:00.400' and now and ts2 != '2024-01-01 12:00:00.300' partition by tbname interval(1s) order by _wend;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=128,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select _wend, count(*) from (select t1.ts ts1, t2.ts ts2, t1.v_bigint v_bigint1, t2.v_bigint v_bigint2, t1.tbname from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 order by t1.ts) where v_bigint1 + v_bigint2 > 0 and ts1 between '2024-01-01 12:00:00.400' and now and ts2 != '2024-01-01 12:00:00.300' partition by tbname interval(1s) order by _wend;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=129,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.t1 into rdb.r0 as select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t1.v_int >= 0 and (cos(t2.t_double) >= 0 or cos(t2.t_double) < 0) order by t1.ts, t2.ts;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=130,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.v1 into rdb.r0 as select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t1.v_int >= 0 and (cos(t2.t_double) >= 0 or cos(t2.t_double) < 0) order by t1.ts, t2.ts;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=131,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.t1 into rdb.r0 as show dnode 1 variables like 'bypassFlag'",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=132,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.v1 into rdb.r0 as show dnode 1 variables like 'bypassFlag'",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=133,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as SELECT a.voltage, count(*) FROM ct_join_1 a left JOIN ct_join_2 b ON a.ts = b.ts group by a.voltage having b.voltage > 14;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=134,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as SELECT a.voltage, count(*) FROM ct_join_1 a left JOIN ct_join_2 b ON a.ts = b.ts group by a.voltage having b.voltage > 14;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=135,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.t1 into rdb.r0 as select tb1.nchar_16_col from test_vtable_join.vtb_virtual_ctb_1 as tb1 join test_vtable_join.vtb_virtual_ctb_2 as tb2 on tb1.ts=tb2.ts where tb1.nchar_16_col is not null group by tb1.nchar_16_col having tb1.nchar_16_col is not null order by 1 slimit 20 limit 20",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=136,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.v1 into rdb.r0 as select tb1.nchar_16_col from test_vtable_join.vtb_virtual_ctb_1 as tb1 join test_vtable_join.vtb_virtual_ctb_2 as tb2 on tb1.ts=tb2.ts where tb1.nchar_16_col is not null group by tb1.nchar_16_col having tb1.nchar_16_col is not null order by 1 slimit 20 limit 20",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=137,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select count (*) group by 1 slimit 1 soffset 1 union select count (*) group by 1 slimit 1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=138,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select varchar+nchar, cint+cuint, ctinyint-cdouble, cfloat*cdouble, cbigint*12, -ctinyint from xx limit 1 offset 1 ",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=139,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select cvarchar like 'a', not like, regexp, not regexp from xx limit 1 offset 1 ",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=140,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.v1 into rdb.r0 as select cvarchar like 'a', not like, regexp, not regexp from xx limit 1 offset 1 ",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=141,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select 1&2, 2|3  and or && || casefrom xx limit 1 offset 1 ",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=142,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.v1 into rdb.r0 as select 1&2, 2|3  and or && || casefrom xx limit 1 offset 1 ",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=143,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select xx where = > >= < <= <> != is NULL is NOT NULL  NOT BETWEEN AND",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=144,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select xx where = > >= < <= <> != is NULL is NOT NULL  NOT BETWEEN AND",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=145,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select json, to_json from %%trows where BETWEEN AND",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=146,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.v1 into rdb.r0 as select json, to_json from %%trows where BETWEEN AND",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=147,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select json, to_json from %%trows where BETWEEN AND",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=148,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id as select cmpl_cd, count(prc) from"
            " (select cmpl_cd, last(clqn_prc) - last(opqn_prc) prc "
            "  from kline_1d ta"
            " where quot_time between {inputDate1} and {inputDate2}"
            "  partition by comp_cd"
            "     interval(1d)"
            "    having (prc > 0)"
            " ) tb"
            "partition by comp_cd"
            "having count(prc) > {inputNum}",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=149,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers as select cmpl_cd, count(prc) from"
            " (select cmpl_cd, last(clqn_prc) - last(opqn_prc) prc "
            "  from kline_1d ta"
            " where quot_time between {inputDate1} and {inputDate2}"
            "  partition by comp_cd"
            "     interval(1d)"
            "    having (prc > 0)"
            " ) tb"
            "partition by comp_cd"
            "having count(prc) > {inputNum}",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=150,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers as  select cmpl_cd, sum(mtch_amt) amount"
            "   from kline_1m"
            "   where quot_time between {inputDate1} and {inputDate2}"
            "   partition by comp_cd"
            "   having sum(mtch_amt) > {inputMount}",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=151,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.t1 as  select cmpl_cd, sum(mtch_amt) amount"
            "   from kline_1m"
            "   where quot_time between {inputDate1} and {inputDate2}"
            "   partition by comp_cd"
            "   having sum(mtch_amt) > {inputMount}",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=152,
            stream="create stream rdb.s0 sliding(5m) from tdb.n1 as  select cmpl_cd, sum(mtch_amt) amount"
            "   from kline_1m"
            "   where quot_time between {inputDate1} and {inputDate2}"
            "   partition by comp_cd"
            "   having sum(mtch_amt) > {inputMount}",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=153,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select 1 as c1, 'abc' as c2, NULL as c3 from information_schema.ins_users union all select 1 as c1, 'abc' as c2, NULL as c3 from information_schema.ins_users union all",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=154,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select 1 as c1, 'abc' as c2, NULL as c3 from information_schema.ins_users union all select 1 as c1, 'abc' as c2, NULL as c3 from information_schema.ins_users union all",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=155,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.t1 into rdb.r0 as select diff(faev) from (select _ts, faev, deviceid from demo union all select _ts + 1s, faev, deviceid from demo order by faev, _ts, deviceid) partition by faev",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=156,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.v1 into rdb.r0 as select diff(faev) from (select _ts, faev, deviceid from demo union all select _ts + 1s, faev, deviceid from demo order by faev, _ts, deviceid) partition by faev",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=157,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select diff(faev) from (select _ts, faev, deviceid from demo union all select _ts + 1s, faev, deviceid from demo order by faev, _ts, deviceid) partition by faev",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=158,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.v1 into rdb.r0 as select from view",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=159,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select from view",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=160,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.v1 into rdb.r0 as select rand() where rand() >= 0 and rand() < 1;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=161,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.t1 into rdb.r0 as select twa(c1) from (select c1 from nest_tb0)",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=162,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.v1 into rdb.r0 as select twa(c1) from (select c1 from nest_tb0)",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=163,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select avg(f1),count(f1),sum(f1),twa(f1) from tb1 group by f1 having twa(f1) > 3",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=164,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select avg(f1),count(f1),sum(f1),twa(f1) from tb1 group by f1 having twa(f1) > 3",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=165,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.t1 into rdb.r0 as select _wstart, twa(k),avg(k),count(1) from t1 where ts>='2015-8-18 00:00:00' and ts<='2015-8-18 00:07:00' interval(1m)",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=166,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.v1 into rdb.r0 as select _wstart, twa(k),avg(k),count(1) from t1 where ts>='2015-8-18 00:00:00' and ts<='2015-8-18 00:07:00' interval(1m)",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=167,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select interp(cbigint) from qdb.v1 where ctinyint > 0 and cint > 2 RANGE('2025-01-01 00:02:00.000', '2025-01-01 00:08:00.000') EVERY (1m) FILL(linear) limit 50;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=168,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select interp(csmallint) from qdb.meters partition by tbname where ctinyint > 0 and cint > 2 RANGE('2025-01-01 00:02:00.000') EVERY (1m) FILL(linear)",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=169,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.t1 into rdb.r0 as select interp(csmallint) from qdb.meters partition by tbname where ctinyint > 0 and cint > 2 RANGE('2025-01-01 00:02:00.000', '2025-01-01 00:08:00.000') EVERY (1m) FILL(linear) limit 50;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=170,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.v1 into rdb.r0 as select interp(csmallint) from qdb.meters partition by tbname where ctinyint > 0 and cint > 2 RANGE('2025-01-01 00:02:00.000', '2025-01-01 00:08:00.000') EVERY (1m) FILL(linear) limit 50;",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=171,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.t1 into rdb.r0 as select interp(cts), interp(cint), interp(cuint), interp(cbigint), interp(cubigint), interp(cfloat), interp(cdouble), interp(csmallint), interp(cusmallint), interp(ctinyint) from qdb.meters partition by %%1 RANGE(_twstart) fill(linear)",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=172,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.v1 into rdb.r0 as select interp(cts), interp(cint), interp(cuint), interp(cbigint), interp(cubigint), interp(cfloat), interp(cdouble), interp(csmallint), interp(cusmallint), interp(ctinyint) from qdb.meters partition by %%1 RANGE(_twstart) fill(linear)",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=173,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select interp(cts), interp(cint), interp(cuint), interp(cbigint), interp(cubigint), interp(cfloat), interp(cdouble), interp(csmallint), interp(cusmallint), interp(ctinyint) from qdb.meters RANGE(_twstart) fill(linear)",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=174,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select interp(cts), interp(cint), interp(cuint), interp(cbigint), interp(cubigint), interp(cfloat), interp(cdouble), interp(csmallint), interp(cusmallint), interp(ctinyint) from qdb.meters RANGE(_twstart) fill(linear)",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=175,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by c1, c2 as show tags from st_json_104",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=176,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.v1 into rdb.r0 as show tags from st_json_104",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=177,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as show tags from st_json_104",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=178,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select ts,jtag from {dbname}.jsons1 order by ts limit 2,3",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=179,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by c1, c2 as select ts,jtag from {dbname}.jsons1 order by ts limit 2,3",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=180,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.v1 into rdb.r0 as select avg(jtag->'tag1'), max from {dbname}.jsons1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=181,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by c1, c2 as select avg(jtag->'tag1'), max from {dbname}.jsons1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=182,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select jtag->'tag2' from {dbname}.jsons1_6 partiton by %%1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=183,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select jtag->'tag2' from {dbname}.jsons1_6 partiton by %%1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=184,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select _wtstart ts, cts, %%tbname tb, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cdecimal8, cdecimal16, cgeometry from qdb.t1 order by cts limit 1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=185,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by c1, c2 as select _wtstart ts, cts, %%tbname tb, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cdecimal8, cdecimal16, cgeometry from qdb.t1 order by cts limit 1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=186,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select _wtstart ts, cts, %%1 tn,  cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cdecimal8, cdecimal16, cgeometry from qdb.n1 order by cts limit 1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=187,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _wtstart ts, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cgeometry from qdb.v1 order by cts limit 1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=188,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select _wtstart ts, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cgeometry from qdb.v1 order by cts limit 1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=189,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by c1, c2 as select _wtstart ts, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cdecimal8, cdecimal16, cgeometry from qdb.meters where tbname='t2' order by cts limit 1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=190,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select _wtstart ts, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cdecimal8, cdecimal16, cgeometry from qdb.meters where tbname='t2' order by cts limit 1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=191,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by c1, c2 as select _wtstart ts, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cgeometry from qdb.vmeters where tbname='t2' order by cts limit 1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=192,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select _wtstart ts, cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cgeometry from qdb.vmeters where tbname='t2' order by cts limit 1",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=193,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _wtstart ts, count(c1), sum(c2) from %trows",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=194,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select _wtstart ts, count(c1), sum(c2) from %trows",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=195,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by c1, c2 as select _wtstart ts, _wstart, count(c1), sum(c2) from %trow interval(1m) fill(1m)",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=196,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers into rdb.r0 as select _wtstart ts, _wstart, count(c1), sum(c2) from %trow interval(1m) fill(1m)",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=197,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r0 as select _wtstart ts, interp(ts), interp(c1), interp(c2), _twend, _twduration, _twrownum from %trows where c1 > _twrownum",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = StreamItem(
            id=198,
            stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r0 as select _wtstart ts, interp(ts), interp(c1), interp(c2), _twend, _twduration, _twrownum from %trows where c1 > _twrownum",
            res_query="select count(current) cnt from rdb.r1 interval(5m)",
            exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
            exp_rows=(0 for _ in range(12)),
        )
        # self.streams.append(stream)

        stream = (
            StreamItem(
                id=199,
                stream="create stream rdb.s0 interval(5m) sliding(5m) from tdb.triggers partition by c1, c2 as select _wstart, count(c1), sum(c2), %%n, %%tbname, _tlocaltime, _tgrpid from %trows count_window(1) ",
                res_query="select count(current) cnt from rdb.r1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
        )

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

    def check6(self):
        tdSql.checkTableType(
            dbname="rdb",
            stbname="r6",
            columns=7,
            tags=1,
        )
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r6",
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

    def check6(self):
        tdSql.checkTableType(
            dbname="rdb",
            stbname="r6",
            columns=7,
            tags=1,
        )
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r6",
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
