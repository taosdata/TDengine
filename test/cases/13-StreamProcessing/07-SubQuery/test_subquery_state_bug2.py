import time
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamSubqueryState:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_subquery_state(self):
        """Subquery in State

        1. Use state trigger mode

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
            "insert into tdb.t2 values ('2025-01-01 00:15:00', 11, 110) ('2025-01-01 00:16:00', 11, 120) ('2025-01-01 00:20:00', 21, 210)",
            "insert into tdb.t3 values ('2025-01-01 00:20:00', 20, 210)",
            "insert into tdb.n1 values ('2025-01-01 00:25:00', 25, 0  ) ('2025-01-01 00:26:00', 25, 10 ) ('2025-01-01 00:30:00', 30, 0)",
            "insert into tdb.t1 values ('2025-01-01 00:06:00', 10, 10 ) ('2025-01-01 00:10:00', 20, 0  ) ('2025-01-01 00:11:00', 20, 10 ) ('2025-01-01 00:30:00', 30, 0) ('2025-01-01 00:31:00', 30, 10) ('2025-01-01 00:35:00', 40, 0) ('2025-01-01 00:36:00', 40, 2)",
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

        # stream = StreamItem(
        #     id=56,
        #     stream="create stream rdb.s56 state_window(c1) from tdb.v1 into rdb.r56 as select _wstart ws, _wend we, _twstart tws, _twend + 4m twe, first(c1) cf, last(c1) cl, count(c1) cc from %%trows where ts >= _twstart and ts < _twend + 4m interval(1m) fill(prev)",
        #     res_query="select * from rdb.r56 where ws >= '2025-01-01 00:00:00.000' and we <= '2025-01-01 00:05:00.000' ",
        #     exp_query="select _wstart ws, _wend we, cast('2025-01-01 00:00:00.000' as timestamp) tws, cast('2025-01-01 00:05:00.000' as timestamp) twe, first(c1) cf, last(c1) cl, count(c1) cc from tdb.v1 where ts >= '2025-01-01 00:00:00.000' and ts < '2025-01-01 00:05:00.000' interval(1m) fill(prev);",
        # )

        # stream = StreamItem(
        #     id=27,
        #     stream="create stream rdb.s27 state_window(c1) from tdb.v1 partition by tbname into rdb.r27 as select _twstart tw, sum(cint) c1, count(cint) c2 from qdb.vmeters where cts >= _twstart and cts < _twstart + 5m and tbname=%%1",
        #     res_query="select * from rdb.r27 where tag_tbname='v1' limit 3",
        #     exp_query="select _wstart, sum(cint), count(cint), tbname from qdb.vmeters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' and tbname='v1' partition by tbname interval(5m);",
        #     check_func=self.check27,
        # )
        # # self.streams.append(stream) TD-36353
        stream = StreamItem(
            id=132,
            stream="create stream rdb.s132 state_window(c1) from tdb.t1 partition by id into rdb.r132 as select tb.cts tbts, ta.ts tats, ta.c1 tac1, ta.c2 tac2, tb.cint tbc1, tb.cuint tbc2, _twstart, _twend from tdb.t1 ta right join qdb.t1 tb on ta.ts=tb.cts where tb.cts >= _twstart and tb.cts < _twend + 4m;",
            res_query="select tbts, tats, tac1, tac2, tbc1, tbc2 from rdb.r132 limit 30",
            exp_query="select tb.cts tbts, ta.ts tats, ta.c1 tac1, ta.c2 tac2, tb.cint tbc1, tb.cuint tbc2 from tdb.t1 ta right join qdb.t1 tb on ta.ts=tb.cts where tb.cts >= '2025-01-01 00:00:00.000' and tb.cts < '2025-01-01 00:15:00.000';",
        )

        stream = StreamItem(
            id=5,
            stream="create stream rdb.s5 state_window(c1) from tdb.triggers partition by tbname into rdb.r5 as select _twstart ts, _twend + 4m te, _twduration * 5 td, _twrownum tw, %%tbname as tb, count(c1) c1, avg(c2) c2 from %%tbname where ts >= _twstart and ts < _twend + 4m",
            res_query="select ts, te, td, tw, tb, c1, c2, tag_tbname from rdb.r5 where tag_tbname='t1' limit 3;",
            exp_query="select _wstart, _wend, _wduration, count(c1), 't1', count(c1), avg(c2), 't1' from tdb.t1 where ts >= '2025-01-01 00:00:00' and ts < '2025-01-01 00:15:00' interval(5m) fill(value, 0, 0, null);",
            check_func=self.check5,
        )
        self.streams.append(stream)

        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            stream.createStream()

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
            and tdSql.compareData(0, 2, 300000)
            and tdSql.compareData(0, 3, 2)
            and tdSql.compareData(0, 4, "t2"),
        )
