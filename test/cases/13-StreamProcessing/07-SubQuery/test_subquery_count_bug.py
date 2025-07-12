import time
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamSubqueryCount:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_subquery_count(self):
        """Subquery in Count

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
        vctb = "create vtable tdb.v1 (tdb.t1.c1, tdb.t1.c2) using tdb.vtriggers tags(1)"
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
        tdLog.info(f"check total:{len(self.streams)} streams result successfully")

    def createStreams(self):
        self.streams = []

        stream = StreamItem(
            id=2,
            stream="create stream rdb.s2 count_window(2, c1) from tdb.triggers partition by tbname into rdb.r2 as select _twstart ts, _twstart + 5m te, _twduration td, _twrownum tw, _tgrpid tg, _tlocaltime tl, tbname tb, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twstart + 5m and _twduration is not null and _twrownum is not null and _tgrpid is not null and _tlocaltime is not null partition by tbname",
            res_query="select ts, te, td, c1, tag_tbname from rdb.r2 where tag_tbname='t1' limit 3;",
            exp_query="select _wstart ts, _wend te, 60000, count(cint) c1, 't1' from qdb.t1 where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:15:00' interval(5m);",
            check_func=self.check2,
        )
        self.streams.append(stream)

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
