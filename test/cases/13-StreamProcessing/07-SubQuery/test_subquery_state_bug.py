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

        # stream = StreamItem(
        #     id=2,
        #     stream="create stream rdb.s2 state_window(c1) from tdb.triggers partition by tbname into rdb.r2 as select _twstart ts, _twstart + 5m te, _twduration td, _twrownum tw, _tgrpid tg, _tlocaltime tl, tbname tb, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twstart + 5m and _twduration is not null and _twrownum is not null and _tgrpid is not null and _tlocaltime is not null partition by tbname",
        #     res_query="select ts, te, td, c1, tag_tbname from rdb.r2 where tag_tbname='t1' limit 3;",
        #     exp_query="select _wstart ts, _wend te, 60000, count(cint) c1, 't1' from qdb.t1 where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:15:00' interval(5m);",
        #     check_func=self.check2,
        # )
        
        # stream = StreamItem(
        #     id=4,
        #     stream="create stream rdb.s4 state_window(c1) from tdb.triggers partition by tbname into rdb.r4 as select _twstart ts, _twstart + 5m te, _twduration td, _twrownum tw, _tgrpid tg, _tlocaltime tl, count(cint) c1, avg(cint) c2 from qdb.meters where cts >= _twstart and cts < _twstart + 5m and _twduration is not null and _twrownum is not null and _tgrpid is not null and _tlocaltime is not null and tbname=%%tbname",
        #     res_query="select ts, te, td, c1, tag_tbname from rdb.r4 where tag_tbname='t1' limit 3;",
        #     exp_query="select _wstart ts, _wend te, _wduration / 5 td, count(cint) c1, 't1' from qdb.t1 where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:15:00' interval(5m);",
        #     check_func=self.check4,
        # )
        

        stream = StreamItem(
            id=27,
            stream="create stream rdb.s27 state_window(c1) from tdb.v1 partition by tbname into rdb.r27 as select _twstart tw, sum(cint) c1, count(cint) c2 from qdb.meters where cts >= _twstart and cts < _twstart + 5m and tbname=%%1",
            res_query="select * from rdb.r27 where tag_tbname='t1' limit 3",
            exp_query="select _wstart, sum(cint), count(cint), tbname from qdb.vmeters where cts >= '2025-01-01 00:00:00.000' and cts < '2025-01-01 00:15:00.000' and tbname='v1' partition by tbname interval(5m);",
        )
        self.streams.append(stream)

        
        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            stream.createStream()



    def check40(self):
        tdSql.checkResultsByFunc(
            "select * from rdb.r40;", func=lambda: tdSql.getRows() == 8
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
            ],
        )

    def check46(self):
        tdSql.checkResultsByFunc(
            sql="select ts, ccnt, csum from rdb.r46;",
            func=lambda: tdSql.getRows() == 7
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 1)
            and tdSql.compareData(0, 2, 0)
            and tdSql.compareData(1, 0, "2025-01-01 00:05:00.000")
            and tdSql.compareData(1, 1, 1)
            and tdSql.compareData(1, 2, 50)
            and tdSql.compareData(6, 0, "2025-01-01 00:30:00.000")
            and tdSql.compareData(6, 1, 2)
            and tdSql.compareData(6, 2, 620),
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
            ],
        )

        tdSql.checkResultsByFunc(
            sql="select count(*) from rdb.r78 where id != 2;",
            func=lambda: tdSql.compareData(0, 0, 7),
        )

    def check79(self):
        tdSql.checkResultsByFunc(
            sql="select `_twstart`, `sum(cdecimal8)`, `count(*)` from rdb.r79 where tag_tbname='t1';",
            func=lambda: tdSql.getRows() == 7
            and tdSql.compareData(3, 0, "2025-01-01 00:15:00.000")
            and tdSql.compareData(3, 1, None)
            and tdSql.compareData(3, 2, 0)
            and tdSql.compareData(4, 0, "2025-01-01 00:20:00.000")
            and tdSql.compareData(4, 1, None)
            and tdSql.compareData(4, 2, 0)
            and tdSql.compareData(5, 0, "2025-01-01 00:25:00.000")
            and tdSql.compareData(5, 1, None)
            and tdSql.compareData(5, 2, 0)
            and tdSql.compareData(6, 0, "2025-01-01 00:30:00.000")
            and tdSql.compareData(6, 1, 56)
            and tdSql.compareData(6, 2, 10),
        )

    def check80(self):
        tdSql.checkResultsByFunc(
            sql="select * from rdb.r80;",
            func=lambda: tdSql.getRows() == 7
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 1)
            and tdSql.compareData(0, 2, 0)
            and tdSql.compareData(1, 0, "2025-01-01 00:05:00.000")
            and tdSql.compareData(1, 1, 1)
            and tdSql.compareData(1, 2, 50)
            and tdSql.compareData(6, 0, "2025-01-01 00:30:00.000")
            and tdSql.compareData(6, 1, 2)
            and tdSql.compareData(6, 2, 620),
        )

    def check81(self):
        tdSql.checkResultsByFunc(
            sql="select count(*) from rdb.r81 where tag_tbname='t1';",
            func=lambda: tdSql.compareData(0, 0, 34),
        )

    def check84(self):
        tdSql.checkResultsByFunc(
            sql="select * from rdb.r84 where tag_tbname='t1';",
            func=lambda: tdSql.getRows() == 14,
        )

    def check123(self):
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r123",
            schema=[
                ["tw", "TIMESTAMP", 8, ""],
                ["te", "TIMESTAMP", 8, ""],
                ["_twstart", "TIMESTAMP", 8, ""],
                ["_twend", "TIMESTAMP", 8, ""],
                ["c1", "BIGINT", 8, ""],
                ["c2", "BIGINT", 8, ""],
                ["id", "INT", 4, "TAG"],
                ["name", "VARCHAR", 16, "TAG"],
            ],
        )
        tdSql.checkResultsByFunc(
            sql="select * from information_schema.ins_tables where db_name='rdb' and stable_name='r123';",
            func=lambda: tdSql.getRows() == 2,
        )

    def check105(self):
        tdSql.checkResultsByFunc(
            sql="select t1, c11, c12, t2, c21, c22 from rdb.r105 where tag_tbname='t1';",
            func=lambda: tdSql.getRows() == 7
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
            and tdSql.compareData(3, 0, "2025-01-01 00:19:30.000")
            and tdSql.compareData(3, 1, 3)
            and tdSql.compareData(3, 2, 39)
            and tdSql.compareData(3, 3, "2025-01-01 00:15:00.000")
            and tdSql.compareData(3, 4, 2)
            and tdSql.compareData(3, 5, 30)
            and tdSql.compareData(4, 0, "2025-01-01 00:24:30.000")
            and tdSql.compareData(4, 1, 1)
            and tdSql.compareData(4, 2, 49)
            and tdSql.compareData(4, 3, "2025-01-01 00:20:00.000")
            and tdSql.compareData(4, 4, 0)
            and tdSql.compareData(4, 5, 40)
            and tdSql.compareData(5, 0, "2025-01-01 00:29:30.000")
            and tdSql.compareData(5, 1, 3)
            and tdSql.compareData(5, 2, 59)
            and tdSql.compareData(5, 3, "2025-01-01 00:25:00.000")
            and tdSql.compareData(5, 4, 2)
            and tdSql.compareData(5, 5, 50)
            and tdSql.compareData(6, 0, "2025-01-01 00:34:30.000")
            and tdSql.compareData(6, 1, 1)
            and tdSql.compareData(6, 2, 69)
            and tdSql.compareData(6, 3, "2025-01-01 00:30:00.000")
            and tdSql.compareData(6, 4, 0)
            and tdSql.compareData(6, 5, 60),
        )
