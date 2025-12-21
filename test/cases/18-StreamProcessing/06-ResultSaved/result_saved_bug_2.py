import time
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamResultSavedComprehensive:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_result_saved_comprehensive(self):
        """Result saved: comprehensive

        1. Test [INTO [db_name.]table_name]
            1.1 Test whether this option exists
                1.1.1 Only notify without calculation and only notify without saving output can omit this option
                1.1.2 Other scenarios must have this option
            1.2 Test whether db_name is specified
            1.3 Test output table types under different trigger conditions
                1.3.1 With trigger grouping: output table is a super table
                1.3.2 Without trigger grouping: output table is a normal table
            1.4 Test scenarios where output table already exists
                1.4.1 Existing table type matches output table type
                1.4.2 Existing table type does not match output table type

        2. Test [OUTPUT_SUBTABLE(tbname_expr)]
            2.1 Test whether this option exists
                2.1.1 With trigger grouping and exists (legal)
                2.1.2 With trigger grouping and not exists (legal)
                2.1.3 Without trigger grouping and exists (illegal)
                2.1.4 Without trigger grouping and not exists (legal)
            2.2 Test whether columns come from trigger table grouping columns
            2.3 Test whether tbname_expr is an expression that outputs strings
            2.4 Test scenarios where output length exceeds table maximum length (truncation)

        3. Test [(column_name1, column_name2 [PRIMARY KEY][, ...])]
            3.1 Test whether this option exists
                3.1.1 Option exists
                    3.1.1.1 Test whether output table already exists
                        3.1.1.1.1 Output table exists and column names match existing table (legal)
                        3.1.1.1.2 Output table exists and column names don't match existing table (illegal)
                        3.1.1.1.3 Output table doesn't exist (legal)
                    3.1.1.2 Test whether [PRIMARY KEY] is specified
                        3.1.1.2.1 [PRIMARY KEY] specified
                            3.1.1.2.1.1 Second column is integer or string type (legal)
                            3.1.1.2.1.2 Second column is other type (illegal)
                        3.1.1.2.2 Not specified
                3.1.2 Option doesn't exist
                    3.1.2.1 Test whether default output table column names match calculation result column names

        4. Test [TAGS (tag_definition [, ...])]
            4.1 Test whether this option exists
                4.1.1 Option exists
                    4.1.1.1 Test whether output table already exists
                        4.1.1.1.1 Output table exists and tag types/names match existing table (legal)
                        4.1.1.1.2 Output table exists and tag types/names don't match existing table (illegal)
                        4.1.1.1.3 Output table doesn't exist (legal)
                4.1.2 Option doesn't exist
                    4.1.2.1 Test whether default tag column definitions and values correspond to trigger grouping columns
                    4.1.2.2 Test whether tag column name is tag_tbname when grouping by table
            4.2 Test whether grouping columns are specified
                4.2.1 Grouping columns specified (legal)
                4.2.2 Grouping columns not specified (illegal)
            4.3 Test whether tag specified expr comes from trigger grouping columns
            4.4 Test whether [COMMENT 'string_value'] is specified
            4.5 Test correctness of generated column names in specified/unspecified scenarios

        5. Test Target Table Management
            5.1 Test deletion of target table after stream creation
                5.1.1 Verify target table creation by stream
                5.1.2 Test table deletion behavior
                5.1.3 Test stream robustness after target table deletion
                5.1.4 Verify error handling for missing target table

        Catalog:
            - Streams:ResultSaved,skip

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-19 Generated from design document

        """

        self.createSnode()
        self.createDatabase()
        self.prepareQueryData()
        self.prepareTriggerTable()
        self.createStreams()
        self.checkStreamStatus()
        self.dropColumns()
        self.writeTriggerData()
        self.checkResults()
        

    def dropColumns(self):
        tdLog.info("drop columns from output table")
        tdSql.execute("alter table rdb.existing_super_drop_cols drop column avg_val;")

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

        # Prepare existing output tables for testing
        tdLog.info("prepare existing output tables for testing")
        
        # Normal table with matching schema
        tdSql.execute("create table rdb.existing_normal (ts timestamp, cnt bigint, avg_val double)")

        # Normal table with different schema
        tdSql.execute("create table rdb.existing_normal_7(ts timestamp, cnt bigint, avg_val double, tag_name varchar(16))")
        
        # Super table with matching schema and tags
        tdSql.execute("create table rdb.existing_super (ts timestamp, cnt bigint, avg_val double) tags(tag_id int, tag_name varchar(16))")
        
        # Table with non-matching schema
        tdSql.execute("create table rdb.existing_mismatch (ts timestamp, different_col int)")

        # Normal table with same column
        tdSql.execute("create table rdb.existing_normal_col_match(ts timestamp, cnt bigint, avg_val double)")

        # Prepare existing output tables for TAGS testing
        tdLog.info("prepare existing output tables for TAGS testing")
        
        # Super table with matching tag schema for 4.1.1.1.1
        tdSql.execute("create table rdb.existing_tags_match (ts timestamp, cnt bigint) tags(trigger_id int, table_name varchar(16))")
        
        # Super table with different tag schema for 4.1.1.1.2
        tdSql.execute("create table rdb.existing_tags_mismatch (ts timestamp, cnt bigint) tags(diff_id bigint, diff_name varchar(32))")

        # Super table with matching schema and tags for drop columns
        tdSql.execute("create table rdb.existing_super_drop_cols (ts timestamp, cnt bigint, avg_val double) tags(tag_tbname varchar(16))")

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
        tdLog.info(f"check total:{len(self.streams)} streams result successfully")

    def createStreams(self):
        self.streams = []
        # Test 6.1: Drop columns from output table
        stream = StreamItem(
            id=25,
            stream="create stream rdb.s25 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.existing_super_drop_cols (ts,cnt,avg_val) as select _twstart ts, count(*) cnt , avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt from rdb.existing_super_drop_cols where tag_tbname='t1';",
            exp_query="select _wstart ts, count(*) cnt from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
        )
        self.streams.append(stream)

        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            stream.createStream()
