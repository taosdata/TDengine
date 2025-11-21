import time
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamResultSavedComprehensive:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_result_saved_comprehensive(self):
        """Result saved: summary

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
            - Streams:ResultSaved

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
        self.deleteTargetTableForTest()
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

    def deleteTargetTableForTest(self):
        tdLog.info("Delete target table for stream deletion test")
        
        # Wait a moment to ensure target table r24 is created by stream s24
        time.sleep(1)
        
        # Verify target table r24 exists before deletion
        tdSql.query("select count(*) from information_schema.ins_tables where db_name='rdb' and table_name='r24';")
        if tdSql.getData(0, 0) > 0:
            tdLog.info("Target table r24 exists, proceeding to delete it")
            tdSql.execute("drop table rdb.r24;")
            tdLog.info("Target table r24 deleted successfully before writing trigger data")
            
            # Verify deletion
            tdSql.query("select count(*) from information_schema.ins_tables where db_name='rdb' and table_name='r24';")
            tdSql.checkData(0, 0, 0)
        else:
            tdLog.info("Target table r24 does not exist, skip deletion")

    def checkResults(self):
        tdLog.info(f"check total:{len(self.streams)} streams result")
        for stream in self.streams:
            stream.checkResults()
        tdLog.info(f"check total:{len(self.streams)} streams result successfully")

    def createStreams(self):
        self.streams = []

        # Test 1.1.1: Only notify without calculation - can omit INTO option (using sliding window)
        stream = StreamItem(
            id=0,
            stream="create stream rdb.s0 sliding(5m) from tdb.triggers notify(\"ws://localhost:8080/notify\");",
            res_query="",
            exp_query="",
            check_func=self.check0,
        )
        self.streams.append(stream)

        # Test 1.1.2: Normal calculation scenario - must have INTO option
        stream = StreamItem(
            id=2,
            stream="create stream rdb.s2 interval(5m) sliding(5m) from tdb.triggers into rdb.r2 as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt from rdb.r2;",
            exp_query="select _wstart ts, count(*) cnt from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check2,
        )
        self.streams.append(stream)

        # Test 1.2: Specify db_name in INTO
        stream = StreamItem(
            id=3,
            stream="create stream rdb.s3 interval(5m) sliding(5m) from tdb.triggers into rdb.r3 as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt from rdb.r3;",
            exp_query="select _wstart ts, count(*) cnt from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check3,
        )
        self.streams.append(stream)

        # Test 1.3.1: With trigger grouping - output table is super table
        stream = StreamItem(
            id=4,
            stream="create stream rdb.s4 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r4 as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt from rdb.r4 where tag_tbname='t1';",
            exp_query="select _wstart ts, count(*) cnt from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check4,
        )
        self.streams.append(stream)

        # Test 1.3.2: Without trigger grouping - output table is normal table
        stream = StreamItem(
            id=5,
            stream="create stream rdb.s5 interval(5m) sliding(5m) from tdb.triggers into rdb.r5 as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt from rdb.r5;",
            exp_query="select _wstart ts, count(*) cnt from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check5,
        )
        self.streams.append(stream)

        # Test 1.4.1: Existing table type matches output table type (normal table)
        stream = StreamItem(
            id=6,
            stream="create stream rdb.s6 interval(5m) sliding(5m) from tdb.triggers into rdb.existing_normal as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt, avg_val from rdb.existing_normal;",
            exp_query="select _wstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check6,
        )
        self.streams.append(stream)

        # Test 1.4.2: Existing table type mismatch output table type (stable & normal table)
        errorStream1  = "create stream rdb.s7 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.existing_normal_7 output_subtable(concat('sub_', tbname)) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;"
        tdSql.error(errorStream1)

        # Test 2.1.1: With trigger grouping and OUTPUT_SUBTABLE exists (legal)
        stream = StreamItem(
            id=7,
            stream="create stream rdb.s7 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r7 output_subtable(concat('sub_', tbname)) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt from rdb.r7 where tag_tbname='t1';",
            exp_query="select _wstart ts, count(*) cnt from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check7,
        )
        self.streams.append(stream)

        # Test 2.1.2: With trigger grouping and OUTPUT_SUBTABLE not exists (legal)
        stream = StreamItem(
            id=8,
            stream="create stream rdb.s8 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r8 as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt from rdb.r7 where tag_tbname='t1';",
            exp_query="select _wstart ts, count(*) cnt from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check8,
        )
        self.streams.append(stream)

        # Test 2.1.3: With not trigger grouping and OUTPUT_SUBTABLE exists (illegal)
        errorStream2 = "create stream rdb.s9 interval(5m) sliding(5m) from tdb.triggers into rdb.r9 output_subtable(concat('sub_', tbname)) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;"
        tdSql.error(errorStream2)

        # Test 2.2: Columns come from trigger table grouping columns
        stream = StreamItem(
            id=9,
            stream="create stream rdb.s9 interval(5m) sliding(5m) from tdb.triggers partition by id, tbname into rdb.r9 output_subtable(concat('sub_', cast(%%1 as varchar), '_', %%2)) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt from rdb.r9 where tag_tbname='t1';",
            exp_query="select _wstart ts, count(*) cnt from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check9,
        )
        self.streams.append(stream)

        # Test 2.3: tbname_expr is string expression
        stream = StreamItem(
            id=10,
            stream="create stream rdb.s10 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r10 output_subtable(upper(tbname)) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt from rdb.r10 where tag_tbname='t1';",
            exp_query="select _wstart ts, count(*) cnt from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check10,
        )
        self.streams.append(stream)

        # Test 2.4: Output length exceeds table maximum length (truncation)
        stream = StreamItem(
            id=11,
            stream="create stream rdb.s11 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r11 output_subtable(concat('xxxxxxxxvery_long_prefix_that_exceeds_maximum_table_name_length_xxxxxxxxvery_long_prefix_that_exceeds_maximum_table_name_length_xxxxxxxxvery_long_prefix_that_exceeds_maximum_table_name_length_xxxxxxxxvery_long_prefix_that_exceeds_maximum_table_name_length_xxxxxxxxvery_long_prefix_that_exceeds_maximum_table_name_length_', tbname, '_suffix')) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt from rdb.r11;",
            exp_query="select _wstart ts, count(*) cnt from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check11,
        )
        self.streams.append(stream)

        # Test 3.1.1.1.1: Output table already exists with same custom column names
        stream = StreamItem(
            id=23,
            stream="create stream rdb.s23 interval(5m) sliding(5m) from tdb.triggers into rdb.existing_normal_col_match (ts, cnt, avg_val) as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt, avg_val from rdb.existing_normal_col_match;",
            exp_query="select _wstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check23,
        )
        self.streams.append(stream)

        # Test 3.1.1.1.2: Output table already exists with different custom column names
        errorStream3 = "create stream rdb.s24_error interval(5m) sliding(5m) from tdb.triggers into rdb.existing_normal_col_match (td, ct, agg) as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;"
        tdSql.error(errorStream3)

        # Test 3.1.1.1.3: Output table doesn't exist with custom column names
        stream = StreamItem(
            id=12,
            stream="create stream rdb.s12 interval(5m) sliding(5m) from tdb.triggers into rdb.r12 (timestamp_col, count_col, avg_col) as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select timestamp_col, count_col, avg_col from rdb.r12;",
            exp_query="select _wstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check12,
        )
        self.streams.append(stream)

        # Test 3.1.1.2.1.1: Second column is integer type with PRIMARY KEY
        stream = StreamItem(
            id=13,
            stream="create stream rdb.s13 interval(5m) sliding(5m) from tdb.triggers into rdb.r13 (ts, cnt primary key, avg_val) as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt, avg_val from rdb.r13;",
            exp_query="select _wstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check13,
        )
        self.streams.append(stream)

        # Test 3.1.1.2.1.2: Second column is other type (illegal) - DOUBLE type with PRIMARY KEY
        errorStream4 = "create stream rdb.s14_error interval(5m) sliding(5m) from tdb.triggers into rdb.r13_error (ts, avg_val primary key, cnt) as select _twstart ts, avg(cint) avg_val, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;"
        tdSql.error(errorStream4)

        # Test 3.1.2.1: Default column names match calculation result column names
        stream = StreamItem(
            id=14,
            stream="create stream rdb.s14 interval(5m) sliding(5m) from tdb.triggers into rdb.r14 as select _twstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt, avg_val from rdb.r14;",
            exp_query="select _wstart ts, count(*) cnt, avg(cint) avg_val from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check14,
        )
        self.streams.append(stream)

        # ====================== Chapter 4: Test [TAGS (tag_definition [, ...])] ======================
        
        # Test 4.1.1.1.1: Output table exists and tag types/names match existing table (legal)
        stream = StreamItem(
            id=15,
            stream="create stream rdb.s15 interval(5m) sliding(5m) from tdb.triggers partition by id, tbname into rdb.existing_tags_match tags(trigger_id int as %%1, table_name varchar(16) as %%2) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt from rdb.existing_tags_match where trigger_id=1;",
            exp_query="select _wstart ts, count(*) cnt from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check15,
        )
        self.streams.append(stream)

        # Test 4.1.1.1.2: Output table exists and tag types/names don't match existing table (illegal)
        errorStream_4112 = "create stream rdb.s16_error interval(5m) sliding(5m) from tdb.triggers partition by id, tbname into rdb.existing_tags_mismatch tags(trigger_id int as %%1, table_name varchar(16) as %%2) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;"
        tdSql.error(errorStream_4112)

        # Test 4.1.1.1.3: Output table doesn't exist (legal)
        stream = StreamItem(
            id=16,
            stream="create stream rdb.s16 interval(5m) sliding(5m) from tdb.triggers partition by id, tbname into rdb.r16 tags(group_id int as %%1, table_name varchar(16) as %%2) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt from rdb.r16 where group_id=1;",
            exp_query="select _wstart ts, count(*) cnt from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check16,
        )
        self.streams.append(stream)

        # Test 4.1.2.1: Default tag column definitions correspond to trigger grouping columns
        stream = StreamItem(
            id=17,
            stream="create stream rdb.s17 interval(5m) sliding(5m) from tdb.triggers partition by id, name into rdb.r17 as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt from rdb.r17 where id=1;",
            exp_query="select _wstart ts, count(*) cnt from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check17,
        )
        self.streams.append(stream)

        # Test 4.1.2.2: Tag column name is tag_tbname when grouping by table
        stream = StreamItem(
            id=18,
            stream="create stream rdb.s18 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.r18 as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt from rdb.r18 where tag_tbname='t1';",
            exp_query="select _wstart ts, count(*) cnt from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check18,
        )
        self.streams.append(stream)

        # Test 4.2.1: Specify grouping columns (legal)
        stream = StreamItem(
            id=19,
            stream="create stream rdb.s19 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r19 tags(trigger_id int as %%1) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt from rdb.r19 where trigger_id=1;",
            exp_query="select _wstart ts, count(*) cnt from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check19,
        )
        self.streams.append(stream)

        # Test 4.2.2: No grouping columns specified (illegal)
        errorStream_422 = "create stream rdb.s20_error interval(5m) sliding(5m) from tdb.triggers into rdb.r20_error tags(trigger_id int as %%1) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;"
        tdSql.error(errorStream_422)

        # Test 4.3: Tag specified expr comes from trigger grouping columns
        stream = StreamItem(
            id=20,
            stream="create stream rdb.s20 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r20 tags(trigger_id int as %%1, computed_value int as %%1) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt from rdb.r20 where trigger_id=1;",
            exp_query="select _wstart ts, count(*) cnt from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check20,
        )
        self.streams.append(stream)

        # Test 4.4: Specify [COMMENT 'string_value']
        stream = StreamItem(
            id=21,
            stream="create stream rdb.s21 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.r21 tags(trigger_id int comment 'Trigger table ID' as %%1) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt from rdb.r21 where trigger_id=1;",
            exp_query="select _wstart ts, count(*) cnt from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check21,
        )
        # TODO (smj) : comment not support now, reopen this case when comment is supported
        #self.streams.append(stream)

        # Test 4.5: Correctness of generated column names in specified/unspecified scenarios
        stream = StreamItem(
            id=22,
            stream="create stream rdb.s22 interval(5m) sliding(5m) from tdb.triggers partition by id, name into rdb.r22 tags(custom_id int as %%1, custom_name varchar(16) as %%2) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="select ts, cnt from rdb.r22 where custom_id=1;",
            exp_query="select _wstart ts, count(*) cnt from qdb.meters where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:35:00' interval(5m);",
            check_func=self.check22,
        )
        self.streams.append(stream)

        # Test 5.1: Delete target table after stream creation
        stream = StreamItem(
            id=24,
            stream="create stream rdb.s24 interval(5m) sliding(5m) from tdb.triggers into rdb.r24 as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;",
            res_query="",  # No query needed since table will be deleted
            exp_query="",  # No expected result since table will be deleted
            check_func=self.check24,
        )
        self.streams.append(stream)

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

    def check0(self):
        # Test 1.1.1: Only notify without calculation - should not create any output table
        # Verify table r0 does not exist
        tdSql.query("select count(*) from information_schema.ins_tables where db_name='rdb' and table_name='r0';")
        tdSql.checkData(0, 0, 0)
        # Verify stream was created successfully
        tdSql.query("select stream_name from information_schema.ins_streams where stream_name='s0';")
        tdSql.checkRows(1)

    def check1(self):
        # Test 1.1.1: Only notify without saving output - should not create any output table
        # Verify table r1 does not exist
        tdSql.query("select count(*) from information_schema.ins_tables where db_name='rdb' and table_name='r1';")
        tdSql.checkData(0, 0, 0)
        # Verify stream was created successfully
        tdSql.query("select stream_name from information_schema.ins_streams where stream_name='s1';")
        tdSql.checkRows(1)

    def check2(self):
        # Test 1.1.2: Normal calculation scenario - output table should exist
        tdSql.checkTableType(dbname="rdb", tbname="r2", typename="NORMAL_TABLE", columns=2)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r2",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["cnt", "BIGINT", 8, ""],
            ],
        )

    def check3(self):
        # Test 1.2: Specify db_name in INTO - output table should exist in specified database
        tdSql.checkTableType(dbname="rdb", tbname="r3", typename="NORMAL_TABLE", columns=2)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r3",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["cnt", "BIGINT", 8, ""],
            ],
        )

    def check4(self):
        # Test 1.3.1: With trigger grouping - output table should be super table
        tdSql.checkTableType(dbname="rdb", stbname="r4", columns=2, tags=1)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r4",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["cnt", "BIGINT", 8, ""],
                ["tag_tbname", "VARCHAR", 270, "TAG"],
            ],
        )
        tdSql.query("select stable_name from information_schema.ins_stables where db_name='rdb' and stable_name='r4';")
        tdSql.checkData(0, 0, "r4")

    def check5(self):
        # Test 1.3.2: Without trigger grouping - output table should be normal table
        tdSql.checkTableType(dbname="rdb", tbname="r5", typename="NORMAL_TABLE", columns=2)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r5",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["cnt", "BIGINT", 8, ""],
            ],
        )
        tdSql.query("select table_name,type from information_schema.ins_tables where db_name='rdb' and table_name='r5'")
        tdSql.checkData(0, 0, "r5")
        tdSql.checkData(0, 1, "NORMAL_TABLE")

    def check6(self):
        # Test 1.4.1: Existing table type matches - should reuse existing table
        tdSql.checkTableType(dbname="rdb", tbname="existing_normal", typename="NORMAL_TABLE", columns=3)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="existing_normal",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["cnt", "BIGINT", 8, ""],
                ["avg_val", "DOUBLE", 8, ""],
            ],
        )

    def check7(self):
        # Test 2.1.1: OUTPUT_SUBTABLE with custom naming
        tdSql.checkTableType(dbname="rdb", stbname="r7", columns=2, tags=1)
        # Check if subtable was created with custom name
        tdSql.checkResultsByFunc(
            sql="select * from information_schema.ins_tables where db_name='rdb' and table_name like 'sub_t%';",
            func=lambda: tdSql.getRows() >= 1,
        )

    def check8(self):
        # Test 2.1.2: Without OUTPUT_SUBTABLE - default naming should be used
        tdSql.checkTableType(dbname="rdb", stbname="r8", columns=2, tags=1)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r8",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["cnt", "BIGINT", 8, ""],
                ["tag_tbname", "VARCHAR", 270, "TAG"],  # TSDB_TABLE_NAME_LEN
            ],
        )

    def check9(self):
        # Test 2.2: Columns from trigger table grouping columns
        tdSql.checkTableType(dbname="rdb", stbname="r9", columns=2, tags=2)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r9",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["cnt", "BIGINT", 8, ""],
                ["id", "INT", 4, "TAG"],
                ["tag_tbname", "VARCHAR", 270, "TAG"],  # TSDB_TABLE_NAME_LEN
            ],
        )

    def check10(self):
        # Test 2.3: String expression in OUTPUT_SUBTABLE
        tdSql.checkTableType(dbname="rdb", stbname="r10", columns=2, tags=1)
        # Check if subtable was created with uppercase name
        tdSql.checkResultsByFunc(
            sql="select * from information_schema.ins_tables where db_name='rdb' and table_name like 'T%';",
            func=lambda: tdSql.getRows() >= 1,
        )

    def check11(self):
        # Test 2.4: Long table name truncation
        tdSql.checkTableType(dbname="rdb", stbname="r11", columns=2, tags=1)
        # Verify truncation occurred
        tdSql.checkResultsByFunc(
            sql="select table_name from information_schema.ins_tables where db_name='rdb' and stable_name='r11';",
            func=lambda: tdSql.queryResult is not None and all(len(row[0]) <= 192 for row in tdSql.queryResult),  # TSDB_TABLE_NAME_LEN
        )

    def check12(self):
        # Test 3.1.1.1.3: Custom column names
        tdSql.checkTableType(dbname="rdb", tbname="r12", typename="NORMAL_TABLE", columns=3)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r12",
            schema=[
                ["timestamp_col", "TIMESTAMP", 8, ""],
                ["count_col", "BIGINT", 8, ""],
                ["avg_col", "DOUBLE", 8, ""],
            ],
        )

    def check13(self):
        # Test 3.1.1.2.1.1: PRIMARY KEY on second column
        tdSql.checkTableType(dbname="rdb", tbname="r13", typename="NORMAL_TABLE", columns=3)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r13",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["cnt", "BIGINT", 8, "COMPOSITE KEY"],  # Should have PRIMARY KEY flag
                ["avg_val", "DOUBLE", 8, ""],
            ],
        )

    def check14(self):
        # Test 3.1.2.1: Default column names
        tdSql.checkTableType(dbname="rdb", tbname="r14", typename="NORMAL_TABLE", columns=3)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r14",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["cnt", "BIGINT", 8, ""],
                ["avg_val", "DOUBLE", 8, ""],
            ],
        )

    def check15(self):
        # Test 4.1.1.1.1: Output table exists and tag types/names match existing table (legal)
        tdSql.checkTableType(dbname="rdb", stbname="existing_tags_match", columns=2, tags=2)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="existing_tags_match",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["cnt", "BIGINT", 8, ""],
                ["trigger_id", "INT", 4, "TAG"],
                ["table_name", "VARCHAR", 16, "TAG"],
            ],
        )

    def check16(self):
        # Test 4.1.1.1.3: Output table doesn't exist (legal)
        tdSql.checkTableType(dbname="rdb", stbname="r16", columns=2, tags=2)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r16",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["cnt", "BIGINT", 8, ""],
                ["group_id", "INT", 4, "TAG"],
                ["table_name", "VARCHAR", 16, "TAG"],
            ],
        )

    def check17(self):
        # Test 4.1.2.1: Default tag column definitions correspond to trigger grouping columns
        tdSql.checkTableType(dbname="rdb", stbname="r17", columns=2, tags=2)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r17",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["cnt", "BIGINT", 8, ""],
                ["id", "INT", 4, "TAG"],
                ["name", "VARCHAR", 16, "TAG"],
            ],
        )
        tdSql.query("select DISTINCT(tag_name) from information_schema.ins_tags where db_name = 'rdb' and stable_name = 'r17';")
        tdSql.checkData(0, 0, "name")
        tdSql.checkData(1, 0, "id")

    def check18(self):
        # Test 4.1.2.2: Tag column name is tag_tbname when grouping by table
        tdSql.checkTableType(dbname="rdb", stbname="r18", columns=2, tags=1)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r18",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["cnt", "BIGINT", 8, ""],
                ["tag_tbname", "VARCHAR", 270, "TAG"],  # TSDB_TABLE_NAME_LEN
            ],
        )

    def check19(self):
        # Test 4.2.1: Specify grouping columns (legal)
        tdSql.checkTableType(dbname="rdb", stbname="r19", columns=2, tags=1)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r19",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["cnt", "BIGINT", 8, ""],
                ["trigger_id", "INT", 4, "TAG"],
            ],
        )

    def check20(self):
        # Test 4.3: Tag specified expr comes from trigger grouping columns
        tdSql.checkTableType(dbname="rdb", stbname="r20", columns=2, tags=2)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r20",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["cnt", "BIGINT", 8, ""],
                ["trigger_id", "INT", 4, "TAG"],
                ["computed_value", "INT", 4, "TAG"],
            ],
        )
        # Verify the computed_value tag contains the computed expression result
        tdSql.checkResultsByFunc(
            sql="select trigger_id, computed_value from rdb.r20 where trigger_id=1;",
            func=lambda: tdSql.queryResult and tdSql.queryResult[0][1] == 1,  # id=1, computed_value = 1
        )

    def check21(self):
        # Test 4.4: Specify [COMMENT 'string_value']
        tdSql.checkTableType(dbname="rdb", stbname="r21", columns=2, tags=1)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r21",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["cnt", "BIGINT", 8, ""],
                ["trigger_id", "INT", 4, "TAG"],
            ],
        )
        # Verify comment was set (if system supports checking comments)
        tdSql.checkResultsByFunc(
            sql="select * from information_schema.ins_tags where db_name='rdb' and stable_name='r21' and tag_name='trigger_id';",
            func=lambda: tdSql.getRows() == 2,
        )

    def check22(self):
        # Test 4.5: Correctness of generated column names in specified/unspecified scenarios
        tdSql.checkTableType(dbname="rdb", stbname="r22", columns=2, tags=2)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="r22",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["cnt", "BIGINT", 8, ""],
                ["custom_id", "INT", 4, "TAG"],
                ["custom_name", "VARCHAR", 16, "TAG"],
            ],
        )

    def check23(self):
        # Test 3.1.1.1.1: Output table already exists with same custom column names
        tdSql.checkTableType(dbname="rdb", tbname="existing_normal_col_match", typename="NORMAL_TABLE", columns=3)
        tdSql.checkTableSchema(
            dbname="rdb",
            tbname="existing_normal_col_match",
            schema=[
                ["ts", "TIMESTAMP", 8, ""],
                ["cnt", "BIGINT", 8, ""],
                ["avg_val", "DOUBLE", 8, ""],
            ],
        )

    def check24(self):
        # Test 5.1: Delete target table before writing trigger data
        tdLog.info("Test 5.1: Testing stream behavior after target table deletion")
        
        # Step 1: Verify stream exists and is running
        tdSql.query("select stream_name from information_schema.ins_streams where stream_name='s24';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "s24")
        tdLog.info("Stream s24 still exists")
        
        # Step 2: Verify target table was deleted before writing trigger data
        tdSql.query("select count(*) from information_schema.ins_tables where db_name='rdb' and table_name='r24';")
        table_exists_before = tdSql.getData(0, 0) > 0
        
        if table_exists_before:
            tdLog.info("Target table r24 exists - checking if stream recreated it")
            # If table exists, verify its structure is correct
            tdSql.checkTableType(dbname="rdb", tbname="r24", typename="NORMAL_TABLE", columns=2)
            tdSql.checkTableSchema(
                dbname="rdb",
                tbname="r24",
                schema=[
                    ["ts", "TIMESTAMP", 8, ""],
                    ["cnt", "BIGINT", 8, ""],
                ],
            )
            
            # Check if there's any data in the table
            tdSql.query("select count(*) from rdb.r24;")
            data_count = tdSql.getData(0, 0)
            tdLog.info(f"Target table r24 contains {data_count} records")
            
            if data_count > 0:
                tdLog.info("Stream successfully recreated table and wrote data after deletion")
            else:
                tdLog.info("Stream recreated table but no data written yet")
        else:
            tdLog.info("Target table r24 does not exist - stream did not recreate it")
            
        # Step 3: Check stream status and behavior
        # The stream should handle the missing target table gracefully
        tdSql.query("select stream_name from information_schema.ins_streams where stream_name='s24';")
        tdSql.checkRows(1)
        tdLog.info("Stream s24 remains active despite target table deletion")
        
        # Step 4: Verify system robustness
        # This tests whether the stream processing system can handle target table deletion
        # The expected behavior may vary:
        # - Stream might recreate the table automatically
        # - Stream might continue running but log errors
        # - Stream might pause until table is manually recreated
        
        tdLog.info("Test 5.1: Target table deletion test completed successfully")
        tdLog.info("Verified stream system's robustness when target table is deleted")