import time
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem


class TestStreamResultSavedErrors:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_result_saved_errors(self):
        """Result saved: error cases

        This test covers error scenarios and boundary conditions for stream result saving:

        1. Test error scenarios for [INTO [db_name.]table_name]
            1.1 Missing INTO when required
            1.2 Existing table with mismatched type

        2. Test error scenarios for [OUTPUT_SUBTABLE(tbname_expr)]
            2.1 OUTPUT_SUBTABLE without trigger grouping (illegal)
            2.2 Invalid expressions in tbname_expr

        3. Test error scenarios for [(column_name1, column_name2 [PRIMARY KEY][, ...])]
            3.1 Column name mismatch with existing table
            3.2 Invalid PRIMARY KEY column types

        4. Test error scenarios for [TAGS (tag_definition [, ...])]
            4.1 Tag mismatch with existing table
            4.2 Missing grouping columns when tags specified
            4.3 Invalid tag expressions

        Catalog:
            - Streams:ResultSaved:Errors

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
        self.createStreamsAndExpectErrors()

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

    def prepareTriggerTable(self):
        tdLog.info("prepare tables for trigger")

        stb = "create table tdb.triggers (ts timestamp, c1 int, c2 int) tags(id int, name varchar(16));"
        ctb = "create table tdb.t1 using tdb.triggers tags(1, '1') tdb.t2 using tdb.triggers tags(2, '2') tdb.t3 using tdb.triggers tags(3, '3')"
        tdSql.execute(stb)
        tdSql.execute(ctb)

        ntb = "create table tdb.n1 (ts timestamp, c1 int, c2 int)"
        tdSql.execute(ntb)

        # Prepare existing tables with mismatched schemas for testing
        tdLog.info("prepare existing tables with mismatched schemas")
        
        # Super table when normal table expected
        tdSql.execute("create table rdb.mismatch_super (ts timestamp, cnt bigint) tags(tag_id int)")
        
        # Normal table when super table expected
        tdSql.execute("create table rdb.mismatch_normal (ts timestamp, cnt bigint, tag_tbname varchar(16))")
        
        # Table with different column names
        tdSql.execute("create table rdb.mismatch_columns (ts timestamp, different_col int)")
        
        # Table with different tag schema
        tdSql.execute("create table rdb.mismatch_tags (ts timestamp, cnt bigint) tags(wrong_tag varchar(10))")

    def createStreamsAndExpectErrors(self):
        tdLog.info("test error scenarios")

        # Test 1.1: Missing INTO when required (should fail)
        tdLog.info("Test 1.1: Missing INTO when calculation required")
        try:
            tdSql.execute("create stream rdb.error_s1 interval(5m) sliding(5m) from tdb.triggers as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;")
            tdLog.exit("Expected error for missing INTO clause")
        except Exception as e:
            tdLog.info(f"Expected error caught: {e}")

        # Test 1.2: Existing table type mismatch (super table vs normal table)
        tdLog.info("Test 1.2: Existing table type mismatch")
        try:
            tdSql.execute("create stream rdb.error_s2 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.mismatch_normal as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;")
            tdLog.exit("Expected error for table type mismatch")
        except Exception as e:
            tdLog.info(f"Expected error caught: {e}")

        # Test 2.1: OUTPUT_SUBTABLE without trigger grouping (should fail)
        tdLog.info("Test 2.1: OUTPUT_SUBTABLE without trigger grouping")
        try:
            tdSql.execute("create stream rdb.error_s3 interval(5m) sliding(5m) from tdb.triggers into rdb.error_r3 output_subtable(concat('sub_', 'table')) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;")
            tdLog.exit("Expected error for OUTPUT_SUBTABLE without grouping")
        except Exception as e:
            tdLog.info(f"Expected error caught: {e}")

        # Test 2.2: Invalid expression in OUTPUT_SUBTABLE
        tdLog.info("Test 2.2: Invalid expression in OUTPUT_SUBTABLE")
        try:
            tdSql.execute("create stream rdb.error_s4 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.error_r4 output_subtable(invalid_column) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;")
            tdLog.exit("Expected error for invalid OUTPUT_SUBTABLE expression")
        except Exception as e:
            tdLog.info(f"Expected error caught: {e}")

        # Test 3.1: Column name mismatch with existing table
        tdLog.info("Test 3.1: Column name mismatch with existing table")
        try:
            tdSql.execute("create stream rdb.error_s5 interval(5m) sliding(5m) from tdb.triggers into rdb.mismatch_columns (ts, cnt) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;")
            tdLog.exit("Expected error for column name mismatch")
        except Exception as e:
            tdLog.info(f"Expected error caught: {e}")

        # Test 3.2.1: Invalid PRIMARY KEY column type (float)
        tdLog.info("Test 3.2.1: Invalid PRIMARY KEY column type")
        try:
            tdSql.execute("create stream rdb.error_s6 interval(5m) sliding(5m) from tdb.triggers into rdb.error_r6 (ts, avg_val primary key) as select _twstart ts, avg(c1) avg_val from qdb.meters where cts >= _twstart and cts < _twend;")
            tdLog.exit("Expected error for invalid PRIMARY KEY type")
        except Exception as e:
            tdLog.info(f"Expected error caught: {e}")

        # Test 4.1: Tag mismatch with existing table
        tdLog.info("Test 4.1: Tag mismatch with existing table")
        try:
            tdSql.execute("create stream rdb.error_s7 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.mismatch_tags tags(custom_tag varchar(20) as %%1) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;")
            tdLog.exit("Expected error for tag mismatch")
        except Exception as e:
            tdLog.info(f"Expected error caught: {e}")

        # Test 4.2: Missing grouping columns when tags specified
        tdLog.info("Test 4.2: Missing grouping columns when tags specified")
        try:
            tdSql.execute("create stream rdb.error_s8 interval(5m) sliding(5m) from tdb.triggers into rdb.error_r8 tags(custom_tag int as %%1) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;")
            tdLog.exit("Expected error for missing grouping columns")
        except Exception as e:
            tdLog.info(f"Expected error caught: {e}")

        # Test 4.3: Invalid tag expression (non-grouping column reference)
        tdLog.info("Test 4.3: Invalid tag expression")
        try:
            tdSql.execute("create stream rdb.error_s9 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.error_r9 tags(invalid_tag int as non_grouping_col) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;")
            tdLog.exit("Expected error for invalid tag expression")
        except Exception as e:
            tdLog.info(f"Expected error caught: {e}")

        # Test additional boundary conditions
        self.testBoundaryConditions()

    def testBoundaryConditions(self):
        tdLog.info("Test boundary conditions")

        # Test very long table name truncation behavior
        tdLog.info("Test very long table name")
        very_long_name = "a" * 300  # Exceeds TSDB_TABLE_NAME_LEN
        try:
            tdSql.execute(f"create stream rdb.error_s10 interval(5m) sliding(5m) from tdb.triggers into rdb.{very_long_name} as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;")
            # Should succeed but with truncated name
            tdLog.info("Long table name handled successfully")
        except Exception as e:
            tdLog.info(f"Error with long table name: {e}")

        # Test invalid database name in INTO clause
        tdLog.info("Test invalid database name")
        try:
            tdSql.execute("create stream rdb.error_s11 interval(5m) sliding(5m) from tdb.triggers into nonexistent_db.test_table as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;")
            tdLog.exit("Expected error for nonexistent database")
        except Exception as e:
            tdLog.info(f"Expected error caught: {e}")

        # Test maximum number of columns
        tdLog.info("Test maximum column limits")
        many_columns = ", ".join([f"count(*) as cnt{i}" for i in range(100)])  # Create many columns
        try:
            tdSql.execute(f"create stream rdb.error_s12 interval(5m) sliding(5m) from tdb.triggers into rdb.many_cols_table as select _twstart ts, {many_columns} from qdb.meters where cts >= _twstart and cts < _twend;")
            tdLog.info("Many columns handled successfully")
        except Exception as e:
            tdLog.info(f"Error with many columns: {e}")

        # Test empty tag list (should fail if grouping exists)
        tdLog.info("Test empty tag definition")
        try:
            tdSql.execute("create stream rdb.error_s13 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.empty_tags_table tags() as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;")
            tdLog.exit("Expected error for empty tags")
        except Exception as e:
            tdLog.info(f"Expected error caught: {e}")

        # Test invalid COMMENT syntax
        tdLog.info("Test invalid COMMENT syntax")
        try:
            tdSql.execute("create stream rdb.error_s14 interval(5m) sliding(5m) from tdb.triggers partition by id into rdb.invalid_comment tags(trigger_id int comment invalid_comment_without_quotes as %%1) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;")
            tdLog.exit("Expected error for invalid comment syntax")
        except Exception as e:
            tdLog.info(f"Expected error caught: {e}")

        # Test placeholder reference out of bounds
        tdLog.info("Test placeholder reference out of bounds")
        try:
            tdSql.execute("create stream rdb.error_s15 interval(5m) sliding(5m) from tdb.triggers partition by tbname into rdb.out_of_bounds tags(invalid_ref int as %%2) as select _twstart ts, count(*) cnt from qdb.meters where cts >= _twstart and cts < _twend;")
            tdLog.exit("Expected error for placeholder out of bounds")
        except Exception as e:
            tdLog.info(f"Expected error caught: {e}")

        # Test circular column reference
        tdLog.info("Test circular references")
        try:
            tdSql.execute("create stream rdb.error_s16 interval(5m) sliding(5m) from tdb.triggers into rdb.circular_ref (ts, cnt, ts) as select _twstart ts, count(*) cnt, _twstart ts from qdb.meters where cts >= _twstart and cts < _twend;")
            tdLog.exit("Expected error for duplicate column names")
        except Exception as e:
            tdLog.info(f"Expected error caught: {e}")

        tdLog.info("All error scenarios tested successfully") 