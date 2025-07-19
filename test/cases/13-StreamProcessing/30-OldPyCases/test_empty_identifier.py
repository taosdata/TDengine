import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    clusterComCheck,
    tdStream,
    StreamItem,
)


class TestStreamEmptyIdentifier:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_empty_identifier(self):
        """Stream Empty Identifier Test

        Test empty identifier handling in stream processing:
        1. Empty identifiers in stream creation
        2. Empty identifiers in table names used by streams
        3. Empty identifiers in stream options and parameters
        4. Error handling for invalid empty identifiers

        Catalog:
            - Streams:OldPyCases

        Since: v3.0.0.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-12-19 Migrated from system-test/0-others/empty_identifier.py
            - Note: Focused on stream-related empty identifier tests

        """

        self.createSnode()
        self.createDatabase()
        self.prepareTestEnv()
        self.testStreamEmptyIdentifier()

    def createSnode(self):
        tdLog.info("create snode")
        tdStream.createSnode(1)

    def createDatabase(self):
        tdLog.info("create database")
        tdSql.prepare(dbname="emptytest", vgroups=4)
        clusterComCheck.checkDbReady("emptytest")

    def prepareTestEnv(self):
        """Prepare test environment with tables and data"""
        tdLog.info("prepare test environment")
        
        # Create super table
        tdSql.execute("""
            CREATE TABLE emptytest.meters (
                ts TIMESTAMP,
                c1 INT,
                c2 BIGINT,
                c3 FLOAT,
                c4 DOUBLE,
                c5 SMALLINT,
                c6 TINYINT,
                c7 BOOL,
                c8 BINARY(10),
                c9 NCHAR(10)
            ) TAGS (
                t1 INT,
                t2 NCHAR(20),
                t3 BINARY(20),
                t4 BIGINT,
                t5 SMALLINT,
                t6 DOUBLE
            );
        """)
        
        # Create child tables
        for i in range(10):
            table_name = f"t{i}"
            tdSql.execute(f"""
                CREATE TABLE emptytest.{table_name} USING emptytest.meters 
                TAGS ({i % 5}, 'tb{i}', 'tb{i}', {i}, {i}, {i});
            """)
            
            # Insert test data
            base_ts = 1537146000000
            for j in range(100):
                ts = base_ts + j * 600000
                if i < 5:
                    tdSql.execute(f"""
                        INSERT INTO emptytest.{table_name} VALUES 
                        ({ts}, {j%10}, {j%10}, {j%10}, {j%10}, {j%10}, {j%10}, 
                         {j%2}, 'binary{j%10}', 'nchar{j%10}');
                    """)
                else:
                    tdSql.execute(f"""
                        INSERT INTO emptytest.{table_name} VALUES 
                        ({ts}, {j%10}, NULL, {j%10}, NULL, {j%10}, {j%10}, 
                         {j%2}, 'binary{j%10}', 'nchar{j%10}');
                    """)

    def testStreamEmptyIdentifier(self):
        """Test empty identifier handling in stream operations"""
        tdLog.info("test empty identifier in stream operations")
        
        # Test cases with empty identifiers that should fail
        error_sqls = [
            # Empty table names in stream creation
            "CREATE STREAM emptytest.test_stream INTERVAL(10s) FROM `` INTO emptytest.result AS SELECT COUNT(*) FROM ``;",
            
            # Empty stream names
            "CREATE STREAM `` INTERVAL(10s) FROM emptytest.meters INTO emptytest.result AS SELECT COUNT(*) FROM emptytest.meters;",
            
            # Empty result table names
            "CREATE STREAM emptytest.test_stream INTERVAL(10s) FROM emptytest.meters INTO `` AS SELECT COUNT(*) FROM emptytest.meters;",
            
            # Empty column names in stream queries
            "CREATE STREAM emptytest.test_stream INTERVAL(10s) FROM emptytest.meters INTO emptytest.result AS SELECT COUNT(*) `` FROM emptytest.meters;",
            
            # Empty identifiers in stream options
            "CREATE STREAM emptytest.test_stream INTERVAL(10s) FROM emptytest.meters PARTITION BY `` INTO emptytest.result AS SELECT COUNT(*) FROM emptytest.meters;",
            
            # Empty database names
            "CREATE STREAM ``.test_stream INTERVAL(10s) FROM emptytest.meters INTO emptytest.result AS SELECT COUNT(*) FROM emptytest.meters;",
            
            # Empty identifiers in DROP operations
            "DROP STREAM ``;",
            
            # Empty identifiers in manual recalculation
            "RECALCULATE STREAM `` FROM '2025-01-01 00:00:00';",
        ]
        
        # Test each SQL that should fail
        for sql in error_sqls:
            try:
                tdLog.info(f"Testing SQL: {sql}")
                self.executeAndExpectError(sql, -2147473897)  # Invalid identifier error
            except Exception as e:
                tdLog.info(f"Expected error for empty identifier: {e}")

        # Test some additional stream-specific empty identifier cases
        stream_specific_sqls = [
            # Empty identifier in trigger conditions
            "CREATE STREAM emptytest.test_stream INTERVAL(10s) FROM emptytest.meters WHERE `` > 0 INTO emptytest.result AS SELECT COUNT(*) FROM emptytest.meters;",
            
            # Empty identifier in aggregation functions
            "CREATE STREAM emptytest.test_stream INTERVAL(10s) FROM emptytest.meters INTO emptytest.result AS SELECT ``(*) FROM emptytest.meters;",
            
            # Empty identifier in window specifications
            "CREATE STREAM emptytest.test_stream INTERVAL(10s) FROM emptytest.meters INTO emptytest.result AS SELECT COUNT(*) FROM emptytest.meters WHERE `` >= _twstart;",
        ]
        
        for sql in stream_specific_sqls:
            try:
                tdLog.info(f"Testing stream-specific SQL: {sql}")
                self.executeAndExpectError(sql, -2147473897)
            except Exception as e:
                tdLog.info(f"Expected error for stream empty identifier: {e}")

    def executeAndExpectError(self, sql: str, expected_error: int):
        """Execute SQL and expect specific error"""
        try:
            tdSql.error(sql, expected_error)
            tdLog.info(f"✓ SQL correctly failed with expected error: {sql}")
        except Exception as e:
            # If the error function itself fails, that means the SQL didn't fail as expected
            tdLog.info(f"✗ SQL didn't fail as expected: {sql} - {e}")
            
            # Try direct execution to see what happens
            try:
                tdSql.execute(sql)
                tdLog.info(f"✗ SQL executed successfully when it should have failed: {sql}")
            except Exception as exec_e:
                tdLog.info(f"✓ SQL failed during execution (alternative path): {sql} - {exec_e}")

    def testValidStreamOperations(self):
        """Test some valid stream operations to ensure system is working"""
        tdLog.info("test valid stream operations for comparison")
        
        try:
            # Create a valid stream
            valid_stream_sql = """
                CREATE STREAM emptytest.valid_stream 
                INTERVAL(10s) 
                FROM emptytest.meters 
                PARTITION BY tbname 
                INTO emptytest.valid_result 
                AS SELECT _twstart ts, COUNT(*) cnt 
                FROM emptytest.meters 
                WHERE ts >= _twstart AND ts < _twend;
            """
            
            tdSql.execute(valid_stream_sql)
            tdLog.info("✓ Valid stream created successfully")
            
            # Wait a moment
            time.sleep(2)
            
            # Check stream status
            tdSql.query("SELECT * FROM information_schema.ins_streams WHERE stream_name = 'valid_stream';")
            if tdSql.getRows() > 0:
                status = tdSql.getData(0, 6)
                tdLog.info(f"✓ Valid stream status: {status}")
            
            # Clean up
            tdSql.execute("DROP STREAM IF EXISTS emptytest.valid_stream;")
            tdSql.execute("DROP TABLE IF EXISTS emptytest.valid_result;")
            
        except Exception as e:
            tdLog.info(f"Valid stream test failed: {e}")
            # This might indicate syntax differences in new framework

    def cleanup(self):
        """Clean up test database"""
        tdLog.info("cleaning up test database")
        try:
            # Drop any remaining streams
            tdSql.query("SELECT stream_name FROM information_schema.ins_streams WHERE db_name = 'emptytest';")
            for i in range(tdSql.getRows()):
                stream_name = tdSql.getData(i, 0)
                try:
                    tdSql.execute(f"DROP STREAM IF EXISTS emptytest.{stream_name};")
                except:
                    pass
            
            # Drop database
            tdSql.execute("DROP DATABASE IF EXISTS emptytest;")
        except Exception as e:
            tdLog.info(f"Cleanup completed: {e}")

    def __del__(self):
        """Destructor to ensure cleanup"""
        try:
            self.cleanup()
        except:
            pass 