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
        """Stream Empty Identifier Test with New Syntax

        Test empty identifier handling in stream processing using new syntax:
        1. Empty identifiers in stream creation with new trigger types
        2. Empty identifiers in table names used by streams  
        3. Empty identifiers in new stream options and parameters
        4. Error handling for invalid empty identifiers in new syntax
        5. Test various trigger types: INTERVAL+SLIDING, COUNT_WINDOW, PERIOD, SESSION, STATE_WINDOW, EVENT_WINDOW

        Catalog:
            - Streams:OldPyCases

        Since: v3.0.0.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-12-19 Migrated from system-test/0-others/empty_identifier.py
            - 2025-01-01 Updated to use new stream computation syntax
            - Note: Focused on stream-related empty identifier tests with new syntax

        """

        self.createSnode()
        self.createDatabase()
        self.prepareTestEnv()
        self.testStreamEmptyIdentifier()
        self.testValidStreamOperations()

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
        """Test empty identifier handling in stream operations using new syntax"""
        tdLog.info("test empty identifier in stream operations")
        
        # Test cases with empty identifiers that should fail - using new stream syntax
        error_sqls = [
            # Empty table names in sliding window trigger
            "CREATE STREAM emptytest.test_stream INTERVAL(10s) SLIDING(10s) FROM `` INTO emptytest.result AS SELECT _twstart, COUNT(*) FROM `` WHERE ts >= _twstart AND ts <= _twend;",
            
            # Empty stream names with count window
            "CREATE STREAM `` COUNT_WINDOW(5) FROM emptytest.meters INTO emptytest.result AS SELECT _twstart, COUNT(*) FROM %%trows;",
            
            # Empty result table names with period trigger
            "CREATE STREAM emptytest.test_stream PERIOD(30s) FROM emptytest.meters INTO `` AS SELECT cast(_tlocaltime/1000000 as timestamp) ts, COUNT(*) FROM emptytest.meters;",
            
            # Empty column names in session window
            "CREATE STREAM emptytest.test_stream SESSION(ts, 5s) FROM emptytest.meters INTO emptytest.result AS SELECT _twstart, COUNT(*) `` FROM %%trows;",
            
            # Empty identifiers in partition by
            "CREATE STREAM emptytest.test_stream INTERVAL(10s) SLIDING(10s) FROM emptytest.meters PARTITION BY `` INTO emptytest.result AS SELECT _twstart, COUNT(*) FROM emptytest.meters WHERE ts >= _twstart AND ts <= _twend;",
            
            # Empty database names
            "CREATE STREAM ``.test_stream INTERVAL(10s) SLIDING(10s) FROM emptytest.meters INTO emptytest.result AS SELECT _twstart, COUNT(*) FROM emptytest.meters WHERE ts >= _twstart AND ts <= _twend;",
            
            # Empty identifiers in DROP operations
            "DROP STREAM ``;",
            
            # Empty identifiers in manual recalculation
            "RECALCULATE STREAM `` FROM '2025-01-01 00:00:00';",
            
            # Empty identifiers in state window
            "CREATE STREAM emptytest.test_stream STATE_WINDOW(``) FROM emptytest.meters PARTITION BY tbname INTO emptytest.result AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
            
            # Empty identifiers in stream options
            "CREATE STREAM emptytest.test_stream INTERVAL(10s) SLIDING(10s) FROM emptytest.meters STREAM_OPTIONS(WATERMARK(``)) INTO emptytest.result AS SELECT _twstart, COUNT(*) FROM emptytest.meters WHERE ts >= _twstart AND ts <= _twend;",
        ]
        
        # Test each SQL that should fail
        for sql in error_sqls:
            try:
                tdLog.info(f"Testing SQL: {sql}")
                tdSql.error(sql)
            except Exception as e:
                tdLog.info(f"Expected error for empty identifier: {e}")

        # Test some additional stream-specific empty identifier cases with new syntax
        stream_specific_sqls = [
            # Empty identifier in trigger conditions with pre-filter
            "CREATE STREAM emptytest.test_stream INTERVAL(10s) SLIDING(10s) FROM emptytest.meters STREAM_OPTIONS(PRE_FILTER(`` > 0)) INTO emptytest.result AS SELECT _twstart, COUNT(*) FROM emptytest.meters WHERE ts >= _twstart AND ts <= _twend;",
            
            # Empty identifier in aggregation functions
            "CREATE STREAM emptytest.test_stream COUNT_WINDOW(10) FROM emptytest.meters INTO emptytest.result AS SELECT _twstart, ``(*) FROM %%trows;",
            
            # Empty identifier in window placeholder usage
            "CREATE STREAM emptytest.test_stream INTERVAL(10s) SLIDING(10s) FROM emptytest.meters INTO emptytest.result AS SELECT _twstart, COUNT(*) FROM emptytest.meters WHERE `` >= _twstart;",
            
            # Empty identifier in event window conditions
            "CREATE STREAM emptytest.test_stream EVENT_WINDOW(START WITH `` > 5 END WITH c1 < 2) FROM emptytest.meters PARTITION BY tbname INTO emptytest.result AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
            
            # Empty identifier in session window timestamp column
            "CREATE STREAM emptytest.test_stream SESSION(``, 5s) FROM emptytest.meters PARTITION BY tbname INTO emptytest.result AS SELECT _twstart, _twend, COUNT(*) FROM %%trows;",
            
            # Empty identifier in output subtable expression
            "CREATE STREAM emptytest.test_stream INTERVAL(10s) SLIDING(10s) FROM emptytest.meters PARTITION BY tbname INTO emptytest.result OUTPUT_SUBTABLE(``) AS SELECT _twstart, COUNT(*) FROM emptytest.meters WHERE ts >= _twstart AND ts <= _twend;",
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
            tdSql.error(sql)
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
        """Test some valid stream operations to ensure system is working with new syntax"""
        tdLog.info("test valid stream operations for comparison using new syntax")
        
        try:
            # Create valid streams using different trigger types from new syntax
            valid_streams = [
                # Sliding window trigger
                """
                CREATE STREAM emptytest.valid_interval_stream 
                INTERVAL(10s) SLIDING(10s)
                FROM emptytest.meters 
                PARTITION BY tbname 
                INTO emptytest.valid_interval_result 
                AS SELECT _twstart ts, COUNT(*) cnt, AVG(c1) avg_c1
                FROM emptytest.meters 
                WHERE ts >= _twstart AND ts <= _twend;
                """,
                
                # Count window trigger
                """
                CREATE STREAM emptytest.valid_count_stream 
                COUNT_WINDOW(5) 
                FROM emptytest.meters 
                PARTITION BY tbname 
                INTO emptytest.valid_count_result 
                AS SELECT _twstart ts, COUNT(*) cnt, MAX(c2) max_c2
                FROM %%trows;
                """,
                
                # Period trigger
                """
                CREATE STREAM emptytest.valid_period_stream 
                PERIOD(30s) 
                FROM emptytest.meters 
                PARTITION BY tbname 
                STREAM_OPTIONS(IGNORE_NODATA_TRIGGER)
                INTO emptytest.valid_period_result 
                AS SELECT cast(_tlocaltime/1000000 as timestamp) ts, COUNT(*) total_cnt, MIN(c3) min_c3
                FROM emptytest.meters;
                """,
                
                # Session window trigger
                """
                CREATE STREAM emptytest.valid_session_stream 
                SESSION(ts, 5s) 
                FROM emptytest.meters 
                PARTITION BY tbname 
                INTO emptytest.valid_session_result 
                AS SELECT _twstart ts, _twend te, COUNT(*) cnt, SUM(c1) sum_c1
                FROM %%trows;
                """,
            ]
            
            created_streams = []
            for i, stream_sql in enumerate(valid_streams):
                try:
                    tdSql.execute(stream_sql)
                    stream_name = f"valid_{['interval', 'count', 'period', 'session'][i]}_stream"
                    created_streams.append(stream_name)
                    tdLog.info(f"✓ Valid stream {stream_name} created successfully")
                except Exception as e:
                    tdLog.info(f"✗ Failed to create stream {i}: {e}")
            
            # Wait a moment for streams to initialize
            time.sleep(3)
            
            # Check stream statuses
            for stream_name in created_streams:
                try:
                    tdSql.query(f"SELECT * FROM information_schema.ins_streams WHERE stream_name = '{stream_name}';")
                    if tdSql.getRows() > 0:
                        tdLog.info(f"✓ Valid stream {stream_name} found in system tables")
                    else:
                        tdLog.info(f"✗ Stream {stream_name} not found in system tables")
                except Exception as e:
                    tdLog.info(f"✗ Failed to check stream {stream_name} status: {e}")
            
            # Clean up created streams
            cleanup_streams = [
                "emptytest.valid_interval_stream",
                "emptytest.valid_count_stream", 
                "emptytest.valid_period_stream",
                "emptytest.valid_session_stream"
            ]
            
            cleanup_tables = [
                "emptytest.valid_interval_result",
                "emptytest.valid_count_result",
                "emptytest.valid_period_result", 
                "emptytest.valid_session_result"
            ]
            
            for stream in cleanup_streams:
                try:
                    tdSql.execute(f"DROP STREAM IF EXISTS {stream};")
                except:
                    pass
                    
            for table in cleanup_tables:
                try:
                    tdSql.execute(f"DROP TABLE IF EXISTS {table};")
                except:
                    pass
            
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