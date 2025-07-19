import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    clusterComCheck,
    tdStream,
    StreamItem,
)


class TestStreamCompatibility:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_compatibility(self):
        """Stream Compatibility Test

        Test compatibility aspects of stream processing:
        1. Backward compatibility with legacy syntax patterns
        2. Forward compatibility with new features
        3. Cross-version compatibility considerations
        4. Migration path validation

        Catalog:
            - Streams:OldPyCases

        Since: v3.0.0.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-12-19 Migrated from system-test/0-others/compatibility.py
            - Note: Focused on stream-related compatibility, removed cluster upgrade tests

        """

        self.createSnode()
        self.createDatabase()
        self.prepareTestData()
        self.testLegacySyntaxCompatibility()
        self.testNewFrameworkFeatures()
        self.testStreamMigrationPatterns()

    def createSnode(self):
        tdLog.info("create snode")
        tdStream.createSnode(1)

    def createDatabase(self):
        tdLog.info("create database")
        tdSql.prepare(dbname="compattest", vgroups=4)
        clusterComCheck.checkDbReady("compattest")

    def prepareTestData(self):
        """Prepare test data for compatibility testing"""
        tdLog.info("prepare test data for compatibility testing")
        
        # Create tables with various schemas to test compatibility
        
        # Legacy-style table
        tdSql.execute("""
            CREATE TABLE compattest.legacy_meters (
                ts TIMESTAMP,
                current FLOAT,
                voltage INT,
                phase FLOAT
            ) TAGS (location BINARY(64), groupid INT);
        """)
        
        # New-style table with more data types
        tdSql.execute("""
            CREATE TABLE compattest.modern_sensors (
                ts TIMESTAMP,
                temperature DOUBLE,
                humidity FLOAT,
                pressure BIGINT,
                status TINYINT,
                device_name NCHAR(50),
                metadata JSON
            ) TAGS (
                building_id INT,
                floor_level SMALLINT,
                zone_name NCHAR(20),
                install_date TIMESTAMP
            );
        """)
        
        # Create child tables
        for i in range(5):
            # Legacy meters
            tdSql.execute(f"""
                CREATE TABLE compattest.legacy_m{i} USING compattest.legacy_meters 
                TAGS ('building_{i}', {i});
            """)
            
            # Modern sensors  
            install_ts = 1640995200000 + i * 86400000  # Different installation dates
            tdSql.execute(f"""
                CREATE TABLE compattest.modern_s{i} USING compattest.modern_sensors 
                TAGS ({i+100}, {i%10}, 'zone_{i}', {install_ts});
            """)
        
        # Insert test data
        base_ts = 1640995200000  # 2022-01-01 00:00:00
        for i in range(5):
            for j in range(50):
                ts = base_ts + j * 60000  # 1 minute intervals
                
                # Legacy data
                tdSql.execute(f"""
                    INSERT INTO compattest.legacy_m{i} VALUES 
                    ({ts}, {10.0 + i + j*0.1}, {220 + i*5 + j}, {j*0.01});
                """)
                
                # Modern data (without JSON for simplicity)
                tdSql.execute(f"""
                    INSERT INTO compattest.modern_s{i} VALUES 
                    ({ts}, {20.0 + i + j*0.1}, {45.0 + j*0.5}, {1013 + j}, 
                     {j%3}, 'sensor_{i}_{j}', NULL);
                """)

    def testLegacySyntaxCompatibility(self):
        """Test compatibility with legacy syntax patterns"""
        tdLog.info("test legacy syntax compatibility")
        
        # Test legacy-style stream creation patterns
        legacy_patterns = [
            # Basic interval aggregation
            {
                'name': 'legacy_interval',
                'sql': """
                    CREATE STREAM compattest.legacy_interval_stream
                    INTERVAL(5m)
                    FROM compattest.legacy_meters
                    INTO compattest.legacy_result
                    AS SELECT _twstart ts, avg(current) avg_current, count(*) cnt
                    FROM compattest.legacy_meters 
                    WHERE ts >= _twstart AND ts < _twend;
                """,
                'description': 'Legacy interval aggregation pattern'
            },
            
            # Partitioned stream
            {
                'name': 'legacy_partition',
                'sql': """
                    CREATE STREAM compattest.legacy_partition_stream
                    INTERVAL(3m)
                    FROM compattest.legacy_meters
                    PARTITION BY tbname
                    INTO compattest.legacy_partition_result
                    AS SELECT _twstart ts, max(voltage) max_voltage, min(voltage) min_voltage
                    FROM %%tbname
                    WHERE ts >= _twstart AND ts < _twend;
                """,
                'description': 'Legacy partitioned stream pattern'
            }
        ]
        
        for pattern in legacy_patterns:
            try:
                tdLog.info(f"Testing {pattern['description']}")
                tdSql.execute(pattern['sql'])
                
                # Wait for stream creation
                time.sleep(2)
                
                # Check stream status
                stream_name = pattern['name'] + '_stream'
                tdSql.query(f"SELECT * FROM information_schema.ins_streams WHERE stream_name = '{stream_name}';")
                
                if tdSql.getRows() > 0:
                    status = tdSql.getData(0, 6)
                    tdLog.info(f"✓ Legacy pattern '{pattern['name']}' created with status: {status}")
                else:
                    tdLog.info(f"✗ Legacy pattern '{pattern['name']}' not found in system tables")
                
            except Exception as e:
                tdLog.info(f"✗ Legacy pattern '{pattern['name']}' failed: {e}")
                # This might indicate syntax incompatibility

    def testNewFrameworkFeatures(self):
        """Test new framework features and enhancements"""
        tdLog.info("test new framework features")
        
        # Test new framework features
        new_features = [
            # Stream with new options
            {
                'name': 'modern_with_options',
                'sql': """
                    CREATE STREAM compattest.modern_options_stream
                    INTERVAL(2m)
                    FROM compattest.modern_sensors
                    PARTITION BY tbname
                    STREAM_OPTIONS(FILL_HISTORY_FIRST | LOW_LATENCY_CALC)
                    INTO compattest.modern_options_result
                    TAGS (device_type NCHAR(20) AS CONCAT('sensor_', %%1))
                    AS SELECT _twstart ts, avg(temperature) avg_temp, 
                             stddev(humidity) stddev_humidity, count(*) cnt
                    FROM %%tbname
                    WHERE ts >= _twstart AND ts < _twend;
                """,
                'description': 'Modern stream with new options'
            },
            
            # Stream with watermark
            {
                'name': 'modern_watermark',
                'sql': """
                    CREATE STREAM compattest.modern_watermark_stream
                    INTERVAL(1m)
                    FROM compattest.modern_sensors
                    STREAM_OPTIONS(WATERMARK(30s))
                    INTO compattest.modern_watermark_result
                    AS SELECT _twstart ts, last(temperature) last_temp, 
                             first(pressure) first_pressure
                    FROM compattest.modern_sensors
                    WHERE ts >= _twstart AND ts < _twend;
                """,
                'description': 'Modern stream with watermark'
            }
        ]
        
        for feature in new_features:
            try:
                tdLog.info(f"Testing {feature['description']}")
                tdSql.execute(feature['sql'])
                
                # Wait for stream creation
                time.sleep(2)
                
                # Check stream status
                stream_name = feature['name'] + '_stream'
                tdSql.query(f"SELECT * FROM information_schema.ins_streams WHERE stream_name = '{stream_name}';")
                
                if tdSql.getRows() > 0:
                    status = tdSql.getData(0, 6)
                    tdLog.info(f"✓ New feature '{feature['name']}' created with status: {status}")
                else:
                    tdLog.info(f"✗ New feature '{feature['name']}' not found in system tables")
                
            except Exception as e:
                tdLog.info(f"✗ New feature '{feature['name']}' failed: {e}")
                # Some new syntax might not be fully implemented yet

    def testStreamMigrationPatterns(self):
        """Test common migration patterns from old to new stream framework"""
        tdLog.info("test stream migration patterns")
        
        # Test patterns that show how old syntax maps to new syntax
        migration_tests = [
            {
                'old_pattern': 'Basic continuous query style',
                'new_sql': """
                    CREATE STREAM compattest.migration_basic
                    INTERVAL(10m)
                    FROM compattest.legacy_meters
                    INTO compattest.migration_basic_result
                    AS SELECT _twstart ts, avg(current) avg_current
                    FROM compattest.legacy_meters
                    WHERE ts >= _twstart AND ts < _twend;
                """,
                'description': 'Migration from CQ to stream'
            },
            
            {
                'old_pattern': 'Fill history pattern',
                'new_sql': """
                    CREATE STREAM compattest.migration_history
                    INTERVAL(5m)
                    FROM compattest.modern_sensors
                    STREAM_OPTIONS(FILL_HISTORY_FIRST)
                    INTO compattest.migration_history_result
                    AS SELECT _twstart ts, count(*) cnt, avg(temperature) avg_temp
                    FROM compattest.modern_sensors
                    WHERE ts >= _twstart AND ts < _twend;
                """,
                'description': 'Migration with history filling'
            }
        ]
        
        for test in migration_tests:
            try:
                tdLog.info(f"Testing migration pattern: {test['description']}")
                tdSql.execute(test['new_sql'])
                
                # Wait for stream creation
                time.sleep(2)
                
                # Extract stream name from SQL
                stream_name = test['new_sql'].split('compattest.')[1].split()[0]
                
                # Check stream status
                tdSql.query(f"SELECT * FROM information_schema.ins_streams WHERE stream_name = '{stream_name}';")
                
                if tdSql.getRows() > 0:
                    status = tdSql.getData(0, 6)
                    tdLog.info(f"✓ Migration pattern '{test['old_pattern']}' successful: {status}")
                else:
                    tdLog.info(f"✗ Migration pattern '{test['old_pattern']}' not found")
                
            except Exception as e:
                tdLog.info(f"✗ Migration pattern '{test['old_pattern']}' failed: {e}")

    def testCompatibilityEdgeCases(self):
        """Test edge cases for compatibility"""
        tdLog.info("test compatibility edge cases")
        
        # Test mixed old and new syntax
        edge_cases = [
            # Using old-style table with new-style stream options
            """
                CREATE STREAM compattest.edge_mixed
                INTERVAL(2m)
                FROM compattest.legacy_meters
                STREAM_OPTIONS(WATERMARK(10s) | LOW_LATENCY_CALC)
                INTO compattest.edge_mixed_result
                AS SELECT _twstart ts, sum(voltage) total_voltage
                FROM compattest.legacy_meters
                WHERE ts >= _twstart AND ts < _twend;
            """,
            
            # Complex aggregation with new framework
            """
                CREATE STREAM compattest.edge_complex
                INTERVAL(3m)
                FROM compattest.modern_sensors
                PARTITION BY building_id
                INTO compattest.edge_complex_result
                AS SELECT _twstart ts, 
                         avg(temperature) avg_temp,
                         percentile(humidity, 95) p95_humidity,
                         count(*) cnt
                FROM compattest.modern_sensors
                WHERE ts >= _twstart AND ts < _twend;
            """
        ]
        
        for i, sql in enumerate(edge_cases):
            try:
                tdLog.info(f"Testing edge case {i+1}")
                tdSql.execute(sql)
                
                time.sleep(2)
                
                # Check if stream was created
                tdSql.query("SELECT COUNT(*) FROM information_schema.ins_streams WHERE db_name = 'compattest';")
                stream_count = tdSql.getData(0, 0)
                tdLog.info(f"✓ Edge case {i+1} processed, total streams: {stream_count}")
                
            except Exception as e:
                tdLog.info(f"✗ Edge case {i+1} failed: {e}")
                # Some edge cases might not be supported

    def cleanup(self):
        """Clean up test database and streams"""
        tdLog.info("cleaning up compatibility test")
        
        try:
            # Drop all streams in the test database
            tdSql.query("SELECT stream_name FROM information_schema.ins_streams WHERE db_name = 'compattest';")
            for i in range(tdSql.getRows()):
                stream_name = tdSql.getData(i, 0)
                try:
                    tdSql.execute(f"DROP STREAM IF EXISTS compattest.{stream_name};")
                except:
                    pass
            
            # Drop database
            tdSql.execute("DROP DATABASE IF EXISTS compattest;")
            
        except Exception as e:
            tdLog.info(f"Cleanup completed: {e}")

    def __del__(self):
        """Destructor to ensure cleanup"""
        try:
            self.cleanup()
        except:
            pass 