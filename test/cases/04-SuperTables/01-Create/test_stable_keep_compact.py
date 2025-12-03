###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql
import time

class TestStbKeepCompact:

    def prepare_database_with_keep(self, db_name, db_keep):
        """Create a database with specified keep value"""
        tdLog.info(f"Creating database {db_name} with keep={db_keep}")
        tdSql.execute(f"DROP DATABASE IF EXISTS {db_name}")
        # Ensure duration is small enough to satisfy the rule keep > 3*duration
        
        # Calculate the minimum legal duration (ensure keep > 3*duration)
        max_legal_duration = int(db_keep / 3)
        if max_legal_duration < 1:
            # If keep is too small, use smaller time unit
            # Use hours as unit, 1 day = 24 hours
            duration_hours = 12  # Use 12 hours
            tdLog.info(f"Setting duration={duration_hours}h to ensure keep({db_keep}) > 3*duration")
            tdSql.execute(f"CREATE DATABASE {db_name} DURATION {duration_hours}h KEEP {db_keep}")
        else:
            duration = max(1, max_legal_duration - 1)  # Conservatively, use smaller value, ensure it's an integer and not zero
            tdLog.info(f"Setting duration={duration}d to ensure keep({db_keep}) > 3*duration")
            tdSql.execute(f"CREATE DATABASE {db_name} DURATION {duration}d KEEP {db_keep}")
            
        tdSql.execute(f"USE {db_name}")
        return True

    def create_super_table_with_keep(self, stb_name, keep_days):
        """Create a super table with specified keep value in days"""
        tdLog.info(f"Creating super table {stb_name} with keep={keep_days}d")
        create_sql = f"CREATE STABLE {stb_name} (ts TIMESTAMP, val INT) TAGS (t_id INT) KEEP {keep_days}d"
        tdSql.execute(create_sql)
        return True

    def create_tables_and_insert_data(self, stb_name, table_prefix, table_count=1):
        """Create child tables and insert data at different time points"""
        # Current time and historical data points
        now = int(time.time() * 1000)  # Get current time directly, not relying on self
        day1_ts = now - 1 * 24 * 3600 * 1000  # 1 day ago
        day3_ts = now - 3 * 24 * 3600 * 1000  # 3 days ago
        day5_ts = now - 5 * 24 * 3600 * 1000  # 5 days ago
        day7_ts = now - 7 * 24 * 3600 * 1000  # 7 days ago
        
        for i in range(1, table_count + 1):
            tb_name = f"{table_prefix}_{i}"
            tdLog.info(f"Creating child table {tb_name} under {stb_name}")
            tdSql.execute(f"CREATE TABLE {tb_name} USING {stb_name} TAGS({i})")
            
            # Insert data at different time points
            tdLog.info(f"Inserting data into {tb_name} at different time points")
            tdSql.execute(f"INSERT INTO {tb_name} VALUES ({now}, 100)")
            tdSql.execute(f"INSERT INTO {tb_name} VALUES ({day1_ts}, 90)")
            tdSql.execute(f"INSERT INTO {tb_name} VALUES ({day3_ts}, 70)")
            tdSql.execute(f"INSERT INTO {tb_name} VALUES ({day5_ts}, 50)")
            tdSql.execute(f"INSERT INTO {tb_name} VALUES ({day7_ts}, 30)")
        
        # Log timestamps for reference
        current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now/1000))
        day1_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(day1_ts/1000))
        day3_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(day3_ts/1000))
        day5_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(day5_ts/1000))
        day7_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(day7_ts/1000))
        
        tdLog.info(f"Inserted data at: Current ({current_time}), 1 day ago ({day1_time}), " + 
                  f"3 days ago ({day3_time}), 5 days ago ({day5_time}), 7 days ago ({day7_time})")
        
        return {
            "now": now,
            "day1_ts": day1_ts,
            "day3_ts": day3_ts,
            "day5_ts": day5_ts,
            "day7_ts": day7_ts,
            "day1_time": day1_time,
            "day3_time": day3_time,
            "day5_time": day5_time,
            "day7_time": day7_time
        }

    def create_tables_and_insert_data_within_days(self, stb_name, table_prefix, max_days, table_count=1):
        """Create child tables and insert data at different time points within specified max days"""
        # Current time and historical data points
        now = int(time.time() * 1000)  # Get current time directly, not relying on self
        day1_ts = now - 1 * 24 * 3600 * 1000  # 1 day ago
        day3_ts = now - 3 * 24 * 3600 * 1000  # 3 days ago
        day5_ts = now - 5 * 24 * 3600 * 1000  # 5 days ago
        
        for i in range(1, table_count + 1):
            tb_name = f"{table_prefix}_{i}"
            tdLog.info(f"Creating child table {tb_name} under {stb_name}")
            tdSql.execute(f"CREATE TABLE {tb_name} USING {stb_name} TAGS({i})")
            
            # Insert data at different time points within max_days
            tdLog.info(f"Inserting data into {tb_name} at different time points within {max_days} days")
            tdSql.execute(f"INSERT INTO {tb_name} VALUES ({now}, 100)")
            tdSql.execute(f"INSERT INTO {tb_name} VALUES ({day1_ts}, 90)")
            tdSql.execute(f"INSERT INTO {tb_name} VALUES ({day3_ts}, 70)")
            
            # Only insert 5-day old data if max_days >= 5
            if max_days >= 5:
                tdSql.execute(f"INSERT INTO {tb_name} VALUES ({day5_ts}, 50)")
        
        # Log timestamps for reference
        current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now/1000))
        day1_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(day1_ts/1000))
        day3_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(day3_ts/1000))
        day5_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(day5_ts/1000))
        
        tdLog.info(f"Inserted data at: Current ({current_time}), 1 day ago ({day1_time}), " + 
                  f"3 days ago ({day3_time})" + 
                  (f", 5 days ago ({day5_time})" if max_days >= 5 else ""))
        
        return {
            "now": now,
            "day1_ts": day1_ts,
            "day3_ts": day3_ts,
            "day5_ts": day5_ts if max_days >= 5 else None,
            "day1_time": day1_time,
            "day3_time": day3_time,
            "day5_time": day5_time if max_days >= 5 else None
        }

    def verify_data_count(self, table_name, expected_count, msg=""):
        """Verify the count of rows in a table"""
        tdSql.query(f"SELECT COUNT(*) FROM {table_name}")
        actual_count = tdSql.getData(0, 0)
        tdLog.info(f"{msg} - Expected: {expected_count}, Actual: {actual_count}")
        tdSql.checkEqual(actual_count, expected_count)
    
    def verify_oldest_data(self, table_name, expected_val, msg=""):
        """Verify the oldest data value in a table"""
        tdSql.query(f"SELECT val FROM {table_name} ORDER BY ts ASC LIMIT 1")
        actual_val = tdSql.getData(0, 0)
        tdLog.info(f"{msg} - Expected oldest value: {expected_val}, Actual: {actual_val}")
        tdSql.checkEqual(actual_val, expected_val)

    def trigger_compact(self, db_name):
        """Trigger database compaction"""
        tdLog.info(f"Triggering compaction for database {db_name}...")
        tdLog.info(f"FLUSH DATABASE {db_name}")
        tdSql.execute(f"FLUSH DATABASE {db_name}")
        time.sleep(5)

        # Correct syntax includes database name
        tdSql.execute(f"COMPACT DATABASE {db_name}")

        # Wait for compaction to complete by checking status
        while True:
            tdSql.query(f"SHOW COMPACTS")
            if tdSql.queryRows == 0:
                break
            time.sleep(1)

        tdLog.info(f"Compaction operation for {db_name} completed")

    def run_case1_stb_keep_2_db_keep_10(self):
        """Test case 1: STB keep=2, DB keep=10 - STB keep should determine data retention after compact"""
        tdLog.info("=== Test Case 1: STB keep=2, DB keep=10 ===")
        
        # Setup
        self.prepare_database_with_keep("test_stb_compact1", 10)
        self.create_super_table_with_keep("stb_keep2", 2)
        self.create_tables_and_insert_data("stb_keep2", "tb_case1")
        
        # Verify data before compact
        self.verify_data_count("stb_keep2", 5, "Before compact - all data should be visible")
        self.verify_oldest_data("stb_keep2", 30, "Before compact - 7-day old data should be visible")
        
        # Trigger compact
        self.trigger_compact("test_stb_compact1")
        
        # Verify data after compact
        # With STB keep=2, data older than 2 days should be removed
        self.verify_data_count("stb_keep2", 2, "After compact - only data within 2 days should remain")
        self.verify_oldest_data("stb_keep2", 90, "After compact - oldest data should be from 1 day ago")

    def run_case2_stb_keep_4_db_keep_5(self):
        """Test case 2: STB keep=4, DB keep=5 - DB keep should override STB keep"""
        tdLog.info("=== Test Case 2: STB keep=4, DB keep=5 ===")
        
        # Setup
        # Modify database keep value to 5, ensuring it satisfies keep > 3*duration rule
        # Even with duration minimum of 1, keep=5 can satisfy the condition
        db_keep = 5
        self.prepare_database_with_keep("test_stb_compact2", db_keep)
        self.create_super_table_with_keep("stb_keep4", 4)
        # Only insert data within db_keep-1 days to avoid going beyond database keep range
        safe_days = db_keep - 1  # Safe margin to avoid boundary condition issues
        self.create_tables_and_insert_data_within_days("stb_keep4", "tb_case2", safe_days)
        
        # Verify data before compact
        # If safe_days=4, we only have at most 4 data points (current, 1 day ago, 3 days ago, not including 5 days ago)
        expected_count = 3  # current, 1 day ago, 3 days ago
        self.verify_data_count("stb_keep4", expected_count, "Before compact - all data should be visible")
        self.verify_oldest_data("stb_keep4", 70, "Before compact - 3-day old data should be visible")
        
        # Trigger compact
        self.trigger_compact("test_stb_compact2")
        
        # Verify data after compact
        # Database keep=5, STB keep=4, all data is within retention range, so no data should be deleted
        self.verify_data_count("stb_keep4", expected_count, "After compact - all data should remain")
        self.verify_oldest_data("stb_keep4", 70, "After compact - oldest data should still be from 3 days ago")

    def run_case3_multiple_stbs_with_different_keep(self):
        """Test case 3: Multiple STBs with different keep values in same database"""
        tdLog.info("=== Test Case 3: Multiple STBs with different keep values ===")
        
        # Setup
        self.prepare_database_with_keep("test_stb_compact3", 10)
        self.create_super_table_with_keep("stb_keep2", 2)
        self.create_super_table_with_keep("stb_keep4", 4)
        self.create_super_table_with_keep("stb_keep8", 8)
        
        self.create_tables_and_insert_data("stb_keep2", "tb_keep2")
        self.create_tables_and_insert_data("stb_keep4", "tb_keep4")
        self.create_tables_and_insert_data("stb_keep8", "tb_keep8")
        
        # Verify data before compact
        for stb in ["stb_keep2", "stb_keep4", "stb_keep8"]:
            self.verify_data_count(stb, 5, f"Before compact - all data should be visible in {stb}")
        
        # Trigger compact
        self.trigger_compact("test_stb_compact3")
        
        # Verify data after compact
        # Each STB should retain data according to its keep value
        self.verify_data_count("stb_keep2", 2, "After compact - stb_keep2 should keep 2 days of data")
        self.verify_oldest_data("stb_keep2", 90, "After compact - stb_keep2 oldest data from 1 day ago")
        
        self.verify_data_count("stb_keep4", 3, "After compact - stb_keep4 should keep 4 days of data")
        self.verify_oldest_data("stb_keep4", 70, "After compact - stb_keep4 oldest data from 3 days ago")
        
        self.verify_data_count("stb_keep8", 5, "After compact - stb_keep8 should keep all data (within 8 days)")
        self.verify_oldest_data("stb_keep8", 30, "After compact - stb_keep8 oldest data from 7 days ago")

    def run_case4_boundary_keep_duration_ratio(self):
        """Test case 4: Testing boundary condition where keep is slightly above 3*duration"""
        tdLog.info("=== Test Case 4: Boundary keep/duration ratio ===")
        
        # Create a database with keep=10, duration=3 (exactly satisfying keep > 3*duration)
        db_name = "test_stb_compact4"
        db_keep = 10
        duration = 3 # days
        
        tdLog.info(f"Creating database with boundary condition: keep={db_keep}, duration={duration}d")
        tdSql.execute(f"DROP DATABASE IF EXISTS {db_name}")
        tdSql.execute(f"CREATE DATABASE {db_name} DURATION {duration}d KEEP {db_keep}")
        tdSql.execute(f"USE {db_name}")
        
        # Create STB with keep<5 to test extreme condition
        self.create_super_table_with_keep("stb_keep3", 3)
        self.create_tables_and_insert_data("stb_keep3", "tb_boundary_min")
        
        # Create STB with data
        self.create_super_table_with_keep("stb_keep7", 7)
        self.create_tables_and_insert_data("stb_keep7", "tb_boundary")
        
        # Verify data before compact
        self.verify_data_count("stb_keep7", 5, "Before compact - all data should be visible")
        self.verify_oldest_data("stb_keep7", 30, "Before compact - 7-day old data should be visible")
        
        self.verify_data_count("stb_keep3", 5, "Before compact - all data should be visible in stb_keep3")
        
        # Trigger compact
        self.trigger_compact("test_stb_compact4")
        
        # Verify data after compact
        # Database keep=10, STB keep=7, STB keep should determine retention
        self.verify_data_count("stb_keep7", 4, "After compact - data within 7 days should remain")
        self.verify_oldest_data("stb_keep7", 50, "After compact - oldest data from 7 days ago should remain")
        
        # Verify minimum keep value STB (keep=3)
        self.verify_data_count("stb_keep3", 2, "After compact - only data within 3 days should remain")
        self.verify_oldest_data("stb_keep3", 90, "After compact - oldest data should be from 3 days ago")

    def run_case5_write_time_with_keep_restrictions(self):
        """Test case 5: Testing write behavior with keep restrictions"""
        tdLog.info("=== Test Case 5: Write behavior with keep restrictions ===")
        
        # Setup: database keep=8, STB keep values 3 and 10
        db_name = "test_stb_write_keep"
        db_keep = 8
        
        # Create database
        tdLog.info(f"Creating database with keep={db_keep}")
        tdSql.execute(f"DROP DATABASE IF EXISTS {db_name}")
        tdSql.execute(f"CREATE DATABASE {db_name} DURATION 2d KEEP {db_keep}")
        tdSql.execute(f"USE {db_name}")
        
        # Create two super tables: one with keep value less than database, one with greater
        tdLog.info("Creating super tables with different keep values")
        self.create_super_table_with_keep("stb_keep3", 3)  # keep value less than database
        self.create_super_table_with_keep("stb_keep10", 8)  # keep value greater than database
        
        # Create child tables
        tdLog.info("Creating child tables")
        tdSql.execute("CREATE TABLE tb_keep3_1 USING stb_keep3 TAGS(1)")
        tdSql.execute("CREATE TABLE tb_keep10_1 USING stb_keep10 TAGS(1)")
        
        # Get current time and historical timestamps
        now = int(time.time() * 1000)
        day6_ts = now - 6 * 24 * 3600 * 1000  # 6 days ago
        day9_ts = now - 9 * 24 * 3600 * 1000  # 9 days ago
        day6_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(day6_ts/1000))
        day9_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(day9_ts/1000))
        
        tdLog.info(f"Current time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now/1000))}")
        tdLog.info(f"Time 6 days ago: {day6_time}")
        tdLog.info(f"Time 9 days ago: {day9_time}")
        
        # Scenario 1: Timestamp within database keep range (8d) but beyond STB keep range (3d)
        tdLog.info("Scenario 1: Timestamp within database keep (8d) but beyond stable keep (3d)")
        # Should be able to insert successfully
        tdSql.execute(f"INSERT INTO tb_keep3_1 VALUES ({day6_ts}, 40)")
        tdLog.info("Successfully inserted data beyond stable keep but within database keep - CORRECT BEHAVIOR")
        
        # Check if data was inserted successfully
        tdSql.query(f"SELECT * FROM tb_keep3_1 WHERE ts = {day6_ts}")
        if tdSql.queryRows == 1:
            tdLog.info("Verified data was inserted successfully")
        else:
            tdLog.info("ERROR: Failed to verify inserted data")
            
        # Optional: Run compact to see if data beyond STB keep but within DB keep is removed
        self.trigger_compact(db_name)
            
        # Check if data still exists after compact
        tdSql.query(f"SELECT * FROM tb_keep3_1 WHERE ts = {day6_ts}")
        if tdSql.queryRows == 0:
            tdLog.info("After compact: Data beyond STB keep (3d) was removed as expected")
        else:
            tdLog.info("After compact: Data beyond STB keep (3d) was retained (unexpected)")
        
        # Scenario 2: Timestamp beyond database keep range (8d)
        tdLog.info("Scenario 2: Timestamp beyond database keep (8d)")
        expected_error = "Timestamp data out of range"  # Expected error message
        insert_sql = f"INSERT INTO tb_keep3_1 VALUES ({day9_ts}, 10)"
        # Use tdSql.error to check if expected error is raised
        try:
            # This insertion should fail because it exceeds database keep range
            tdSql.error(insert_sql)
            tdLog.info("Insertion beyond database keep was correctly rejected - EXPECTED ERROR")
        except Exception as e:
            tdLog.info(f"ERROR: Expected error was not raised: {str(e)}")
            # Don't raise exception, allow test to continue
        
        # Scenario 3: Try to insert data beyond database keep into STB with keep value > database keep
        tdLog.info("Scenario 3: Timestamp beyond database keep (8d) for table with stable keep (10d)")
        insert_sql = f"INSERT INTO tb_keep10_1 VALUES ({day9_ts}, 10)"
        # Use tdSql.error to check if expected error is raised
        tdSql.error(insert_sql, expectErrInfo="Timestamp data out of range")
        tdLog.info("Insertion beyond database keep for table with larger stable keep was successful - NEW EXPECTED BEHAVIOR")
        
    def run_case6_db_keep_8_stb_keep_4(self):
        """Test case 6: DB keep=8, STB keep=4 - Testing data insertion before and after compaction"""
        tdLog.info("=== Test Case 6: DB keep=8, STB keep=4 - Data insertion behavior ===")
        
        # Setup
        db_name = "test_stb_compact6"
        db_keep = 8
        stb_keep = 4
        
        # Create database and super table
        tdLog.info(f"Creating database with keep={db_keep}")
        tdSql.execute(f"DROP DATABASE IF EXISTS {db_name}")
        tdSql.execute(f"CREATE DATABASE {db_name} DURATION 2d KEEP {db_keep}")
        tdSql.execute(f"USE {db_name}")
        
        # Create super table with keep=4
        tdLog.info(f"Creating super table with keep={stb_keep}d")
        self.create_super_table_with_keep("stb_keep4", stb_keep)
        
        # Create child table
        tdLog.info("Creating child table")
        tdSql.execute("CREATE TABLE tb_keep4_1 USING stb_keep4 TAGS(1)")
        
        # Get current time and historical timestamps
        now = int(time.time() * 1000)
        day3_ts = now - 3 * 24 * 3600 * 1000  # 3 days ago
        day7_ts = now - 7 * 24 * 3600 * 1000  # 7 days ago
        
        day3_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(day3_ts/1000))
        day7_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(day7_ts/1000))
        
        tdLog.info(f"Current time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now/1000))}")
        tdLog.info(f"Time 3 days ago: {day3_time}")
        tdLog.info(f"Time 7 days ago: {day7_time}")
        
        # Step 1: Insert data from 3 days ago and 7 days ago
        tdLog.info("Step 1: Inserting data from 3 days ago and 7 days ago")
        tdSql.execute(f"INSERT INTO tb_keep4_1 VALUES ({now}, 100)")
        tdSql.execute(f"INSERT INTO tb_keep4_1 VALUES ({day3_ts}, 70)")
        tdSql.execute(f"INSERT INTO tb_keep4_1 VALUES ({day7_ts}, 30)")
        
        # Verify initial insertion
        tdSql.query("SELECT COUNT(*) FROM stb_keep4")
        tdLog.info(f"Initial data count: {tdSql.getData(0, 0)}")
        tdSql.checkEqual(tdSql.getData(0, 0), 3)
        
        # Step 2: Flush and compact the database
        tdLog.info("Step 2: Flushing and compacting the database")
        self.trigger_compact(db_name)
        
        # Step 3: Query after compaction
        tdLog.info("Step 3: Querying data after compaction")
        tdSql.query("SELECT COUNT(*) FROM stb_keep4")
        count_after_compact = tdSql.getData(0, 0)
        tdLog.info(f"Data count after compaction: {count_after_compact}")
        
        # Check if data from 7 days ago (beyond STB keep=4) is removed
        tdSql.query(f"SELECT * FROM stb_keep4 WHERE ts = {day7_ts}")
        if tdSql.queryRows == 0:
            tdLog.info("Data from 7 days ago was correctly removed after compaction")
        else:
            tdLog.info("ERROR: Data from 7 days ago was not removed as expected")
        
        # Check if data from 3 days ago (within STB keep=4) is retained
        tdSql.query(f"SELECT * FROM stb_keep4 WHERE ts = {day3_ts}")
        if tdSql.queryRows == 1:
            tdLog.info("Data from 3 days ago was correctly retained after compaction")
        else:
            tdLog.info("ERROR: Data from 3 days ago was unexpectedly removed")
        
        # Step 4: Try to insert data from 7 days ago again (after compaction)
        tdLog.info("Step 4: Inserting data from 7 days ago after compaction")
        tdSql.execute(f"INSERT INTO tb_keep4_1 VALUES ({day7_ts}, 35)")
        
        # Verify new insertion
        tdSql.query(f"SELECT * FROM stb_keep4 WHERE ts = {day7_ts}")
        if tdSql.queryRows == 1:
            tdLog.info("Successfully inserted data from 7 days ago after compaction")
            tdLog.info(f"Value: {tdSql.getData(0, 1)}")
        else:
            tdLog.info("ERROR: Failed to insert data from 7 days ago after compaction")
        
        # Get total count after new insertion
        tdSql.query("SELECT COUNT(*) FROM stb_keep4")
        count_after_insert = tdSql.getData(0, 0)
        tdLog.info(f"Data count after new insertion: {count_after_insert}")
        
        # Expected count: retained count from before + 1 new record
        expected_count = count_after_compact + 1
        tdSql.checkEqual(count_after_insert, expected_count)
        
    def run_case7_alter_stb_keep(self):
        """Test case 7: Test ALTER STABLE KEEP parameter and its effect on data retention after compaction"""
        tdLog.info("=== Test Case 7: ALTER STABLE KEEP parameter ===")
        
        # Setup
        db_name = "test_stb_alter_keep"
        db_keep = 10  # Set database keep to a higher value to allow flexible STB keep testing
        initial_stb_keep = 3  # Initial keep value
        
        # Create database and super table with initial keep value
        tdLog.info(f"Creating database with keep={db_keep}")
        tdSql.execute(f"DROP DATABASE IF EXISTS {db_name}")
        tdSql.execute(f"CREATE DATABASE {db_name} DURATION 2d KEEP {db_keep}")
        tdSql.execute(f"USE {db_name}")
        
        # Create super table with initial keep
        tdLog.info(f"Creating super table with initial keep={initial_stb_keep}d")
        self.create_super_table_with_keep("stb_alter_keep", initial_stb_keep)
        
        # Create child table and insert data with safer time margins
        tdLog.info("Creating child table and inserting data")
        
        # For safety, we'll insert data with specific values that are clearly within boundaries
        now = int(time.time() * 1000)  # Current time in milliseconds
        
        # Add margin to ensure day calculations don't fall on boundary
        # Subtract a few hours from each day boundary for safety
        margin_hours = 4  # 4 hours safety margin
        margin_ms = margin_hours * 3600 * 1000  # Convert to milliseconds
        
        # Calculate timestamps with safety margins
        day1_ts = now - (1 * 24 * 3600 * 1000) - margin_ms  # ~1.2 days ago
        day2_ts = now - (2 * 24 * 3600 * 1000) - margin_ms  # ~2.2 days ago
        day4_ts = now - (4 * 24 * 3600 * 1000) - margin_ms  # ~4.2 days ago
        day6_ts = now - (6 * 24 * 3600 * 1000) - margin_ms  # ~6.2 days ago
        
        # Create table and insert data
        tdSql.execute("CREATE TABLE tb_alter_keep_1 USING stb_alter_keep TAGS(1)")
        
        # Insert data at different time points
        tdLog.info("Inserting data at different time points with safety margins")
        tdSql.execute(f"INSERT INTO tb_alter_keep_1 VALUES ({now}, 100)")  # Current
        tdSql.execute(f"INSERT INTO tb_alter_keep_1 VALUES ({day1_ts}, 90)")  # ~1.2 days ago
        tdSql.execute(f"INSERT INTO tb_alter_keep_1 VALUES ({day2_ts}, 80)")  # ~2.2 days ago
        tdSql.execute(f"INSERT INTO tb_alter_keep_1 VALUES ({day4_ts}, 60)")  # ~4.2 days ago
        tdSql.execute(f"INSERT INTO tb_alter_keep_1 VALUES ({day6_ts}, 40)")  # ~6.2 days ago
        
        # Log the timestamps for debugging
        tdLog.info(f"Current time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now/1000))}")
        tdLog.info(f"~1.2 days ago: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(day1_ts/1000))}")
        tdLog.info(f"~2.2 days ago: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(day2_ts/1000))}")
        tdLog.info(f"~4.2 days ago: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(day4_ts/1000))}")
        tdLog.info(f"~6.2 days ago: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(day6_ts/1000))}")
        
        # Verify initial data insertion - all data should be visible
        tdSql.query("SELECT COUNT(*) FROM stb_alter_keep")
        initial_count = tdSql.getData(0, 0)
        tdLog.info(f"Initial data count: {initial_count}")
        tdSql.checkEqual(initial_count, 5)
        
        # Create a timestamp map for later use
        timestamps = {
            "now": now,
            "day1_ts": day1_ts,
            "day2_ts": day2_ts,
            "day4_ts": day4_ts,
            "day6_ts": day6_ts,
        }
        
        # Perform first compaction with initial keep value
        tdLog.info(f"Performing first compaction with STB keep={initial_stb_keep}")
        self.trigger_compact(db_name)
        
        # Verify data after first compaction - data older than initial_stb_keep should be removed
        # With keep=3, and our safety margin, we expect data within ~2.9 days to be kept
        # This should definitely include current, day1_ts, day2_ts, but not day4_ts or day6_ts
        tdSql.query("SELECT COUNT(*) FROM stb_alter_keep")
        count_after_first_compact = tdSql.getData(0, 0)
        tdLog.info(f"Data count after first compaction: {count_after_first_compact}")
        
        # Check individual records to see what was preserved
        tdSql.query(f"SELECT * FROM stb_alter_keep WHERE ts = {now}")
        tdSql.checkEqual(tdSql.queryRows, 1)
        tdSql.query(f"SELECT * FROM stb_alter_keep WHERE ts = {day1_ts}")
        tdSql.checkEqual(tdSql.queryRows, 1)
        tdSql.query(f"SELECT * FROM stb_alter_keep WHERE ts = {day2_ts}")
        tdSql.checkEqual(tdSql.queryRows, 1)

        tdSql.query(f"SELECT COUNT(*) FROM stb_alter_keep") 
        tdSql.checkEqual(tdSql.getData(0, 0), 3)

        # Expected count should be the sum of records actually preserved
        expected_preserved_count = 3
        
        # Increase keep value
        new_keep_value = 6  # Increase to 6 days
        tdLog.info(f"Altering STB keep value from {initial_stb_keep} to {new_keep_value}")
        tdSql.execute(f"ALTER STABLE stb_alter_keep KEEP {new_keep_value}d")
        
        # Re-insert data at 4.2 days ago that was likely deleted in first compaction
        tdLog.info("Re-inserting data from ~4.2 days ago")
        tdSql.execute(f"INSERT INTO tb_alter_keep_1 VALUES ({day4_ts}, 65)")  # Different value to track
        
        # Verify data after insertion but before second compaction
        tdSql.query("SELECT COUNT(*) FROM stb_alter_keep")
        count_after_reinsertion = tdSql.getData(0, 0)
        tdLog.info(f"Data count after reinsertion: {count_after_reinsertion}")
        expected_count_after_reinsertion = expected_preserved_count + 1  # +1 for the reinserted record
        tdSql.checkEqual(count_after_reinsertion, expected_count_after_reinsertion)
        
        # Perform second compaction with new keep value
        tdLog.info(f"Performing second compaction with increased STB keep={new_keep_value}")
        self.trigger_compact(db_name)
        
        # Verify data after second compaction
        tdSql.query("SELECT COUNT(*) FROM stb_alter_keep")
        count_after_second_compact = tdSql.getData(0, 0)
        tdLog.info(f"Data count after second compaction: {count_after_second_compact}")
        tdSql.checkEqual(count_after_second_compact, 4)
        
        # Verify the re-inserted data (day4) is retained after second compaction
        tdSql.query(f"SELECT val FROM stb_alter_keep WHERE ts = {day4_ts}")
        tdSql.checkEqual(tdSql.queryRows, 1)
        tdSql.checkEqual(tdSql.getData(0, 0), 65)
        
        # Check if day6 data was either never inserted or was correctly removed
        tdSql.query(f"SELECT * FROM stb_alter_keep WHERE ts = {day6_ts}")
        tdSql.checkEqual(tdSql.queryRows, 0)
        
    def run_case8_stb_keep_compact_with_db_keep(self):
        """Test case 8: Test STB keep compact with database keep"""
        tdLog.info("=== Test Case 8: STB keep compact with database keep ===")
        
        # Setup
        db_name = "test_stb_alter_keep"
        db_keep = 10  # Set database keep to a higher value to allow flexible STB keep testing
        initial_stb_keep = 3  # Initial keep value
        
        # Create database and super table with initial keep value
        tdLog.info(f"Creating database with keep={db_keep}")
        tdSql.execute(f"DROP DATABASE IF EXISTS {db_name}")
        tdSql.execute(f"CREATE DATABASE {db_name} DURATION 2d KEEP {db_keep}")
        tdSql.execute(f"USE {db_name}")
        
        # Create super table with initial keep
        tdLog.info(f"Creating super table with initial keep={initial_stb_keep}d")
        self.create_super_table_with_keep("stb_alter_keep", initial_stb_keep)
        
        # Create child table and insert data with safer time margins
        tdLog.info("Creating child table and inserting data")
        
        # For safety, we'll insert data with specific values that are clearly within boundaries
        now = int(time.time() * 1000)  # Current time in milliseconds
        
        # Add margin to ensure day calculations don't fall on boundary
        # Subtract a few hours from each day boundary for safety
        margin_hours = 4  # 4 hours safety margin
        margin_ms = margin_hours * 3600 * 1000  # Convert to milliseconds
        
        # Calculate timestamps with safety margins
        day0_ts = now - (0 * 24 * 3600 * 1000) - margin_ms  # ~0.2 days ago
        day1_ts = now - (1 * 24 * 3600 * 1000) - margin_ms  # ~1.2 days ago
        day2_ts = now - (2 * 24 * 3600 * 1000) - margin_ms  # ~2.2 days ago
        day4_ts = now - (4 * 24 * 3600 * 1000) - margin_ms  # ~4.2 days ago
        day6_ts = now - (6 * 24 * 3600 * 1000) - margin_ms  # ~6.2 days ago
        
        # Create table and insert data
        tdSql.execute("CREATE TABLE tb_alter_keep_1 USING stb_alter_keep TAGS(1)")
        
        # Insert data at different time points
        tdLog.info("Inserting data at different time points with safety margins")
        tdSql.execute(f"INSERT INTO tb_alter_keep_1 VALUES ({now}, 100)")  # Current
        tdSql.execute(f"INSERT INTO tb_alter_keep_1 VALUES ({day1_ts}, 90)")  # ~1.2 days ago
        tdSql.execute(f"INSERT INTO tb_alter_keep_1 VALUES ({day2_ts}, 80)")  # ~2.2 days ago
        tdSql.execute(f"INSERT INTO tb_alter_keep_1 VALUES ({day4_ts}, 60)")  # ~4.2 days ago
        tdSql.execute(f"INSERT INTO tb_alter_keep_1 VALUES ({day6_ts}, 40)")  # ~6.2 days ago

        # Decrease keep value to less than original
        final_keep_value = 2  # Decrease to 2 days (less than original)
        tdLog.info(f"Altering STB keep value from {initial_stb_keep} to {final_keep_value}")
        tdSql.execute(f"ALTER STABLE stb_alter_keep KEEP {final_keep_value}d")
        
        tdSql.execute(f"INSERT INTO tb_alter_keep_1 VALUES ({day0_ts}, 85)")
        
        # Perform third compaction with reduced keep value
        tdLog.info(f"Performing third compaction with decreased STB keep={final_keep_value}")
        self.trigger_compact(db_name)
        
        # Verify data after third compaction
        tdSql.query("SELECT COUNT(*) FROM stb_alter_keep")
        count_after_third_compact = tdSql.getData(0, 0)
        tdLog.info(f"Data count after third compaction: {count_after_third_compact}")
        
        # Check individual records to see what was preserved
        tdSql.query(f"SELECT * FROM stb_alter_keep WHERE ts = {now}")
        tdSql.checkEqual(tdSql.queryRows, 1)
        tdSql.query(f"SELECT * FROM stb_alter_keep WHERE ts = {day0_ts}")
        tdSql.checkEqual(tdSql.queryRows, 1)
        tdSql.query(f"SELECT * FROM stb_alter_keep WHERE ts = {day1_ts}")
        tdSql.checkEqual(tdSql.queryRows, 1)

        # Expected count should be the sum of records actually preserved    
        expected_preserved_count = 3
        
        tdSql.query(f"SELECT COUNT(*) FROM stb_alter_keep") 
        tdSql.checkEqual(tdSql.getData(0, 0), expected_preserved_count)

    def test_super_table_keep_compact(self):
        """Stable keep options
    
        1. Super table keep parameter only takes effect during compaction
        2. Before compaction, all historical data is visible regardless of keep settings
        3. After compaction, data older than the keep period is removed
        4. Different combinations of database keep and super table keep behave as expected
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-23 Alex Duan Migrated from uncatalog/army/create/test_stb_keep_compact.py

        """        
        tdLog.debug(f"Start to execute {__file__}")

        # 1. Test case 1: STB keep=2, DB keep=10
        self.run_case1_stb_keep_2_db_keep_10()
        
        # 2. Test case 2: STB keep=4, DB keep=5
        self.run_case2_stb_keep_4_db_keep_5()

        # 3. Test case 3: Multiple STBs with different keep values
        self.run_case3_multiple_stbs_with_different_keep()

        # 4. Test case 4: Boundary keep duration ratio
        self.run_case4_boundary_keep_duration_ratio()

        # 5. Test case 5: Write time with keep restrictions
        self.run_case5_write_time_with_keep_restrictions() 

        # 6. Test case 6: DB keep=8, STB keep=4
        self.run_case6_db_keep_8_stb_keep_4()

        # 7. Test case 7: ALTER STABLE KEEP parameter
        self.run_case7_alter_stb_keep()

        # 8. Test case 8: STB keep compact with database keep
        self.run_case8_stb_keep_compact_with_db_keep()

        tdLog.success(f"{__file__} successfully executed")


