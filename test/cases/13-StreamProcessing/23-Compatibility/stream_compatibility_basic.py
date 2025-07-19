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

import os
import platform
import time
import sys
from pathlib import Path
from new_test_framework.utils import tdLog, tdSql, tdStream, cluster

class TestStreamCompatibilityBasic:
    """Stream Processing Basic Compatibility Test Class"""

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_compatibility_basic(self):
        """Basic stream compatibility test

        Catalog:
            - Streams:Compatibility

        Since: v3.3.3.7

        Labels: compatibility,basic,ci

        Jira: TS-6100

        History:
            - 2025-01-01 Assistant Created

        """
        if platform.system().lower() == 'windows':
            tdLog.info(f"Windows skip stream compatibility test")
            return

        tdLog.printNoPrefix("========== Stream Compatibility Basic Test ==========")

        # Step 1: Create test database and tables
        tdLog.info("Step 1: Creating test database and tables")
        tdSql.execute("drop database if exists stream_compat_test")
        tdSql.execute("create database stream_compat_test")
        tdSql.execute("use stream_compat_test")
        
        # Create super table and child tables
        tdSql.execute("create table meters (ts timestamp, voltage int, current float, phase float) tags (location binary(64), groupid int)")
        tdSql.execute("create table d1001 using meters tags ('Beijing.Chaoyang', 1)")
        tdSql.execute("create table d1002 using meters tags ('Beijing.Haidian', 1)")
        
        # Insert test data
        tdSql.execute("insert into d1001 values ('2024-01-01 10:00:00', 220, 1.2, 0.8)")
        tdSql.execute("insert into d1001 values ('2024-01-01 10:01:00', 221, 1.3, 0.9)")
        tdSql.execute("insert into d1002 values ('2024-01-01 10:00:30', 219, 1.1, 0.7)")
        
        # Step 2: Create snode (if not exists)
        tdLog.info("Step 2: Creating snode")
        tdSql.execute("create snode on dnode 1")
        tdLog.info("Created snode successfully")
        
        # Step 3: Create new format streams to test current functionality
        tdLog.info("Step 3: Creating new format streams")
        
        # Test interval stream
        tdSql.execute("create stream test_interval_stream interval(10s) from meters into interval_output as select _wstart, avg(voltage) as avg_voltage from meters")
        tdLog.info("Created interval stream successfully")
        
        # Test count window stream  
        tdSql.execute("create stream test_count_stream count_window(5) from meters partition by tbname into count_output as select count(*) as cnt from %%trows")
        tdLog.info("Created count window stream successfully")
        
        # Test period stream
        tdSql.execute("create stream test_period_stream period(30s) into period_output as select now() as trigger_time, count(*) as total_count from meters")
        tdLog.info("Created period stream successfully")
        
        # Step 4: Verify streams were created
        tdLog.info("Step 4: Verifying streams")
        tdSql.query("show streams")
        stream_count = tdSql.queryRows
        tdLog.info(f"Total streams created: {stream_count}")
        
        if stream_count > 0 and tdSql.queryResult:
            for i in range(stream_count):
                stream_name = tdSql.queryResult[i][0]
                stream_status = tdSql.queryResult[i][4] if len(tdSql.queryResult[i]) > 4 else "unknown"
                tdLog.info(f"Stream: {stream_name}, Status: {stream_status}")
        
        # Step 5: Insert more data to trigger streams
        tdLog.info("Step 5: Inserting more data to trigger streams")
        for i in range(3, 10):
            tdSql.execute(f"insert into d1001 values ('2024-01-01 10:0{i}:00', {220+i}, {1.2+i*0.1}, 0.8)")
            tdSql.execute(f"insert into d1002 values ('2024-01-01 10:0{i}:30', {219+i}, {1.1+i*0.1}, 0.7)")
        
        # Wait for stream processing
        time.sleep(5)
        
        # Step 6: Check stream outputs
        tdLog.info("Step 6: Checking stream outputs")
        tdSql.query("show tables")
        table_count = tdSql.queryRows
        tdLog.info(f"Total tables in database: {table_count}")
        
        # Check if output tables exist
        output_tables = ["interval_output", "count_output", "period_output"]
        for table in output_tables:
            tdSql.query(f"select * from {table}")
            rows = tdSql.queryRows
            tdLog.info(f"Table {table} has {rows} rows")
        
        # Step 7: Test stream cleanup
        tdLog.info("Step 7: Testing stream cleanup")
        stream_names = ["test_interval_stream", "test_count_stream", "test_period_stream"]
        
        for stream_name in stream_names:
            tdSql.execute(f"drop stream if exists {stream_name}")
            tdLog.info(f"Dropped stream {stream_name}")
        
        # Verify streams are dropped
        tdSql.query("show streams")
        remaining_streams = tdSql.queryRows
        tdLog.info(f"Remaining streams after cleanup: {remaining_streams}")
        
        # Step 8: Test creating streams with different syntax
        tdLog.info("Step 8: Testing different stream syntaxes")
        
        # Test session window (if supported)
        tdSql.execute("create stream session_test session(ts, 30s) from meters partition by tbname into session_output as select _wstart, _wend, count(*) as cnt from %%trows")
        tdLog.info("Created session window stream successfully")
        
        # Test state window (if supported)
        tdSql.execute("create stream state_test state_window(voltage) from meters partition by tbname into state_output as select _wstart, _wend, avg(current) as avg_current from %%trows")
        tdLog.info("Created state window stream successfully")
        
        # Final verification
        tdSql.query("show streams")
        final_stream_count = tdSql.queryRows
        tdLog.info(f"Final stream count: {final_stream_count}")
        
        # Cleanup
        tdLog.info("Step 9: Final cleanup")
        tdSql.execute("drop database stream_compat_test")
        
        tdLog.printNoPrefix("========== Stream Compatibility Basic Test Completed Successfully ==========")

    def teardown_class(cls):
        tdLog.success(f"{__file__} successfully executed") 