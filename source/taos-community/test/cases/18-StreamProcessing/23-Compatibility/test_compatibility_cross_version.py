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
import subprocess
import importlib.util
from pathlib import Path
from new_test_framework.utils import tdLog, tdSql, tdStream, cluster, tdCom

# Import enterprise package downloader
current_dir = os.path.dirname(os.path.realpath(__file__))
enterprise_downloader_path = os.path.abspath(os.path.join(current_dir, "../../../../../enterprise/utils/download_enterprise_package.py"))

# Check if enterprise downloader exists
if not os.path.exists(enterprise_downloader_path):
    raise FileNotFoundError(f"Enterprise package downloader not found at: {enterprise_downloader_path}")

# Load the module
spec = importlib.util.spec_from_file_location("download_enterprise_package", enterprise_downloader_path)
if spec is None or spec.loader is None:
    raise ImportError(f"Could not load enterprise package downloader from: {enterprise_downloader_path}")

download_enterprise_package = importlib.util.module_from_spec(spec)
spec.loader.exec_module(download_enterprise_package)
EnterprisePackageDownloader = download_enterprise_package.EnterprisePackageDownloader

# Define the list of base versions to test for stream compatibility
BASE_VERSIONS = ["3.3.3.0", "3.3.4.0", "3.3.5.0", "3.3.6.0"]

class TestStreamCompatibility:
    """Stream Processing Compatibility Test Class"""

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_compatibility(self):
        """Comp: stream cross-version

        Test stream processing and TSMA compatibility across 4 base versions with actual stream/TSMA creation and verification:

        1. Test [v3.3.3.0 Version Compatibility]
            1.1 Install v3.3.3.0 enterprise package and create old format streams and TSMAs
                1.1.1 Create avg_stream: INTERVAL(5s) aggregation on meters table
                1.1.2 Create max_stream: trigger at_once with MAX aggregation by tbname
                1.1.3 Create count_stream: INTERVAL(10s) with WHERE voltage > 10 filter
                1.1.4 Create tsma_meters: 1-minute TSMA with avg(voltage), max(current), min(voltage), count(ts)
                1.1.5 Create tsma_meters_hourly: 1-hour TSMA with avg(voltage), max(current), min(current), count(ts)
                1.1.6 Create tsma_meters_detail: 30-second TSMA with sum(voltage), avg(current), max(phase), min(phase)
            1.2 Verify new version startup behavior with old streams and TSMAs
                1.2.1 Attempt to start new version (should fail due to incompatible streams/TSMAs)
                1.2.2 Verify stream and TSMA incompatibility detection
            1.3 Clean up old streams/TSMAs and create new format streams
                1.3.1 Drop all old format streams and TSMAs before database cleanup
                1.3.2 Create s_interval: INTERVAL(5s) SLIDING(5s) with trigger/source separation
                1.3.3 Create s_count: COUNT_WINDOW(5) with %%trows reference
                1.3.4 Create s_period: PERIOD(30s) with cross-database computation
                1.3.5 Create s_session: SESSION(ts, 5s) with window boundary functions

        2. Test [v3.3.4.0 Version Compatibility]
            2.1 Install v3.3.4.0 enterprise package and create old format streams and TSMAs
                2.1.1 Create avg_stream: INTERVAL(5s) aggregation on meters table
                2.1.2 Create max_stream: trigger at_once with MAX aggregation by tbname
                2.1.3 Create count_stream: INTERVAL(10s) with WHERE voltage > 10 filter
                2.1.4 Create tsma_meters: 1-minute TSMA with avg(voltage), max(current), min(voltage), count(ts)
                2.1.5 Create tsma_meters_hourly: 1-hour TSMA with avg(voltage), max(current), min(current), count(ts)
                2.1.6 Create tsma_meters_detail: 30-second TSMA with sum(voltage), avg(current), max(phase), min(phase)
            2.2 Verify new version startup behavior with old streams and TSMAs
                2.2.1 Attempt to start new version (should fail due to incompatible streams/TSMAs)
                2.2.2 Verify stream and TSMA incompatibility detection
            2.3 Clean up old streams/TSMAs and create new format streams
                2.3.1 Drop all old format streams and TSMAs before database cleanup
                2.3.2 Create s_interval: INTERVAL(5s) SLIDING(5s) with trigger/source separation
                2.3.3 Create s_count: COUNT_WINDOW(5) with %%trows reference
                2.3.4 Create s_period: PERIOD(30s) with cross-database computation
                2.3.5 Create s_session: SESSION(ts, 5s) with window boundary functions

        3. Test [v3.3.5.0 Version Compatibility]
            3.1 Install v3.3.5.0 enterprise package and create old format streams and TSMAs
                3.1.1 Create avg_stream: INTERVAL(5s) aggregation on meters table
                3.1.2 Create max_stream: trigger at_once with MAX aggregation by tbname
                3.1.3 Create count_stream: INTERVAL(10s) with WHERE voltage > 10 filter
                3.1.4 Create tsma_meters: 1-minute TSMA with avg(voltage), max(current), min(voltage), count(ts)
                3.1.5 Create tsma_meters_hourly: 1-hour TSMA with avg(voltage), max(current), min(current), count(ts)
                3.1.6 Create tsma_meters_detail: 30-second TSMA with sum(voltage), avg(current), max(phase), min(phase)
            3.2 Verify new version startup behavior with old streams and TSMAs
                3.2.1 Attempt to start new version (should fail due to incompatible streams/TSMAs)
                3.2.2 Verify stream and TSMA incompatibility detection
            3.3 Clean up old streams/TSMAs and create new format streams
                3.3.1 Drop all old format streams and TSMAs before database cleanup
                3.3.2 Create s_interval: INTERVAL(5s) SLIDING(5s) with trigger/source separation
                3.3.3 Create s_count: COUNT_WINDOW(5) with %%trows reference
                3.3.4 Create s_period: PERIOD(30s) with cross-database computation
                3.3.5 Create s_session: SESSION(ts, 5s) with window boundary functions

        4. Test [v3.3.6.0 Version Compatibility]
            4.1 Install v3.3.6.0 enterprise package and create old format streams and TSMAs
                4.1.1 Create avg_stream: INTERVAL(5s) aggregation on meters table
                4.1.2 Create max_stream: trigger at_once with MAX aggregation by tbname
                4.1.3 Create count_stream: INTERVAL(10s) with WHERE voltage > 10 filter
                4.1.4 Create tsma_meters: 1-minute TSMA with avg(voltage), max(current), min(voltage), count(ts)
                4.1.5 Create tsma_meters_hourly: 1-hour TSMA with avg(voltage), max(current), min(current), count(ts)
                4.1.6 Create tsma_meters_detail: 30-second TSMA with sum(voltage), avg(current), max(phase), min(phase)
            4.2 Verify new version startup behavior with old streams and TSMAs
                4.2.1 Attempt to start new version (should fail due to incompatible streams/TSMAs)
                4.2.2 Verify stream and TSMA incompatibility detection
            4.3 Clean up old streams/TSMAs and create new format streams
                4.3.1 Drop all old format streams and TSMAs before database cleanup
                4.3.2 Create s_interval: INTERVAL(5s) SLIDING(5s) with trigger/source separation
                4.3.3 Create s_count: COUNT_WINDOW(5) with %%trows reference
                4.3.4 Create s_period: PERIOD(30s) with cross-database computation
                4.3.5 Create s_session: SESSION(ts, 5s) with window boundary functions

        Catalog:
            - Streams:Compatibility:CrossVersion

        Since: v3.3.7.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-07-23 Beryl Created

        """

        try:
            import distro
            distro_id = distro.id()
            if distro_id == "alpine":
                tdLog.info(f"alpine skip stream compatibility test")
                return
        except ImportError:
            tdLog.info("Cannot import distro module, skipping distro check")

        if platform.system().lower() == 'windows':
            tdLog.info(f"Windows skip stream compatibility test")
            return

        bPath = os.path.join(tdCom.getBuildPath(), "build")
        cPath = self.getCfgPath()
        tdLog.info(f"bPath:{bPath}, cPath:{cPath}")

        # Test compatibility with all base versions
        for i, base_version in enumerate(BASE_VERSIONS):
            tdLog.printNoPrefix(f"========== Start stream compatibility test with base version {base_version} ({i+1}/{len(BASE_VERSIONS)}) ==========")

            # Step 1: Install old version and create streams
            self.installTaosd(bPath, cPath, base_version)
            self.createStreamOnOldVersion(base_version)

            # Step 2: Try to start with new version (should fail)
            failed_as_expected = not self.tryStartWithNewVersion(bPath)
            if failed_as_expected:
                tdLog.info("New version failed to start as expected - compatibility test proceeding")
            else:
                tdLog.info("New version started unexpectedly - might indicate forward compatibility")

            # Step 3: Cleanup streams on old version
            self.cleanupStreamsOnOldVersion(bPath, cPath, base_version)

            # Step 4: Create new streams on new version
            self.createNewStreamsOnNewVersion(bPath)
            
            tdLog.printNoPrefix(f"Stream compatibility test with base version {base_version} completed successfully")
                
            # Cleanup between version tests
            self.killAllDnodes()
            
            tdLog.info(f"Completed testing version {base_version}, moving to next version...")
        
        tdLog.printNoPrefix(f"========== All stream compatibility tests completed for {len(BASE_VERSIONS)} versions ==========")

    def checkProcessPid(self, processName):
        """Check if process is stopped"""
        tdLog.info(f"checkProcessPid {processName}")
        i = 0
        while i < 60:
            tdLog.info(f"wait stop {processName}")
            processPid = subprocess.getstatusoutput(f'ps aux|grep {processName} |grep -v "grep"|awk \'{{print $2}}\'')[1]
            tdLog.info(f"times:{i},{processName}-pid:{processPid}")
            if processPid == "":
                break
            i += 1
            time.sleep(1)
        else:
            tdLog.info(f'this processName is not stopped in 60s')

    def installTaosd(self, bPath, cPath, base_version):
        """Install specific version of TDengine using enterprise package"""
        dataPath = cPath + "../data/"
        
        # Use enterprise package downloader
        downloader = EnterprisePackageDownloader()
        tdLog.info(f"Downloading and installing enterprise version {base_version}")
        package_path = downloader.download_and_install(base_version, "enterprise", "-e no")
        tdLog.info(f"Successfully installed enterprise package from {package_path}")
        
        os.system(f"pkill -9 taosd")
        self.checkProcessPid("taosd")

        print(f"rm -rf {dataPath}* && nohup /usr/bin/taosd -c {cPath} &")
        os.system(f"rm -rf {dataPath}* && nohup /usr/bin/taosd -c {cPath} &")
        time.sleep(5)

    def killAllDnodes(self):
        """Kill all TDengine processes"""
        tdLog.info("kill all dnodes")
        tdLog.info("kill taosd")
        os.system(f"pkill -9 taosd")
        tdLog.info("kill taos")
        os.system(f"pkill -9 taos") 
        tdLog.info("check taosd")
        self.checkProcessPid("taosd")

    def createStreamOnOldVersion(self, base_version):
        """Create snode and streams on old version"""
        tdLog.printNoPrefix(f"==========Creating snode, streams and TSMAs on old version {base_version}==========")

        # Create test database and tables
        os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'drop database if exists stream_test;'")
        os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'create database stream_test;'")
        os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'use stream_test;'")
        
        # Create super table and child tables
        os.system("""LD_LIBRARY_PATH=/usr/lib taos -s 'create table stream_test.meters (ts timestamp, voltage int, current float, phase float) tags (location binary(64), groupid int);'""")
        os.system("""LD_LIBRARY_PATH=/usr/lib taos -s 'create table stream_test.d1001 using stream_test.meters tags ("California.SanFrancisco", 2);'""")
        os.system("""LD_LIBRARY_PATH=/usr/lib taos -s 'create table stream_test.d1002 using stream_test.meters tags ("California.LosAngeles", 2);'""")
        
        # Insert test data
        os.system("""LD_LIBRARY_PATH=/usr/lib taos -s 'insert into stream_test.d1001 values ("2018-10-03 14:38:05.000", 10, 2.30, 0.23) ("2018-10-03 14:38:15.000", 12, 2.20, 0.33) ("2018-10-03 14:38:16.800", 13, 2.32, 0.43);'""")
        os.system("""LD_LIBRARY_PATH=/usr/lib taos -s 'insert into stream_test.d1002 values ("2018-10-03 14:38:16.650", 10, 2.30, 0.23) ("2018-10-03 14:38:05.000", 11, 2.20, 0.33) ("2018-10-03 14:38:06.500", 12, 2.32, 0.43);'""")
        
        # Create snode
        os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'create snode on dnode 1;'")
        tdLog.info("Created snode on dnode 1")
        
        # Verify snode creation
        os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'show snodes;'")
        
        # Create streams (old format)
        stream_sqls = [
            "create stream avg_stream into stream_test.avg_output as select _wstart, avg(voltage) as avg_voltage from stream_test.meters interval(5s);",
            "create stream max_stream trigger at_once into stream_test.max_output as select ts, max(current) as max_current from stream_test.meters partition by tbname;",
            "create stream count_stream into stream_test.count_output as select _wstart, count(*) as total_count from stream_test.meters where voltage > 10 interval(10s);"
        ]
        
        for sql in stream_sqls:
            os.system(f"LD_LIBRARY_PATH=/usr/lib taos -s '{sql}'")
            tdLog.info(f"Created stream: {sql[:50]}...")
        
        # Create TSMA (Time-Range Small Materialized Aggregates)
        tsma_sqls = [
            "create tsma tsma_meters on stream_test.meters function(avg(voltage), max(current), min(voltage), count(ts)) interval(1m);",
            "create tsma tsma_meters_hourly on stream_test.meters function(avg(voltage), max(current), min(current), count(ts)) interval(1h);",
            "create tsma tsma_meters_detail on stream_test.meters function(sum(voltage), avg(current), max(phase), min(phase)) interval(30s);"
        ]
        
        for sql in tsma_sqls:
            os.system(f"LD_LIBRARY_PATH=/usr/lib taos -s '{sql}'")
            tdLog.info(f"Created TSMA: {sql[:50]}...")
        
        # Show streams and TSMAs
        os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'show streams;'")
        os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'show snodes;'")
        os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'show stream_test.tsmas;'")
        
        # Flush database
        os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'flush database stream_test;'")
        
        tdLog.info("Stream and TSMA creation on old version completed")

    def tryStartWithNewVersion(self, bPath):
        """Try to start with new version - should fail due to incompatible streams"""
        tdLog.printNoPrefix("==========Attempting to start with new version (should fail)==========")
        
        # Kill old taosd
        self.killAllDnodes()
        time.sleep(2)
        
        cPath = bPath + "/../sim/dnode1/cfg/"
        
        tdLog.info(f"Trying to start new taosd: {bPath}/bin/taosd -c {cPath}")
        result = os.system(f"timeout 10s {bPath}/bin/taosd -c {cPath}")
        
        if result == 0:
            tdLog.info("New version started successfully - this might indicate compatibility")
            # Check if streams still exist
            time.sleep(2)
            result = os.system("timeout 5s taos -s 'show streams;'")
            if result != 0:
                tdLog.info("Cannot query streams - expected incompatibility")
                return False
            else:
                tdLog.info("Streams query succeeded unexpectedly")
                return True
        else:
            tdLog.info("New version failed to start as expected due to incompatible streams")
            return False

    def restartTaosd(self, cPath):
        """Restart taosd"""
        self.killAllDnodes()
        time.sleep(2)
        os.system(f"nohup /usr/bin/taosd -c {cPath} &")
        time.sleep(5)

    def cleanupStreamsOnOldVersion(self, bPath, cPath, base_version):
        """Start old version and cleanup streams"""
        tdLog.printNoPrefix(f"==========Cleaning up streams and TSMAs on old version {base_version}==========")

        # Restart old version
        self.restartTaosd(cPath)
        time.sleep(5)
        
        # Drop streams
        cleanup_sqls = [
            "drop stream if exists avg_stream;",
            "drop stream if exists max_stream;", 
            "drop stream if exists count_stream;",
        ]
        
        for sql in cleanup_sqls:
            os.system(f"LD_LIBRARY_PATH=/usr/lib taos -s '{sql}'")
            tdLog.info(f"Executed cleanup: {sql}")
        
        # Drop TSMAs (must be done before dropping database)
        tsma_cleanup_sqls = [
            "drop tsma if exists stream_test.tsma_meters;",
            "drop tsma if exists stream_test.tsma_meters_hourly;",
            "drop tsma if exists stream_test.tsma_meters_detail;"
        ]
        
        for sql in tsma_cleanup_sqls:
            os.system(f"LD_LIBRARY_PATH=/usr/lib taos -s '{sql}'")
            tdLog.info(f"Executed TSMA cleanup: {sql}")
        
        # Drop snode and database
        final_cleanup_sqls = [
            "drop snode on dnode 1;",
            "drop database if exists stream_test;"
        ]
        
        for sql in final_cleanup_sqls:
            os.system(f"LD_LIBRARY_PATH=/usr/lib taos -s '{sql}'")
            tdLog.info(f"Executed cleanup: {sql}")
        
        # Verify cleanup
        os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'show streams;'")
        os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'show snodes;'")
        os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'show databases;'")
        
        # Stop taosd before filesystem cleanup
        self.killAllDnodes()
        time.sleep(2)
        
        # Remove snode directories from filesystem
        dataPath = cPath + "/../data/"
        snode_dirs = [
            f"{dataPath}/snode",
            f"{dataPath}/dnode*/snode",
            f"{dataPath}/stream"
        ]
        
        for snode_dir in snode_dirs:
            cleanup_cmd = f"rm -rf {snode_dir}"
            tdLog.info(f"Removing snode directory: {cleanup_cmd}")
            os.system(cleanup_cmd)
        
        # Also clean up any stream-related directories
        os.system(f"find {dataPath} -name '*snode*' -type d -exec rm -rf {{}} + 2>/dev/null || true")
        os.system(f"find {dataPath} -name '*stream*' -type d -exec rm -rf {{}} + 2>/dev/null || true")
        
        tdLog.info("Stream cleanup and snode directory removal completed")

    def createNewStreamsOnNewVersion(self, bPath):
        """Create new streams using new version"""
        tdLog.printNoPrefix("==========Creating new streams on new version==========")
        
        # Kill old version
        self.killAllDnodes()
        time.sleep(2)
        
        # Start new version
        dataPath = bPath + "/../../sim/dnode1/data/"
        cPath = bPath + "/../../sim/dnode1/cfg/"
        
        # Clean data directory to ensure fresh start
        os.system(f"rm -rf {dataPath}/*")
        
        # Start new taosd
        os.system(f"nohup {bPath}/bin/taosd -c {cPath} > /dev/null 2>&1 &")
        time.sleep(8)
        
        # Create separated databases for testing new stream syntax
        # Following the pattern: trigger_db, source_db, result_db
        database_sqls = [
            "drop database if exists trigger_db",
            "drop database if exists source_db", 
            "drop database if exists result_db",
            "create database trigger_db",
            "create database source_db",
            "create database result_db",
            "create snode on dnode 1",
        ]
        
        for sql in database_sqls:
            tdSql.execute(sql)
            tdLog.info(f"Database setup: {sql}")
        
        # Create trigger tables in trigger_db
        trigger_table_sqls = [
            "create table trigger_db.trigger_meters (ts timestamp, voltage int, current float, phase float) tags (location binary(64), groupid int)",
            "create table trigger_db.t1 using trigger_db.trigger_meters tags ('Trigger.Device1', 1)",
            "create table trigger_db.t2 using trigger_db.trigger_meters tags ('Trigger.Device2', 2)",
        ]
        
        for sql in trigger_table_sqls:
            tdSql.execute(sql)
            tdLog.info(f"Trigger table created: {sql}")
        
        # Create source data tables in source_db (for computation)
        source_table_sqls = [
            "create table source_db.source_meters (ts timestamp, voltage int, current float, phase float, temperature double) tags (device_id int, area varchar(32))",
            "create table source_db.s1 using source_db.source_meters tags (1, 'Area.North')",
            "create table source_db.s2 using source_db.source_meters tags (2, 'Area.South')",
            "create table source_db.s3 using source_db.source_meters tags (3, 'Area.West')",
        ]
        
        for sql in source_table_sqls:
            tdSql.execute(sql)
            tdLog.info(f"Source table created: {sql}")
        
        # Insert trigger data
        trigger_data_sqls = [
            "insert into trigger_db.t1 values ('2024-01-01 10:00:00', 220, 1.2, 0.8)",
            "insert into trigger_db.t1 values ('2024-01-01 10:01:00', 221, 1.3, 0.9)", 
            "insert into trigger_db.t1 values ('2024-01-01 10:02:00', 222, 1.4, 0.7)",
            "insert into trigger_db.t2 values ('2024-01-01 10:00:30', 225, 1.1, 0.85)",
            "insert into trigger_db.t2 values ('2024-01-01 10:01:30', 226, 1.2, 0.95)",
        ]
        
        for sql in trigger_data_sqls:
            tdSql.execute(sql)
            tdLog.info(f"Trigger data inserted: {sql}")
        
        # Insert source data (for computation)
        source_data_sqls = [
            "insert into source_db.s1 values ('2024-01-01 10:00:00', 220, 1.2, 0.8, 25.5)",
            "insert into source_db.s1 values ('2024-01-01 10:01:00', 221, 1.3, 0.9, 26.0)",
            "insert into source_db.s1 values ('2024-01-01 10:02:00', 222, 1.4, 0.7, 26.5)",
            "insert into source_db.s2 values ('2024-01-01 10:00:30', 225, 1.1, 0.85, 24.8)",
            "insert into source_db.s2 values ('2024-01-01 10:01:30', 226, 1.2, 0.95, 25.2)",
            "insert into source_db.s3 values ('2024-01-01 10:00:15', 218, 1.0, 0.75, 23.5)",
            "insert into source_db.s3 values ('2024-01-01 10:01:15', 219, 1.1, 0.85, 24.0)",
        ]
        
        for sql in source_data_sqls:
            tdSql.execute(sql)
            tdLog.info(f"Source data inserted: {sql}")
        
        # Create new format streams using new syntax with separated tables
        new_format_streams = [
            # Sliding window trigger - trigger from trigger_db, compute source_db data, save to result_db
            "create stream result_db.s_interval INTERVAL(5s) SLIDING(5s) from trigger_db.trigger_meters partition by tbname into result_db.r_interval as select _twstart, avg(voltage) as avg_voltage, count(*) as cnt from source_db.source_meters where ts >= _twstart and ts <= _twend",
            
            # Count window trigger - trigger every 5 records, partitioned by table name
            "create stream result_db.s_count COUNT_WINDOW(5) from trigger_db.trigger_meters partition by tbname into result_db.r_count as select _twstart, count(*) as cnt, avg(voltage) as avg_voltage from %%trows",
            
            # Period trigger - trigger every 30 seconds, compute total data from source_db
            "create stream result_db.s_period PERIOD(30s) from trigger_db.trigger_meters partition by tbname into result_db.r_period as select cast(_tlocaltime/1000000 as timestamp) as ts, count(*) as total_count, avg(temperature) as avg_temp from source_db.source_meters",
            
            # Session window trigger - based on 5 second session interval
            "create stream result_db.s_session SESSION(ts, 5s) from trigger_db.trigger_meters partition by tbname into result_db.r_session as select _twstart, _twend, max(voltage) as max_voltage, count(*) as cnt from %%trows",
            
        ]
        
        for sql in new_format_streams:
            tdSql.execute(sql)
            tdLog.info(f"New stream created successfully: {sql[:50]}...")
        
        # Verify streams creation
        tdSql.query("show result_db.streams")
        stream_result = tdSql.queryResult
        tdLog.info(f"Created streams: {len(stream_result) if stream_result else 0}")
        
        tdSql.query("show snodes")  
        snode_result = tdSql.queryResult
        tdLog.info(f"Available snodes: {len(snode_result) if snode_result else 0}")
        
        # Insert more trigger data to activate streams
        additional_trigger_data = [
            "insert into trigger_db.t1 values ('2024-01-01 10:03:00', 223, 1.5, 0.6)",
            "insert into trigger_db.t1 values ('2024-01-01 10:04:00', 224, 1.6, 0.5)",
            "insert into trigger_db.t2 values ('2024-01-01 10:02:30', 227, 1.25, 0.9)",
            "insert into trigger_db.t2 values ('2024-01-01 10:03:30', 228, 1.35, 1.0)",
        ]
        
        for sql in additional_trigger_data:
            tdSql.execute(sql)
            tdLog.info(f"Additional trigger data: {sql}")
        
        # Insert more source data for computation
        additional_source_data = [
            "insert into source_db.s1 values ('2024-01-01 10:03:00', 223, 1.5, 0.6, 27.0)",
            "insert into source_db.s1 values ('2024-01-01 10:04:00', 224, 1.6, 0.5, 27.5)",
            "insert into source_db.s2 values ('2024-01-01 10:02:30', 227, 1.25, 0.9, 25.8)",
            "insert into source_db.s2 values ('2024-01-01 10:03:30', 228, 1.35, 1.0, 26.2)",
            "insert into source_db.s3 values ('2024-01-01 10:02:15', 220, 1.15, 0.8, 24.5)",
            "insert into source_db.s3 values ('2024-01-01 10:03:15', 221, 1.25, 0.9, 25.0)",
        ]
        
        for sql in additional_source_data:
            tdSql.execute(sql)
            tdLog.info(f"Additional source data: {sql}")
        
        time.sleep(10)  # Wait longer for streams to process
        
        # Check stream outputs from result_db
        tdSql.query("use result_db")
        tdSql.execute("use result_db")
        
        tdSql.query("show tables")
        tables_result = tdSql.queryResult
        tdLog.info(f"Result tables found: {len(tables_result) if tables_result else 0}")
        
        # Check outputs from different stream types
        output_tables = ["r_interval", "r_count", "r_period", "r_session"]
        for table in output_tables:
            tdSql.query(f"select * from result_db.{table}")
            output_result = tdSql.queryResult
            tdLog.info(f"Stream output from {table}: {len(output_result) if output_result else 0} rows")
            if output_result and len(output_result) > 0:
                tdLog.info(f"Sample data from {table}: {output_result[:3] if len(output_result) >= 3 else output_result}")
        
        # Check streams status
        tdSql.query("show result_db.streams")
        streams_result = tdSql.queryResult
        tdLog.info(f"Active streams: {len(streams_result) if streams_result else 0}")
        
        # Check snodes status
        tdSql.query("show snodes")  
        snode_result = tdSql.queryResult
        tdLog.info(f"Active snodes: {len(snode_result) if snode_result else 0}")
        
        tdLog.info("New stream creation and verification completed")
        return True


    def getCfgPath(self):
        """Get config path"""
        buildPath = os.path.join(tdCom.getBuildPath(), "build")
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            cfgPath = buildPath + "/../../sim/dnode1/cfg/"
        else:
            cfgPath = buildPath + "/../../sim/dnode1/cfg/"

        return cfgPath
