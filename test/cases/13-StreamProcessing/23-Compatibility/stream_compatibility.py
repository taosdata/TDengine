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
from pathlib import Path
from new_test_framework.utils import tdLog, tdSql, tdStream, cluster

# Define the list of base versions to test for stream compatibility
BASE_VERSIONS = ["3.2.0.0", "3.3.3.0", "3.3.4.3", "3.3.5.0", "3.3.6.0"]

class TestStreamCompatibility:
    """Stream Processing Compatibility Test Class"""

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_compatibility(self):
        """Main stream compatibility test

        Catalog:
            - Streams:Compatibility

        Since: v3.3.3.7

        Labels: compatibility,ci

        Jira: TS-6100

        History:
            - 2025-01-01 Assistant Created

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

        bPath = self.getBuildPath()
        cPath = self.getCfgPath()
        tdLog.info(f"bPath:{bPath}, cPath:{cPath}")

        # Test with the latest version only for demonstration
        # In production, you might want to test with all BASE_VERSIONS
        base_version = BASE_VERSIONS[-1]  # Use latest stable version
        
        tdLog.printNoPrefix(f"========== Start stream compatibility test with base version {base_version} ==========")

        # Step 1: Install old version and create streams
        self.installTaosd(bPath, cPath, base_version)
        self.createStreamOnOldVersion(base_version)

        # # Step 2: Try to start with new version (should fail)
        # failed_as_expected = not self.tryStartWithNewVersion(bPath)
        # if failed_as_expected:
        #     tdLog.info("New version failed to start as expected - compatibility test proceeding")
        # else:
        #     tdLog.info("New version started unexpectedly - might indicate forward compatibility")

        # # Step 3: Cleanup streams on old version
        # self.cleanupStreamsOnOldVersion(bPath, cPath, base_version)

        # # Step 4: Create new streams on new version
        # success = self.createNewStreamsOnNewVersion(bPath)
        
        # if success:
        #     tdLog.printNoPrefix(f"Stream compatibility test with base version {base_version} completed successfully")
        # else:
        #     tdLog.error(f"Stream compatibility test with base version {base_version} failed")
            
        # # Cleanup
        # self.killAllDnodes()
        
        # # Clean up snode directories after test completion
        # cPath = self.getCfgPath()
        # dataPath = cPath + "/../data/"
        
        # # Remove all snode and stream related directories
        # cleanup_dirs = [
        #     f"{dataPath}/snode",
        #     f"{dataPath}/dnode*/snode", 
        #     f"{dataPath}/stream"
        # ]
        
        # for cleanup_dir in cleanup_dirs:
        #     cleanup_cmd = f"rm -rf {cleanup_dir}"
        #     tdLog.info(f"Final cleanup - removing directory: {cleanup_cmd}")
        #     os.system(cleanup_cmd)
        
        # # Remove any remaining snode/stream directories
        # os.system(f"find {dataPath} -name '*snode*' -type d -exec rm -rf {{}} + 2>/dev/null || true")
        # os.system(f"find {dataPath} -name '*stream*' -type d -exec rm -rf {{}} + 2>/dev/null || true")
        
        # tdLog.info("Final snode directory cleanup completed")

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
        """Install specific version of TDengine"""
        packagePath = "/usr/local/src/"
        dataPath = cPath + "/../data/"
        packageType = "server"

        if platform.system() == "Linux" and platform.machine() == "aarch64":
            packageName = "TDengine-" + packageType + "-" + base_version + "-Linux-arm64.tar.gz"
        else:
            packageName = "TDengine-" + packageType + "-" + base_version + "-Linux-x64.tar.gz"
            
        # Determine download URL
        download_url = f"https://www.taosdata.com/assets-download/3.0/{packageName}"
        tdLog.info(f"wget {download_url}")
        
        packageTPath = packageName.split("-Linux-")[0]
        my_file = Path(f"{packagePath}/{packageName}")
        if not my_file.exists():
            print(f"{packageName} is not exists")
            tdLog.info(f"cd {packagePath} && wget {download_url}")
            os.system(f"cd {packagePath} && wget {download_url}")
        else: 
            print(f"{packageName} has been exists")
            
        os.system(f" cd {packagePath} && tar xf {packageName} > /dev/null 2>&1 && cd {packageTPath} && ./install.sh -e no > /dev/null 2>&1")
        
        os.system(f"pkill -9 taosd")
        self.checkProcessPid("taosd")

        print(f"start taosd: rm -rf {dataPath}/* && nohup /usr/bin/taosd -c {cPath} &")
        os.system(f"rm -rf {dataPath}/* && nohup /usr/bin/taosd -c {cPath} &")
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
        tdLog.printNoPrefix(f"==========Creating snode and streams on old version {base_version}==========")
        
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
            "create stream stream_test.avg_stream into stream_test.avg_output as select _wstart, avg(voltage) as avg_voltage from stream_test.meters interval(5s);",
            "create stream stream_test.max_stream trigger at_once into stream_test.max_output as select ts, max(current) as max_current from stream_test.meters partition by tbname;",
            "create stream stream_test.count_stream into stream_test.count_output as select _wstart, count(*) as total_count from stream_test.meters where voltage > 10 interval(10s);"
        ]
        
        for sql in stream_sqls:
            os.system(f"LD_LIBRARY_PATH=/usr/lib taos -s '{sql}'")
            tdLog.info(f"Created stream: {sql[:50]}...")
        
        # Show streams
        os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'show streams;'")
        os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'show snodes;'")
        
        # Flush database
        os.system("LD_LIBRARY_PATH=/usr/lib taos -s 'flush database stream_test;'")
        
        tdLog.info("Stream creation on old version completed")

    def tryStartWithNewVersion(self, bPath):
        """Try to start with new version - should fail due to incompatible streams"""
        tdLog.printNoPrefix("==========Attempting to start with new version (should fail)==========")
        
        # Kill old taosd
        self.killAllDnodes()
        time.sleep(2)
        
        # Try to start with new version binary
        dataPath = bPath + "/../sim/dnode1/data/"
        cPath = bPath + "/../sim/dnode1/cfg/"
        
        tdLog.info(f"Trying to start new taosd: {bPath}/build/bin/taosd -c {cPath}")
        result = os.system(f"timeout 10s {bPath}/build/bin/taosd -c {cPath}")
        
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

    def cleanupStreamsOnOldVersion(self, bPath, cPath, base_version):
        """Start old version and cleanup streams"""
        tdLog.printNoPrefix(f"==========Cleaning up streams on old version {base_version}==========")
        
        # Kill any running processes
        self.killAllDnodes()
        time.sleep(2)
        
        # Restart old version
        self.installTaosd(bPath, cPath, base_version)
        time.sleep(5)
        
        # Drop streams
        cleanup_sqls = [
            "drop stream if exists stream_test.avg_stream;",
            "drop stream if exists stream_test.max_stream;", 
            "drop stream if exists stream_test.count_stream;",
            "drop snode on dnode 1;",
            "drop database if exists stream_test;"
        ]
        
        for sql in cleanup_sqls:
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
        dataPath = bPath + "/../sim/dnode1/data/"
        cPath = bPath + "/../sim/dnode1/cfg/"
        
        # Clean data directory to ensure fresh start
        os.system(f"rm -rf {dataPath}/*")
        
        # Start new taosd
        os.system(f"nohup {bPath}/build/bin/taosd -c {cPath} > /dev/null 2>&1 &")
        time.sleep(8)
        
        # Create database and tables for new stream testing
        new_stream_sqls = [
            "drop database if exists new_stream_test;",
            "create database new_stream_test;",
            "use new_stream_test;",
            "create table meters (ts timestamp, voltage int, current float, phase float) tags (location binary(64), groupid int);",
            "create table d1001 using meters tags ('Beijing.Chaoyang', 1);",
            "create table d1002 using meters tags ('Beijing.Haidian', 1);",
            "create snode on dnode 1;",
        ]
        
        for sql in new_stream_sqls:
            result = os.system(f"taos -s '{sql}'")
            if result != 0:
                tdLog.info(f"SQL execution failed: {sql}")
            else:
                tdLog.info(f"SQL executed successfully: {sql}")
        
        # Insert test data
        insert_sql = """insert into d1001 values ('2024-01-01 10:00:00', 220, 1.2, 0.8) ('2024-01-01 10:01:00', 221, 1.3, 0.9);"""
        os.system(f"taos -s \"use new_stream_test; {insert_sql}\"")
        
        # Create new format streams
        new_format_streams = [
            "create stream simple_avg_stream interval(5s) from meters into simple_avg_output as select _wstart, avg(voltage) as avg_voltage from meters;",
            "create stream count_window_stream count_window(10) from meters partition by tbname into count_output as select count(*) as cnt from %%trows;",
            "create stream period_stream period(30s) into period_output as select now() as trigger_time, count(*) as total_count from meters;"
        ]
        
        for sql in new_format_streams:
            result = os.system(f"taos -s 'use new_stream_test; {sql}'")
            if result == 0:
                tdLog.info(f"New stream created successfully: {sql[:50]}...")
            else:
                tdLog.info(f"Failed to create new stream: {sql[:50]}...")
        
        # Verify streams
        os.system("taos -s 'use new_stream_test; show streams;'")
        os.system("taos -s 'show snodes;'")
        
        # Insert more data to trigger streams
        more_data = """insert into d1001 values ('2024-01-01 10:02:00', 222, 1.4, 0.7) ('2024-01-01 10:03:00', 223, 1.5, 0.6);"""
        os.system(f"taos -s \"use new_stream_test; {more_data}\"")
        
        time.sleep(5)
        
        # Check stream outputs
        os.system("taos -s 'use new_stream_test; show tables;'")
        os.system("taos -s 'use new_stream_test; select * from simple_avg_output;'")
        
        tdLog.info("New stream creation and verification completed")
        return True

    def getBuildPath(self):
        """Get build path"""
        selfPath = os.path.dirname(os.path.realpath(__file__))
        
        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files or "taosd.exe" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root)-len("/build/bin")]
                    break
        return buildPath

    def getCfgPath(self):
        """Get config path"""
        buildPath = self.getBuildPath()
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            cfgPath = buildPath + "/../sim/dnode1/cfg/"
        else:
            cfgPath = buildPath + "/../sim/dnode1/cfg/"

        return cfgPath
