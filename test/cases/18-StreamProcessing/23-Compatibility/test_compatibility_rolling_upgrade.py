import pytest,os,platform,time

from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdDnodes,
    clusterComCheck,
    tdStream,
    StreamItem,
    tdCb,
    tdCom
)


class TestCompatibilityRollingUpgrade:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_compatibility_rolling_upgrade(self):
        """Comp: rolling upgrade

        Test incremental rolling upgrade of individual nodes with stream processing validation:

        1. Test [Version Detection and Upgrade Mode Selection]
            1.1 Get current server version and calculate last big version
                1.1.1 Query SELECT SERVER_VERSION() to get current version
                1.1.2 Calculate lastBigVersion as major.minor.patch.0 format
                1.1.3 Verify version format and compatibility
            1.2 Setup upgrade environment
                1.2.1 Stop all dnodes with tdDnodes.stopAll()
                1.2.2 Get dnode paths for 3 nodes (dnode1, dnode2, dnode3)
                1.2.3 Verify base version package availability

        2. Test [Base Version Installation and Cluster Setup]
            2.1 Install old version for rolling upgrade
                2.1.1 Install TDengine using tdCb.installTaosdForRollingUpgrade()
                2.1.2 Verify successful installation of base version
                2.1.3 Start old version services
            2.2 Create multi-node cluster
                2.2.1 Create dnode with hostname:6130 port
                2.2.2 Create dnode with hostname:6230 port
                2.2.3 Wait 10 seconds for cluster stabilization
                2.2.4 Verify cluster formation and node status

        3. Test [Data Preparation on Old Version]
            3.1 Create test data using tdCb.prepareDataOnOldVersion()
                3.1.1 Create test databases and tables with taosBenchmark
                3.1.2 Insert sample data across multiple tables
                3.1.3 Create stream processing objects
                3.1.4 Verify data consistency before upgrade
            3.2 Setup stream processing infrastructure
                3.2.1 Create streams with various window types
                3.2.2 Setup TMQ topics and consumers
                3.2.3 Verify stream functionality on old version
                3.2.4 Flush databases to ensure data persistence

        4. Test [Rolling Upgrade Execution - Mode 0 (Single Node)]
            4.1 Execute upgrade using tdCb.updateNewVersion() with mode 0
                4.1.1 Upgrade single dnode incrementally (mode=0)
                4.1.2 Monitor upgrade process for individual node
                4.1.3 Verify mixed-version cluster operation
                4.1.4 Wait 10 seconds for upgrade completion
            4.2 Verify cluster stability during incremental upgrade
                4.2.1 Check upgraded node is running new version
                4.2.2 Verify remaining nodes still on old version
                4.2.3 Confirm cluster connectivity maintained
                4.2.4 Validate data accessibility during upgrade

        5. Test [Post-Upgrade Data Verification]
            5.1 Verify data integrity using tdCb.verifyData()
                5.1.1 Check table counts and row counts consistency
                5.1.2 Verify stream processing functionality
                5.1.3 Test TMQ consumer operations
                5.1.4 Validate aggregation results accuracy
            5.2 Verify new features and compatibility
                5.2.1 Test stream recalculation features
                5.2.2 Verify tag size modifications
                5.2.3 Check configuration parameter compatibility
                5.2.4 Validate error handling improvements

        6. Test [SQL Syntax Compatibility Verification]
            6.1 Test backticks in SQL using tdCb.verifyBackticksInTaosSql()
                6.1.1 Test database operations with backticks
                6.1.2 Test table operations with backticks
                6.1.3 Test stream operations with backticks
                6.1.4 Verify error handling for invalid backtick usage

        Catalog:
            - Streams:Compatibility:RollingUpgrade

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-07-23 Beryl migrated from system-test/0-others/compatibility_rolling_upgrade.py
            - Note: Maintains original cb.* calls but adapted for pytest framework

        """
        hostname = self.host
        tdLog.info(f"hostname: {hostname}")

        lastBigVersion = self.getLastBigVersion()

        tdDnodes.stopAll()
        
        baseVersionExist = tdCb.installTaosdForRollingUpgrade(self.getDnodePath(), lastBigVersion)
        if not baseVersionExist:
            tdLog.info(f"Base version {lastBigVersion} does not exist")
        

        if baseVersionExist:
            tdSql.execute(f"CREATE DNODE '{hostname}:6130'")
            tdSql.execute(f"CREATE DNODE '{hostname}:6230'")

            time.sleep(10)

            tdCb.prepareDataOnOldVersion(lastBigVersion, tdCom.getBuildPath(),corss_major_version=False)

            tdCb.updateNewVersion(tdCom.getBuildPath(),self.getDnodePath(),0)

            time.sleep(10)

            tdCb.verifyData(corss_major_version=False)

            tdCb.verifyBackticksInTaosSql(tdCom.getBuildPath())

        tdLog.printNoPrefix("========== Rolling Upgrade Compatibility Test Completed Successfully ==========")

    def getLastBigVersion(self):
        tdSql.query(f"SELECT SERVER_VERSION();")
        nowServerVersion=tdSql.queryResult[0][0]
        tdLog.info(f"Now server version is {nowServerVersion}")
        # get the last big version
        lastBigVersion = nowServerVersion.split(".")[0]+"."+nowServerVersion.split(".")[1]+"."+nowServerVersion.split(".")[2]+"."+"0"

        tdLog.info(f"Last big version is {lastBigVersion}")
        return lastBigVersion

    def getDnodePath(self):
        buildPath = tdCom.getBuildPath()
        dnodePaths = [buildPath + "/../sim/dnode1/", buildPath + "/../sim/dnode2/", buildPath + "/../sim/dnode3/"]
        return dnodePaths
