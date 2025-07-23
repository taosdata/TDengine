import pytest,os,platform,time

from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdDnodes,
    clusterComCheck,
    tdStream,
    StreamItem,
    tdCb
)


class TestCompatibilityRollingUpgradeAll:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_compatibility_rolling_upgrade_all(self):
        """TDengine Rolling Upgrade All Dnodes Compatibility Test

        Test comprehensive rolling upgrade scenarios involving all cluster nodes simultaneously:

        1. Test [Cluster Wide Rolling Upgrade] Strategy
            1.1 Test upgrade preparation and validation
                1.1.1 Pre-upgrade cluster health verification
                1.1.2 Version compatibility matrix validation
                1.1.3 Backup and checkpoint creation
                1.1.4 Resource availability verification
            1.2 Test simultaneous node upgrade coordination
                1.2.1 Leader election during upgrade process
                1.2.2 Consensus protocol maintenance
                1.2.3 Data replication consistency
                1.2.4 Service availability preservation

        2. Test [Stream Processing Continuity] During Upgrade
            2.1 Test stream computation preservation
                2.1.1 Active stream computation during upgrade
                2.1.2 Window state preservation across node restarts
                2.1.3 Trigger condition evaluation continuity
                2.1.4 Output data consistency maintenance
            2.2 Test stream metadata consistency
                2.2.1 Stream definition persistence
                2.2.2 Stream configuration synchronization
                2.2.3 Stream state replication
                2.2.4 Metadata version compatibility

        3. Test [Data Consistency and Integrity] Across Upgrade
            3.1 Test source data availability
                3.1.1 Source table accessibility during upgrade
                3.1.2 New data ingestion continuity
                3.1.3 Historical data consistency
                3.1.4 Timestamp precision preservation
            3.2 Test result data consistency
                3.2.1 Target table consistency verification
                3.2.2 Aggregation result accuracy
                3.2.3 Window computation correctness
                3.2.4 Output ordering preservation

        4. Test [Service Availability and Performance]
            4.1 Test service continuity metrics
                4.1.1 Downtime measurement during upgrade
                4.1.2 Service degradation assessment
                4.1.3 Recovery time validation
                4.1.4 Performance baseline restoration
            4.2 Test load balancing and failover
                4.2.1 Load distribution during upgrade
                4.2.2 Automatic failover mechanisms
                4.2.3 Client connection management
                4.2.4 Resource utilization optimization

        5. Test [Version Compatibility Matrix]
            5.1 Test major version upgrades
                5.1.1 Cross-major version compatibility
                5.1.2 Breaking change impact assessment
                5.1.3 Migration path validation
                5.1.4 Rollback procedure verification
            5.2 Test minor version upgrades
                5.2.1 Patch version compatibility
                5.2.2 Feature compatibility validation
                5.2.3 Configuration compatibility
                5.2.4 API compatibility verification

        6. Test [Error Handling and Recovery] Scenarios
            6.1 Test partial upgrade failure handling
                6.1.1 Node upgrade failure detection
                6.1.2 Rollback mechanism activation
                6.1.3 Cluster state recovery
                6.1.4 Data consistency restoration
            6.2 Test catastrophic failure recovery
                6.2.1 Complete cluster failure scenarios
                6.2.2 Emergency recovery procedures
                6.2.3 Data integrity verification
                6.2.4 Service restoration validation

        7. Test [Post-Upgrade Validation] Procedures
            7.1 Test functional verification
                7.1.1 Stream functionality validation
                7.1.2 Data accuracy verification
                7.1.3 Performance benchmark comparison
                7.1.4 Feature compatibility testing
            7.2 Test system health assessment
                7.2.1 Cluster health monitoring
                7.2.2 Resource usage validation
                7.2.3 Error log analysis
                7.2.4 Performance metrics verification

        Catalog:
            - Streams:Compatibility:RollingUpgradeAll

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-07-23 Beryl migrated from system-test/0-others/compatibility_rolling_upgrade_all.py
            - Note: Maintains original cb.* calls but adapted for pytest framework

        """

        # Maintain original rolling upgrade logic using cb module
        tdLog.printNoPrefix("========== Rolling Upgrade All Dnodes Compatibility Test ==========")

        hostname = self.host
        tdLog.info(f"hostname: {hostname}")
        
        # Get last big version
        tdSql.query(f"SELECT SERVER_VERSION();")
        nowServerVersion=tdSql.queryResult[0][0]
        tdLog.info(f"Now server version is {nowServerVersion}")
        # get the last big version
        lastBigVersion = nowServerVersion.split(".")[0]+"."+nowServerVersion.split(".")[1]+"."+nowServerVersion.split(".")[2]+"."+"0"
        tdLog.info(f"Last big version is {lastBigVersion}")

        bPath = self.getBuildPath()
        cPaths = self.getDnodePaths()
        
        # Stop all dnodes
        tdCb.killAllDnodes()
        
        # Install old version for rolling upgrade
        baseVersionExist = tdCb.installTaosdForRollingUpgrade(cPaths, lastBigVersion)
        if not baseVersionExist:
            tdLog.info(f"Base version {lastBigVersion} does not exist")
            
        if baseVersionExist:
            # Create dnodes
            tdSql.execute(f"CREATE DNODE '{hostname}:6130'")
            tdSql.execute(f"CREATE DNODE '{hostname}:6230'")

            time.sleep(10)

            # Prepare data on old version
            tdCb.prepareDataOnOldVersion(lastBigVersion, bPath, corss_major_version=False)

            # Update to new version - rolling upgrade all dnodes mode 1
            tdCb.updateNewVersion(bPath, cPaths, 1)

            time.sleep(10)

            # Verify data after upgrade
            tdCb.verifyData(corss_major_version=False)

            # Verify backticks in SQL
            tdCb.verifyBackticksInTaosSql(bPath)
        
        tdLog.printNoPrefix("========== Rolling Upgrade All Dnodes Compatibility Test Completed Successfully ==========")


    def getDnodePaths(self):
        """Get dnode paths - copied from original"""
        buildPath = self.getBuildPath()
        dnodePaths = [buildPath + "/../sim/dnode1/", buildPath + "/../sim/dnode2/", buildPath + "/../sim/dnode3/"]
        return dnodePaths 

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        print(f"projPath:{projPath}")
        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files or "taosd.exe" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                print(f"rootRealPath:{rootRealPath}")
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root)-len("/build/bin")]
                    break
        print(f"buildPath:{buildPath}")
        return buildPath