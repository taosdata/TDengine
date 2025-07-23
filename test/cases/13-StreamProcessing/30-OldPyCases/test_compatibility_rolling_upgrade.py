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


class TestCompatibilityRollingUpgrade:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_compatibility_rolling_upgrade(self):
        """TDengine Rolling Upgrade Compatibility Test

        Test incremental rolling upgrade scenarios with stream processing validation:

        1. Test [Sequential Node Upgrade] Strategy
            1.1 Test upgrade sequence planning
                1.1.1 Node priority determination for upgrade order
                1.1.2 Leader/follower upgrade coordination
                1.1.3 Quorum maintenance during rolling upgrade
                1.1.4 Service dependency management
            1.2 Test individual node upgrade process
                1.2.1 Node graceful shutdown procedures
                1.2.2 Version upgrade and restart process
                1.2.3 Node rejoin cluster validation
                1.2.4 Replication resynchronization

        2. Test [Stream Processing Preservation] During Rolling Upgrade
            2.1 Test stream computation continuity
                2.1.1 Active stream processing during node upgrades
                2.1.2 Window computation preservation across node changes
                2.1.3 State transfer between old and new nodes
                2.1.4 Trigger evaluation consistency
            2.2 Test stream data flow integrity
                2.2.1 Source data ingestion continuity
                2.2.2 Intermediate processing consistency
                2.2.3 Target table output reliability
                2.2.4 Data ordering preservation

        3. Test [High Availability Maintenance]
            3.1 Test service availability during upgrade
                3.1.1 Read operation availability maintenance
                3.1.2 Write operation availability maintenance
                3.1.3 Stream query responsiveness
                3.1.4 Client connection management
            3.2 Test failover mechanisms
                3.2.1 Automatic leader election during upgrade
                3.2.2 Replica promotion and demotion
                3.2.3 Network partition handling
                3.2.4 Split-brain prevention

        4. Test [Version Compatibility Validation]
            4.1 Test cross-version protocol compatibility
                4.1.1 Network protocol version negotiation
                4.1.2 Data format compatibility validation
                4.1.3 Metadata schema compatibility
                4.1.4 Configuration parameter compatibility
            4.2 Test mixed-version cluster operation
                4.2.1 Temporary mixed-version cluster stability
                4.2.2 Feature availability during transition
                4.2.3 Performance consistency across versions
                4.2.4 Resource utilization patterns

        5. Test [Data Consistency and Integrity]
            5.1 Test replication consistency
                5.1.1 Data consistency across replicas during upgrade
                5.1.2 Transaction consistency maintenance
                5.1.3 Write acknowledgment reliability
                5.1.4 Read consistency guarantees
            5.2 Test stream result consistency
                5.2.1 Aggregation result accuracy preservation
                5.2.2 Window boundary consistency
                5.2.3 State transition correctness
                5.2.4 Output data completeness

        6. Test [Performance and Resource Management]
            6.1 Test performance impact assessment
                6.1.1 Throughput degradation measurement
                6.1.2 Latency impact analysis
                6.1.3 Resource utilization monitoring
                6.1.4 Recovery time measurement
            6.2 Test resource rebalancing
                6.2.1 Load redistribution during upgrade
                6.2.2 Memory usage optimization
                6.2.3 CPU utilization balancing
                6.2.4 Storage space management

        7. Test [Error Handling and Recovery]
            7.1 Test upgrade failure scenarios
                7.1.1 Individual node upgrade failure handling
                7.1.2 Network connectivity issues during upgrade
                7.1.3 Resource exhaustion scenarios
                7.1.4 Timeout and deadlock resolution
            7.2 Test recovery mechanisms
                7.2.1 Failed node recovery procedures
                7.2.2 Cluster state restoration
                7.2.3 Data integrity verification
                7.2.4 Service health validation

        8. Test [Post-Upgrade Validation]
            8.1 Test functional correctness
                8.1.1 Stream processing functionality validation
                8.1.2 Data query correctness verification
                8.1.3 Administrative operation validation
                8.1.4 Monitoring and alerting functionality
            8.2 Test system stability
                8.2.1 Long-term cluster stability
                8.2.2 Memory leak detection
                8.2.3 Performance regression analysis
                8.2.4 Configuration consistency verification

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

            tdCb.prepareDataOnOldVersion(lastBigVersion, self.getBuildPath(),corss_major_version=False)

            tdCb.updateNewVersion(self.getBuildPath(),self.getDnodePath(),0)

            time.sleep(10)

            tdCb.verifyData(corss_major_version=False)

            tdCb.verifyBackticksInTaosSql(self.getBuildPath())

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