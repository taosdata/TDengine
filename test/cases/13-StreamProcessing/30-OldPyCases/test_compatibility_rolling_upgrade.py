import os
import platform
import socket
import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    clusterComCheck,
)
# Import the compatibility basic module from new location
from .compatibility_basic import cb


class TestCompatibilityRollingUpgrade:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_compatibility_rolling_upgrade(self):
        """TDengine Rolling Upgrade Compatibility Test

        Test rolling upgrade of TDengine nodes.
        Maintains original logic using cb module but adapted for pytest framework.

        Catalog:
            - Streams:OldPyCases

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-12-19 Migrated from system-test/0-others/compatibility_rolling_upgrade.py
            - Note: Maintains original cb.* calls but adapted for pytest framework

        """

        # Maintain original rolling upgrade logic using cb module
        tdLog.printNoPrefix("========== Rolling Upgrade Compatibility Test ==========")

        hostname = socket.gethostname()
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
        cb.killAllDnodes()
        
        # Install old version for rolling upgrade
        cb.installTaosdForRollingUpgrade(cPaths, lastBigVersion)
        
        # Create dnodes
        tdSql.execute(f"CREATE DNODE '{hostname}:6130'")
        tdSql.execute(f"CREATE DNODE '{hostname}:6230'")

        time.sleep(10)

        # Prepare data on old version
        cb.prepareDataOnOldVersion(lastBigVersion, bPath)

        # Update to new version - rolling upgrade mode 0
        cb.updateNewVersion(bPath, cPaths, 0)

        time.sleep(10)

        # Verify data after upgrade
        cb.verifyData(corss_major_version=False)

        # Verify backticks in SQL
        cb.verifyBackticksInTaosSql(bPath)

        tdLog.printNoPrefix("========== Rolling Upgrade Compatibility Test Completed Successfully ==========")

    def test_compatibility_rolling_upgrade_cross_major_version(self):
        """Cross major version rolling upgrade test"""
        tdLog.printNoPrefix("========== Cross Major Version Rolling Upgrade Test ==========")

        hostname = socket.gethostname()
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
        cb.killAllDnodes()
        
        # Install old version for rolling upgrade
        cb.installTaosdForRollingUpgrade(cPaths, lastBigVersion)
        
        # Create dnodes  
        tdSql.execute(f"CREATE DNODE '{hostname}:6130'")
        tdSql.execute(f"CREATE DNODE '{hostname}:6230'")

        time.sleep(10)

        # Prepare data on old version for cross major version
        cb.prepareDataOnOldVersion(lastBigVersion, bPath, corss_major_version=True)

        # Update to new version - rolling upgrade mode 0
        cb.updateNewVersion(bPath, cPaths, 0)

        time.sleep(10)

        # Verify data after upgrade for cross major version
        cb.verifyData(corss_major_version=True)

        # Verify backticks in SQL
        cb.verifyBackticksInTaosSql(bPath)

        tdLog.printNoPrefix("========== Cross Major Version Rolling Upgrade Test Completed Successfully ==========")

    def test_dnodeHasStreams(self):
        """Test if dnode has streams"""  
        tdLog.printNoPrefix("========== Testing Dnode Has Streams ==========")

        hostname = socket.gethostname()
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
        cb.killAllDnodes()
        
        # Install old version for rolling upgrade
        cb.installTaosdForRollingUpgrade(cPaths, lastBigVersion)
        
        # Create dnodes
        tdSql.execute(f"CREATE DNODE '{hostname}:6130'")
        tdSql.execute(f"CREATE DNODE '{hostname}:6230'")

        time.sleep(10)

        # Create a database and stream on old version
        tdSql.execute("create database db2")
        tdSql.execute("use db2")
        tdSql.execute("create table stb (ts timestamp, c1 int) tags (t1 int)")
        tdSql.execute("create table ct1 using stb tags(1000)")
        
        # Create stream on old version
        tdSql.execute("create stream stream_db2 fill_history 1 into streammt as select _wstart,count(*) from stb interval(10s)")
        
        time.sleep(10)

        # Check if dnode has streams
        result = cb.dnodeHasStreams()
        tdLog.info(f"Dnode has streams check result: {result}")

        # Update to new version - rolling upgrade mode 0  
        cb.updateNewVersion(bPath, cPaths, 0)

        time.sleep(10)

        # Verify streams still exist after upgrade
        cb.verifyStreamsAfterUpgrade()

        tdLog.printNoPrefix("========== Dnode Has Streams Test Completed Successfully ==========")

    def test_killAllTaosd(self):
        """Test kill all taosd processes"""
        tdLog.printNoPrefix("========== Testing Kill All Taosd ==========")

        # Stop all taosd processes
        cb.killAllTaosd()

        time.sleep(5)

        # Check that all processes are stopped
        cb.checkAllTaosdStopped()

        tdLog.printNoPrefix("========== Kill All Taosd Test Completed Successfully ==========")

    def test_installTaosd(self):
        """Test install taosd"""
        tdLog.printNoPrefix("========== Testing Install Taosd ==========")

        # Get version info
        tdSql.query(f"SELECT SERVER_VERSION();")
        nowServerVersion=tdSql.queryResult[0][0]
        tdLog.info(f"Now server version is {nowServerVersion}")
        # get the last big version
        lastBigVersion = nowServerVersion.split(".")[0]+"."+nowServerVersion.split(".")[1]+"."+nowServerVersion.split(".")[2]+"."+"0"
        tdLog.info(f"Last big version is {lastBigVersion}")

        cPath = self.getDnodePaths()[0]
        
        # Install taosd for specific version
        cb.installTaosd(lastBigVersion, cPath)

        # Verify installation
        cb.verifyTaosdInstallation(lastBigVersion)

        tdLog.printNoPrefix("========== Install Taosd Test Completed Successfully ==========")

    def test_updateNewVersionNormal(self):
        """Test update to new version normal mode"""
        tdLog.printNoPrefix("========== Testing Update New Version Normal ==========")

        bPath = self.getBuildPath()
        cPaths = self.getDnodePaths()

        # Update to new version - normal mode
        cb.updateNewVersionNormal(bPath, cPaths)

        time.sleep(10)

        # Verify update
        cb.verifyVersionUpdate()

        tdLog.printNoPrefix("========== Update New Version Normal Test Completed Successfully ==========")

    def test_restartDnodes(self):
        """Test restart dnodes"""
        tdLog.printNoPrefix("========== Testing Restart Dnodes ==========")

        cPaths = self.getDnodePaths()

        # Restart all dnodes
        cb.restartDnodes(cPaths)

        time.sleep(10)

        # Verify dnodes are running
        cb.verifyDnodesRunning()

        tdLog.printNoPrefix("========== Restart Dnodes Test Completed Successfully ==========")

    def test_stopDnodes(self):
        """Test stop dnodes"""
        tdLog.printNoPrefix("========== Testing Stop Dnodes ==========")

        cPaths = self.getDnodePaths()

        # Stop all dnodes
        cb.stopDnodes(cPaths)

        time.sleep(5)

        # Verify dnodes are stopped
        cb.verifyDnodesStopped()

        tdLog.printNoPrefix("========== Stop Dnodes Test Completed Successfully ==========")

    def test_startDnodes(self):
        """Test start dnodes"""
        tdLog.printNoPrefix("========== Testing Start Dnodes ==========")

        bPath = self.getBuildPath()
        cPaths = self.getDnodePaths()

        # Start all dnodes
        cb.startDnodes(bPath, cPaths)

        time.sleep(10)

        # Verify dnodes are running
        cb.verifyDnodesRunning()

        tdLog.printNoPrefix("========== Start Dnodes Test Completed Successfully ==========")

    def test_prepareOldVersionData(self):
        """Test prepare old version data"""
        tdLog.printNoPrefix("========== Testing Prepare Old Version Data ==========")

        # Get version info
        tdSql.query(f"SELECT SERVER_VERSION();")
        nowServerVersion=tdSql.queryResult[0][0]
        tdLog.info(f"Now server version is {nowServerVersion}")
        # get the last big version
        lastBigVersion = nowServerVersion.split(".")[0]+"."+nowServerVersion.split(".")[1]+"."+nowServerVersion.split(".")[2]+"."+"0"
        tdLog.info(f"Last big version is {lastBigVersion}")

        bPath = self.getBuildPath()
        
        # Prepare data on old version
        cb.prepareDataOnOldVersion(lastBigVersion, bPath)

        # Verify data preparation
        cb.verifyDataPreparation()

        tdLog.printNoPrefix("========== Prepare Old Version Data Test Completed Successfully ==========")

    def getBuildPath(self):
        """Get build path - copied from original"""
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

    def getDnodePaths(self):
        """Get dnode paths - copied from original"""
        buildPath = self.getBuildPath()
        dnodePaths = [buildPath + "/../sim/dnode1/", buildPath + "/../sim/dnode2/", buildPath + "/../sim/dnode3/"]
        return dnodePaths 