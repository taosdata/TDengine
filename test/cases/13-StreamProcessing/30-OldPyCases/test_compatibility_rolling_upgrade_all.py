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


class TestCompatibilityRollingUpgradeAll:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_compatibility_rolling_upgrade_all(self):
        """TDengine Rolling Upgrade All Dnodes Compatibility Test

        Test rolling upgrade of all dnodes simultaneously.
        Maintains original logic using cb module but adapted for pytest framework.

        Catalog:
            - Streams:OldPyCases

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-12-19 Migrated from system-test/0-others/compatibility_rolling_upgrade_all.py
            - Note: Maintains original cb.* calls but adapted for pytest framework

        """

        # Maintain original rolling upgrade logic using cb module
        tdLog.printNoPrefix("========== Rolling Upgrade All Dnodes Compatibility Test ==========")

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
        cb.prepareDataOnOldVersion(lastBigVersion, bPath, corss_major_version=False)

        # Update to new version - rolling upgrade all dnodes mode 1
        cb.updateNewVersion(bPath, cPaths, 1)

        time.sleep(10)

        # Verify data after upgrade
        cb.verifyData(corss_major_version=False)

        # Verify backticks in SQL
        cb.verifyBackticksInTaosSql(bPath)
        
        tdLog.printNoPrefix("========== Rolling Upgrade All Dnodes Compatibility Test Completed Successfully ==========")

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