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