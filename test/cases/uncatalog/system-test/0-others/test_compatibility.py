from new_test_framework.utils import tdLog, tdSql
from urllib.parse import uses_relative
import os
import platform
from taos.tmq import Consumer
from taos.tmq import *

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from compatibility_basic import cb

from pathlib import Path

# Define the list of base versions to test
BASE_VERSIONS = ["3.2.0.0","3.3.3.0","3.3.4.3","3.3.5.0","3.3.6.0"]  # Add more versions as needed

class TestCompatibility:
    def caseDescription(self):
        f'''
        TDengine Data Compatibility Test 
        Testing compatibility from the following base versions to current version: {BASE_VERSIONS}
        '''
        return

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def getBuildPath(self):
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
        buildPath = self.getBuildPath()
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            cfgPath = buildPath + "/../sim/dnode1/cfg/"
        else:
            cfgPath = buildPath + "/../sim/dnode1/cfg/"

        return cfgPath
    
    def test_compatibility(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx
        """
        scriptsPath = os.path.dirname(os.path.realpath(__file__))
        try:
            import distro
            distro_id = distro.id()
            if distro_id == "alpine":
                tdLog.info(f"alpine skip compatibility test")
                return True
        except ImportError:
            tdLog.info("Cannot import distro module, skipping distro check")

        if platform.system().lower() == 'windows':
            tdLog.info(f"Windows skip compatibility test")
            return True

        bPath = self.getBuildPath()
        cPath = self.getCfgPath()
        tdLog.info(f"bPath:{bPath}, cPath:{cPath}")

        # Get the last version defined in the list
        last_version_in_list = BASE_VERSIONS[-1]
        corss_major_version = True
        for base_version in BASE_VERSIONS:
            if base_version == last_version_in_list:
                corss_major_version = False

            tdLog.printNoPrefix(f"========== Start testing compatibility with base version {base_version} ==========")

            cb.installTaosd(bPath,cPath,base_version)

            cb.prepareDataOnOldVersion(base_version, bPath,corss_major_version)

            cb.killAllDnodes()

            cb.updateNewVersion(bPath,cPaths=[],upgrade=2)

            cb.verifyData(corss_major_version)

            cb.verifyBackticksInTaosSql(bPath)

            tdLog.printNoPrefix(f"Compatibility test cycle with base version {base_version} completed successfully")

        tdLog.success(f"{__file__} successfully executed")


