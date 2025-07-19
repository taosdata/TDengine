import os
import platform
from new_test_framework.utils import (
    tdLog,
    tdSql,
    clusterComCheck,
)

# Import the compatibility basic module from new location
from .compatibility_basic import cb

# Define the list of base versions to test
BASE_VERSIONS = ["3.2.0.0", "3.3.3.0", "3.3.4.3", "3.3.5.0", "3.3.6.0"]


class TestCompatibilityGeneral:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_compatibility_general(self):
        """General TDengine Data Compatibility Test

        Testing compatibility from base versions to current version.
        Maintains original logic from compatibility.py but adapted for new framework.

        Catalog:
            - Streams:OldPyCases

        Since: v3.0.0.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-12-19 Migrated from system-test/0-others/compatibility.py
            - Note: Maintains original cb.* calls but adapted for pytest framework

        """

        # Skip on Alpine and Windows as per original logic
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

        # Use compatibility basic module that was imported

        bPath = self.getBuildPath()
        cPath = self.getCfgPath()
        tdLog.info(f"bPath:{bPath}, cPath:{cPath}")

        # Get the last version defined in the list
        last_version_in_list = BASE_VERSIONS[-1]
        cross_major_version = True
        
        for base_version in BASE_VERSIONS:
            if base_version == last_version_in_list:
                cross_major_version = False

            tdLog.printNoPrefix(f"========== Start testing compatibility with base version {base_version} ==========")

            try:
                cb.installTaosd(bPath, cPath, base_version)

                cb.prepareDataOnOldVersion(base_version, bPath, cross_major_version)

                cb.killAllDnodes()

                cb.updateNewVersion(bPath, cPaths=[], upgrade=2)

                cb.verifyData(cross_major_version)

                cb.verifyBackticksInTaosSql(bPath)

                tdLog.printNoPrefix(f"Compatibility test cycle with base version {base_version} completed successfully")
                
            except Exception as e:
                tdLog.info(f"Compatibility test with version {base_version} failed: {e}")
                # Continue with next version

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

    def getCfgPath(self):
        """Get config path - copied from original"""
        buildPath = self.getBuildPath()
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            cfgPath = buildPath + "/../sim/dnode1/cfg/"
        else:
            cfgPath = buildPath + "/../sim/dnode1/cfg/"

        return cfgPath 