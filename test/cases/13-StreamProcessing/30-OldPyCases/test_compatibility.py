import pytest,os,platform

from new_test_framework.utils import (
    tdLog,
    tdSql,
    clusterComCheck,
    tdStream,
    StreamItem,
    tdCb
)
# Define the list of base versions to test
BASE_VERSIONS = ["3.2.0.0","3.3.3.0","3.3.4.3","3.3.5.0","3.3.6.0"]  # Add more versions as needed

class TestStreamCompatibility:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_compatibility(self):
        """Stream Compatibility Test

        Test compatibility aspects of stream processing:
        1. Backward compatibility with legacy syntax patterns
        2. Forward compatibility with new features
        3. Cross-version compatibility considerations
        4. Migration path validation

        Catalog:
            - Streams:OldPyCases

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-12-19 Migrated from system-test/0-others/compatibility.py
            - Note: Focused on stream-related compatibility, removed cluster upgrade tests

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

            tdCb.installTaosd(bPath,cPath,base_version)

            tdCb.prepareDataOnOldVersion(base_version, bPath,corss_major_version)

            tdCb.killAllDnodes()

            tdCb.updateNewVersion(bPath,cPaths=[cPath],upgrade=2)

            tdCb.verifyData(corss_major_version)

            tdCb.verifyBackticksInTaosSql(bPath)

            tdLog.printNoPrefix(f"Compatibility test cycle with base version {base_version} completed successfully")

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

    def getCfgPath(self):
        buildPath = self.getBuildPath()
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            cfgPath = buildPath + "/../sim/dnode1/cfg/"
        else:
            cfgPath = buildPath + "/../sim/dnode1/cfg/"

        return cfgPath
    