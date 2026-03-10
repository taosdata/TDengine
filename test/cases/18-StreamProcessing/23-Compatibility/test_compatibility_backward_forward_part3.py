import pytest,os,platform

from new_test_framework.utils import (
    tdLog,
    tdSql,
    clusterComCheck,
    tdStream,
    StreamItem,
    tdCb,
    tdCom
)
# Define the list of base versions to test
BASE_VERSIONS = ["3.3.6.0","3.3.7.0"]  # Part 3: Test versions 3.3.6.0 and 3.3.7.0

class TestStreamCompatibility:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_compatibility(self):
        """Comp: stream backward and forward (Part 3/3)

        Test compatibility across baseline versions 3.3.6.0 and 3.3.7.0 with stream processing validation:

        1. Test [v3.3.6.0 Base Version Compatibility]
            1.1 Install v3.3.6.0 and prepare data using tdCb.prepareDataOnOldVersion()
                1.1.1 Create test databases and tables with taosBenchmark
                1.1.2 Insert sample data and create streams
                1.1.3 Setup TMQ topics and consumers
                1.1.4 Verify stream functionality on v3.3.6.0
            1.2 Upgrade to new version with mode 2 (no upgrade mode)
                1.2.1 Kill all dnodes and update to new version
                1.2.2 Start new version with existing data
                1.2.3 Verify cross-major version compatibility (corss_major_version=True)
            1.3 Verify data and functionality using tdCb.verifyData()
                1.3.1 Check table counts and row counts consistency
                1.3.2 Verify stream processing functionality
                1.3.3 Test TMQ consumer operations
                1.3.4 Validate aggregation results accuracy

        2. Test [v3.3.7.0 Base Version Compatibility]
            2.1 Install v3.3.7.0 and prepare data using tdCb.prepareDataOnOldVersion()
                2.1.1 Create test databases and tables with taosBenchmark
                2.1.2 Insert sample data and create streams
                2.1.3 Setup TMQ topics and consumers
                2.1.4 Verify stream functionality on v3.3.7.0
            2.2 Upgrade to new version with mode 2 (no upgrade mode)
                2.2.1 Kill all dnodes and update to new version
                2.2.2 Start new version with existing data
                2.2.3 Verify compatibility (corss_major_version=False as final version)
            2.3 Verify data and functionality using tdCb.verifyData()
                2.3.1 Check table counts and row counts consistency
                2.3.2 Verify stream processing functionality
                2.3.3 Test TMQ consumer operations
                2.3.4 Validate aggregation results accuracy

        3. Test [SQL Syntax Compatibility Verification]
            3.1 Test backticks in SQL using tdCb.verifyBackticksInTaosSql()
                3.1.1 Test database operations with backticks
                3.1.2 Test table operations with backticks
                3.1.3 Test stream operations with backticks
                3.1.4 Verify error handling for invalid backtick usage

        Catalog:
            - Streams:Compatibility:BackwardForward:Part3

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-07-23 Beryl migrated from system-test/0-others/compatibility.py
            - 2026-01-13 Split into part 3 of 3 (versions 3.3.6.0 and 3.3.7.0)
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

        bPath = tdCom.getBuildPath()
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

            tdCb.verifyBackticksInTaosSql(bPath,base_version)

            tdLog.printNoPrefix(f"Compatibility test cycle with base version {base_version} completed successfully")

    

    def getCfgPath(self):
        buildPath = tdCom.getBuildPath()
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            cfgPath = buildPath + "/../sim/dnode1/cfg/"
        else:
            cfgPath = buildPath + "/../sim/dnode1/cfg/"

        return cfgPath
    
