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
BASE_VERSIONS = ["3.2.0.0","3.3.3.0","3.3.4.3","3.3.5.0","3.3.6.0","3.3.7.0"]  # Add more versions as needed

class TestStreamCompatibility:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_compatibility(self):
        """Comp: stream backward and forward

        Test compatibility across 5 baseline versions with stream processing validation:

        1. Test [v3.2.0.0 Base Version Compatibility]
            1.1 Install v3.2.0.0 and prepare data using tdCb.prepareDataOnOldVersion()
                1.1.1 Create test databases and tables with taosBenchmark
                1.1.2 Insert sample data and create streams
                1.1.3 Setup TMQ topics and consumers
                1.1.4 Verify stream functionality on v3.2.0.0
            1.2 Upgrade to new version with mode 2 (no upgrade mode)
                1.2.1 Kill all dnodes and update to new version
                1.2.2 Start new version with existing data
                1.2.3 Verify cross-major version compatibility (corss_major_version=True)
            1.3 Verify data and functionality using tdCb.verifyData()
                1.3.1 Check table counts and row counts consistency
                1.3.2 Verify stream processing functionality
                1.3.3 Test TMQ consumer operations
                1.3.4 Validate aggregation results accuracy

        2. Test [v3.3.3.0 Base Version Compatibility]
            2.1 Install v3.3.3.0 and prepare data using tdCb.prepareDataOnOldVersion()
                2.1.1 Create test databases and tables with taosBenchmark
                2.1.2 Insert sample data and create streams
                2.1.3 Setup TMQ topics and consumers
                2.1.4 Verify stream functionality on v3.3.3.0
            2.2 Upgrade to new version with mode 2 (no upgrade mode)
                2.2.1 Kill all dnodes and update to new version
                2.2.2 Start new version with existing data
                2.2.3 Verify compatibility (corss_major_version=True)
            2.3 Verify data and functionality using tdCb.verifyData()
                2.3.1 Check table counts and row counts consistency
                2.3.2 Verify stream processing functionality
                2.3.3 Test TMQ consumer operations
                2.3.4 Validate aggregation results accuracy

        3. Test [v3.3.4.3 Base Version Compatibility]
            3.1 Install v3.3.4.3 and prepare data using tdCb.prepareDataOnOldVersion()
                3.1.1 Create test databases and tables with taosBenchmark
                3.1.2 Insert sample data and create streams
                3.1.3 Setup TMQ topics and consumers
                3.1.4 Verify stream functionality on v3.3.4.3
            3.2 Upgrade to new version with mode 2 (no upgrade mode)
                3.2.1 Kill all dnodes and update to new version
                3.2.2 Start new version with existing data
                3.2.3 Verify compatibility (corss_major_version=True)
            3.3 Verify data and functionality using tdCb.verifyData()
                3.3.1 Check table counts and row counts consistency
                3.3.2 Verify stream processing functionality
                3.3.3 Test TMQ consumer operations
                3.3.4 Validate aggregation results accuracy

        4. Test [v3.3.5.0 Base Version Compatibility]
            4.1 Install v3.3.5.0 and prepare data using tdCb.prepareDataOnOldVersion()
                4.1.1 Create test databases and tables with taosBenchmark
                4.1.2 Insert sample data and create streams
                4.1.3 Setup TMQ topics and consumers
                4.1.4 Verify stream functionality on v3.3.5.0
            4.2 Upgrade to new version with mode 2 (no upgrade mode)
                4.2.1 Kill all dnodes and update to new version
                4.2.2 Start new version with existing data
                4.2.3 Verify compatibility (corss_major_version=True)
            4.3 Verify data and functionality using tdCb.verifyData()
                4.3.1 Check table counts and row counts consistency
                4.3.2 Verify stream processing functionality
                4.3.3 Test TMQ consumer operations
                4.3.4 Validate aggregation results accuracy

        5. Test [v3.3.6.0 Base Version Compatibility - Final]
            5.1 Install v3.3.6.0 and prepare data using tdCb.prepareDataOnOldVersion()
                5.1.1 Create test databases and tables with taosBenchmark
                5.1.2 Insert sample data and create streams
                5.1.3 Setup TMQ topics and consumers
                5.1.4 Verify stream functionality on v3.3.6.0
            5.2 Upgrade to new version with mode 2 (no upgrade mode)
                5.2.1 Kill all dnodes and update to new version
                5.2.2 Start new version with existing data
                5.2.3 Verify compatibility (corss_major_version=False as final version)
            5.3 Verify data and functionality using tdCb.verifyData()
                5.3.1 Check table counts and row counts consistency
                5.3.2 Verify stream processing functionality
                5.3.3 Test TMQ consumer operations
                5.3.4 Validate aggregation results accuracy

        6. Test [SQL Syntax Compatibility Verification]
            6.1 Test backticks in SQL using tdCb.verifyBackticksInTaosSql()
                6.1.1 Test database operations with backticks
                6.1.2 Test table operations with backticks
                6.1.3 Test stream operations with backticks
                6.1.4 Verify error handling for invalid backtick usage

        Catalog:
            - Streams:Compatibility:BackwardForward

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-07-23 Beryl migrated from system-test/0-others/compatibility.py
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

            tdCb.verifyBackticksInTaosSql(bPath)

            tdLog.printNoPrefix(f"Compatibility test cycle with base version {base_version} completed successfully")

    

    def getCfgPath(self):
        buildPath = tdCom.getBuildPath()
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            cfgPath = buildPath + "/../sim/dnode1/cfg/"
        else:
            cfgPath = buildPath + "/../sim/dnode1/cfg/"

        return cfgPath
    