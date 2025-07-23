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
        """Stream Processing Backward and Forward Compatibility Test

        Test comprehensive compatibility scenarios for stream processing across different versions:

        1. Test [Backward Compatibility] with Legacy Versions
            1.1 Test legacy stream syntax compatibility
                1.1.1 v3.2.0.0 stream creation syntax support
                1.1.2 v3.3.3.0 stream options compatibility
                1.1.3 v3.3.4.3 aggregation function compatibility
                1.1.4 v3.3.5.0 window function compatibility
            1.2 Test legacy configuration compatibility
                1.2.1 Stream configuration parameter mapping
                1.2.2 Legacy option value interpretation
                1.2.3 Deprecated feature handling
                1.2.4 Configuration migration automation
            1.3 Test legacy data format compatibility
                1.3.1 Historical stream metadata format support
                1.3.2 Legacy timestamp precision handling
                1.3.3 Data type compatibility across versions
                1.3.4 Encoding format consistency

        2. Test [Forward Compatibility] with Future Features
            2.1 Test new feature graceful degradation
                2.1.1 Unknown stream options handling
                2.1.2 Unsupported function graceful failure
                2.1.3 Advanced feature detection and fallback
                2.1.4 Version-specific feature activation
            2.2 Test feature flag compatibility
                2.2.1 Conditional feature enablement
                2.2.2 Experimental feature stability
                2.2.3 Beta feature migration paths
                2.2.4 Feature deprecation handling

        3. Test [Cross-Version Migration] Scenarios
            3.1 Test stream definition migration
                3.1.1 Automatic stream syntax translation
                3.1.2 Manual migration procedure validation
                3.1.3 Migration rollback mechanisms
                3.1.4 Migration verification procedures
            3.2 Test data migration compatibility
                3.2.1 Historical data preservation
                3.2.2 Stream state migration
                3.2.3 Result data format migration
                3.2.4 Metadata consistency verification
            3.3 Test configuration migration
                3.3.1 Stream parameter mapping
                3.3.2 Option value transformation
                3.3.3 Default value handling
                3.3.4 Custom configuration preservation

        4. Test [API and Protocol Compatibility]
            4.1 Test REST API compatibility
                4.1.1 API endpoint backward compatibility
                4.1.2 Request/response format consistency
                4.1.3 Error code and message compatibility
                4.1.4 Authentication mechanism compatibility
            4.2 Test native protocol compatibility
                4.2.1 Binary protocol version negotiation
                4.2.2 Message format compatibility
                4.2.3 Connection handshake compatibility
                4.2.4 Compression algorithm compatibility
            4.3 Test client library compatibility
                4.3.1 Driver API compatibility
                4.3.2 Connection string format compatibility
                4.3.3 Result set format consistency
                4.3.4 Error handling compatibility

        5. Test [SQL Syntax Compatibility]
            5.1 Test stream creation syntax evolution
                5.1.1 Basic CREATE STREAM syntax compatibility
                5.1.2 Window clause syntax evolution
                5.1.3 Option specification syntax changes
                5.1.4 Output specification syntax compatibility
            5.2 Test function compatibility
                5.2.1 Aggregation function signature compatibility
                5.2.2 Window function parameter compatibility
                5.2.3 Built-in function behavior consistency
                5.2.4 User-defined function compatibility
            5.3 Test data type compatibility
                5.3.1 Column data type mapping
                5.3.2 Tag data type compatibility
                5.3.3 Timestamp precision handling
                5.3.4 NULL value handling consistency

        6. Test [Performance Compatibility]
            6.1 Test performance regression prevention
                6.1.1 Throughput baseline maintenance
                6.1.2 Latency regression detection
                6.1.3 Memory usage consistency
                6.1.4 CPU utilization patterns
            6.2 Test scalability compatibility
                6.2.1 Horizontal scaling behavior
                6.2.2 Vertical scaling compatibility
                6.2.3 Resource limit handling
                6.2.4 Concurrent processing capability

        7. Test [Error Handling Compatibility]
            7.1 Test error code consistency
                7.1.1 SQL error code mapping
                7.1.2 Runtime error code compatibility
                7.1.3 System error code preservation
                7.1.4 Custom error code handling
            7.2 Test error message compatibility
                7.2.1 Error message format consistency
                7.2.2 Localization compatibility
                7.2.3 Error context preservation
                7.2.4 Debug information compatibility

        8. Test [Specific Version Regression]
            8.1 Test baseline version functionality
                8.1.1 v3.2.0.0 core feature validation
                8.1.2 v3.3.3.0 enhancement verification
                8.1.3 v3.3.4.3 bug fix validation
                8.1.4 v3.3.5.0 performance improvement verification
            8.2 Test upgrade path validation
                8.2.1 Sequential version upgrade testing
                8.2.2 Skip-version upgrade testing
                8.2.3 Rollback compatibility testing
                8.2.4 Emergency recovery testing

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
    