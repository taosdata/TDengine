import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStreamOldCaseInterpPartitionBy:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_interp_partitionby(self):
        """Stream interp partition by

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldCase
        Since: v3.0.0.0
        Labels: common,ci
        Jira: None
        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpPartitionBy0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpPartitionBy1.sim
        """

        # self.streamInterpPartitionBy0()
        # self.streamInterpPartitionBy1()

    def streamInterpPartitionBy0(self):
        tdLog.info(f"streamInterpPartitionBy0")
        clusterComCheck.check_stream_status()

    def streamInterpPartitionBy1(self):
        tdLog.info(f"streamInterpPartitionBy1")
        clusterComCheck.check_stream_status()

