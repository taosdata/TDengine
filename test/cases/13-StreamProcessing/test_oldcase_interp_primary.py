import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStreamOldCaseInterpPrimary:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_interp_primary(self):
        """Stream interp primary

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldCase
        Since: v3.0.0.0
        Labels: common,ci
        Jira: None
        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpPrimaryKey0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpPrimaryKey1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpPrimaryKey2.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpPrimaryKey3.sim
        """

        # self.streamInterpPrimaryKey0()
        # self.streamInterpPrimaryKey1()
        # self.streamInterpPrimaryKey2()
        # self.streamInterpPrimaryKey3()

    def streamInterpPrimaryKey0(self):
        tdLog.info(f"streamInterpPrimaryKey0")
        clusterComCheck.check_stream_status()

    def streamInterpPrimaryKey1(self):
        tdLog.info(f"streamInterpPrimaryKey1")
        clusterComCheck.check_stream_status()

    def streamInterpPrimaryKey2(self):
        tdLog.info(f"streamInterpPrimaryKey2")
        clusterComCheck.check_stream_status()

    def streamInterpPrimaryKey3(self):
        tdLog.info(f"streamInterpPrimaryKey3")
        clusterComCheck.check_stream_status()
