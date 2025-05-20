import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStreamOldCaseSnode:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_snode(self):
        """Stream snode

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldCase
        Since: v3.0.0.0
        Labels: common,ci
        Jira: None
        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/schedSnode.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/snodeCheck.sim
        """

        # self.schedSnode()
        # self.snodeCheck()

    def schedSnode(self):
        tdLog.info(f"schedSnode")
        clusterComCheck.check_stream_status()

    def snodeCheck(self):
        tdLog.info(f"snodeCheck")
        clusterComCheck.check_stream_status()

