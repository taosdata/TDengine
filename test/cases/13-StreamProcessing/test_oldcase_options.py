import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStreamOldCaseOptions:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_options(self):
        """Stream options

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldCase
        Since: v3.0.0.0
        Labels: common,ci
        Jira: None
        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/ignoreCheckUpdate.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/ignoreExpiredData.sim
        """

        # self.ignoreCheckUpdate()
        # self.ignoreExpiredData()

    def ignoreCheckUpdate(self):
        tdLog.info(f"ignoreCheckUpdate")
        clusterComCheck.check_stream_status()

    def ignoreExpiredData(self):
        tdLog.info(f"ignoreExpiredData")
        clusterComCheck.check_stream_status()


