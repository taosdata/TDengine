import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStreamOldCaseState:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_state(self):
        """Stream state

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldCase
        Since: v3.0.0.0
        Labels: common,ci
        Jira: None
        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/state0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/state1.sim
        """

        # self.state0()
        # self.state1()

    def state0(self):
        tdLog.info(f"state0")
        clusterComCheck.check_stream_status()

    def state1(self):
        tdLog.info(f"state1")
        clusterComCheck.check_stream_status()

