import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStreamOldCaseSession:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_session(self):
        """Stream session

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldCase
        Since: v3.0.0.0
        Labels: common,ci
        Jira: None
        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/session0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/session1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/triggerSession0.sim
        """

        # self.session0()
        # self.session1()
        # self.triggerSession0()

    def session0(self):
        tdLog.info(f"session0")
        drop_all_streams_and_dbs()

    def session1(self):
        tdLog.info(f"session1")
        drop_all_streams_and_dbs()

    def triggerSession1(self):
        tdLog.info(f"triggerSession1")
        drop_all_streams_and_dbs()



