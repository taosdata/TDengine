import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStreamOldCaseEvent:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_event(self):
        """Stream event

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldCase
        Since: v3.0.0.0
        Labels: common,ci
        Jira: None
        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/event0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/event1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/event2.sim
        """

        # self.event0()
        # self.event1()
        # self.event2()

    def event0(self):
        tdLog.info(f"event0")
        drop_all_streams_and_dbs()

    def event1(self):
        tdLog.info(f"event1")
        drop_all_streams_and_dbs()

    def event2(self):
        tdLog.info(f"event2")
        drop_all_streams_and_dbs()


