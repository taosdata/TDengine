import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStreamOldCaseBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_basic(self):
        """Stream basic test

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldCase
        Since: v3.0.0.0
        Labels: common,ci
        Jira: None
        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaError.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaFwcFill.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaFwcFillPrimaryKey.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaFwcInterval.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaFwcIntervalPrimaryKey.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaInterpFwc.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaInterpFwcCheckpoint.sim
        """

        # self.stream_basic_0()
        # self.stream_basic_1()
        # self.stream_basic_2()
        # self.stream_basic_3()
        self.stream_basic_4()
        # self.stream_basic_5()

