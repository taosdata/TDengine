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
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpHistory.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpHistory1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpLarge.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpLinear0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpLinear1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpNext0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpNext1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpOther.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpOther1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpOther2.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpPrev0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpPrev1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpScalar.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpValue0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpValue1.sim
        """

        # self.stream_basic_0()
        # self.stream_basic_1()
        # self.stream_basic_2()
        # self.stream_basic_3()
        self.stream_basic_4()
        # self.stream_basic_5()


