import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStreamOldCaseInterpFill:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_interp_fill(self):
        """Stream interp fill

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldCase
        Since: v3.0.0.0
        Labels: common,ci
        Jira: None
        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpLarge.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpLinear0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpNext0.sim
            # - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpPrev0.sim
            # - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpPrev1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpValue0.sim
        """

        # self.streamInterpLarge()
        # self.streamInterpLinear0()
        # self.streamInterpNext0()
        # self.streamInterpPrev0()
        # self.streamInterpPrev1()
        # self.streamInterpValue0()

    def streamInterpHistory(self):
        tdLog.info(f"streamInterpHistory")
        clusterComCheck.check_stream_status()

    def streamInterpHistory1(self):
        tdLog.info(f"streamInterpHistory1")
        clusterComCheck.check_stream_status()

    def streamInterpLarge(self):
        tdLog.info(f"streamInterpLarge")
        clusterComCheck.check_stream_status()

    def streamInterpLinear0(self):
        tdLog.info(f"streamInterpLinear0")
        clusterComCheck.check_stream_status()

    def streamInterpNext0(self):
        tdLog.info(f"streamInterpNext0")
        clusterComCheck.check_stream_status()



    def streamInterpOther(self):
        tdLog.info(f"streamInterpOther")
        clusterComCheck.check_stream_status()

    def streamInterpOther1(self):
        tdLog.info(f"streamInterpOther1")
        clusterComCheck.check_stream_status()


    def streamInterpPrev0(self):
        tdLog.info(f"streamInterpPrev0")
        clusterComCheck.check_stream_status()


    def streamInterpPrev1(self):
        tdLog.info(f"streamInterpPrev1")
        clusterComCheck.check_stream_status()


    def streamInterpValue0(self):
        tdLog.info(f"streamInterpValue0")
        clusterComCheck.check_stream_status()


