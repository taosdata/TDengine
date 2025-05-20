import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStreamOldCaseInterpHistory:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_interp_history(self):
        """Stream interp history

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldCase
        Since: v3.0.0.0
        Labels: common,ci
        Jira: None
        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpHistory.sim
            # - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpHistory1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpOther.sim
            # - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpOther1.sim
        """

        # self.streamInterpHistory()
        # self.streamInterpHistory1()
        # self.streamInterpOther()
        # self.streamInterpOther1()

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


