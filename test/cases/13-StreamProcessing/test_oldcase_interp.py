import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStreamOldCaseInterp

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_interp(self):
        """Stream interp

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
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpLarge.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpLinear0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpNext0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpOther.sim
            # - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpOther1.sim
            # - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpPrev0.sim
            # - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpPrev1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpValue0.sim
        """

        # self.streamInterpHistory()
        # self.streamInterpHistory1()
        # self.streamInterpLarge()
        # self.streamInterpLinear0()
        # self.streamInterpNext0()
        # self.streamInterpOther()
        # self.streamInterpOther1()
        # self.streamInterpPrev0()
        # self.streamInterpPrev1()
        # self.streamInterpValue0()

    def streamInterpHistory(self):
        tdLog.info(f"streamInterpHistory")
        drop_all_streams_and_dbs()

    def streamInterpHistory1(self):
        tdLog.info(f"streamInterpHistory1")
        drop_all_streams_and_dbs()

    def streamInterpLarge(self):
        tdLog.info(f"streamInterpLarge")
        drop_all_streams_and_dbs()

    def streamInterpLinear0(self):
        tdLog.info(f"streamInterpLinear0")
        drop_all_streams_and_dbs()

    def streamInterpNext0(self):
        tdLog.info(f"streamInterpNext0")
        drop_all_streams_and_dbs()



    def streamInterpOther(self):
        tdLog.info(f"streamInterpOther")
        drop_all_streams_and_dbs()

    def streamInterpOther1(self):
        tdLog.info(f"streamInterpOther1")
        drop_all_streams_and_dbs()


    def streamInterpPrev0(self):
        tdLog.info(f"streamInterpPrev0")
        drop_all_streams_and_dbs()


    def streamInterpPrev1(self):
        tdLog.info(f"streamInterpPrev1")
        drop_all_streams_and_dbs()


    def streamInterpValue0(self):
        tdLog.info(f"streamInterpValue0")
        drop_all_streams_and_dbs()


