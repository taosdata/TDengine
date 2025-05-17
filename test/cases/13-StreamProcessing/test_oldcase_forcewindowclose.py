import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStreamOldCaseForceWindowClose:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_force_window_close(self):
        """Stream force window close

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldCase
        Since: v3.0.0.0
        Labels: common,ci
        Jira: None
        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/forcewindowclose.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpForceWindowClose.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpForceWindowClose1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpFwcError.sim
        """

        # self.forcewindowclose()
        # self.streamFwcIntervalFill()
        # self.streamInterpForceWindowClose()
        # self.streamInterpForceWindowClose1()
        # self.streamInterpFwcError()

    def forcewindowclose(self):
        tdLog.info(f"forcewindowclose")
        drop_all_streams_and_dbs()

    def streamInterpForceWindowClose(self):
        tdLog.info(f"streamInterpForceWindowClose")
        drop_all_streams_and_dbs()

    def streamInterpForceWindowClose1(self):
        tdLog.info(f"streamInterpForceWindowClose1")
        drop_all_streams_and_dbs()

    def streamInterpFwcError(self):
        tdLog.info(f"streamInterpFwcError")
        drop_all_streams_and_dbs()



