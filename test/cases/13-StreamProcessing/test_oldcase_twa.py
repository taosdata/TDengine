import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStreamOldCaseTwa:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_twa(self):
        """Stream twa

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldCase
        Since: v3.0.0.0
        Labels: common,ci
        Jira: None
        History:
            ## - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaError.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaFwcFill.sim
            ## - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaFwcFillPrimaryKey.sim
            ## - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaFwcInterval.sim
            ## - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaFwcIntervalPrimaryKey.sim
            ## - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaInterpFwc.sim
        """

        # self.streamTwaError()
        # self.streamTwaFwcFill()
        # self.streamTwaFwcFillPrimaryKey()
        # self.streamTwaFwcIntervalPrimaryKey()
        # self.streamTwaInterpFwc()

    def streamTwaError(self):
        tdLog.info(f"streamTwaError")
        clusterComCheck.check_stream_status()

    def streamTwaFwcFill(self):
        tdLog.info(f"streamTwaFwcFill")
        clusterComCheck.check_stream_status()

    def streamTwaFwcFillPrimaryKey(self):
        tdLog.info(f"streamTwaFwcFillPrimaryKey")
        clusterComCheck.check_stream_status()

    def streamTwaFwcInterval(self):
        tdLog.info(f"streamTwaFwcInterval")
        clusterComCheck.check_stream_status()
        
    def streamTwaFwcIntervalPrimaryKey(self):
        tdLog.info(f"streamTwaFwcIntervalPrimaryKey")
        clusterComCheck.check_stream_status()

    def streamTwaInterpFwc(self):
        tdLog.info(f"streamTwaInterpFwc")
        clusterComCheck.check_stream_status()

