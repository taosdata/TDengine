import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStreamOldCaseCommon:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_common(self):
        """Stream common

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldCase
        Since: v3.0.0.0
        Labels: common,ci
        Jira: None
        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/pauseAndResume.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/sliding.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/tag.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/triggerInterval0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/windowClose.sim
        """

        # self.pauseAndResume()
        # self.sliding()
        # self.tag()
        # self.triggerInterval0()
        # self.windowClose()

    def pauseAndResume(self):
        tdLog.info(f"pauseAndResume")
        clusterComCheck.check_stream_status()

    def sliding(self):
        tdLog.info(f"sliding")
        clusterComCheck.check_stream_status()

    def tag(self):
        tdLog.info(f"tag")
        clusterComCheck.check_stream_status()

    def triggerInterval0(self):
        tdLog.info(f"triggerInterval0")
        clusterComCheck.check_stream_status()

    def windowClose(self):
        tdLog.info(f"windowClose")
        clusterComCheck.check_stream_status()


