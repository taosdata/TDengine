import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStreamOldCaseOptions:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_options(self):
        """Stream options

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldCase
        Since: v3.0.0.0
        Labels: common,ci
        Jira: None
        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/ignoreCheckUpdate.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/ignoreExpiredData.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/pauseAndResume.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/sliding.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/tag.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/triggerInterval0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/windowClose.sim
        """

        # self.ignoreCheckUpdate()
        # self.ignoreExpiredData()
        # self.pauseAndResume()
        # self.sliding()
        # self.tag()
        # self.triggerInterval0()
        # self.windowClose()

    def ignoreCheckUpdate(self):
        tdLog.info(f"ignoreCheckUpdate")
        drop_all_streams_and_dbs()

    def ignoreExpiredData(self):
        tdLog.info(f"ignoreExpiredData")
        drop_all_streams_and_dbs()

    def pauseAndResume(self):
        tdLog.info(f"pauseAndResume")
        drop_all_streams_and_dbs()

    def sliding(self):
        tdLog.info(f"sliding")
        drop_all_streams_and_dbs()

    def tag(self):
        tdLog.info(f"tag")
        drop_all_streams_and_dbs()

    def triggerInterval0(self):
        tdLog.info(f"triggerInterval0")
        drop_all_streams_and_dbs()

    def windowClose(self):
        tdLog.info(f"windowClose")
        drop_all_streams_and_dbs()


