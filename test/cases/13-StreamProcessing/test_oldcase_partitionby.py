import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStreamOldCasePartitionBt

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_partitionby(self):
        """Stream partition by

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldCase
        Since: v3.0.0.0
        Labels: common,ci
        Jira: None
        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/partitionby.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/partitionby1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/partitionbyColumnInterval.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/partitionbyColumnOther.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/partitionbyColumnSession.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/partitionbyColumnState.sim
        """

        # self.partitionby()
        # self.partitionby1()
        # self.partitionbyColumnInterval()
        # self.partitionbyColumnOther()
        # self.partitionbyColumnSession()
        # self.partitionbyColumnState()

    def partitionby(self):
        tdLog.info(f"partitionby")
        clusterComCheck.drop_all_streams_and_dbs()

    def partitionby1(self):
        tdLog.info(f"partitionby1")
        clusterComCheck.drop_all_streams_and_dbs()

    def partitionbyColumnInterval(self):
        tdLog.info(f"partitionbyColumnInterval")
        clusterComCheck.drop_all_streams_and_dbs()

    def partitionbyColumnOther(self):
        tdLog.info(f"partitionbyColumnOther")
        clusterComCheck.drop_all_streams_and_dbs()

    def partitionbyColumnSession(self):
        tdLog.info(f"partitionbyColumnSession")
        clusterComCheck.drop_all_streams_and_dbs()

    def partitionbyColumnState(self):
        tdLog.info(f"partitionbyColumnState")
        clusterComCheck.drop_all_streams_and_dbs()


