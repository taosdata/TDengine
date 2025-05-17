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
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpPartitionBy0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpPartitionBy1.sim
        """

        # self.partitionby()
        # self.partitionby1()
        # self.partitionbyColumnInterval()
        # self.partitionbyColumnOther()
        # self.partitionbyColumnSession()
        # self.partitionbyColumnState()
        # self.streamInterpPartitionBy0()
        # self.streamInterpPartitionBy1()

    def partitionby(self):
        tdLog.info(f"partitionby")
        drop_all_streams_and_dbs()

    def partitionby1(self):
        tdLog.info(f"partitionby1")
        drop_all_streams_and_dbs()

    def partitionbyColumnInterval(self):
        tdLog.info(f"partitionbyColumnInterval")
        drop_all_streams_and_dbs()

    def partitionbyColumnOther(self):
        tdLog.info(f"partitionbyColumnOther")
        drop_all_streams_and_dbs()

    def partitionbyColumnSession(self):
        tdLog.info(f"partitionbyColumnSession")
        drop_all_streams_and_dbs()

    def partitionbyColumnState(self):
        tdLog.info(f"partitionbyColumnState")
        drop_all_streams_and_dbs()

    def streamInterpPartitionBy0(self):
        tdLog.info(f"streamInterpPartitionBy0")
        drop_all_streams_and_dbs()

    def streamInterpPartitionBy1(self):
        tdLog.info(f"streamInterpPartitionBy1")
        drop_all_streams_and_dbs()

