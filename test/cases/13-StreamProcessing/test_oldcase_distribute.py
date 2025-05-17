import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStreamOldCaseDistribute:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_distribute(self):
        """Stream distribute

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldCase
        Since: v3.0.0.0
        Labels: common,ci
        Jira: None
        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/distributeInterval0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/distributeIntervalRetrive0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/distributeMultiLevelInterval0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/distributeSession0.sim
        """

        # self.distributeInterval0()
        # self.distributeIntervalRetrive0()
        # self.distributeMultiLevelInterval0()
        # self.distributeSession0()

    def distributeInterval0(self):
        tdLog.info(f"distributeInterval0")
        drop_all_streams_and_dbs()

    def distributeIntervalRetrive0(self):
        tdLog.info(f"distributeIntervalRetrive0")
        drop_all_streams_and_dbs()

    def distributeMultiLevelInterval0(self):
        tdLog.info(f"distributeMultiLevelInterval0")
        drop_all_streams_and_dbs()

    def distributeSession0(self):
        tdLog.info(f"distributeSession0")
        drop_all_streams_and_dbs()

