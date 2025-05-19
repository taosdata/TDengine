import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStreamOldCaseInterpPrimary:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_interp_primary(self):
        """Stream interp primary

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldCase
        Since: v3.0.0.0
        Labels: common,ci
        Jira: None
        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamPrimaryKey0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamPrimaryKey1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamPrimaryKey2.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamPrimaryKey3.sim
        """

        # self.streamInterpPrimaryKey0()
        # self.streamInterpPrimaryKey1()
        # self.streamInterpPrimaryKey2()
        # self.streamInterpPrimaryKey3()
        # self.streamPrimaryKey0()
        # self.streamPrimaryKey1()
        # self.streamPrimaryKey2()
        # self.streamPrimaryKey3()

    def streamInterpPrimaryKey0(self):
        tdLog.info(f"streamInterpPrimaryKey0")
        drop_all_streams_and_dbs()

    def streamInterpPrimaryKey1(self):
        tdLog.info(f"streamInterpPrimaryKey1")
        drop_all_streams_and_dbs()

    def streamInterpPrimaryKey2(self):
        tdLog.info(f"streamInterpPrimaryKey2")
        drop_all_streams_and_dbs()

    def streamInterpPrimaryKey3(self):
        tdLog.info(f"streamInterpPrimaryKey3")
        drop_all_streams_and_dbs()

    def streamPrimaryKey0(self):
        tdLog.info(f"streamPrimaryKey0")
        drop_all_streams_and_dbs()

    def streamPrimaryKey1(self):
        tdLog.info(f"streamPrimaryKey1")
        drop_all_streams_and_dbs()

    def streamPrimaryKey2(self):
        tdLog.info(f"streamPrimaryKey2")
        drop_all_streams_and_dbs()

    def streamPrimaryKey3(self):
        tdLog.info(f"streamPrimaryKey3")
        drop_all_streams_and_dbs()

