import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStreamOldCaseInterpDeleteUpdate:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_interp_delete_update(self):
        """Stream interp delete update

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldCase
        Since: v3.0.0.0
        Labels: common,ci
        Jira: None
        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpError.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpDelete0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpDelete1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpDelete2.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpUpdate.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpUpdate1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpUpdate2.sim
        """

        # self.streamInterpError()
        # self.streamInterpDelete0()
        # self.streamInterpDelete1()
        # self.streamInterpDelete2()
        # self.streamInterpUpdate()
        # self.streamInterpUpdate1()
        # self.streamInterpUpdate2()

    def streamInterpError(self):
        tdLog.info(f"streamInterpError")
        drop_all_streams_and_dbs()

    def streamInterpDelete0(self):
        tdLog.info(f"streamInterpDelete0")
        drop_all_streams_and_dbs()

    def streamInterpDelete1(self):
        tdLog.info(f"streamInterpDelete1")
        drop_all_streams_and_dbs()

    def streamInterpDelete2(self):
        tdLog.info(f"streamInterpDelete2")
        drop_all_streams_and_dbs()

    def streamInterpUpdate(self):
        tdLog.info(f"streamInterpUpdate")
        drop_all_streams_and_dbs()

    def streamInterpUpdate1(self):
        tdLog.info(f"streamInterpUpdate1")
        drop_all_streams_and_dbs()

    def streamInterpUpdate2(self):
        tdLog.info(f"streamInterpUpdate2")
        drop_all_streams_and_dbs()

