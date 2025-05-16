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

def check_stream_status(stream_name=""):
    for loop in range(60):
        if stream_name == "":
            tdSql.query(f"select * from information_schema.ins_stream_tasks")
            if tdSql.getRows() == 0:
                continue
            tdSql.query(
                f'select * from information_schema.ins_stream_tasks where status != "ready"'
            )
            if tdSql.getRows() == 0:
                return
        else:
            tdSql.query(
                f'select stream_name, status from information_schema.ins_stream_tasks where stream_name = "{stream_name}" and status == "ready"'
            )
            if tdSql.getRows() == 1:
                return
        time.sleep(1)

    tdLog.exit(f"stream task status not ready in {loop} seconds")


def drop_all_streams_and_dbs():
    dbList = tdSql.query("show databases", row_tag=True)
    for r in range(len(dbList)):
        if (
            dbList[r][0] != "information_schema"
            and dbList[r][0] != "performance_schema"
        ):
            tdSql.execute(f"drop database {dbList[r][0]}")

    streamList = tdSql.query("show streams", row_tag=True)
    for r in range(len(streamList)):
        tdSql.execute(f"drop stream {streamList[r][0]}")

    tdLog.info(f"drop {len(dbList)} databases, {len(streamList)} streams")
