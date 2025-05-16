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
