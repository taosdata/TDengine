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
