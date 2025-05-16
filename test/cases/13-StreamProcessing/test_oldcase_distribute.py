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
