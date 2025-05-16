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
