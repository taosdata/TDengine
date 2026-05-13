import os
import pytest
from new_test_framework.utils import tdLog, tdSql, clusterComCheck, tdStream, StreamItem, sc


class TestStreamCheckpoint:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")


    def test_stream_dev_basic(self):
        """Checkpoint

        Verification testing during the development process.

        Catalog:
            - Streams:Snode

        Since: v3.3.3.7

        Labels: common,ci,skip

        Jira: None

        History:
            - 2025-06-25 dapan created

        """

        # self.case_1()
        self.case_2()


    def case_2(self):
        self.num_snode = 1
        self.num_vgroups = 4
        self.streams = []
        self.stream_id = 1

        self.create_env()
        self.prepare_source_table()

        self.create_streams()
        tdStream.checkStreamStatus()

        self.do_write_data()
        self.check_results()

        # no checkpoint yet
        # self.checkpoint()

        # sc.dnodeStopAll()
        # sc.dnodeStartAll()
        #
        # self.query_after_restart()

        tdSql.execute("create snode on dnode 2")

        while True:
            if clusterComCheck.checkDnodes(2):
                break

        # wait for 10min to check if the checkpoint transfer to second snode


    def create_env(self):
        tdLog.info(f"create {self.num_snode} snode(s)")
        for i in range(self.num_snode):
            tdStream.createSnode(i+1)

        self.create_database()

    def create_database(self) -> None:
        tdLog.info(f"create database")
        tdSql.prepare(dbname="db", vgroups=self.num_vgroups)
        clusterComCheck.checkDbReady("db")

    def prepare_query_data(self) -> None:
        tdLog.info("prepare child tables for query")
        tdStream.prepareChildTables(tbBatch=1, rowBatch=1, rowsPerBatch=400)

        tdLog.info("prepare normal tables for query")
        tdStream.prepareNormalTables(tables=10, rowBatch=1)

    def prepare_source_table(self) -> None:
        tdLog.info("prepare tables for trigger")

        tdSql.execute("use db")

        stb = "create table source_table (ts timestamp, k int, c1 varchar(12), c2 double) tags(a int)"
        tdSql.execute(stb)

        ctb = "create table c1 using source_table tags(1)"
        tdSql.execute(ctb)


    def do_write_data(self):
        tdLog.info("write data to trigger table")

        sqls = [
            "insert into c1 values ('2025-01-01 00:00:00', 0,  '1', 0) ('2025-01-01 00:05:00', 5, '1', 50) ('2025-01-01 00:10:00', 10, '1', 100)",
            "insert into c1 values ('2025-01-01 00:11:00', 11, '1', 110) ('2025-01-01 00:12:00', 12, '1', 120) ('2025-01-01 00:15:00', 15, '1', 150)",
            "insert into c1 values ('2025-01-01 00:21:00', 21, '1', 210)",
        ]
        tdSql.executes(sqls)

    def wait_for_stream_completed(self) -> None:
        tdLog.info(f"wait total:{len(self.streams)} streams run finish")
        tdStream.checkStreamStatus()

    def check_results(self) -> None:
        tdLog.info(f"check total:{len(self.streams)} streams result")
        for stream in self.streams:
            stream.checkResults()

    def create_streams(self) ->None:
        self.streams = []

        stream = StreamItem(
            id=1,
            stream="create stream s5 interval(10s) sliding(10s) from source_table partition by tbname into r5 as "
                   "select _twstart ts, _twend te, _twduration td, _twrownum tw, %%tbname as tb, count(c1) c1, avg(c2) c2, now() "
                   "from %%tbname "
                   "where ts >= _twstart and ts < _twend",
            res_query="select ts, te, td, c1, c2 from r5",
            exp_query="select _wstart ts, _wend te, _wduration td, count(c1) c1, avg(c2) c2 "
                      "from source_table "
                      "where ts<'2025-1-1 00:15:10' and ts>='2025-1-1' "
                      "partition by tbname "
                      "interval(10s) sliding(10s) fill(value, 0, null)",
            check_func=self.check5,
        )
        self.streams.append(stream)

        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            stream.createStream()

    def check5(self) ->None:
        tdSql.checkResultsByFunc(
            sql="select * from information_schema.ins_tags where db_name='db' and stable_name='source_table' and tag_name='a'",
            func=lambda: tdSql.getRows() == 1,
        )

        tdSql.checkResultsByFunc(
            sql="select ts, te, td, c1, tag_tbname from r5 where tag_tbname='c1'",
            func=lambda: tdSql.getRows() == 91
        )

    def checkpoint(self) -> None:
        tdLog.info("do check checkpoint info")
        base = os.getcwd() + "/new_test_framework/utils/sim/dnode1/data/snode/"

        if not os.path.exists(base + "checkpoint"):
            print("checkpoint file not exists")
            raise Exception("checkpoint not exists")

    def query_after_restart(self) -> None:
        tdLog.info("start query after restart")

        tdSql.query("select * from r5")
        tdSql.checkRows(91)

    def case_1(self):
        self.num_snode = 2
        self.num_vgroups = 4
        self.streams = []
        self.stream_id = 1

        # while True:
        #     if clusterComCheck.checkDnodes(2):
        #         break

        self.create_env()
        self.prepare_source_table()

        self.create_streams()
        tdStream.checkStreamStatus()

        self.do_write_data()
        self.check_results()

        # no checkpoint yet
        # self.checkpoint()

        sc.dnodeStopAll()
        sc.dnodeStartAll()

        self.query_after_restart()