import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdStream


class TestStreamSubqueryBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_subquery_basic(self):
        """As SubQuery basic test

        1. -

        Catalog:
            - Streams:SubQuery

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-13 Simon Guan Create Case

        """

        self.createSnode()
        self.createDatabase()
        self.prepareQueryData()
        self.prepareTriggerData()
        return
        self.createStream()
        self.trigStream()
        self.checkStreamStatus()
        self.checkResults()

        tdSql.pause()

    def createSnode(self):
        tdLog.info("create snode")
        tdStream.createSnode(1)

    def createDatabase(self):
        tdLog.info(f"create database")

        tdSql.prepare(dbname="qdb", vgroups=1)
        tdSql.prepare(dbname="tdb", vgroups=1)
        # tdSql.prepare("rdb", vgroups=1)
        # tdSql.prepare("qdb2", vgroups=1)
        clusterComCheck.checkDbReady("qdb")
        clusterComCheck.checkDbReady("tdb")
        # clusterComCheck.checkDbReady("rdb")
        # clusterComCheck.checkDbReady("qdb2")

    def prepareQueryData(self):
        tdLog.info("prepare child tables for query")
        tdStream.prepareChildTables(db="qdb", stb="meters", tbBatch=2, rowBatch=2)

        tdLog.info("prepare normal tables for query")
        tdStream.prepareNormalTables(db="qdb", tables=10, rowBatch=2)

        tdLog.info("prepare virtual tables for query")
        tdStream.prepareVirtualTables(db="qdb", tables=10)
        
        tdSql.query("select cint from qdb.v0")

    def prepareTriggerData(self):
        tdLog.info("prepare child tables for trigger")
        tdSql.execute(
            "create table tdb.triggers (ts timestamp, c1 int, c2 int) tags(t1 int);"
        )
        tdSql.execute("create table tdb.t1 using tdb.triggers tags(1)")
        tdSql.execute("create table tdb.t2 using tdb.triggers tags(1)")
        tdSql.execute("create table tdb.t3 using tdb.triggers tags(1)")

    def createStream(self):
        self.streams = [
            self.TestStreamSubqueryBaiscItem(
                id=0,
                trigger="interval(5m) sliding(5m) from qdb.meters partition by tbname",
                output="tags(gid bigint as _tgrpid)",
                sub_query="select _twstart ts, count(current) cnt from qdb.meters where ts >= _twstart and ts < _twend;",
                res_query="select ts, cnt from rdb.s0",
                exp_query="select _wstart ts, count(current) cnt from qdb.meters interval(5m)",
                exp_rows=[],
            ),
            self.TestStreamSubqueryBaiscItem(
                id=1,
                trigger="create stream s0 interval(5m) sliding(5m) from qdb.meters into rdb.rs0 tags (gid bigint as _tgrpid)",
                output="",
                sub_query="select _wstart ts, count(current) cnt from qdb.meters",
                res_query="select count(current) cnt from rsb.s1 interval(5m)",
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
        ]

        self.test_list = [0]

        tdLog.info(f"create total:{len(self.test_list)} streams")
        for stream in self.streams:
            if stream.id in self.test_list:
                stream.createStream()

    def checkStreamStatus(self):
        tdLog.info(f"wait total:{len(self.test_list)} streams run finish")
        tdStream.checkStreamStatus()

    def trigStream(self):
        tdLog.info("write data to trig stream")
        sqls = [
            "insert into tdb.t1 values ('2025-01-01 00:00:00', 0, 0), ('2025-01-01 00:05:00', 1, 1), ('2025-01-01 00:10:00', 2, 2)"
        ]
        tdSql.executes(sqls)

    def checkResults(self):
        tdLog.info(f"check total:{len(self.test_list)} streams result")
        for stream in self.streams:
            if stream.id in self.test_list:
                stream.checkResults(print=True)

    class TestStreamSubqueryBaiscItem:
        def __init__(
            self, id, trigger, output, sub_query, res_query, exp_query, exp_rows=[]
        ):
            self.id = id
            self.name = f"s{id}"
            self.trigger = trigger
            self.output = output
            self.sub_query = sub_query
            self.res_query = res_query
            self.exp_query = exp_query
            self.exp_rows = exp_rows
            self.exp_result = []

        def createStream(self):
            sql = f"create stream s{self.id} {self.trigger} into rdb.s{self.id} {self.output} as {self.sub_query}"
            tdLog.info(f"create stream:{self.name}, sql:{sql}")

        def checkResults(self, print=False):
            tdLog.info(f"check stream:{self.name} result")

            tmp_result = tdSql.getResult(self.exp_query)
            if self.exp_rows == []:
                self.exp_rows = range(len(tmp_result))
            for r in self.exp_rows:
                self.exp_result.append(tmp_result[r])
            if print:
                tdSql.printResult(
                    f"{self.name} expect",
                    input_result=self.exp_result,
                    input_sql=self.exp_query,
                )

            tdSql.checkResultsByArray(self.res_query, self.exp_result, self.exp_query)
            tdLog.info(f"check stream:{self.name} result successfully")
