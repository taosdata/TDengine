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

        self.init_variables()
        self.create_snode()
        self.create_db()
        self.create_stb()
        self.create_stream()
        self.write_data()
        self.wait_stream_run_finish()
        self.check_result()

    def init_variables(self):
        tdLog.info("init variables")

        self.child_tb_num = 10
        self.batch_per_tb = 3
        self.rows_per_batch = 40
        self.rows_per_tb = self.batch_per_tb * self.rows_per_batch
        self.total_rows = self.rows_per_tb * self.child_tb_num
        self.ts_start = 1704038400000  # 2024-01-01 00:00:00
        self.ts_interval = 30 * 1000

    def create_snode(self):
        tdLog.info("create snode")
        tdStream.createSnode(1)

    def create_db(self):
        tdLog.info(f"create database")

        tdSql.prepare("qdb", vgroups=1)
        tdSql.prepare("rdb", vgroups=1)
        # tdSql.prepare("qdb2", vgroups=1)
        clusterComCheck.checkDbReady("qdb")
        clusterComCheck.checkDbReady("rdb")
        # clusterComCheck.checkDbReady("qdb2")

    def create_stb(self):
        tdLog.info(f"create super table")

        tdSql.execute(
            f"create stable qdb.meters (ts timestamp, current float, voltage int, phase float) TAGS (location varchar(64), group_id int)"
        )

        for table in range(self.child_tb_num):
            if table % 2 == 1:
                group_id = 1
                location = "California.SanFrancisco"
            else:
                group_id = 2
                location = "California.LosAngeles"

            tdSql.execute(
                f"create table qdb.d{table} using qdb.meters tags('{location}', {group_id})"
            )

        tdSql.query("show qdb.tables")
        tdSql.checkRows(self.child_tb_num)

    def write_data(self):
        tdLog.info(
            f"write total:{self.total_rows} rows, {self.child_tb_num} tables, {self.rows_per_tb} rows per table"
        )

        for batch in range(self.batch_per_tb):
            for table in range(self.child_tb_num):
                insert_sql = f"insert into qdb.d{table} values"
                for row in range(self.rows_per_batch):
                    rows = row + batch * self.rows_per_batch
                    ts = self.ts_start + self.ts_interval * rows
                    insert_sql += f"({ts}, {batch}, {row}, {rows})"
                tdSql.execute(insert_sql)

        tdSql.query(f"select count(*) from qdb.meters")
        tdSql.checkData(0, 0, self.total_rows)

    def create_stream(self):
        self.streams = [
            self.TestStreamSubqueryBaiscItem(
                id=0,
                trigger="interval(5m)",
                sub_query="select _twstart ts, count(current) cnt from qdb.meters where ts >= _twstart and ts < _twend;",
                res_query="select ts, cnt from rdb.s0",
                exp_query="select _wstart ts, count(current) cnt from qdb.meters interval(5m)",
                exp_rows=[],
            ),
            self.TestStreamSubqueryBaiscItem(
                id=1,
                trigger="interval(5m)",
                sub_query="select _wstart ts, count(current) cnt from qdb.meters",
                res_query="select count(current) cnt from qdb.meters interval(5m)",  # select cnt from rdb.s1
                exp_query="select count(current) cnt from qdb.meters where ts >= 1704038400000 and ts < 1704038700000",
                exp_rows=(0 for _ in range(12)),
            ),
        ]

        self.test_list = [0]

        tdLog.info(f"create total:{len(self.streams)} streams")
        for stream in self.streams:
            if stream.id in self.test_list:
                stream.create_stream()

    def wait_stream_run_finish(self):
        tdLog.info(f"wait total:{len(self.streams)} streams run finish")
        for stream in self.streams:
            if stream.id in self.test_list:
                stream.wait_stream_run_finish()

    def check_result(self):
        tdLog.info(f"check total:{len(self.streams)} streams result")
        for stream in self.streams:
            if stream.id in self.test_list:
                stream.check_result(print=True)

    class TestStreamSubqueryBaiscItem:
        def __init__(self, id, trigger, sub_query, res_query, exp_query, exp_rows=[]):
            self.trigger = trigger
            self.id = id
            self.name = f"s{id}"
            self.sub_query = sub_query
            self.res_query = res_query
            self.exp_query = exp_query
            self.exp_rows = exp_rows
            self.exp_result = []

        def create_stream(self):
            sql = f"create stream s{self.id} {self.trigger} from qdb.meters into rdb.rs{self.id} as {self.sub_query}"
            tdLog.info(f"create stream:{self.name}, sql:{sql}")

        def wait_stream_run_finish(self):
            tdStream.checkStreamStatus()
            tdLog.info(f"wait stream:{self.name} run finish")

        def check_result(self, print=False):
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

            tdSql.checkResultsByArray(self.res_query, tmp_result)
            tdLog.info(f"check stream:{self.name} result successfully")
