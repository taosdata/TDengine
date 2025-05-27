import time
import math
from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamDevBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_dev_basic(self):
        """basic test 3

        Verification testing during the development process.

        Catalog:
            - Streams:Others

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-26 Simon Guan Created

        """

        self.basic1()
        # self.basic2()

    def basic1(self):
        tdLog.info(f"basic test 1")
        tdStream.dropAllStreamsAndDbs()
        tdStream.createSnode()

        tdLog.info(f"=============== create database")
        tdSql.prepare(dbname="test", vgroups=1)

        tdLog.info(f"=============== create super table")
        tdSql.execute(
            f"create stable stream_query (ts timestamp, id int, c1 int) tags(t1 int);"
        )
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== write query data")
        sqls = [
            "insert into t1 using stream_query tags(1) values ('2025-01-01 00:00:00', 0, 0);",
            "insert into t2 using stream_query tags(2) values ('2025-01-01 00:00:00.102', 1, 0)",
            "insert into t1 using stream_query tags(1) values ('2025-01-01 00:00:01', 1, 1);",
            "insert into t2 using stream_query tags(2) values ('2025-01-01 00:00:01.400', 2, 1);",
            "insert into t1 using stream_query tags(1) values ('2025-01-01 00:00:02', 2, 2);",
            "insert into t2 using stream_query tags(2) values ('2025-01-01 00:00:02.600', 3, 2);",
        ]
        tdSql.executes(sqls)
        tdSql.query("select _wstart, avg(id) from stream_query interval(1s)")
        tdSql.printResult()

        tdLog.info(f"=============== create trigger table")
        tdSql.execute("create table stream_trigger (ts timestamp, id int, c1 int);")
        tdSql.query(f"show tables")
        tdSql.checkKeyExist("stream_trigger")

        tdLog.info(f"=============== create stream")
        tdSql.execute(
            "create stream s5 sliding (1s)  from stream_trigger into stream_out5 as select now ts , _tcurrent_ts c1 , count(*) c2, avg(id) c3 from stream_query;"
        )
        tdStream.checkStreamStatus()

        tdLog.info(f"=============== write trigger data")
        tdSql.execute(
            "insert into stream_trigger values ('2025-01-01 00:00:00', 0, 0), ('2025-01-01 00:00:01', 1, 1), ('2025-01-01 00:00:02', 2, 2);"
        )

        tdLog.info(f"=============== check stream result")
        result_sql = "select ts, c1, c2, c3 from test.stream_out5"

        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2,
        )
