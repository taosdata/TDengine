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
        tdSql.execute(f"create stable stb (ts timestamp, v1 int, v2 int) tags(t1 int);")
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== write query data")
        sqls = [
            "insert into t1 using stb tags(1) values ('2025-01-01 00:00:00'    , 0, 0);",
            "insert into t2 using stb tags(2) values ('2025-01-01 00:00:00.102', 1, 0);",
            "insert into t1 using stb tags(1) values ('2025-01-01 00:00:01'    , 1, 1);",
            "insert into t2 using stb tags(2) values ('2025-01-01 00:00:01.400', 2, 1);",
            "insert into t1 using stb tags(1) values ('2025-01-01 00:00:02'    , 2, 2);",
            "insert into t2 using stb tags(2) values ('2025-01-01 00:00:02.600', 3, 2);",
        ]
        tdSql.executes(sqls)
        tdSql.query("select _wstart, avg(v1) from stb interval(1s)")
        tdSql.printResult()

        tdLog.info(f"=============== create trigger table")
        tdSql.execute("create table stream_trigger (ts timestamp, v1 int, v2 int);")
        tdSql.query(f"show tables")
        tdSql.checkKeyExist("stream_trigger")

        tdLog.info(f"=============== create stream")
        sql = "create stream s9 PERIOD(10s, 10a) into out9 as select _tlocaltime, _tprev_localtime, _tnext_localtime, now, max(v1) from stb;"
        tdSql.execute(sql)

        tdStream.checkStreamStatus()

        tdLog.info(f"=============== write trigger data")
        tdSql.execute(
            "insert into stream_trigger values ('2025-01-01 00:00:00', 0, 0), ('2025-01-01 00:00:01', 1, 1), ('2025-01-01 00:00:02', 2, 2);"
        )

        tdLog.info(f"=============== check stream result")

        result_sql9 = "select * from test.out9"
        tdSql.checkResultsByFunc(sql=result_sql9, func=lambda: tdSql.getRows() > 0)
