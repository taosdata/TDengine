import time
import math
from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamDevBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_dev_basic(self):
        """Stream basic development testing 1

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
            f"create stable stb (ts timestamp, v1 int, v2 int) tags(t1 int);"
        )
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== write query data")
        sqls = [
            "insert into t1 using stb tags(1) values ('2025-01-01 00:00:00', 0, 0);",
            "insert into t2 using stb tags(2) values ('2025-01-01 00:00:00.102', 1, 0)",
            "insert into t1 using stb tags(1) values ('2025-01-01 00:00:01', 1, 1);",
            "insert into t2 using stb tags(2) values ('2025-01-01 00:00:01.400', 2, 1);",
            "insert into t1 using stb tags(1) values ('2025-01-01 00:00:02', 2, 2);",
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
        tdSql.execute(
            "create stream s1 interval(1s) sliding(1s) from stream_trigger partition by tbname into out tags (gid bigint as _tgrpid) as select _twstart ts, count(*) c1, avg(v1) c2  from stb where ts >= _twstart and ts < _twend;"
        )
        tdStream.checkStreamStatus()

        tdLog.info(f"=============== write trigger data")
        tdSql.execute(
            "insert into stream_trigger values ('2025-01-01 00:00:00', 0, 0), ('2025-01-01 00:00:01', 1, 1), ('2025-01-01 00:00:02', 2, 2);"
        )

        tdLog.info(f"=============== check stream result")
        result_sql = "select ts, c1, c2 from test.out"

        tdSql.checkResultsByFunc(f"show stables", lambda: tdSql.getRows() == 2)

        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 2)
            and tdSql.compareData(0, 2, 0.5)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:01.000")
            and tdSql.compareData(1, 1, 2)
            and tdSql.compareData(1, 2, 1.5),
        )

        tdSql.checkResultsByFunc(
            result_sql,
            lambda: tdSql.getRows() >= 2
            and tdSql.getData(0, 2) == 0.5
            and tdSql.getData(1, 2) == 1.5,
            retry=0,
        )

        exp_sql = "select _wstart, count(*), avg(v1) from test.stb interval(1s) limit 2"
        exp_result = tdSql.getResult(exp_sql)
        tdSql.checkResultsByArray(
            sql=result_sql,
            exp_result=exp_result
        )

        tdSql.checkResultsBySql(
            sql=result_sql,
            exp_sql=exp_sql,
        )
