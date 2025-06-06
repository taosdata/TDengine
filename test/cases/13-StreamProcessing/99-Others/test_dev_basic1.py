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

        tdLog.info(f"=============== create database")
        tdSql.prepare(dbname="qdb", vgroups=1)

        tdLog.info(f"=============== create super table")
        tdSql.execute(
            f"create stable meters (cts timestamp, cint int, cuint int unsigned) tags(tint int);"
        )
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== write query data")
        sqls = [
            "insert into t1 using meters tags(1) values ('2025-01-01 00:00:00'    , 0, 0);",
            "insert into t2 using meters tags(2) values ('2025-01-01 00:00:00.102', 1, 0)",
            "insert into t1 using meters tags(1) values ('2025-01-01 00:00:01'    , 1, 1);",
            "insert into t2 using meters tags(2) values ('2025-01-01 00:00:01.400', 2, 1);",
            "insert into t1 using meters tags(1) values ('2025-01-01 00:00:02'    , 2, 2);",
            "insert into t2 using meters tags(2) values ('2025-01-01 00:00:02.600', 3, 2);",
        ]
        tdSql.executes(sqls)
        tdSql.query("select _wstart, avg(cint) from meters interval(1s)")
        tdSql.printResult()

        tdLog.info(f"=============== create trigger table")
        tdSql.execute("create table stream_trigger (ts timestamp, c1 int, c2 int);")
        tdSql.query(f"show tables")
        tdSql.checkKeyExist("stream_trigger")

        tdLog.info(f"=============== create stream")
        sql = "create stream s1 interval(1s) sliding(1s) from stream_trigger partition by tbname into st1 tags (gid bigint as _tgrpid) as select _twstart ts, count(*) c1, avg(cint) c2  from meters where cts >= _twstart and cts < _twend;"
        tdSql.error(sql)

        tdStream.createSnode()
        tdSql.execute(sql)

        tdStream.checkStreamStatus()

        tdLog.info(f"=============== write trigger data")
        tdSql.execute(
            "insert into stream_trigger values ('2025-01-01 00:00:00', 0, 0), ('2025-01-01 00:00:01', 1, 1), ('2025-01-01 00:00:02', 2, 2);"
        )

        tdLog.info(f"=============== check stream result")
        result_sql = "select ts, c1, c2 from qdb.st1"

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

        exp_sql = (
            "select _wstart, count(*), avg(cint) from qdb.meters interval(1s) limit 2"
        )
        exp_result = tdSql.getResult(exp_sql)
        tdSql.checkResultsByArray(sql=result_sql, exp_result=exp_result)

        tdSql.checkResultsBySql(
            sql=result_sql,
            exp_sql=exp_sql,
        )
