import time
import math
from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamDevBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_dev_basic(self):
        """basic test

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
        sql1 = "create stream s1 interval(1s) sliding(1s) from stream_trigger partition by tbname into out1 tags (gid bigint as _tgrpid) as select _twstart ts, count(*) c1, avg(v1) c2 from stb where ts >= _twstart and ts < _twend;"
        
        tdSql.execute(sql1)
        
        tdStream.checkStreamStatus()

        tdLog.info(f"=============== write trigger data")
        tdSql.execute(
            "insert into stream_trigger values ('2025-01-01 00:00:00', 0, 0), ('2025-01-01 00:00:01', 1, 1), ('2025-01-01 00:00:02', 2, 2);"
        )

        tdLog.info(f"=============== check stream result")

        result_sql1 = "select ts, c1, c2 from test.out1"
        tdSql.checkResultsByFunc(
            sql=result_sql1,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 2)
            and tdSql.compareData(0, 2, 0.5)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:01.000")
            and tdSql.compareData(1, 1, 2)
            and tdSql.compareData(1, 2, 1.5),
        )
        
        tdSql.query("desc test.out1")
        tdSql.printResult()
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "c1")
        tdSql.checkData(2, 0, "c2")
        tdSql.checkData(3, 0, "gid")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 1, "BIGINT")
        tdSql.checkData(2, 1, "DOUBLE")
        tdSql.checkData(3, 1, "BIGINT")
        tdSql.checkData(0, 2, "8")
        tdSql.checkData(1, 2, "8")
        tdSql.checkData(2, 2, "8")
        tdSql.checkData(3, 2, "8")
        tdSql.checkData(3, 3, "TAG")

