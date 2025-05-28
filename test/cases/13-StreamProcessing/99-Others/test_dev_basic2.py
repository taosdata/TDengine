import time
import math
from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamDevBasic2:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_dev_basic2(self):
        """basic test 2

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
            "insert into t1 using stream_query tags(1) values ('2025-01-01 00:00:00'    , 0, 0);",
            "insert into t2 using stream_query tags(2) values ('2025-01-01 00:00:00.102', 1, 0);",
            "insert into t1 using stream_query tags(1) values ('2025-01-01 00:00:01'    , 1, 1);",
            "insert into t2 using stream_query tags(2) values ('2025-01-01 00:00:01.400', 2, 1);",
            "insert into t1 using stream_query tags(1) values ('2025-01-01 00:00:02'    , 2, 2);",
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
        sql1 = "create stream s1 interval(1s) sliding(1s) from stream_trigger partition by tbname into stream_out1 tags (gid bigint as _tgrpid) as select _twstart ts, count(*) c1, avg(id) c2 from stream_query where ts >= _twstart and ts < _twend;"
        sql2 = "create stream s2 interval(1s) sliding(1s) from stream_trigger partition by tbname into stream_out2                              as select _twstart ts, count(*) c1, avg(id)    from stream_query where ts >= _twstart and ts < _twend;"
        sql3 = "create stream s3 state_window (id)        from stream_trigger partition by tbname into stream_out3                              as select _twstart ts, count(*) c1, avg(id) c2 from stream_query;"
        sql4 = "create stream s4 state_window (id)        from stream_trigger                     into stream_out4                              as select _twstart ts, count(*) c1, avg(id) c2 from stream_query;"

        tdSql.execute(sql1)
        tdSql.execute(sql2)
        tdSql.execute(sql3)
        tdSql.execute(sql4)

        tdStream.checkStreamStatus()

        tdLog.info(f"=============== write trigger data")
        tdSql.execute(
            "insert into stream_trigger values ('2025-01-01 00:00:00', 0, 0), ('2025-01-01 00:00:01', 1, 1), ('2025-01-01 00:00:02', 2, 2);"
        )

        tdLog.info(f"=============== check stream result")

        result_sql1 = "select ts, c1, c2 from test.stream_out1"
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

        result_sql2 = "select ts, c1, `avg(id)` from test.stream_out2"
        tdSql.checkResultsByFunc(
            sql=result_sql2,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 2)
            and tdSql.compareData(0, 2, 0.5)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:01.000")
            and tdSql.compareData(1, 1, 2)
            and tdSql.compareData(1, 2, 1.5),
        )
        
        result_sql3 = "select ts, c1, c2 from test.stream_out3"
        tdSql.checkResultsByFunc(
            sql=result_sql3,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 6)
            and tdSql.compareData(0, 2, 1.5)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:01.000")
            and tdSql.compareData(1, 1, 6)
            and tdSql.compareData(1, 2, 1.5),
        )
        
        result_sql4 = "select ts, c1, c2 from test.stream_out4"
        tdSql.checkResultsByFunc(
            sql=result_sql4,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 6)
            and tdSql.compareData(0, 2, 1.5)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:01.000")
            and tdSql.compareData(1, 1, 6)
            and tdSql.compareData(1, 2, 1.5),
        )
