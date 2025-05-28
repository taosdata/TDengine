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
        sql1 = "create stream s1 interval(1s) sliding(1s) from stream_trigger partition by tbname into out1 tags (gid bigint as _tgrpid)    as select _twstart ts, count(*) c1, avg(v1) c2 from stb where ts >= _twstart and ts < _twend;"
        sql2 = "create stream s2 interval(1s) sliding(1s) from stream_trigger partition by tbname into out2                                 as select _twstart ts, count(*) c1, avg(v1)    from stb where ts >= _twstart and ts < _twend;"
        sql3 = "create stream s3 state_window (v1)        from stream_trigger partition by tbname into out3                                 as select _twstart ts, count(*) c1, avg(v1) c2 from stb;"
        sql4 = "create stream s4 state_window (v1)        from stream_trigger                     into out4                                 as select _twstart ts, count(*) c1, avg(v1) c2 from stb;"
        sql6 = "create stream s6 sliding (1s)             from stream_trigger                     into out6                                 as select _tcurrent_ts, now, count(v1) from stb;"
        sql7 = "create stream s7 state_window (v1)        from stream_trigger partition by tbname options(fill_history_first(1)) into out7  as select _twstart, avg(v1), count(v1) from stb;"
        sql8 = "create stream s8 state_window (v1)        from stream_trigger partition by tbname into out8                                 as select _twstart ts, count(*) c1, avg(v1) c2, _twstart + 1 as ts2 from stb;"

        tdSql.execute(sql1)
        tdSql.execute(sql2)
        tdSql.execute(sql3)
        tdSql.execute(sql4)
        tdSql.execute(sql6)
        tdSql.execute(sql7)

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

        result_sql2 = "select ts, c1, `avg(v1)` from test.out2"
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

        result_sql3 = "select ts, c1, c2 from test.out3"
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

        result_sql4 = "select ts, c1, c2 from test.out4"
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

        result_sql6 = "select * from test.out6"
        tdSql.checkResultsByFunc(
            sql=result_sql6,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.999")
            and tdSql.compareData(0, 2, 6)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:01.999")
            and tdSql.compareData(1, 2, 6),
        )

        result_sql7 = "select * from test.out7"
        tdSql.checkResultsByFunc(
            sql=result_sql7,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
            and tdSql.compareData(0, 2, 6)
            and tdSql.compareData(0, 3, "stream_trigger")
            and tdSql.compareData(1, 0, "2025-01-01 00:00:01")
            and tdSql.compareData(1, 2, 6)
            and tdSql.compareData(1, 3, "stream_trigger"),
        )

        result_sql8 = "select ts, c1, c2, ts2 from test.out8"
        tdSql.checkResultsByFunc(
            sql=result_sql8,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 6)
            and tdSql.compareData(0, 3, "2025-01-01 00:00:00.001")
            and tdSql.compareData(0, 2, 1.5)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:01.000")
            and tdSql.compareData(1, 1, 6)
            and tdSql.compareData(1, 2, 1.5)
            and tdSql.compareData(1, 3, "2025-01-01 00:00:01.001"),
        )
