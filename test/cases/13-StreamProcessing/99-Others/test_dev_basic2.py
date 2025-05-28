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

        tdStream.dropAllStreamsAndDbs()
        tdStream.createSnode()

        tdLog.info(f"=============== create database")
        tdSql.prepare(dbname="test", vgroups=1)

        tdLog.info(f"=============== create super table")
        tdSql.execute(f"create stable stb (ts timestamp, v1 int, v2 int) tags(t1 int);")

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
        sql = "create table stream_trigger (ts timestamp, v1 int, v2 int);"
        tdSql.execute(sql)
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

        streams = [
            self.StreamItem(sql1, self.checks1),
            self.StreamItem(sql2, self.checks2),
            self.StreamItem(sql3, self.checks3),
            self.StreamItem(sql4, self.checks4),
            self.StreamItem(sql6, self.checks6),
            self.StreamItem(sql7, self.checks7),
            self.StreamItem(sql8, self.checks8),
        ]

        for stream in streams:
            tdSql.execute(stream.sql)
        tdStream.checkStreamStatus()

        tdLog.info(f"=============== write trigger data")
        sql = "insert into stream_trigger values ('2025-01-01 00:00:00', 0, 0), ('2025-01-01 00:00:01', 1, 1), ('2025-01-01 00:00:02', 2, 2);"
        tdSql.execute(sql)

        tdLog.info(f"=============== check stream result")
        for stream in streams:
            stream.check()

    def checks1(self):
        result_sql = "select ts, c1, c2 from test.out1"
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

    def checks2(self):
        result_sql = "select ts, c1, `avg(v1)` from test.out2"
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

    def checks3(self):
        result_sql = "select ts, c1, c2 from test.out3"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 6)
            and tdSql.compareData(0, 2, 1.5)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:01.000")
            and tdSql.compareData(1, 1, 6)
            and tdSql.compareData(1, 2, 1.5),
        )

    def checks4(self):
        result_sql = "select ts, c1, c2 from test.out4"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, 6)
            and tdSql.compareData(0, 2, 1.5)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:01.000")
            and tdSql.compareData(1, 1, 6)
            and tdSql.compareData(1, 2, 1.5),
        )

    def checks6(self):
        result_sql = "select * from test.out6"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.999")
            and tdSql.compareData(0, 2, 6)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:01.999")
            and tdSql.compareData(1, 2, 6),
        )

    def checks7(self):
        result_sql = "select * from test.out7"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00")
            and tdSql.compareData(0, 2, 6)
            and tdSql.compareData(0, 3, "stream_trigger")
            and tdSql.compareData(1, 0, "2025-01-01 00:00:01")
            and tdSql.compareData(1, 2, 6)
            and tdSql.compareData(1, 3, "stream_trigger"),
        )

    def checks8(self):
        result_sql = "select ts, c1, c2, ts2 from test.out8"
        tdSql.checkResultsByFunc(
            sql=result_sql,
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

    class StreamItem:
        def __init__(self, sql, checkfunc):
            self.sql = sql
            self.checkfunc = checkfunc

        def check(self):
            self.checkfunc()
