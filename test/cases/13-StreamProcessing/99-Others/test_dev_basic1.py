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
        self.basic2()

    def basic1(self):
        tdLog.info(f"basic test 1")
        tdSql.error("show streams")
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

    def basic2(self):
        tdLog.info(f"streamTwaError")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"step1")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

        tdSql.execute(
            f"create stream streams1 period(2s) from st partition by tbname, ta options(expired_time(0s)|ignore_disorder|force_output) into streamt as select _tprev_localtime, twa(a) from st where tbname=%%1 and ta=%%2 and ts >= _tprev_localtime and ts < _tlocaltime;"
        )
        tdSql.execute(
            f"create stream streams2 interval(2s) sliding(2s) from st partition by tbname, ta options(force_output) into streamt2 as select _twstart, twa(a) from st where tbname=%%1 and ta=%%2;"
        )
        tdSql.execute(
            f"create stream streams3 interval(2s) sliding(2s) from st partition by tbname, ta options(expired_time(0s)|force_output) into streamt3 as select _twstart, twa(a) from %%trows;"
        )
        tdSql.execute(
            f"create stream streams4 interval(2s) sliding(2s) from st partition by tbname, ta options(max_delay(5s)|force_output) into streamt4 as select _twstart, twa(a) from %%trows;"
        )
        tdSql.execute(
            f"create stream streams5 interval(2s) sliding(2s) from st options(expired_time(0s)|ignore_disorder) into streamt5 as select _twstart, twa(a) from %%trows;"
        )
        tdSql.execute(
            f"create stream streams6 period(2s) from st partition by tbname, ta into streamt6 as select last(ts), twa(a) from %%trows;"
        )
        tdSql.execute(
            f"create stream streams7 session(ts, 2s) from st partition by tbname, ta options(expired_time(0s)|ignore_disorder) into streamt7 as select _twstart, twa(a) from %%trows;"
        )
        tdSql.execute(
            f"create stream streams8 state_window(a) from st partition by tbname, ta options(expired_time(0s)|ignore_disorder) into streamt8 as select _twstart, twa(a) from %%trows;;"
        )
        tdSql.execute(
            f"create stream streams9 interval(2s) sliding(2s) from st partition by tbname, ta options(max_delay(1s)|expired_time(0s)|ignore_disorder|force_output) into streamt9 as select _twstart, elapsed(ts) from st where tbname=%%1 and ta=%%2;"
        )
        tdSql.execute(
            f"create stream streams10 interval(2s, 1s) sliding(1s) from st partition by tbname, ta options(expired_time(0s)|ignore_disorder) into streamt10 as select _twstart, sum(a) from %%trows;"
        )
        tdSql.error(
            f"create stream streams11 interval(2s, 2s) sliding(2s) from st partition by tbname, ta options(expired_time(0s)|ignore_disorder) into streamt11 as select _twstart, avg(a) from %%trows;"
        )
        tdSql.execute(
            f"create stream streams12 interval(2s) sliding(2s) from st options(expired_time(0s)|ignore_disorder) into streams12 as select _twstart, sum(a) from st where ts >= _twstart and ts < _twend;"
        )
        tdSql.execute(
            f"create stream streams13 interval(2s) sliding(2s) from st options(expired_time(0s)|ignore_disorder) into streams10 as select _twstart, sum(a) from st where ts >= _twstart and ts < _twend;"
        )

        tdSql.query("show qdb.streams;")
        tdSql.checkRows(1)
        tdSql.query("show test.streams;")
        tdSql.checkRows(12)
        tdSql.query("select * from information_schema.ins_streams;")
        tdSql.checkRows(13)
        tdSql.query("select * from information_schema.ins_streams where db_name='qdb';")
        tdSql.checkRows(1)
        tdSql.query("select * from information_schema.ins_streams where db_name='test';")
        tdSql.checkRows(12)
        