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
        self.udTableAndCol0()

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
        tdSql.checkData(0, 0, "s1")

        tdSql.query("show test.streams;")
        tdSql.checkRows(12)
        tdSql.checkKeyData("streams1", 0, "streams1")
        tdSql.checkKeyData("streams2", 0, "streams2")
        tdSql.checkKeyData("streams3", 0, "streams3")

        tdSql.query("select * from information_schema.ins_streams;")
        tdSql.checkRows(13)
        tdSql.checkKeyData("s1", 1, "qdb")
        tdSql.checkKeyData("streams5", 1, "test")
        tdSql.checkKeyData("streams6", 1, "test")

        tdSql.query("select * from information_schema.ins_streams where db_name='qdb';")
        tdSql.checkRows(1)
        tdSql.query(
            "select * from information_schema.ins_streams where db_name='test';"
        )
        tdSql.checkRows(12)

        tdStream.dropAllStreamsAndDbs()
        tdSql.query("select * from information_schema.ins_streams;")
        tdSql.checkRows(0)
        tdSql.query("show databases")
        tdSql.checkRows(2)

    def udTableAndCol0(self):
        tdLog.info("udTableAndCol0")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info("===== step2")
        tdLog.info("===== table name")

        tdSql.execute("create database test vgroups 1;")
        tdSql.execute("use test;")
        tdSql.execute(
            "create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
        )
        tdSql.execute("create table t1 using st tags(1, 1, 1);")
        tdSql.execute("create table t2 using st tags(2, 2, 2);")

        tdSql.error(
            "create stream streams1 interval(10s) sliding(10s) from st into streamt1(a, b, c, d) as select _twstart, count(*) c1, max(a) from st where ts >= _twstart and ts < _twend;"
        )
        tdSql.error(
            "create stream streams2 interval(10s) sliding(10s) from st into streamt2(a, b) as select _twstart, count(*) c1, max(a) from st where ts >= _twstart and ts < _twend;"
        )
        tdSql.error(
            "create stream streams3 interval(10s) sliding(10s) from st into streamt3(a, b) as select count(*) c1, max(a) from st where ts >= _twstart and ts < _twend;"
        )
        tdSql.execute(
            "create stream streams4 interval(10s) sliding(10s) from st into streamt4(a, b, c) as select _twstart, count(*) c1, max(a) from st where ts >= _twstart and ts < _twend;"
        )
        tdSql.execute(
            "create stream streams5 interval(10s) sliding(10s) from st partition by tbname into streamt5(a, b, c) as select _twstart, count(*) c1, max(a) from st where ts >= _twstart and ts < _twend partition by tbname;"
        )
        tdSql.execute(
            "create stream streams6 interval(10s) sliding(10s) from st partition by tbname into streamt6(a, b, c) tags(tbn varchar(60) as %%tbname) as select _twstart, count(*) c1, max(a) from st where ts >= _twstart and ts < _twend partition by tbname tbn;"
        )
        tdSql.execute(
            "create stream streams7 interval(10s) sliding(10s) from st partition by tbname into streamt7(a, b primary key, c) tags(tbn varchar(60) as %%tbname) as select _twstart, count(*) c1, max(a) from st where ts >= _twstart and ts < _twend partition by tbname tbn;"
        )
        tdSql.error(
            "create stream streams8 interval(10s) sliding(10s) from st into streamt8(a, b, c primary key) as select _twstart, count(*) c1, max(a) from st where ts >= _twstart and ts < _twend;"
        )
        tdSql.error(
            "create stream streams9 interval(10s) sliding(10s) from st into streamt9(a primary key, b, c) as select _twstart, count(*) c1, max(a) from st where ts >= _twstart and ts < _twend;"
        )
        tdSql.error(
            "create stream streams10 interval(10s) sliding(10s) from st into streamt10(a, b primary key, c) as select count(*) c1, max(a), max(b) from st where ts >= _twstart and ts < _twend;"
        )
        tdSql.error(
            "create stream streams11 interval(10s) sliding(10s) from st into streamt11(a, b, a) as select _twstart, count(*) c1, max(b) from st where ts >= _twstart and ts < _twend;"
        )
        tdSql.error(
            "create stream streams12 interval(10s) sliding(10s) from st partition by tbname into streamt12(a, b, c, d) tags(c varchar(60) as %%tbname) as select _twstart, count(*) c1, max(a), max(b) from st where tbname=%%tbname and ts >= _twstart and ts < _twend;"
        )
        tdSql.error(
            "create stream streams13 interval(10s) sliding(10s) from st partition by tbname, tc options(max_delay(1s)) into streamt13(a, b, c, d) tags(tx varchar(60)) as select _twstart, count(*) c1, max(a) c2, max(b) from %%trows where ts >= _twstart and ts < _twend;"
        )
        tdSql.error(
            "create stream streams14 interval(10s) sliding(10s) from st partition by tbname, tc into streamt14 tags(tx varchar(60) as tc) as select _twstart, count(*) tc, max(a) c1, max(b) from st where tbname=%%tbname and tc=%%2 and ts >= _twstart and ts < _twend;"
        )
        tdSql.execute(
            "create stream streams14 interval(10s) sliding(10s) from st partition by tbname, tc into streamt14 tags(tx int as tc) as select _twstart, count(*) tc, max(a) c1, max(b) from st where tbname=%%tbname and tc=%%2 and ts >= _twstart and ts < _twend;"
        )
        tdSql.execute(
            "create stream streams15 interval(10s) sliding(10s) from st partition by tbname, tc into streamt15 tags(tx int as tc, tz varchar(50) as '12') as select _twstart, count(*) c1, max(a) from st where tbname=%%1 and tc=%%2 and ts >= _twstart and ts < _twend;"
        )
        tdSql.execute(
            "create stream streams16 interval(10s) sliding(10s) from st partition by tbname, tc into streamt16 tags(tx int as tc, tb varchar(32) as %%tbname) as select _twstart, count(*) c1, max(a) from st where tbname=%%1 and tc=%%2 and ts >= _twstart and ts < _twend;"
        )
        
        tdStream.checkStreamStatus()

        tdSql.checkTableSchema(
            dbname="test",
            tbname="streamt5;",
            schema=[
                ["a", "TIMESTAMP", 8, ""],
                ["b", "BIGINT", 8, ""],
                ["c", "INT", 4, ""],
                ["tag_tbname", "VARCHAR", 270, "TAG"],
            ],
        )

        tdSql.checkTableSchema(
            dbname="test",
            tbname="streamt6;",
            schema=[
                ["a", "TIMESTAMP", 8, ""],
                ["b", "BIGINT", 8, ""],
                ["c", "INT", 4, ""],
                ["tbn", "VARCHAR", 60, "TAG"],
            ],
        )

        tdSql.checkTableSchema(
            dbname="test",
            tbname="streamt7;",
            schema=[
                ["a", "TIMESTAMP", 8, ""],
                ["b", "BIGINT", 8, ""],
                ["c", "INT", 4, ""],
                ["tbn", "VARCHAR", 60, "TAG"],
            ],
        )

        tdSql.checkTableSchema(
            dbname="test",
            tbname="streamt14;",
            schema=[
                ["_twstart", "TIMESTAMP", 8, ""],
                ["tc", "BIGINT", 8, ""],
                ["c1", "INT", 4, ""],
                ["max(b)", "INT", 4, ""],
                ["tx", "INT", 4, "TAG"],
            ],
        )

        tdSql.checkTableSchema(
            dbname="test",
            tbname="streamt15;",
            schema=[
                ["_twstart", "TIMESTAMP", 8, ""],
                ["c1", "BIGINT", 8, ""],
                ["max(a)", "INT", 4, ""],
                ["tx", "INT", 4, "TAG"],
                ["tz", "VARCHAR", 50, "TAG"],
            ],
        )

        tdSql.checkTableSchema(
            dbname="test",
            tbname="streamt16;",
            schema=[
                ["_twstart", "TIMESTAMP", 8, ""],
                ["c1", "BIGINT", 8, ""],
                ["max(a)", "INT", 4, ""],
                ["tx", "INT", 4, "TAG"],
                ["tb", "VARCHAR", 32, "TAG"],
            ],
        )

        tdSql.checkTableSchema(
            dbname="test",
            tbname="streamt5;",
            schema=[
                ["a", "TIMESTAMP", 8, ""],
                ["b", "BIGINT", 8, ""],
                ["c", "INT", 4, ""],
                ["tag_tbname", "VARCHAR", 270, "TAG"],
            ],
        )

        tdSql.execute("create database test1 vgroups 1;")
        tdSql.execute("use test1;")
        tdSql.execute(
            "create stable st(ts timestamp, a int primary key, b int, c int) tags(ta int, tb int, tc int);"
        )
        tdSql.execute("create table t1 using st tags(1, 1, 1);")
        tdSql.execute("create table t2 using st tags(2, 2, 2);")

        tdSql.execute(
            "create stream streams16 interval(10s) sliding(10s) from st into streamt16 as select _twstart, count(*) c1, max(a) from st partition by tbname tc state_window(b);"
        )
        tdSql.execute(
            "create stream streams17 interval(10s) sliding(10s) from st into streamt17 as select _twstart, count(*) c1, max(a) from st partition by tbname tc event_window start with a = 0 end with a = 9;"
        )
        tdSql.execute(
            "create stream streams18 interval(10s) sliding(10s) from st  options(watermark(10s)) into streamt18 as select _twstart, count(*) c1, max(a) from st partition by tbname tc count_window(2);"
        )

        tdLog.info("===== step2")
        tdLog.info("===== scalar")

        tdStream.dropAllStreamsAndDbs()
        tdSql.execute("create database test2 vgroups 1;")
        tdSql.execute("use test2;")

        tdSql.execute("create table t1 (ts timestamp, a int, b int);")
        tdSql.execute(
            "create table rst(ts timestamp, a int primary key, b int) tags(ta varchar(100));"
        )
        tdSql.execute('create table rct1 using rst tags("aa");')
        tdSql.execute(
            "create table rst6(ts timestamp, a int primary key, b int) tags(ta varchar(100));"
        )
        tdSql.execute(
            "create table rst7(ts timestamp, a int primary key, b int) tags(ta varchar(100));"
        )
        tdSql.execute(
            "create stream streams19 sliding(1s) from t1 into streamt19 as select ts, a, b from t1;"
        )
        tdSql.execute(
            "create stream streams20 sliding(1s) from t1 into streamt20(ts, a primary key, b) as select ts, a, b from t1;"
        )
        tdSql.error(
            "create stream streams21 sliding(1s) from t1 into rst as select ts, a, b from t1;"
        )
        tdSql.execute(
            "create stream streams22 sliding(1s) from rct1 into streamt22 as select ts, 1, b from rct1;"
        )
        tdSql.execute(
            "create stream streams23 sliding(1s) from rct1 into streamt23 as select ts, a, b from rct1;"
        )
        tdSql.execute(
            "create stream streams24 sliding(1s) from rct1 into streamt24(ts, a primary key, b) as select ts, a, b from rct1;"
        )
        tdSql.error(
            "create stream streams25 sliding(1s) from rct1 into rst6 as select ts, a, b from rct1;"
        )
        tdSql.error(
            "create stream streams26 sliding(1s) from rct1 into rst7 as select ts, 1, b from rct1;"
        )
        tdSql.execute(
            "create stream streams27 sliding(1s) from rct1 into streamt27(ts, a primary key, b) as select ts, 1, b from rct1;"
        )

        tdLog.info("======over")