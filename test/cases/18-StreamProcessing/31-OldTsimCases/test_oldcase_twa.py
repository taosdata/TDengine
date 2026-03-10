import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


class TestStreamOldCaseTwa:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_twa(self):
        """OldTsim: twa

        Verify the behavior of the legacy TWA function in the new streaming computation system

        Catalog:
            - Streams:OldTsimCases

        Since: v3.3.7.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamTwaError.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamTwaFwcFill.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamTwaFwcFillPrimaryKey.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamTwaFwcInterval.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamTwaFwcIntervalPrimaryKey.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/streamTwaInterpFwc.sim

        """

        tdStream.createSnode()

        streams = []
        # streams.append(self.TwaError()) pass
        # streams.append(self.TwaFill1()) pass
        # streams.append(self.TwaFill2()) pass
        # streams.append(self.TwaFill3()) pass
        # streams.append(self.TwaFwcFillPK1()) TD-37328
        # streams.append(self.TwaFwcFillPK2()) pass
        # streams.append(self.TwaFwcInterval1()) TD-37328
        # streams.append(self.TwaFwcInterval2()) TD-37328
        # streams.append(self.TwaFwcInterval3()) TD-37328
        # streams.append(self.TwaFwcIntervalPK()) TD-37328
        # streams.append(self.TwaFwcInterp1()) interp
        # streams.append(self.TwaFwcInterp2()) interp
        # streams.append(self.TwaFwcInterp3()) interp

        tdStream.checkAll(streams)

    class TwaError(StreamCheckItem):
        def __init__(self):
            self.db = "twaerror"

        def create(self):
            tdSql.execute(f"create database twaerror vgroups 1 buffer 8;")
            tdSql.execute(f"use twaerror;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream streams1 period(2s) from st partition by tbname, ta stream_options(expired_time(0s)|ignore_disorder|force_output) into streamt as select _tprev_localtime, twa(a) from st where tbname=%%1 and ta=%%2 and ts >= _tprev_localtime and ts < _tlocaltime;"
            )
            tdSql.execute(
                f"create stream streams2 interval(2s) sliding(2s) from st partition by tbname, ta stream_options(force_output) into streamt2 as select _twstart, twa(a) from st where tbname=%%1 and ta=%%2;"
            )
            tdSql.execute(
                f"create stream streams3 interval(2s) sliding(2s) from st partition by tbname, ta stream_options(expired_time(0s)|force_output) into streamt3 as select _twstart, twa(a) from %%trows;"
            )
            tdSql.execute(
                f"create stream streams4 interval(2s) sliding(2s) from st partition by tbname, ta stream_options(max_delay(5s)|force_output) into streamt4 as select _twstart, twa(a) from %%trows;"
            )
            tdSql.execute(
                f"create stream streams5 interval(2s) sliding(2s) from st stream_options(expired_time(0s)|ignore_disorder) into streamt5 as select _twstart, twa(a) from %%trows;"
            )
            tdSql.execute(
                f"create stream streams6 period(2s) from st partition by tbname, ta into streamt6 as select last(ts), twa(a) from %%trows;"
            )
            tdSql.execute(
                f"create stream streams7 session(ts, 2s) from st partition by tbname, ta stream_options(expired_time(0s)|ignore_disorder) into streamt7 as select _twstart, twa(a) from %%trows;"
            )
            tdSql.execute(
                f"create stream streams8 state_window(a) from st partition by tbname, ta stream_options(expired_time(0s)|ignore_disorder) into streamt8 as select _twstart, twa(a) from %%trows;;"
            )
            tdSql.execute(
                f"create stream streams9 interval(2s) sliding(2s) from st partition by tbname, ta stream_options(max_delay(3s)|expired_time(0s)|ignore_disorder|force_output) into streamt9 as select _twstart, elapsed(ts) from st where tbname=%%1 and ta=%%2;"
            )
            tdSql.execute(
                f"create stream streams10 interval(2s, 1s) sliding(1s) from st partition by tbname, ta stream_options(expired_time(0s)|ignore_disorder) into streamt10 as select _twstart, sum(a) from %%trows;"
            )
            tdSql.error(
                f"create stream streams11 interval(2s, 2s) sliding(2s) from st partition by tbname, ta stream_options(expired_time(0s)|ignore_disorder) into streamt11 as select _twstart, avg(a) from %%trows;"
            )
            tdSql.execute(
                f"create stream streams12 interval(2s) sliding(2s) from st stream_options(expired_time(0s)|ignore_disorder) into streams12 as select _twstart, sum(a) from st where ts >= _twstart and ts < _twend;"
            )
            tdSql.execute(
                f"create stream streams13 interval(2s) sliding(2s) from st stream_options(expired_time(0s)|ignore_disorder) into streams10 as select _twstart, sum(a) from st where ts >= _twstart and ts < _twend;"
            )

        def check1(self):
            tdSql.query("show twaerror.streams;")
            tdSql.checkRows(12)
            tdSql.checkKeyData("streams1", 0, "streams1")
            tdSql.checkKeyData("streams2", 0, "streams2")
            tdSql.checkKeyData("streams3", 0, "streams3")

            tdSql.query(
                "select * from information_schema.ins_streams where db_name = 'twaerror';"
            )
            tdSql.checkRows(12)
            tdSql.checkKeyData("streams5", 1, "twaerror")
            tdSql.checkKeyData("streams6", 1, "twaerror")

    class TwaFill1(StreamCheckItem):
        def __init__(self):
            self.db = "twafill1"

        def create(self):
            tdSql.execute(f"create database twafill1 vgroups 1 buffer 32;")
            tdSql.execute(f"use twafill1;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream streams1 interval(2s) sliding(2s) from st partition by tbname, ta stream_options(expired_time(0s)|ignore_disorder) into streamt as select _twstart, twa(a), twa(b), elapsed(ts), now, timezone() from %%trows;"
            )

        def insert1(self):
            tdSql.execute(
                f"insert into t1 values('2025-06-01 00:00:03', 1, 1, 1) ('2025-06-01 00:00:04', 10, 1, 1)  ('2025-06-01 00:00:07', 20, 2, 2) ('2025-06-01 00:00:08', 30, 3, 3) ('2025-06-01 00:00:11', 40, 4, 4) ('2025-06-01 00:00:12', 50, 5, 5);"
            )
            tdSql.execute(
                f"insert into t2 values('2025-06-01 00:00:05', 1, 1, 1) ('2025-06-01 00:00:06', 10, 1, 1)  ('2025-06-01 00:00:09', 20, 2, 2) ('2025-06-01 00:00:10', 30, 3, 3) ('2025-06-01 00:00:13', 40, 4, 4) ('2025-06-01 00:00:14', 50, 5, 5);"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from twafill1.streamt where ta == 1;",
                lambda: tdSql.getRows() == 5,
            )

            tdSql.checkResultsByFunc(
                f"select * from twafill1.streamt where ta == 2;",
                lambda: tdSql.getRows() == 5,
            )

    class TwaFill2(StreamCheckItem):
        def __init__(self):
            self.db = "twafill2"

        def create(self):
            tdSql.execute(f"create database twafill2 vgroups 1 buffer 32;")
            tdSql.execute(f"use twafill2;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream streams2 period(2s) stream_options(expired_time(0s) | ignore_disorder) into streamt as select cast(_tprev_localtime / 1000000 as timestamp) tp, cast(_tlocaltime / 1000000 as timestamp) tl, cast(_tnext_localtime / 1000000 as timestamp) tn, twa(a), twa(b), elapsed(ts), now, timezone() from st;"
            )

        def insert1(self):
            tdSql.execute(
                f"insert into t1 values(now +  1s, 1, 1, 1)(now +  2s, 10, 1, 1)(now + 3s, 20, 2, 2)(now + 4s, 30, 3, 3)(now + 5s, 30, 3, 3)(now + 6s, 30, 3, 3)(now + 6s, 30, 3, 3)(now + 8s, 30, 3, 3)(now + 9s, 30, 3, 3)(now + 10s, 30, 3, 3);"
            )
            tdSql.execute(
                f"insert into t2 values(now +  1s, 1, 1, 1)(now +  2s, 10, 1, 1)(now + 3s, 20, 2, 2)(now + 4s, 30, 3, 3)(now + 5s, 30, 3, 3)(now + 6s, 30, 3, 3)(now + 6s, 30, 3, 3)(now + 8s, 30, 3, 3)(now + 9s, 30, 3, 3)(now + 10s, 30, 3, 3);"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from twafill2.streamt;",
                lambda: tdSql.getRows() > 0,
                retry=100,
            )

            sql = "select TIMEDIFF(tp, tl), TIMEDIFF(tl, tn), `twa(a)`, `twa(b)`, `elapsed(ts)` from streamt limit 1"
            exp_sql = "select -2000, -2000, twa(a), twa(b), elapsed(ts) from st"
            tdSql.checkResultsBySql(sql, exp_sql, retry=1)

            tdSql.query("select cast(tp as bigint) from streamt limit 1;")
            tcalc = tdSql.getData(0, 0)

            tdSql.query("select cast(ts as bigint) from t1 limit 1")
            tnow = tdSql.getData(0, 0)
            tdLog.info(f"calc:{tcalc}, now:{tnow}")

            if tcalc - tnow > 60000:
                tdLog.exit(f"not triggered within 60000 ms (actual:{tcalc - tnow} ms).")
            else:
                tdLog.info(f"triggered within 60000 ms (actual:{tcalc - tnow} ms).")

    class TwaFill3(StreamCheckItem):
        def __init__(self):
            self.db = "twafill3"

        def create(self):
            tdSql.execute(f"create database twafill3 vgroups 1 buffer 8;")
            tdSql.execute(f"use twafill3;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
            tdSql.execute(
                f"create stream streams3 interval(2s) sliding(2s) from st partition by tbname stream_options(expired_time(0s) | ignore_disorder) into streamt as select ts, case t1 when null then 100 else t1 end t1, case t2 when null then 100 else t2 end t2, case t3 when null then 100 else t3 end t3, t4, t5, ta from (select _twstart ts, twa(a) t1, twa(b) t2, elapsed(ts) t3, now t4, timezone() t5, ta from %%trows);"
            )

        def insert1(self):
            tdSql.execute(
                f"insert into t1 values(now +  3s, 1, 1, 1) (now +  4s, 10, 1, 1)  (now + 7s, 20, 2, 2) (now + 8s, 30, 3, 3);"
            )
            tdSql.execute(
                f"insert into t2 values(now +  4s, 1, 1, 1) (now +  5s, 10, 1, 1)  (now + 8s, 20, 2, 2) (now + 9s, 30, 3, 3);"
            )

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt where ta == 1;",
                lambda: tdSql.getRows() < 5,
            )
            tdSql.checkResultsByFunc(
                f"select * from streamt where ta == 2;",
                lambda: tdSql.getRows() < 5,
            )

    class TwaFwcFillPK1(StreamCheckItem):
        def __init__(self):
            self.db = "twa_fwc_fill_pk1"

        def create(self):
            tdSql.execute(f"create database test vgroups 1 buffer 8;")
            tdSql.execute(f"use test;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int primary key, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream streams1 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into streamt as select _wstart, twa(b), count(*), ta from st partition by tbname, ta interval(2s) fill(prev);"
            )

        def insert1(self):
            tdSql.execute(
                f"insert into t1 values(now +  3s, 1, 1, 1) (now +  3s, 2, 10, 10) (now +  3s, 3, 30, 30);"
            )
            tdSql.execute(
                f"insert into t2 values(now +  4s, 1, 1, 1) (now +  4s, 2, 10, 10) (now +  4s, 3, 30, 30);"
            )

        def check1(self):
            tdSql.query(
                f"select _wstart, twa(b), count(*), ta from t1 partition by tbname, ta interval(2s);"
            )
            query1_data = tdSql.getData(0, 1)

            tdSql.query(
                f"select _wstart, twa(b), count(*), ta from t2 partition by tbname, ta interval(2s);"
            )
            query2_data = tdSql.getData(0, 1)

            tdSql.checkResultsByFunc(
                f"select * from streamt where ta == 1;",
                lambda: tdSql.getRows() >= 6 and tdSql.getData(0, 1) == query1_data,
            )

            tdSql.checkResultsByFunc(
                f"select * from streamt where ta == 2;",
                lambda: tdSql.getRows() >= 6 and tdSql.getData(0, 1) == query2_data,
            )

    class TwaFwcFillPK2(StreamCheckItem):
        def __init__(self):
            self.db = "twafwcfillpk2"

        def create(self):
            tdSql.execute(f"create database twafwcfillpk2 vgroups 1 buffer 8;")
            tdSql.execute(f"use twafwcfillpk2;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int primary key, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
            tdSql.execute(
                f"create stream streams2 interval(2s) sliding(2s) from st partition by tbname, ta stream_options(expired_time(0s) | ignore_disorder | FORCE_OUTPUT | max_delay(3s)) into streamt as select _twstart, twa(b), %%2 as ta2 from %%tbname where ts  >= _twstart and ts < _twend;"
            )

        def insert1(self):
            tdSql.execute(
                f"insert into t1 values(now +  3s, 1, 1, 1) (now +  3s, 2, 10, 10) (now +  3s, 3, 30, 30);"
            )
            tdSql.execute(
                f"insert into t2 values(now +  4s, 1, 1, 1) (now +  4s, 2, 10, 10) (now +  4s, 3, 30, 30);"
            )

        def check1(self):
            tdSql.query(
                f"select _wstart, twa(b), count(*), ta from t1 partition by tbname, ta interval(2s);"
            )
            query1_data = tdSql.getData(0, 1)

            tdSql.query(
                f"select _wstart, twa(b), count(*), ta from t2 partition by tbname, ta interval(2s);"
            )
            query2_data = tdSql.getData(0, 1)

            tdSql.checkResultsByFunc(
                f"select * from streamt where ta == 1;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == query1_data,
            )

            tdLog.info(f"2 sql select * from streamt where ta == 2;")
            tdSql.checkResultsByFunc(
                f"select * from streamt where ta == 2;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == query2_data,
            )

    class TwaFwcInterval1(StreamCheckItem):
        def __init__(self):
            self.db = "twafwcinterval1"

        def create(self):
            tdLog.info(f"step1")
            tdLog.info(f"=============== create database")
            tdSql.execute(f"create database twafwcinterval1 vgroups 1 buffer 16;")
            tdSql.execute(f"use twafwcinterval1;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream streams1 interval(2s) sliding(2s) from st partition by tbname, ta stream_options(expired_time(0s) | ignore_disorder | max_delay(3s)) into streamt as select _twstart, twa(a), _twend from %%tbname where ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            tdSql.execute(
                f"insert into t1 values('2025-08-06 16:16:45.885', 1, 1, 1) ('2025-08-06 16:16:45.985', 5, 10, 10) ('2025-08-06 16:16:46.085', 5, 10, 10)  ('2025-08-06 16:16:47.985', 20, 1, 1) ('2025-08-06 16:16:48.085', 30, 10, 10) ('2025-08-06 16:16:48.185', 40, 10, 10);"
            )
            tdSql.execute(
                f"insert into t2 values('2025-08-06 16:16:45.885', 1, 1, 1) ('2025-08-06 16:16:45.985', 2, 10, 10) ('2025-08-06 16:16:46.085', 30, 10, 10)  ('2025-08-06 16:16:47.985', 10, 1, 1) ('2025-08-06 16:16:48.085', 40, 10, 10) ('2025-08-06 16:16:48.185', 7, 10, 10);"
            )

        def check1(self):
            tdSql.query(f"select _wstart, twa(a), _wend from t1 interval(2s);")
            query1_data01 = tdSql.getData(0, 1)
            query1_data11 = tdSql.getData(1, 1)

            tdSql.query(f"select _wstart, twa(a), _wend from t2 interval(2s);")
            query2_data01 = tdSql.getData(0, 1)
            query2_data11 = tdSql.getData(1, 1)

            tdSql.checkResultsByFunc(
                f"select * from streamt where ta == 1;",
                lambda: tdSql.getRows() >= 2
                and tdSql.getData(0, 1) == query1_data01
                and tdSql.getData(1, 1) == query1_data11,
            )

            tdSql.checkResultsByFunc(
                f"select * from streamt where ta == 2;",
                lambda: tdSql.getRows() >= 2
                and tdSql.getData(0, 1) == query2_data01
                and tdSql.getData(1, 1) == query2_data11,
            )

    class TwaFwcInterval2(StreamCheckItem):
        def __init__(self):
            self.db = "twafwcinterval2"

        def create(self):
            tdSql.execute(f"create database twafwcinterval2 vgroups 4 buffer 16;")
            tdSql.execute(f"use twafwcinterval2;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream streams2 interval(2s) sliding(2s) from st partition by tbname, ta stream_options(expired_time(0s) | ignore_disorder | max_delay(3s)) into streamt as select _wstart, count(*) from %%trows;"
            )

        def insert1(self):
            tdSql.execute(
                f"insert into t1 values(now +  3000a, 1, 1, 1) (now +  3100a, 3, 10, 10) (now +  3200a, 5, 10, 10) (now + 5100a, 20, 1, 1) (now + 5200a, 30, 10, 10) (now + 5300a, 40, 10, 10);"
            )
            tdSql.execute(
                f"insert into t2 values(now +  3000a, 1, 1, 1) (now +  3100a, 3, 10, 10) (now +  3200a, 5, 10, 10) (now + 5100a, 10, 1, 1) (now + 5200a, 40, 10, 10) (now + 5300a, 7, 10, 10);"
            )

        def check1(self):
            tdLog.info(f"sql select _wstart, count(*) from t1 interval(2s) order by 1;")
            tdSql.query(f"select _wstart, count(*) from t1 interval(2s) order by 1;")
            tdSql.printResult()
            query1_data01 = tdSql.getData(0, 1)
            query1_data11 = tdSql.getData(1, 1)

            tdLog.info(f"2 sql select * from streamt where ta == 1 order by 1;")
            tdSql.checkResultsByFunc(
                f"select * from streamt where ta == 1 order by 1;",
                lambda: tdSql.getRows() >= 2
                and tdSql.getData(0, 1) == query1_data01
                and tdSql.getData(1, 1) == query1_data11,
            )

        def insert2(self):
            tdSql.execute(
                f"insert into t1 values(now +  3000a, 1, 1, 1) (now +  3100a, 3, 10, 10) (now +  3200a, 5, 10, 10) (now + 5100a, 20, 1, 1) (now + 5200a, 30, 10, 10) (now + 5300a, 40, 10, 10);"
            )
            tdSql.execute(
                f"insert into t2 values(now +  3000a, 1, 1, 1) (now +  3100a, 3, 10, 10) (now +  3200a, 5, 10, 10) (now + 5100a, 10, 1, 1) (now + 5200a, 40, 10, 10) (now + 5300a, 7, 10, 10);"
            )

        def check2(self):
            tdLog.info(f"sql select _wstart, count(*) from t1 interval(2s) order by 1;")
            tdSql.query(f"select _wstart, count(*) from t1 interval(2s) order by 1;")
            tdSql.printResult()
            query1_data21 = tdSql.getData(2, 1)
            query1_data31 = tdSql.getData(3, 1)

            tdLog.info(f"2 sql select * from streamt where ta == 1 order by 1;")
            tdSql.checkResultsByFunc(
                f"select * from streamt where ta == 1 order by 1;",
                lambda: tdSql.getRows() >= 4
                and tdSql.getData(2, 1) == query1_data21
                and tdSql.getData(3, 1) == query1_data31,
            )

        def insert3(self):
            tdSql.execute(
                f"insert into t1 values(now +  3000a, 1, 1, 1) (now +  3100a, 3, 10, 10) (now +  3200a, 5, 10, 10) (now + 5100a, 20, 1, 1) (now + 5200a, 30, 10, 10) (now + 5300a, 40, 10, 10);"
            )
            tdSql.execute(
                f"insert into t2 values(now +  3000a, 1, 1, 1) (now +  3100a, 3, 10, 10) (now +  3200a, 5, 10, 10) (now + 5100a, 10, 1, 1) (now + 5200a, 40, 10, 10) (now + 5300a, 7, 10, 10);"
            )

        def check3(self):
            tdLog.info(f"sql select _wstart, count(*) from t1 interval(2s) order by 1;")
            tdSql.query(f"select _wstart, count(*) from t1 interval(2s) order by 1;")
            tdSql.printResult()
            query1_data41 = tdSql.getData(4, 1)
            query1_data51 = tdSql.getData(5, 1)

            tdLog.info(f"2 sql select * from streamt where ta == 1 order by 1;")
            tdSql.checkResultsByFunc(
                f"select * from streamt where ta == 1 order by 1;",
                lambda: tdSql.getRows() >= 6
                and tdSql.getData(4, 1) == query1_data41
                and tdSql.getData(5, 1) == query1_data51,
            )

    class TwaFwcInterval3(StreamCheckItem):
        def __init__(self):
            self.db = "twafwcinterval3"

        def create(self):
            tdLog.info(f"======step3")
            tdSql.execute(f"create database twafwcinterval3 vgroups 1 buffer 16;")
            tdSql.execute(f"use twafwcinterval3;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream streams3 interval(2s) sliding(2s) from st partition by tbname, ta stream_options(expired_time(0s) | ignore_disorder | max_delay(3s)) into streamt3 as select _wstart, twa(a), ta from st where tbname=%%tbname and ts >= _twstart and ts < _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(now +  3000a, 1, 1, 1);")
            tdSql.execute(f"flush database test;")
            tdSql.execute(f"insert into t1 values(now +  3001a, 10, 10, 10);")
            tdSql.execute(f"insert into t1 values(now +  13s, 50, 50, 50);")

        def check1(self):
            tdLog.info(
                f"sql select _wstart, twa(a), ta from st partition by tbname, ta interval(10s) order by 1;"
            )
            tdSql.query(
                f"select _wstart, twa(a), ta from st partition by tbname, ta interval(10s) order by 1;"
            )
            tdSql.printResult()
            query_data01 = tdSql.getData(0, 1)

            tdLog.info(f"2 sql select * from streamt3 order by 1;")
            tdSql.checkResultsByFunc(
                f"select * from streamt3 order by 1;",
                lambda: tdSql.getRows() >= 1 and tdSql.getData(0, 1) == query_data01,
            )

    class TwaFwcIntervalPK(StreamCheckItem):
        def __init__(self):
            self.db = "twafwcintervalpk"

        def create(self):
            tdLog.info(f"step1")
            tdLog.info(f"=============== create database")
            tdSql.execute(f"create database twafwcintervalpk vgroups 1 buffer 8;")
            tdSql.execute(f"use twafwcintervalpk;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int primary key, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream streams1 interval(2s) sliding(2s) from st partition by tbname, ta stream_options(expired_time(0s) | ignore_disorder | max_delay(3s)) into streamt as select _wstart, count(*), ta from %%trows;"
            )

        def insert1(self):
            tdSql.execute(
                f"insert into t1 values(now +  3s, 1, 1, 1) (now +  3s, 2, 10, 10) (now +  3s, 3, 30, 30) (now +  11s, 1, 1, 1) (now + 11s, 2, 10, 10);"
            )
            tdSql.execute(
                f"insert into t2 values(now +  4s, 1, 1, 1) (now +  4s, 2, 10, 10) (now +  4s, 3, 30, 30) (now +  12s, 1, 1, 1) (now + 12s, 2, 10, 10);"
            )

        def check1(self):
            tdLog.info(
                f"sql select _wstart, count(*) from st partition by tbname, ta interval(2s);"
            )
            tdSql.query(
                f"select _wstart, count(*) from st partition by tbname, ta interval(2s);"
            )

            tdLog.info(f"2 sql select * from streamt order by")
            tdSql.checkResultsByFunc(
                f"select * from streamt where ta == 1;",
                lambda: tdSql.getRows() >= 2
                and tdSql.getData(0, 1) == 3
                and tdSql.getData(1, 1) == 2,
            )

            tdLog.info(f"2 sql select * from streamt where ta == 2;")
            tdSql.checkResultsByFunc(
                f"select * from streamt where ta == 2;",
                lambda: tdSql.getRows() >= 2
                and tdSql.getData(0, 1) == 3
                and tdSql.getData(1, 1) == 2,
            )

    class TwaFwcInterp1(StreamCheckItem):
        def __init__(self):
            self.db = "twafwcinterp1"

        def create(self):

            tdLog.info(f"step1")
            tdLog.info(f"=============== create database")
            tdSql.execute(f"create database twafwcinterp1 vgroups 4 buffer 8;")
            tdSql.execute(f"use twafwcinterp1;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stream streams1 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into streamt1 as select _wstart, count(a), sum(b), now, timezone(), ta from st partition by tbname, ta interval(2s) fill(value, 100, 200);"
            )
            tdSql.execute(
                f"create stream streams2 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into streamt2 as select _wstart, count(a), twa(a), sum(b), now, timezone(), ta from st partition by tbname, ta interval(2s) fill(prev);"
            )
            tdSql.execute(
                f"create stream streams3 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into streamt3 as select _irowts, interp(a), interp(b), interp(c), now, timezone(), ta from st partition by tbname, ta every(2s) fill(value, 100, 200, 300);"
            )
            tdSql.execute(
                f"create stream streams4 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into streamt4 as select _irowts, interp(a), interp(b), interp(c), now, timezone(), ta from st partition by tbname, ta every(2s) fill(prev);"
            )
            tdSql.execute(
                f"create stream streams5 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into streamt5 as select _wstart, count(a), sum(b), now, timezone(), ta from st partition by tbname, ta interval(2s);"
            )

        def insert1(self):
            tdSql.execute(
                f"insert into t1 values(now +  3000a, 1, 1, 1) (now +  3100a, 5, 10, 10) (now +  3200a, 5, 10, 10)  (now + 5100a, 20, 1, 1) (now + 5200a, 30, 10, 10) (now + 5300a, 40, 10, 10);"
            )
            tdSql.execute(
                f"insert into t2 values(now +  3000a, 1, 1, 1) (now +  3100a, 2, 10, 10) (now +  3200a, 30, 10, 10) (now + 5100a, 10, 1, 1) (now + 5200a, 40, 10, 10) (now + 5300a, 7, 10, 10);"
            )

        def check1(self):
            tdLog.info(
                f"sql  select _wstart, count(a), sum(b), now, timezone(), ta from st partition by tbname, ta interval(2s) order by 1, 2;"
            )
            tdSql.query(
                f" select _wstart, count(a), sum(b), now, timezone(), ta from st partition by tbname, ta interval(2s) order by 1, 2;"
            )
            tdSql.printResult()
            query1_rows = tdSql.getRows()
            query1_data01 = tdSql.getData(0, 1)

            tdLog.info(
                f"select last(*) from (select _wstart, count(a), sum(b), now, timezone(), ta from st partition by tbname, ta interval(2s)) order by 1, 2 desc;"
            )
            tdSql.query(
                f" select _wstart, count(a), sum(b), now, timezone(), ta from st partition by tbname, ta interval(2s) order by 1, 2 desc;"
            )

            tdLog.info(f"sql select * from streamt1 order by 1, 2;")
            tdSql.checkResultsByFunc(
                f"select * from streamt1 order by 1, 2;",
                lambda: tdSql.getRows() >= query1_rows
                and tdSql.getData(0, 1) == query1_data01,
            )

            tdLog.info(f"sql select * from streamt2 order by 1, 2;")
            tdSql.checkResultsByFunc(
                f"select * from streamt2 order by 1, 2;",
                lambda: tdSql.getRows() >= query1_rows
                and tdSql.getData(0, 1) == query1_data01,
            )

            tdLog.info(f"sql select * from streamt3 order by 1, 2;")
            tdSql.checkResultsByFunc(
                f"select * from streamt3 order by 1, 2;",
                lambda: tdSql.getRows() >= query1_rows,
            )

            tdLog.info(f"sql select * from streamt4 order by 1, 2;")
            tdSql.checkResultsByFunc(
                f"select * from streamt4 order by 1, 2;",
                lambda: tdSql.getRows() >= query1_rows,
            )

            tdLog.info(f"sql select * from streamt5 order by 1, 2;")
            tdSql.checkResultsByFunc(
                f"select * from streamt5 order by 1, 2;",
                lambda: tdSql.getRows() >= query1_rows
                and tdSql.getData(0, 1) == query1_data01,
            )

    class TwaFwcInterp2(StreamCheckItem):
        def __init__(self):
            self.db = "twafwcinterp2"

        def create(self):
            tdSql.execute(f"create database twafwcinterp2 vgroups 4 buffer 8;")
            tdSql.execute(f"use twafwcinterp2;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f'create stream streams6 trigger FORCE_WINDOW_CLOSE IGNORE EXPIRED 1 IGNORE UPDATE 1 into streamt6 TAGS(cc int) SUBTABLE(concat(concat("tbn-", tbname), "_1")) as select _irowts, interp(a), _isfilled as a1 from st partition by tbname, b as cc every(2s) fill(prev);'
            )
            tdSql.execute(
                f'create stream streams7 trigger FORCE_WINDOW_CLOSE IGNORE EXPIRED 1 IGNORE UPDATE 1 into streamt7 TAGS(cc int) SUBTABLE(concat(concat("tbn-", tbname), "_2")) as select _wstart, twa(a) from st partition by tbname, b as cc interval(2s) fill(NULL);'
            )
            tdSql.execute(
                f'create stream streams8 trigger FORCE_WINDOW_CLOSE IGNORE EXPIRED 1 IGNORE UPDATE 1 into streamt8 TAGS(cc int) SUBTABLE(concat(concat("tbn-", tbname), "_3")) as select _wstart, count(a) from st partition by tbname, b as cc interval(2s);'
            )

        def insert1(self):

            tdSql.execute(f"insert into t1 values(now +  3s, 1, 1, 1);")

        def check1(self):
            tdLog.info(f"2 sql select cc,* from streamt6;")
            tdSql.checkResultsByFunc(
                f"select cc,* from streamt6;",
                lambda: tdSql.getRows() >= 2 and tdSql.getData(0, 0) == 1,
            )

            tdLog.info(
                f'3 sql select * from information_schema.ins_tables where stable_name = "streamt6";'
            )
            tdSql.query(
                f'select * from information_schema.ins_tables where stable_name = "streamt6";'
            )
            tdSql.checkRows(1)

            tdLog.info(
                f'4 sql select * from information_schema.ins_tables where stable_name = "streamt6" and table_name like "tbn-t1_1%";'
            )
            tdSql.query(
                f'select * from information_schema.ins_tables where stable_name = "streamt6" and table_name like "tbn-t1_1%";'
            )
            tdSql.checkRows(1)

            tdLog.info(f"2 sql select cc,* from streamt7;")
            tdSql.checkResultsByFunc(
                f"select cc,* from streamt7;",
                lambda: tdSql.getRows() >= 2 and tdSql.getData(0, 0) == 1,
            )

            tdLog.info(
                f'3 sql select * from information_schema.ins_tables where stable_name = "streamt7";'
            )
            tdSql.query(
                f'select * from information_schema.ins_tables where stable_name = "streamt7";'
            )
            tdSql.checkRows(1)

            tdLog.info(
                f'4 sql select * from information_schema.ins_tables where stable_name = "streamt7" and table_name like "tbn-t1_2%";'
            )
            tdSql.query(
                f'select * from information_schema.ins_tables where stable_name = "streamt7" and table_name like "tbn-t1_2%";'
            )
            tdSql.checkRows(1)

            tdLog.info(f"2 sql select cc,* from streamt8;")
            tdSql.checkResultsByFunc(
                f"select cc,* from streamt8;",
                lambda: tdSql.getRows() >= 1 and tdSql.getData(0, 0) == 1,
            )

            tdLog.info(
                f'3 sql select * from information_schema.ins_tables where stable_name = "streamt8";'
            )
            tdSql.query(
                f'select * from information_schema.ins_tables where stable_name = "streamt8";'
            )
            tdSql.checkRows(1)

            tdLog.info(
                f'4 sql select * from information_schema.ins_tables where stable_name = "streamt8" and table_name like "tbn-t1_3%";'
            )
            tdSql.query(
                f'select * from information_schema.ins_tables where stable_name = "streamt8" and table_name like "tbn-t1_3%";'
            )
            tdSql.checkRows(1)

    class TwaFwcInterp3(StreamCheckItem):
        def __init__(self):
            self.db = "twafwcinterp3"

        def create(self):

            tdLog.info(f"=============== create database")
            tdSql.execute(f"create database twafwcinterp3 vgroups 4 buffer 8;")
            tdSql.execute(f"use twafwcinterp3;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1234567890t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t1234567890t2 using st tags(2, 2, 2);")

            tdSql.execute(
                f"create stable streamt9(ts timestamp, a varchar(10), b tinyint, c tinyint) tags(ta varchar(3), cc int, tc int);"
            )
            tdSql.execute(
                f"create stable streamt10(ts timestamp, a varchar(10), b tinyint, c tinyint) tags(ta varchar(3), cc int, tc int);"
            )
            tdSql.execute(
                f"create stable streamt11(ts timestamp, a varchar(10), b tinyint, c tinyint) tags(ta varchar(3), cc int, tc int);"
            )

            tdSql.execute(
                f'create stream streams9 trigger FORCE_WINDOW_CLOSE IGNORE EXPIRED 1 IGNORE UPDATE 1 into streamt9 TAGS(cc, ta) SUBTABLE(concat(concat("tbn-", tbname), "_1")) as select _irowts, interp(a), _isfilled as a1, interp(b) from st partition by tbname as ta, b as cc every(2s) fill(value, 100000, 200000);'
            )
            tdSql.execute(
                f'create stream streams10 trigger FORCE_WINDOW_CLOSE IGNORE EXPIRED 1 IGNORE UPDATE 1 into streamt10 TAGS(cc, ta) SUBTABLE(concat(concat("tbn-", tbname), "_2")) as select _wstart, twa(a), sum(b), max(c) from st partition by tbname as ta, b as cc interval(2s) fill(NULL);'
            )
            tdSql.execute(
                f'create stream streams11 trigger FORCE_WINDOW_CLOSE IGNORE EXPIRED 1 IGNORE UPDATE 1 into streamt11 TAGS(cc, ta) SUBTABLE(concat(concat("tbn-", tbname), "_3")) as select _wstart, count(a), avg(c), min(b) from st partition by tbname as ta, b as cc interval(2s);'
            )

        def insert1(self):
            tdSql.execute(f"insert into t1234567890t1 values(now +  3s, 100000, 1, 1);")

        def check1(self):
            tdLog.info(f"2 sql select cc, ta, * from streamt9;")
            tdSql.checkResultsByFunc(
                f"select cc, ta, * from streamt9;",
                lambda: tdSql.getRows() >= 2
                and tdSql.getData(0, 0) == 1
                and tdSql.getData(0, 1) == "t12"
                and tdSql.getData(0, 3) == "100000"
                and tdSql.getData(0, 4) == 1
                and tdSql.getData(0, 5) == 64,
            )

            tdLog.info(
                f'3 sql select * from information_schema.ins_tables where stable_name = "streamt9";'
            )
            tdSql.query(
                f'select * from information_schema.ins_tables where stable_name = "streamt9";'
            )
            tdSql.checkRows(1)

            tdLog.info(
                f'4 sql select * from information_schema.ins_tables where stable_name = "streamt9" and table_name like "tbn-t1234567890t1_1%";'
            )
            tdSql.query(
                f'select * from information_schema.ins_tables where stable_name = "streamt9" and table_name like "tbn-t1234567890t1_1%";'
            )
            tdSql.checkRows(1)

            tdLog.info(f"2 sql select cc, ta, * from streamt10;")
            tdSql.checkResultsByFunc(
                f"select cc, ta, * from streamt10;",
                lambda: tdSql.getRows() >= 2
                and tdSql.getData(0, 0) == 1
                and tdSql.getData(0, 1) == "t12"
                and tdSql.getData(0, 3) == "100000"
                and tdSql.getData(0, 4) == 1
                and tdSql.getData(0, 5) == 1,
            )

            tdLog.info(
                f'3 sql select * from information_schema.ins_tables where stable_name = "streamt10";'
            )
            tdSql.query(
                f'select * from information_schema.ins_tables where stable_name = "streamt10";'
            )
            tdSql.checkRows(1)

            tdLog.info(
                f'4 sql select * from information_schema.ins_tables where stable_name = "streamt10" and table_name like "tbn-t1234567890t1_2%";'
            )
            tdSql.query(
                f'select * from information_schema.ins_tables where stable_name = "streamt10" and table_name like "tbn-t1234567890t1_2%";'
            )
            tdSql.checkRows(1)

            tdLog.info(f"2 sql select cc, ta,* from streamt11;")
            tdSql.checkResultsByFunc(
                f"select cc, ta,* from streamt11;",
                lambda: tdSql.getRows() >= 1
                and tdSql.getData(0, 0) == 1
                and tdSql.getData(0, 1) == "t12"
                and tdSql.getData(0, 3) == "1"
                and tdSql.getData(0, 4) == 1
                and tdSql.getData(0, 5) == 1,
            )

            tdLog.info(
                f'3 sql select * from information_schema.ins_tables where stable_name = "streamt11";'
            )
            tdSql.query(
                f'select * from information_schema.ins_tables where stable_name = "streamt11";'
            )
            tdSql.checkRows(1)

            tdLog.info(
                f'4 sql select * from information_schema.ins_tables where stable_name = "streamt11" and table_name like "tbn-t1234567890t1_3%";'
            )
            tdSql.query(
                f'select * from information_schema.ins_tables where stable_name = "streamt11" and table_name like "tbn-t1234567890t1_3%";'
            )
            tdSql.checkRows(1)

            tdLog.info(f"end")
