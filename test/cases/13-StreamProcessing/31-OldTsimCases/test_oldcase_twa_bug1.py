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
        """Stream twa

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldTsimCases

        Since: v3.0.0.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaError.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaFwcFill.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaFwcFillPrimaryKey.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaFwcInterval.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaFwcIntervalPrimaryKey.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/streamTwaInterpFwc.sim

        """

        tdStream.createSnode()

        streams = []
        streams.append(self.TwaFwcFill2())
        tdStream.checkAll(streams)

    class TwaFwcFill2(StreamCheckItem):
        def __init__(self):
            self.db = "FwcFIll2"

        def create(self):
            tdSql.execute(f"create database FwcFIll2 vgroups 1 buffer 32;")
            tdSql.execute(f"use FwcFIll2;")

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
                f"select * from FwcFIll2.streamt;",
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

            if tcalc - tnow > 20000:
                tdLog.exit(f"not triggered within 20000 ms (actual:{tcalc - tnow} ms).")
            else:
                tdLog.info(f"triggered within 20000 ms (actual:{tcalc - tnow} ms).")
