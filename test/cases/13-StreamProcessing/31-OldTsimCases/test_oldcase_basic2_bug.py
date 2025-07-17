import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdStream


class TestStreamOldCaseBasic2:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_basic2(self):
        """Stream basic test 2

        1.

        Catalog:
            - Streams:OldTsimCases

        Since: v3.0.0.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/pauseAndResume.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/sliding.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/tag.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/triggerInterval0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/windowClose.sim

        """

        tdStream.createSnode()

        tdLog.info(f"sliding")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.query(f"select * from information_schema.ins_databases;")
        tdSql.checkRows(3)

        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

        tdSql.execute(
            f"create stream streams1 interval(10s) sliding (5s) from t1 stream_options(max_delay(3s)) into streamt as select _twstart, _twend, first(ts), last(ts), count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from t1 where ts >= _twstart and ts < _twend;"
        )

        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791210000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791216000, 2, 2, 3, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791220000, 3, 2, 3, 2.1);")

        tdSql.execute(f"insert into t1 values(1648791210000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t1 values(1648791216000, 2, 2, 3, 1.1);")
        tdSql.execute(f"insert into t1 values(1648791220000, 3, 2, 3, 2.1);")

        tdSql.execute(f"insert into t2 values(1648791210000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791216000, 2, 2, 3, 1.1);")
        tdSql.execute(f"insert into t2 values(1648791220000, 3, 2, 3, 2.1);")

        tdSql.execute(f"insert into t2 values(1648791210000, 1, 2, 3, 1.0);")
        tdSql.execute(f"insert into t2 values(1648791216000, 2, 2, 3, 1.1);")
        tdSql.execute(f"insert into t2 values(1648791220000, 3, 2, 3, 2.1);")

        tdLog.info(f"step 0")
        tdSql.checkResultsByFunc(
            f"select * from streamt;", lambda: tdSql.getRows() == 4
        )
        tdSql.checkResultsBySql(
            sql="select * from streamt",
            exp_sql="select _wstart, _wend, first(ts), last(ts), count(*) c1, sum(a) c3, max(b) c4, min(c) c5 from t1 interval(10s) sliding (5s);",
            retry=1,
        )
