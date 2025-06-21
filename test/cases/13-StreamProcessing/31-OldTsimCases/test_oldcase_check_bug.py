import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdStream


class TestStreamOldCaseCheck:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_check(self):
        """Stream check stable

        1. -

        Catalog:
            - Streams:OldTsimCases

        Since: v3.0.0.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/checkStreamSTable.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/checkStreamSTable1.sim

        """

        tdStream.createSnode()

        tdLog.info(f"checkStreamSTable")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info(f"===== step2")
        tdSql.execute(f"create database result vgroups 1;")
        tdSql.execute(f"create database test vgroups 4;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
        tdSql.execute(f"create table t2 using st tags(2, 2, 2);")

        tdSql.execute(
            "create stream streams0 interval(10s) sliding(10s) from st partition by tbname, ta, tb, tc options(MAX_DELAY(1s)) into result.streamt0 tags(tag_tbname varchar(270) as %%1, ta int as %%2, tb int as %%3, tc int as %%4) as select _twstart, count(*) c1, max(a) c2 from %%tbname;"
        )
        tdStream.checkStreamStatus()

        tdSql.execute(f"insert into t1 values(1648791213000, 1, 2, 3);")
        tdSql.execute(f"insert into t2 values(1648791213000, 2, 2, 3);")

        tdSql.checkResultsByFunc(
            f"select * from  result.streamt0", func=lambda: tdSql.getRows() == 2
        )
