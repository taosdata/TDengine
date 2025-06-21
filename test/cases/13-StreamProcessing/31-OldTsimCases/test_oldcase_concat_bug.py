import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdStream


class TestStreamOldCaseConcat:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_concat(self):
        """Stream concat

        1. -

        Catalog:
            - Streams:OldTsimCases

        Since: v3.0.0.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/udTableAndCol0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/udTableAndTag0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/udTableAndTag1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/udTableAndTag2.sim

        """

        tdStream.createSnode()


        tdLog.info("udTableAndTag2")
        tdStream.dropAllStreamsAndDbs()

        tdLog.info("===== step2")
        tdLog.info("===== table name")

        tdSql.execute("create database result vgroups 1;")
        tdSql.execute("create database test vgroups 4;")
        tdSql.execute("use test;")

        tdSql.execute(
            "create stable st(ts timestamp, a int, b int, c int) tags(ta int, tb int, tc int);"
        )
        tdSql.execute("create table t1 using st tags(1, 1, 1);")
        tdSql.execute("create table t2 using st tags(2, 2, 2);")

        tdSql.execute(
            'create stream streams1 interval(10s) sliding(10s) from st partition by tbname options(max_delay(1s)) into result.streamt OUTPUT_SUBTABLE("aaa") as select _twstart, count(*) c1 from st ;'
        )

        tdStream.checkStreamStatus()

        tdLog.info("===== insert into 1")
        tdSql.execute("insert into t1 values(1648791213000, 1, 2, 3);")
        tdSql.execute("insert into t2 values(1648791213000, 2, 2, 3);")

        tdSql.checkResultsByFunc(
            'select table_name from information_schema.ins_tables where db_name="result" order by 1;',
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 0) == "aaa",
        )

        tdSql.checkResultsByFunc(
            "select * from result.streamt;",
            lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 2,
        )
        
        return
