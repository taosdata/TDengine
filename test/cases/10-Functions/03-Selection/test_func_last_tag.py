from new_test_framework.utils import tdLog, tdSql, tdCom


class TestFuncLastTag:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_func_last_lru(self):
        """-

        1.

        Catalog:
            - Function:Selection

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/query/cache_last_tag.sim

        """

        tdSql.execute(f'alter local "multiResultFunctionStarReturnTags" "0";')

        tdLog.info(f"step1=====================")
        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"create database test  vgroups 4 CACHEMODEL 'both';")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(f"create table t3 using st tags(3,3,3);")
        tdSql.execute(f"create table t4 using st tags(NULL,4,4);")

        tdSql.execute(f"insert into t1 values(1648791211000,1,1,1);")
        tdSql.execute(f"insert into t1 values(1648791211001,2,2,2);")
        tdSql.execute(f"insert into t2 values(1648791211002,3,3,3);")
        tdSql.execute(f"insert into t2 values(1648791211003,4,4,4);")
        tdSql.execute(f"insert into t3 values(1648791211004,5,5,5);")
        tdSql.execute(f"insert into t3 values(1648791211005,6,6,6);")
        tdSql.execute(f"insert into t4 values(1648791211007,NULL,NULL,NULL);")

        tdSql.query(f"select last(*),last_row(*) from st;")

        tdSql.checkCols(8)

        tdSql.execute(f'alter local "multiResultFunctionStarReturnTags" "1";')

        tdSql.query(f"select last(*),last_row(*) from st;")

        tdSql.checkCols(14)

        tdSql.query(f"select last(*) from st;")

        tdSql.checkCols(7)

        tdSql.query(f"select last_row(*) from st;")

        tdSql.checkCols(7)

        tdSql.query(f"select last(*),last_row(*) from t1;")

        tdSql.checkCols(8)

        tdLog.info(f"step2=====================")

        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"create database test1  vgroups 4 CACHEMODEL 'last_row';")
        tdSql.execute(f"use test1;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(f"create table t3 using st tags(3,3,3);")
        tdSql.execute(f"create table t4 using st tags(NULL,4,4);")

        tdSql.execute(f"insert into t1 values(1648791211000,1,1,1);")
        tdSql.execute(f"insert into t1 values(1648791211001,2,2,2);")
        tdSql.execute(f"insert into t2 values(1648791211002,3,3,3);")
        tdSql.execute(f"insert into t2 values(1648791211003,4,4,4);")
        tdSql.execute(f"insert into t3 values(1648791211004,5,5,5);")
        tdSql.execute(f"insert into t3 values(1648791211005,6,6,6);")
        tdSql.execute(f"insert into t4 values(1648791211007,NULL,NULL,NULL);")

        tdSql.query(f"select last(*),last_row(*) from st;")

        tdSql.checkCols(14)

        tdSql.query(f"select last(*) from st;")

        tdSql.checkCols(7)

        tdSql.query(f"select last_row(*) from st;")

        tdSql.checkCols(7)

        tdSql.query(f"select last(*),last_row(*) from t1;")

        tdSql.checkCols(8)

        tdLog.info(f"step3=====================")

        tdSql.execute(f"drop database if exists test1;")
        tdSql.execute(f"create database test2  vgroups 4 CACHEMODEL 'last_value';")
        tdSql.execute(f"use test2;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(f"create table t3 using st tags(3,3,3);")
        tdSql.execute(f"create table t4 using st tags(NULL,4,4);")

        tdSql.execute(f"insert into t1 values(1648791211000,1,1,1);")
        tdSql.execute(f"insert into t1 values(1648791211001,2,2,2);")
        tdSql.execute(f"insert into t2 values(1648791211002,3,3,3);")
        tdSql.execute(f"insert into t2 values(1648791211003,4,4,4);")
        tdSql.execute(f"insert into t3 values(1648791211004,5,5,5);")
        tdSql.execute(f"insert into t3 values(1648791211005,6,6,6);")
        tdSql.execute(f"insert into t4 values(1648791211007,NULL,NULL,NULL);")

        tdSql.query(f"select last(*),last_row(*) from st;")

        tdSql.checkCols(14)

        tdSql.query(f"select last(*) from st;")

        tdSql.checkCols(7)

        tdSql.query(f"select last_row(*) from st;")

        tdSql.checkCols(7)

        tdSql.query(f"select last(*),last_row(*) from t1;")

        tdSql.checkCols(8)

        tdSql.execute(f"drop database if exists test2;")
        tdSql.execute(f"create database test4  vgroups 4;")
        tdSql.execute(f"use test4;")
        tdSql.execute(
            f"create stable st(ts timestamp,a int,b int,c int) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")
        tdSql.execute(f"create table t3 using st tags(3,3,3);")
        tdSql.execute(f"create table t4 using st tags(NULL,4,4);")

        tdSql.execute(f"insert into t1 values(1648791211000,1,1,1);")
        tdSql.execute(f"insert into t1 values(1648791211001,2,2,2);")
        tdSql.execute(f"insert into t2 values(1648791211002,3,3,3);")
        tdSql.execute(f"insert into t2 values(1648791211003,4,4,4);")
        tdSql.execute(f"insert into t3 values(1648791211004,5,5,5);")
        tdSql.execute(f"insert into t3 values(1648791211005,6,6,6);")
        tdSql.execute(f"insert into t4 values(1648791211007,NULL,NULL,NULL);")

        tdSql.query(f"select last(*),last_row(*) from st;")

        tdSql.checkCols(14)

        tdSql.query(f"select last(*) from st;")

        tdSql.checkCols(7)

        tdSql.query(f"select last_row(*) from st;")

        tdSql.checkCols(7)

        tdSql.query(f"select last(*),last_row(*) from t1;")

        tdSql.checkCols(8)

    def test_last_tag(self):
        """summary: test last/last_row with tag column

        description: verify the behavior of selecting last/last_row with tag column outside.
                    For example: select last(ts), tag1, tag2 from stable group by tbname.
                    In this case, we should read cache data to get the tag column value.

        Since: v3.3.6

        Labels: last/last_row, tag

        Jira: TS-6146

        Catalog:
            - xxx:xxx

        History:
            - Tony Zhang, 2025/10/10, Created

        """
        tdSql.execute("create database test_last_tag cachemodel 'both' keep 3650;")
        tdSql.execute("use test_last_tag;")
        tdSql.execute("create table stb (ts timestamp, c1 int) tags (tag1 int, tag2 float)")

        tdSql.execute("create table tb1 using stb tags (1, 1.1);")
        tdSql.execute("create table tb2 using stb tags (2, 2.2);")

        tdSql.execute("insert into tb1 values ('2024-10-10 10:00:00', 0);")
        tdSql.execute("insert into tb1 values ('2024-10-10 10:00:02', 2);")
        tdSql.execute("insert into tb1 values ('2024-10-10 10:00:04', 4);")
        tdSql.execute("insert into tb2 values ('2024-10-10 10:00:01', 1);")
        tdSql.execute("insert into tb2 values ('2024-10-10 10:00:03', 3);")
        tdSql.execute("insert into tb2 values ('2024-10-10 10:00:05', null);")

        tdCom.compare_testcase_result(
            "cases/10-Functions/in/last_tag.in",
            "cases/10-Functions/ans/last_tag.csv",
            "test_last_tag")
