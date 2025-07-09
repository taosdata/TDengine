from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


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
