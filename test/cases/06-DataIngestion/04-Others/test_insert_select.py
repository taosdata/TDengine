from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, sc


class TestInsertSelect:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_insert_select(self):
        """insert sub table (select)

        1. create table
        2. insert data
        3. query data

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated to new test framework, from tests/script/tsim/insert/insert_select.sim

        """

        tdLog.info(f"======== step1")
        tdSql.prepare(dbname="db1", vgroups=3)
        tdSql.execute(f"use db1;")
        tdSql.execute(
            f"create stable st1 (ts timestamp, f1 int, f2 binary(200)) tags(t1 int);"
        )
        tdSql.execute(f"create table tb1 using st1 tags(1);")
        tdSql.execute(f"insert into tb1 values ('2022-07-07 10:01:01', 11, 'aaa');")
        tdSql.execute(f"insert into tb1 values ('2022-07-07 11:01:02', 12, 'bbb');")
        tdSql.execute(f"create table tb2 using st1 tags(2);")
        tdSql.execute(f"insert into tb2 values ('2022-07-07 10:02:01', 21, 'aaa');")
        tdSql.execute(f"insert into tb2 values ('2022-07-07 11:02:02', 22, 'bbb');")
        tdSql.execute(f"create table tb3 using st1 tags(3);")
        tdSql.execute(f"insert into tb3 values ('2022-07-07 10:03:01', 31, 'aaa');")
        tdSql.execute(f"insert into tb3 values ('2022-07-07 11:03:02', 32, 'bbb');")
        tdSql.execute(f"create table tb4 using st1 tags(4);")
        tdSql.execute(f"insert into tb4 select * from tb1;")
        tdSql.query(f"select * from tb4;")
        tdSql.checkRows(2)

        tdSql.execute(f"insert into tb4 select ts,f1,f2 from st1;")
        tdSql.query(f"select * from tb4;")
        tdSql.checkRows(6)

        tdSql.execute(
            f"create table tba (ts timestamp, f1 binary(10), f2 bigint, f3 double);"
        )
        tdSql.error(f"insert into tba select * from tb1;")
        tdSql.execute(f"insert into tba (ts,f2,f1) select * from tb1;")
        tdSql.query(f"select * from tba;")
        tdSql.checkRows(2)

        tdSql.execute(
            f"create table tbb (ts timestamp, f1 binary(10), f2 bigint, f3 double);"
        )
        tdSql.execute(f"insert into tbb (f2,f1,ts) select f1+1,f2,ts+3 from tb2;")
        tdSql.query(f"select * from tbb;")
        tdSql.checkRows(2)

        tdLog.info(f"======== step2")
        tdSql.execute(f"drop database if exists db1;")
        tdSql.execute(f"create database db1 vgroups 1;")
        tdSql.execute(f"use db1;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int );")
        tdSql.execute(f"create table t2(ts timestamp, a int, b int );")
        tdSql.execute(f"insert into t1 values(1648791211000,1,2);")
        tdSql.execute(f"insert into t2 (ts, b, a) select ts, a, b from t1;")
        tdSql.query(f"select * from t2;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 2)

        tdSql.checkData(0, 2, 1)

        tdSql.execute(f"insert into t2 (ts, b, a) select ts + 1, 11, 12 from t1;")
        tdSql.query(f"select * from t2;")
        tdSql.checkRows(2)

        tdSql.checkData(0, 1, 2)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(1, 1, 12)

        tdSql.checkData(1, 2, 11)
