from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestConstCol:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_const_column(self):
        """Const Column

        1. -

        Catalog:
            - Query:SelectList

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/constCol.sim

        """

        tdSql.execute(f"create database db;")
        tdSql.execute(f"use db;")
        tdSql.execute(f"create table t (ts timestamp, i int);")
        tdSql.execute(f"create table st1 (ts timestamp, f1 int) tags(t1 int);")
        tdSql.execute(f"create table st2 (ts timestamp, f2 int) tags(t2 int);")
        tdSql.execute(f"create table t1 using st1 tags(1);")
        tdSql.execute(f"create table t2 using st2 tags(1);")

        tdSql.execute(f"insert into t1 values(1575880055000, 1);")
        tdSql.execute(f"insert into t1 values(1575880059000, 1);")
        tdSql.execute(f"insert into t1 values(1575880069000, 1);")
        tdSql.execute(f"insert into t2 values(1575880055000, 2);")

        tdSql.query(
            f"select st1.ts, st1.f1, st2.f2 from db.st1, db.st2 where st1.t1=st2.t2 and st1.ts=st2.ts"
        )

        tdLog.info(f"==============select with user-defined columns")
        tdSql.query(f"select 'abc' as f, ts,f1 from t1")
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "abc")

        tdSql.checkData(0, 1, "2019-12-09 16:27:35")

        tdSql.checkData(0, 2, 1)

        tdSql.query(f"select 'abc', ts, f1 from t1")
        tdSql.checkRows(3)

        tdSql.checkData(0, 1, "2019-12-09 16:27:35")

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(1, 0, "abc")

        tdSql.query(f"select 'abc' from t1")
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "abc")

        tdSql.query(f"select 'abc' as f1 from t1")
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "abc")

        tdSql.query(f"select 1 from t1")
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(1, 0, 1)

        tdSql.checkData(2, 0, 1)

        tdSql.query(f"select 1 as f1 from t1")
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select 1 as f, f1 from t1")
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select 1.123 as f, f1 from t1")
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, 1.123000000)

        tdSql.checkData(1, 0, 1.123000000)

        tdSql.query(f"select 1, f1 from t1")
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select 1.2391, f1 from t1")
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, 1.239100000)

        tdSql.checkData(1, 0, 1.239100000)

        tdSql.checkData(0, 1, 1)

        tdLog.info(f"===================== user-defined columns with agg functions")
        tdSql.query(f"select 1 as t, count(*) from t1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(0, 1, 3)

        tdSql.query(f"select 1, sum(f1) from t1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(0, 1, 3)

        tdSql.query(f"select 1,2,3, sum(f1)*99, 4,5,6,7,8,9,10 from t1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(0, 1, 2)

        tdSql.checkData(0, 2, 3)

        tdSql.checkData(0, 3, 297.000000000)

        tdSql.query(f"select sum(f1)*avg(f1)+12, 1,2,3,4,5,6,7,8,9,10 from t1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 15.000000000)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 2)

        tdSql.query(f"select 1.2987, f1, 'k' from t1 where f1=1")
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, 1.298700000)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, "k")

        tdLog.info(f"====================user-defined columns with union")
        tdSql.query(f"select f1, 'f1' from t1 union all select f1, 'f1' from t1;")
        tdSql.checkRows(6)

        tdSql.checkData(0, 0, 1)

        tdSql.checkData(0, 1, "f1")

        tdSql.checkData(1, 0, 1)

        tdSql.checkData(1, 1, "f1")

        tdLog.info(f"=====================udc with join")
        tdSql.query(
            f"select st1.ts, st1.f1, st2.f2, 'abc', 1.9827 from db.st1, db.st2 where st1.t1=st2.t2 and st1.ts=st2.ts"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2019-12-09 16:27:35")

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 2)

        tdSql.checkData(0, 3, "abc")

        tdSql.checkData(0, 4, 1.982700000)

        tdLog.info(f"======================udc with interval")
        tdSql.query(f"select count(*), 'uuu' from t1 interval(1s);")
        tdSql.checkRows(3)

        tdLog.info(f"======================udc with tags")
        tdSql.query(f"select distinct t1,'abc',tbname from st1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, "abc")

        tdSql.checkData(0, 2, "t1")

        tdLog.info(f"======================udc with arithmetic")
        tdSql.query(f"select 1+1 from t1")
        tdSql.checkRows(3)

        tdSql.query(f"select 0.1 + 0.2 from t1")
        tdSql.checkRows(3)

        tdLog.info(f"=============================> td-2036")
        tdSql.checkData(0, 0, 0.300000000)

        tdLog.info(f"=============================> td-3996")
        tdSql.query(f"select 'abc' as res from t1 where f1 < 0")
        tdSql.checkRows(0)

        tdLog.info(f"======================udc with normal column group by")
        tdSql.error(f"select from t1")
        tdSql.error(f"select abc from t1")
        tdSql.error(f"select abc as tu from t1")

        tdLog.info(f"========================> td-1756")
        tdSql.query(f"select * from t1 where ts>now-1y")
        tdSql.query(f"select * from t1 where ts>now-1n")

        tdLog.info(f"========================> td-1752")
        tdSql.query(f"select * from db.st2 where t2 < 200 and t2 is not null;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2019-12-09 16:27:35")

        tdSql.checkData(0, 1, 2)

        tdSql.checkData(0, 2, 1)

        tdSql.query(f"select * from db.st2 where t2 > 200 or t2 is null;")
        tdSql.checkRows(0)

        tdSql.query(f"select * from st2 where t2 < 200 and t2 is null;")
        tdSql.checkRows(0)
