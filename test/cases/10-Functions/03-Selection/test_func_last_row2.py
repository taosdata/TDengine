from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestFuncLastBoth:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_func_last_both(self):
        """Last 函数

        1. -

        Catalog:
            - Function:Selection

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-10 Simon Guan Migrated from tsim/parser/lastrow2.sim

        """

        tdSql.execute(f"create database d1;")
        tdSql.execute(f"use d1;")

        tdLog.info(f"========>td-1317, empty table last_row query crashed")
        tdSql.execute(f"drop table if exists m1;")
        tdSql.execute(f"create table m1(ts timestamp, k int) tags (a int);")
        tdSql.execute(f"create table t1 using m1 tags(1);")
        tdSql.execute(f"create table t2 using m1 tags(2);")

        tdSql.query(f"select last_row(*) from t1")
        tdSql.checkRows(0)

        tdSql.query(f"select last_row(*) from m1")
        tdSql.checkRows(0)

        tdSql.query(f"select last_row(*) from m1 where tbname in ('t1')")
        tdSql.checkRows(0)

        tdSql.execute(f"insert into t1 values('2019-1-1 1:1:1', 1);")
        tdLog.info(
            f"===================> last_row query against normal table along with ts/tbname"
        )
        tdSql.query(f"select last_row(*),ts,'k' from t1;")
        tdSql.checkRows(1)

        tdLog.info(
            f"===================> last_row + user-defined column + normal tables"
        )
        tdSql.query(f"select last_row(ts), 'abc', 1234.9384, ts from t1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, "abc")

        tdSql.checkData(0, 2, 1234.938400000)

        tdSql.checkData(0, 3, "2019-01-01 01:01:01")

        tdLog.info(
            f"===================> last_row + stable + ts/tag column + condition + udf"
        )
        tdSql.query(f"select last_row(*), ts, 'abc', 123.981, tbname from m1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 2, "2019-01-01 01:01:01")

        tdSql.checkData(0, 3, "abc")

        tdSql.checkData(0, 4, 123.981000000)

        tdSql.execute(f"create table tu(ts timestamp, k int)")
        tdSql.query(f"select last_row(*) from tu")
        tdSql.checkRows(0)

        tdLog.info(f"=================== last_row + nested query")
        tdSql.execute(f"create table lr_nested(ts timestamp, f int)")
        tdSql.execute(f"insert into lr_nested values(now, 1)")
        tdSql.execute(f"insert into lr_nested values(now+1s, null)")
        tdSql.query(f"select last_row(*) from (select * from lr_nested)")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, None)
