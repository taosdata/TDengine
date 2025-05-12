from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableDeleteReuse1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_delete_reuse1(self):
        """drop normal table（continue write data）

        1. create a background process that continuously writes data.
        2. create normal table
        3. insert data
        4. drop table
        5. continue 20 times

        Catalog:
            - Table:NormalTable:Drop

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/table/delete_reuse1.sim

        """

        tdLog.info(f"======== step1")
        tdSql.execute(f"create database d1 replica 1")
        tdSql.execute(f"create table d1.t1 (ts timestamp, i int)")
        tdSql.execute(f"insert into d1.t1 values(now, 1)")

        tdSql.query(f"select * from d1.t1")
        tdSql.checkRows(1)

        tdLog.info(f"======== step2")
        tdSql.execute(f"drop table d1.t1")
        tdSql.error(f"insert into d1.t1 values(now, 2)")

        tdLog.info(f"========= step3")
        tdSql.execute(f"create table d1.t1 (ts timestamp, i int)")
        tdSql.execute(f"insert into d1.t1 values(now, 2)")
        tdSql.query(f"select * from d1.t1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 2)

        tdLog.info(f"========= step4")
        x = 0
        while x < 20:

            tdSql.execute(f"drop table d1.t1")
            tdSql.error(f"insert into d1.t1 values(now, -1)")

            tdSql.execute(f"create table d1.t1 (ts timestamp, i int)")
            tdSql.execute(f"insert into d1.t1 values(now, {x} )")
            tdSql.query(f"select * from d1.t1")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, x)
            x = x + 1
