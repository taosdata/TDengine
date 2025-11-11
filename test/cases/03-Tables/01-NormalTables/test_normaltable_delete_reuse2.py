from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableDeleteReuse2:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_delete_reuse2(self):
        """Repeatedly drop new name

        1. Create a normal table (new name)
        2. Insert data 
        3. Query data
        4. Repeat 20 timeses


        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/table/delete_reuse2.sim

        """

        tdLog.info(f"======== step1")
        tdSql.execute(f"create database db1 replica 1")
        tdSql.execute(f"create table db1.t1 (ts timestamp, i int)")
        tdSql.execute(f"insert into db1.t1 values(now, 1)")

        tdSql.query(f"select * from db1.t1")
        tdSql.checkRows(1)

        tdLog.info(f"======== step2")
        tdSql.execute(f"drop table db1.t1")
        tdSql.error(f"insert into db1.t1 values(now, 2)")

        tdLog.info(f"========= step3")
        tdSql.execute(f"create table db1.tb1 (ts timestamp, i int)")
        tdSql.execute(f"insert into db1.tb1 values(now, 2)")
        tdSql.query(f"select * from db1.tb1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 2)

        tdSql.execute(f"use db1")
        tdLog.info(f"========= step4")
        x = 1
        while x < 20:
            tb = "tb" + str(x)
            tdSql.execute(f"drop table {tb}")
            tdSql.error(f"insert into {tb} values(now, -1)")

            x = x + 1
            tb = "tb" + str(x)

            tdSql.execute(f"create table {tb} (ts timestamp, i int)")
            tdSql.execute(f"insert into {tb} values(now, {x} )")
            tdSql.query(f"select * from {tb}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, x)

            tdLog.info(f"===> loop times: {x}")
