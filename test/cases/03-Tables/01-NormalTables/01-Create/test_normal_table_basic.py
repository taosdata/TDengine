from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableBasic2:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_basic2(self):
        """create normal table

        1. create normal table
        2. insert data
        3. query from normal table

        Catalog:
            - Table:NormalTable:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/table/basic2.sim

        """

        tdLog.info(f"=============== one table")
        tdSql.prepare(dbname="d1")
        tdSql.execute(f"create table d1.n1 (ts timestamp, i int)")
        tdSql.execute(f"create table d1.n2 (ts timestamp, i int)")
        tdSql.execute(f"create table d1.n3 (ts timestamp, i int)")
        tdSql.execute(f"create table d1.n4 (ts timestamp, i int)")

        tdSql.execute(f"drop table d1.n1")
        tdSql.execute(f"drop table d1.n2")

        tdSql.query(f"show d1.tables")
        tdSql.checkRows(2)

        tdLog.info(f"=============== show")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkData(2, 2, 2)

        tdSql.checkData(2, 4, 1)

        tdSql.query(f"show d1.vgroups")
        tdSql.checkData(0, 0, 2)

        tdSql.checkData(0, 1, "d1")

        tdLog.info(f"=============== insert data1")
        tdSql.error(f"insert into d1.n1 values(now, 1)")
        tdSql.error(f"insert into d1.n2 values(now, 1)")

        tdLog.info(f"=============== insert data2")
        tdSql.execute(f"insert into d1.n3 values(now, 1)")
        tdSql.execute(f"insert into d1.n3 values(now+1s, 2)")
        tdSql.execute(f"insert into d1.n3 values(now+2s, 3)")

        tdLog.info(f"=============== query data")
        tdSql.query(f"select * from d1.n3")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(3)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(1, 1, 2)

        tdSql.checkData(2, 1, 3)
