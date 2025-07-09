from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableColumnName:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_column_name(self):
        """create normal table (name)

        1. create normal table
        2. insert data
        3. query from normal table

        Catalog:
            - Table:NormalTable:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/table/column_name.sim

        """

        i = 0
        dbPrefix = "lm_cm_db"
        tbPrefix = "lm_cm_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.prepare(db)
        tdSql.execute(f"use {db}")

        tdSql.error(f"drop table dd ")
        tdSql.error(f"create table {tb}(ts timestamp, int) ")

        tdSql.query(f"show tables")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step2")
        tdSql.execute(f"create table {tb} (ts timestamp, s int)")
        tdSql.query(f"show tables")
        tdSql.checkRows(1)

        tdSql.execute(f"drop table {tb}")
        tdSql.query(f"show tables")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step3")
        tdSql.execute(f"create table {tb} (ts timestamp, a0123456789 int)")
        tdSql.query(f"show tables")
        tdSql.checkRows(1)

        tdSql.execute(f"drop table {tb}")
        tdSql.query(f"show tables")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step4")
        tdSql.execute(
            f"create table {tb} (ts timestamp, a0123456789012345678901234567890123456789 int)"
        )
        tdSql.execute(f"drop table {tb}")

        tdSql.query(f"show tables")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step5")
        tdSql.execute(f"create table {tb} (ts timestamp, a0123456789 int)")
        tdSql.query(f"show tables")
        tdSql.checkRows(1)

        tdSql.execute(f"insert into {tb} values (now , 1)")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(1)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
