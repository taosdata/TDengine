from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableColumnValue:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_column_value(self):
        """create normal table (value)

        1. create normal table
        2. insert data
        3. query from normal table

        Catalog:
            - Table:NormalTable:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/table/column_value.sim

        """

        i = 0
        dbPrefix = "lm_cv_db"
        tbPrefix = "lm_cv_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.prepare(dbname=db)
        tdSql.execute(f"use {db}")

        tdSql.execute(
            f"create table {tb} (ts timestamp, speed int, v1 binary(100), v2 binary(100), v3 binary(100), v4 binary(100), v5 binary(100))"
        )
        tdSql.query(f"show tables")
        tdSql.checkRows(1)

        tdSql.execute(f"insert into {tb} values(now, 1, '1', '2', '3', '4', '5')")
        tdSql.execute(f"insert into {tb} values(now+1a, 1, '1', '2', '3', '4', '5')")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(2)

        tdSql.execute(f"drop table {tb}")
        tdSql.query(f"show tables")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step2")
        tdSql.execute(
            f"create table  {tb} (ts timestamp, speed bigint, v1 binary(1500), v2 binary(1500), v3 binary(1500), v4 binary(500), v5 binary(500))"
        )
        tdSql.query(f"show tables")
        tdSql.checkRows(1)

        tdSql.execute(f"drop table {tb}")

        tdLog.info(f"=============== step3")
        tdSql.execute(
            f"create table {tb} (ts timestamp, speed float, v1 binary(100), v2 binary(100), v3 binary(100), v4 binary(100), v5 binary(100))"
        )
        tdSql.query(f"show tables")
        tdSql.checkRows(1)

        tdSql.execute(f"insert into {tb} values(now+2a, 1, '1', '2', '3', '4', '5')")
        tdSql.execute(f"insert into {tb} values(now+3a, 1, '1', '2', '3', '4', '5')")
        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(2)

        tdSql.execute(f"drop table {tb}")
        tdSql.query(f"show tables")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step4")
        tdSql.execute(
            f"create table  {tb} (ts timestamp, speed double, v1 binary(1500), v2 binary(1500), v3 binary(1500), v4 binary(500), v5 binary(500))"
        )
        tdSql.query(f"show tables")
        tdSql.checkRows(1)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
