from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableDescribe:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_describe(self):
        """create normal table (describe)

        1. create normal table
        2. insert data
        3. drop table
        4. show tables

        Catalog:
            - Table:NormalTable:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/table/describe.sim

        """

        i = 0
        dbPrefix = "de_in_db"
        tbPrefix = "de_in_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.prepare(dbname=db)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, speed int)")

        tdSql.query(f"describe {tb}")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "ts")

        tdSql.checkData(0, 1, "TIMESTAMP")

        tdSql.checkData(0, 2, 8)

        tdSql.checkData(1, 0, "speed")

        tdSql.checkData(1, 1, "INT")

        tdSql.checkData(1, 2, 4)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
