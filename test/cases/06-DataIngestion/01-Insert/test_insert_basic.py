from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestInsertBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_insert_basic(self):
        """insert use ns precision

        1. create table
        2. insert data
        3. query data

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/insert/basic.sim

        """

        i = 0
        dbPrefix = "d"
        tbPrefix = "t"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.execute(f"create database {db} vgroups 2 precision 'ns'")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {tb} (ts timestamp, speed int)")

        x = 0
        while x < 110:
            cc = x * 60000
            ms = 1601481600000000000 + cc

            tdSql.execute(f"insert into {tb} values ({ms} , {x} )")
            x = x + 1

        tdLog.info(f"=============== step 2")
        x = 0
        while x < 110:
            cc = x * 60000
            ms = 1551481600000000000 + cc

            tdSql.execute(f"insert into {tb} values ({ms} , {x} )")
            x = x + 1

        tdSql.query(f"select * from {tb}")

        tdLog.info(f"{tdSql.getRows()}) points data are retrieved")
        tdSql.checkRows(220)
