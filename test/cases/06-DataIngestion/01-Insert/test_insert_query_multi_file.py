from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestInsertQueryBlock1File:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_insert_query_multi_file(self):
        """insert sub table then query

        1. create table
        2. insert data
        3. query data

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/insert/query_multi_file.sim

        """

        i = 0
        dbPrefix = "tb_mf_db"
        tbPrefix = "tb_mf_tb"
        db = dbPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.prepare(dbname=db)
        tdSql.execute(f"use {db}")

        tdSql.execute(f"create table {tb} (ts timestamp, speed int)")

        N = 20000

        x = 0

        while x < N:
            ms = str(x) + "s"
            # print insert into $tb values (now + $ms , $x )
            tdSql.execute(f"insert into {tb} values (now + {ms} , {x} )")
            x = x + 1

        tdSql.query(f"select * from {tb}")
        tdLog.info(f"{tdSql.getRows()}) points data are retrieved -> exepct {N} rows")
        tdSql.checkAssert(tdSql.getRows() >= N)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
