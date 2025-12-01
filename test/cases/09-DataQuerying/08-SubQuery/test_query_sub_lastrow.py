from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSubQueryLastRow:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_subquery_lastrow(self):
        """Subquery with lastrow

        1. Create db
        2. Create supper table and sub table
        3. Insert data into sub table
        4. Query last row from sub table as a sub query, it should return the last row data

        Catalog:
            - Query:SubQuery

        Since: v3.0.0.0

        Labels: common,ci

        Jira: TS-6365

        History:
            - 2025-7-29 Ethan liu adds test for a sub query use last row

        """

        tdLog.info(f"========== start sub query test")
        tdSql.execute(f"drop database if exists test_sub_query")
        tdSql.execute(f"create database test_sub_query")
        tdSql.execute(f"use test_sub_query")

        # create super table and sub table
        tdSql.execute(
            f"create table super_t (ts timestamp, flag int) tags (t1 VARCHAR(10))"
        )
        tdSql.execute(f"create table sub_t0 using super_t tags('t1')")

        tdSql.execute(f"insert into sub_t0 values (now, 0)")

        tdSql.execute(f"alter local 'keepColumnName' '1'")

        tdSql.execute(f"select flag from (select last_row(flag) from sub_t0) as t")
        tdSql.checkRows(1)

        tdLog.info(f"end sub query test successfully")
