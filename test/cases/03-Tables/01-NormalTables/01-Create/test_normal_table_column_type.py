from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableColumnType:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_column_type(self):
        """create normal table (type)

        1. create normal table
        2. insert data
        3. query from normal table

        Catalog:
            - Table:NormalTable:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/table/column_type.sim

        """

        tdLog.info(f"=============== step1")
        tdSql.prepare(dbname="db")
        tdSql.execute(f"use db")
        tdSql.execute(
            f"create table tt (ts timestamp, a int, b float, c smallint, d double, e tinyint, f bigint, g timestamp, h binary(10), i bool);"
        )
        tdSql.execute(
            f"insert into tt values (now + 1m , 1 , 1 , 1 , 1 ,  1 , 10, 150000000 , '11' , true )"
        )

        tdSql.query(f"select * from tt")
        tdSql.checkRows(1)

        tdSql.execute(f"drop database db")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
