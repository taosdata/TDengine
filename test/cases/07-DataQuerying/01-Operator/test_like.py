from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestAndOr:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_like(self):
        """Like

        1. -

        Catalog:
            - Query:Operator

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-10 Simon Guan Migrated from tsim/parser/like.sim

        """

        tdLog.info(f"======================== dnode1 start")

        db = "testdb"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} cachemodel 'last_value'")
        tdSql.execute(f"use {db}")

        table1 = "table_name"
        table2 = "tablexname"

        tdSql.execute(f"create table {table1} (ts timestamp, b binary(20))")
        tdSql.execute(f"create table {table2} (ts timestamp, b binary(20))")

        tdSql.execute(f'insert into {table1} values(now,    "table_name")')
        tdSql.execute(f'insert into {table1} values(now-3m, "tablexname")')
        tdSql.execute(f'insert into {table1} values(now-2m, "tablexxx")')
        tdSql.execute(f'insert into {table1} values(now-1m, "table")')

        tdSql.query(f"select b from {table1}")
        tdSql.checkRows(4)

        tdSql.query(f"select b from {table1} where b like 'table_name'")
        tdSql.checkRows(2)

        tdSql.query(f"select b from {table1} where b like 'table\_name'")
        tdSql.checkRows(1)

        tdSql.query(f"show tables;")
        tdSql.checkRows(2)

        tdSql.query(f"show tables like 'table_name'")
        tdSql.checkRows(2)

        tdSql.query(f"show tables like 'table\_name'")
        tdSql.checkRows(1)

        view1 = "view1_name"
        view2 = "view2_name"

        tdSql.execute(f"CREATE VIEW {view1} as select * from {table1}")
        tdSql.execute(f"CREATE VIEW {view2} AS select * from {table2}")

        tdSql.query(f"show views like 'view%'")
        tdSql.checkRows(2)

        tdSql.query(f"show views like 'view1%'")
        tdSql.checkRows(1)
