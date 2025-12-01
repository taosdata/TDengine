from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestAndOr:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_like(self):
        """Operator like

        1. Using LIKE operator in SELECT statements
        2. Using LIKE operator in SHOW statements
        3. Using LIKE operator in tag queries

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-10 Simon Guan Migrated from tsim/parser/like.sim
            - 2025-5-10 Simon Guan Migrated from tsim/query/tagLikeFilter.sim

        """

        self.Like()
        tdStream.dropAllStreamsAndDbs()
        self.Tag()
        tdStream.dropAllStreamsAndDbs()
        
    def Like(self):
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

    def Tag(self):
        tdSql.execute(f"drop database if exists db1;")
        tdSql.execute(f"create database if not exists db1 vgroups 10;")
        tdSql.execute(f"use db1;")
        tdSql.execute(f"create stable sta (ts timestamp, f1 double, f2 binary(200)) tags(t1 binary(100));")
        tdSql.execute(f"create table tba1 using sta tags('ZQMPvstuzZVzCRjFTQawILuGSqZKSqlJwcBtZMxrAEqBbzChHWVDMiAZJwESzJAf');")
        tdSql.execute(f"create table tba2 using sta tags('ieofwehughkreghughuerugu34jf9340aieefjalie28ffj8fj8fafjaekdfjfii');")
        tdSql.execute(f"create table tba3 using sta tags('ZQMPvstuzZVzCRjFTQawILuGSqabSqlJwcBtZMxrAEqBbzChHWVDMiAZJwESzJAf');")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:01', 1.0, \"a\");")
        tdSql.execute(f"insert into tba2 values ('2022-04-26 15:15:01', 1.0, \"a\");")
        tdSql.execute(f"insert into tba2 values ('2022-04-26 15:15:02', 1.0, \"a\");")
        tdSql.execute(f"insert into tba3 values ('2022-04-26 15:15:01', 1.0, \"a\");")
        tdSql.execute(f"insert into tba3 values ('2022-04-26 15:15:02', 1.0, \"a\");")
        tdSql.execute(f"insert into tba3 values ('2022-04-26 15:15:03', 1.0, \"a\");")
        tdSql.query(f"select t1 from sta where t1 like '%ab%';")
        tdSql.checkRows(3)

        tdSql.query(f"select t1 from sta where t1 like '%ax%';")
        tdSql.checkRows(0)

        tdSql.query(f"select t1 from sta where t1 like '%cd%';")
        tdSql.checkRows(0)

        tdSql.query(f"select t1 from sta where t1 like '%ii';")
        tdSql.checkRows(2)
