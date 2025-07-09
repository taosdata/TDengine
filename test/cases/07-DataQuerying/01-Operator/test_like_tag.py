from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestLikeTag:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_like_tag(self):
        """Like tag

        1. -

        Catalog:
            - Query:Operator

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-10 Simon Guan Migrated from tsim/query/tagLikeFilter.sim

        """

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
