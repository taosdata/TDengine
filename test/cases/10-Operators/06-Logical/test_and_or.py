from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestAndOr:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_and_or(self):
        """Logical and & or

        1. Mixed usage of AND and OR operators

        Catalog:
            - Operator

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/scalar/filter.sim

        """

        tdLog.info(f"======== step1")
        tdSql.prepare("db1", drop=True, vgroups=3)
        tdSql.execute(f"use db1;")
        tdSql.execute(
            f"create stable st1 (fts timestamp, fbool bool, ftiny tinyint, fsmall smallint, fint int, fbig bigint, futiny tinyint unsigned, fusmall smallint unsigned, fuint int unsigned, fubig bigint unsigned, ffloat float, fdouble double, fbin binary(10), fnchar nchar(10)) tags(tts timestamp, tbool bool, ttiny tinyint, tsmall smallint, tint int, tbig bigint, tutiny tinyint unsigned, tusmall smallint unsigned, tuint int unsigned, tubig bigint unsigned, tfloat float, tdouble double, tbin binary(10), tnchar nchar(10));"
        )
        tdSql.execute(
            f"create table tb1 using st1 tags('2022-07-10 16:31:00', true, 1, 1, 1, 1, 1, 1, 1, 1, 1.0, 1.0, 'a', 'a');"
        )
        tdSql.execute(
            f"create table tb2 using st1 tags('2022-07-10 16:32:00', false, 2, 2, 2, 2, 2, 2, 2, 2, 2.0, 2.0, 'b', 'b');"
        )
        tdSql.execute(
            f"create table tb3 using st1 tags('2022-07-10 16:33:00', true, 3, 3, 3, 3, 3, 3, 3, 3, 3.0, 3.0, 'c', 'c');"
        )

        tdSql.execute(
            f"insert into tb1 values ('2022-07-10 16:31:01', false, 1, 1, 1, 1, 1, 1, 1, 1, 1.0, 1.0, 'a', 'a');"
        )
        tdSql.execute(
            f"insert into tb1 values ('2022-07-10 16:31:02', true, 2, 2, 2, 2, 2, 2, 2, 2, 2.0, 2.0, 'b', 'b');"
        )
        tdSql.execute(
            f"insert into tb1 values ('2022-07-10 16:31:03', false, 3, 3, 3, 3, 3, 3, 3, 3, 3.0, 3.0, 'c', 'c');"
        )
        tdSql.execute(
            f"insert into tb1 values ('2022-07-10 16:31:04', true, 4, 4, 4, 4, 4, 4, 4, 4, 4.0, 4.0, 'd', 'd');"
        )
        tdSql.execute(
            f"insert into tb1 values ('2022-07-10 16:31:05', false, 5, 5, 5, 5, 5, 5, 5, 5, 5.0, 5.0, 'e', 'e');"
        )

        tdSql.execute(
            f"insert into tb2 values ('2022-07-10 16:32:01', false, 1, 1, 1, 1, 1, 1, 1, 1, 1.0, 1.0, 'a', 'a');"
        )
        tdSql.execute(
            f"insert into tb2 values ('2022-07-10 16:32:02', true, 2, 2, 2, 2, 2, 2, 2, 2, 2.0, 2.0, 'b', 'b');"
        )
        tdSql.execute(
            f"insert into tb2 values ('2022-07-10 16:32:03', false, 3, 3, 3, 3, 3, 3, 3, 3, 3.0, 3.0, 'c', 'c');"
        )
        tdSql.execute(
            f"insert into tb2 values ('2022-07-10 16:32:04', true, 4, 4, 4, 4, 4, 4, 4, 4, 4.0, 4.0, 'd', 'd');"
        )
        tdSql.execute(
            f"insert into tb2 values ('2022-07-10 16:32:05', false, 5, 5, 5, 5, 5, 5, 5, 5, 5.0, 5.0, 'e', 'e');"
        )

        tdSql.execute(
            f"insert into tb3 values ('2022-07-10 16:33:01', false, 1, 1, 1, 1, 1, 1, 1, 1, 1.0, 1.0, 'a', 'a');"
        )
        tdSql.execute(
            f"insert into tb3 values ('2022-07-10 16:33:02', true, 2, 2, 2, 2, 2, 2, 2, 2, 2.0, 2.0, 'b', 'b');"
        )
        tdSql.execute(
            f"insert into tb3 values ('2022-07-10 16:33:03', false, 3, 3, 3, 3, 3, 3, 3, 3, 3.0, 3.0, 'c', 'c');"
        )
        tdSql.execute(
            f"insert into tb3 values ('2022-07-10 16:33:04', true, 4, 4, 4, 4, 4, 4, 4, 4, 4.0, 4.0, 'd', 'd');"
        )
        tdSql.execute(
            f"insert into tb3 values ('2022-07-10 16:33:05', false, 5, 5, 5, 5, 5, 5, 5, 5, 5.0, 5.0, 'e', 'e');"
        )

        tdSql.query(f"select * from st1 where (ttiny > 2 or ftiny < 5) and ftiny > 2;")
        tdSql.checkRows(7)
