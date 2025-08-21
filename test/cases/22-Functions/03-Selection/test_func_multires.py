from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdCom


class TestFuncBottom:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_func_multires(self):
        """-

        1. -

        Catalog:
            - Function:Selection

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/query/multires_func.sim

        """

        tdSql.execute(f"create database test")
        tdSql.execute(f"use test")
        tdSql.execute(
            f"CREATE TABLE `tb` (`ts` TIMESTAMP, `c0` INT, `c1` FLOAT, `c2` BINARY(10))"
        )

        tdSql.execute(
            f'insert into tb values("2022-05-15 00:01:08.000", 1, 1.0, "abc")'
        )
        tdSql.execute(
            f'insert into tb values("2022-05-16 00:01:08.000", 2, 2.0, "bcd")'
        )
        tdSql.execute(
            f'insert into tb values("2022-05-17 00:01:08.000", 3, 3.0, "cde")'
        )

        resultfile = tdCom.generate_query_result(
            "cases/10-Functions/03-Selection/t/multires_func.sql", "test_func_multires"
        )
        tdLog.info(f"resultfile: {resultfile}")
        tdCom.compare_result_files(
            resultfile, "cases/10-Functions/03-Selection/r/multires_func.result"
        )
