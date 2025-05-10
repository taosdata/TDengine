from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, tdCom


class TestExplainTsOrder:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_explain_tsorder(self):
        """explain

        1.

        Catalog:
            - Query:Explain

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/query/explain_tsorder.sim

        """

        tdSql.execute(f"create database test")
        tdSql.execute(f"use test")
        tdSql.execute(
            f"CREATE STABLE `meters` (`ts` TIMESTAMP, `c2` INT) TAGS (`cc` VARCHAR(3))"
        )

        tdSql.execute(
            f'insert into d1 using meters tags("MY") values("2022-05-15 00:01:08.000 ",234)'
        )
        tdSql.execute(
            f'insert into d1 using meters tags("MY") values("2022-05-16 00:01:08.000 ",136)'
        )
        tdSql.execute(
            f'insert into d1 using meters tags("MY") values("2022-05-17 00:01:08.000 ", 59)'
        )
        tdSql.execute(
            f'insert into d1 using meters tags("MY") values("2022-05-18 00:01:08.000 ", 58)'
        )
        tdSql.execute(
            f'insert into d1 using meters tags("MY") values("2022-05-19 00:01:08.000 ",243)'
        )
        tdSql.execute(
            f'insert into d1 using meters tags("MY") values("2022-05-20 00:01:08.000 ",120)'
        )
        tdSql.execute(
            f'insert into d1 using meters tags("MY") values("2022-05-21 00:01:08.000 ", 11)'
        )
        tdSql.execute(
            f'insert into d1 using meters tags("MY") values("2022-05-22 00:01:08.000 ",196)'
        )
        tdSql.execute(
            f'insert into d1 using meters tags("MY") values("2022-05-23 00:01:08.000 ",116)'
        )
        tdSql.execute(
            f'insert into d1 using meters tags("MY") values("2022-05-24 00:01:08.000 ",210)'
        )

        tdSql.execute(
            f'insert into d2 using meters tags("HT") values("2022-05-15 00:01:08.000", 234)'
        )
        tdSql.execute(
            f'insert into d2 using meters tags("HT") values("2022-05-16 00:01:08.000", 136)'
        )
        tdSql.execute(
            f'insert into d2 using meters tags("HT") values("2022-05-17 00:01:08.000",  59)'
        )
        tdSql.execute(
            f'insert into d2 using meters tags("HT") values("2022-05-18 00:01:08.000",  58)'
        )
        tdSql.execute(
            f'insert into d2 using meters tags("HT") values("2022-05-19 00:01:08.000", 243)'
        )
        tdSql.execute(
            f'insert into d2 using meters tags("HT") values("2022-05-20 00:01:08.000", 120)'
        )
        tdSql.execute(
            f'insert into d2 using meters tags("HT") values("2022-05-21 00:01:08.000",  11)'
        )
        tdSql.execute(
            f'insert into d2 using meters tags("HT") values("2022-05-22 00:01:08.000", 196)'
        )
        tdSql.execute(
            f'insert into d2 using meters tags("HT") values("2022-05-23 00:01:08.000", 116)'
        )
        tdSql.execute(
            f'insert into d2 using meters tags("HT") values("2022-05-24 00:01:08.000", 210)'
        )

        resultfile = tdCom.generate_query_result(
            "cases/07-DataQuerying/14-Explain/t/explain_tsorder.sql", "explain_tsorder"
        )
        tdLog.info(f"resultfile: {resultfile}")
        tdCom.compare_result_files(
            resultfile, "cases/07-DataQuerying/14-Explain/r/explain_tsorder.result"
        )


# sleep 10000000
# system taos -P7100 -s 'source tsim/query/t/explain_tsorder.sql' | grep -v 'Query OK' | grep -v 'Client Version' > /tmp/explain_tsorder.result
# tdLog.info("echo ----------------------diff start-----------------------")
# tdLog.info("git diff --exit-code --color tsim/query/r/explain_tsorder.result /tmp/explain_tsorder.result
# system echo ----------------------diff succeed-----------------------
