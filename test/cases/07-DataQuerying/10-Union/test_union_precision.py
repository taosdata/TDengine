from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestUnionPrecision:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_union_precision(self):
        """union

        1. -

        Catalog:
            - Query:Union

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/query/union_precision.sim

        """

        tdSql.execute(f"create  database tt precision 'us';")
        tdSql.execute(f"use tt ;")
        tdSql.execute(f"CREATE TABLE t_test_table (   ts TIMESTAMP,   a NCHAR(80),   b NCHAR(80),   c NCHAR(80) );")
        tdSql.execute(f"insert into t_test_table values('2024-04-07 14:30:22.823','aa','aa', 'aa');")
        tdSql.query(f"select * from t_test_table t  union all  select * from t_test_table t  ;")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2024-04-07 14:30:22.823")

