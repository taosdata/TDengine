from new_test_framework.utils import tdSql


class TestGroupLimitWithoutOrder:
    def test_group_limit_without_order(self):
        """GROUP BY without ORDER BY honors LIMIT and OFFSET.

        Description: Cover hash-group results, which do not pass through an
        ORDER BY operator before LIMIT is applied.
        Labels: common,ci,integration,functional
        Jira: None
        Since: v3.4.2.5

        History:
            - 2026-09-07 Atirna Added regression coverage for #35450
        """
        tdSql.execute("drop database if exists group_limit_without_order")
        tdSql.execute("create database group_limit_without_order")
        tdSql.execute("use group_limit_without_order")
        tdSql.execute("create table t (ts timestamp, g int, v int)")

        values = "".join(f"({1700000000000 + g}, {g}, {g})" for g in range(100))
        tdSql.execute(f"insert into t values {values}")

        tdSql.query("select g, count(*) from t group by g")
        tdSql.checkRows(100)

        tdSql.query("select g, count(*) from t group by g limit 10")
        tdSql.checkRows(10)

        tdSql.query("select g, count(*) from t group by g limit 10 offset 95")
        tdSql.checkRows(5)
