from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestDelete:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_delete(self):
        """delete

        1. -

        Catalog:
            - DataIngestion:Delete

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/query/delete_and_query.sim

        """

        tdSql.execute(f"create database if not exists test")
        tdSql.execute(f"use test")
        tdSql.execute(f"create table t1 (ts timestamp, c2 int)")
        tdSql.execute(f"insert into t1 values(now, 1)")

        tdSql.execute(f"delete from t1 where ts is null")
        tdSql.execute(f"delete from t1 where ts < now")
        tdSql.query(f"select ts from t1 order by ts asc")

        tdLog.info(f"----------rows:  {tdSql.getRows()})")
        tdSql.checkRows(0)

        tdSql.query(f"select ts from t1 order by ts desc")
        tdLog.info(f"----------rows:  {tdSql.getRows()})")
        tdSql.checkRows(0)
