import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseShowCreateDb:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_show_create_db(self):
        """show create db

        1. -

        Catalog:
            - Database:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/db/show_create_db.sim

        """

        tdLog.info(f"=============== step2")
        tdSql.execute(f"create database db")
        tdSql.query(f"show create database db")

        tdSql.checkRows(1)

        tdLog.info(f"=============== step3")
        tdSql.execute(f"use db")
        tdSql.query(f"show create database db")

        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "db")

        tdSql.execute(f"drop database db")
