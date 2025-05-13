import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseDeleteWriting1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_delete_writing1(self):
        """db writing 1

        1. -

        Catalog:
            - Database:Drop

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/db/delete_writing1.sim

        """

        tdSql.execute(f"create database db")
        tdSql.execute(f"create table db.tb (ts timestamp, i int)")
        tdSql.execute(f"insert into db.tb values(now, 1)")

        tdLog.info(f"======== start back")

        # self.threadLoop

        tdLog.info(f"======== step1")
        x = 1
        while x < 10:
            tdLog.info(f"drop database times {x}")
            tdSql.execute(f"drop database if exists db")
            tdSql.execute(f"create database db")
            tdSql.execute(f"create table db.tb (ts timestamp, i int)")
            time.sleep(1)
            x = x + 1

        # self.threadLoopStop()

    def threadLoop(self):
        x = 0
        while True:
            tdSql.isErrorSql(f"insert into db.tb values(now, {x} ")
            x = x + 1
