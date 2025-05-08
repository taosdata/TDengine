import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseDeleteWriting2:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_delete_writing2(self):
        """db writing 2

        1. -

        Catalog:
            - Database:Drop

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/db/delete_writing2.sim

        """
        
        tdSql.execute(f"create database db")
        tdSql.execute(f"create table db.tb (ts timestamp, i int)")
        tdSql.execute(f"insert into db.tb values(now, 1)")

        tdSql.execute(f"create database db2")
        tdSql.execute(f"create table db2.tb2 (ts timestamp, i int)")
        tdSql.execute(f"insert into db2.tb2 values(now, 1)")

        tdSql.execute(f"create database db3")
        tdSql.execute(f"create table db3.tb3 (ts timestamp, i int)")
        tdSql.execute(f"insert into db3.tb3 values(now, 1)")

        tdSql.execute(f"create database db4")
        tdSql.execute(f"create table db4.tb4 (ts timestamp, i int)")
        tdSql.execute(f"insert into db4.tb4 values(now, 1)")

        tdLog.info(f'======== start back')
        # run_back tsim/db/back_insert.sim
        
        tdLog.info(f'======== step1')
        x = 1
        while x < 10:

            tdLog.info(f'drop database times {x}')
            tdSql.execute(f"drop database if exists db")

            tdSql.execute(f"create database db")
            tdSql.execute(f"create table db.tb (ts timestamp, i int)")


            time.sleep(1)
            x = x + 1
