import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableDeleteWriting:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_delete_writing(self):
        """drop normal table（continue write data）

        1. create a background process that continuously writes data.
        2. create normal table
        3. insert data
        4. drop table
        5. continue 20 times

        Catalog:
            - Table:NormalTable:Drop

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated to new test framework, from tests/script/tsim/table/delete_writing.sim

        """

        tdLog.info(f"============================ dnode1 start")

        tdSql.execute(f"create database db")
        tdSql.execute(f"create table db.tb (ts timestamp, i int)")
        tdSql.execute(f"insert into db.tb values(now, 1)")

        tdLog.info(f"======== start back")
        # run_back tsim/table/back_insert.sim

        tdLog.info(f"======== step1")
        x = 1
        while x < 10:

            tdLog.info(f"drop table times {x}")
            tdSql.execute(f"drop table db.tb ")
            time.sleep(1)
            tdSql.execute(f"create table db.tb (ts timestamp, i int)")
            time.sleep(1)
            x = x + 1
