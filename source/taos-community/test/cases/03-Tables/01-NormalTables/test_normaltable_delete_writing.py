import time
import threading
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNormalTableDeleteWriting:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_normal_table_delete_writing(self):
        """Normal table drop while writing

        1. Create a background process that continuously writes data
        2. Create a normal table
        3. Insert data 
        4. Query data
        5. Repeat 20 timeses

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/table/delete_writing.sim

        """

        tdLog.info(f"============================ dnode1 start")

        tdSql.execute(f"create database db")
        tdSql.execute(f"create table db.tb (ts timestamp, i int)")
        tdSql.execute(f"insert into db.tb values(now, 1)")

        tdLog.info(f"======== start back")
        self.running = True
        self.threadId = threading.Thread(target=self.threadLoop)
        self.threadId.start()
        
        tdLog.info(f"======== step1")
        for x in range(10):
            tdLog.info(f"drop table times {x}")
            tdSql.execute(f"drop table db.tb ")
            time.sleep(1)
            tdSql.execute(f"create table db.tb (ts timestamp, i int)")
            time.sleep(1)
        self.running = False
        self.threadId.join()

    def threadLoop(self):
        tdLog.info(f"thread is running ")
        new_sql = tdCom.newTdSql()
        x = 1
        while self.running:
            result = new_sql.is_err_sql(f"insert into db.tb values(now, {x})")
            tdLog.info(f"execute result:{result}, times:{x}")
            x = x + 1
            time.sleep(0.1)
        tdLog.info(f"thread is stopped ")
        new_sql.close()
