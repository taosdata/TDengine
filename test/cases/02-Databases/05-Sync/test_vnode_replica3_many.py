import time
import threading
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestVnodeReplica3Many:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_vnode_replica3_many(self):
        """Write: replica-3 restart

        1. Start a 4-node cluster.
        2. Create four 1-vgroup, 3-replica databases and a normal table in each.
        3. In a background thread, insert one record into every table every 0.1 s (ignore any failures).
        4. Sequentially restart dnode1 → dnode2 → dnode3 → dnode4; repeat this full cycle 8 times.
        5. After every restart, confirm that row counts never decrease.

        Catalog:
            - DataBase:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/vnode/replica3_many.sim

        """

        tdLog.info(f"========== step0")
        clusterComCheck.checkDnodes(4)

        tdLog.info(f"========= step1")
        tdSql.execute(f"create database db1 replica 3 vgroups 1")
        tdSql.execute(f"create database db2 replica 3 vgroups 1")
        tdSql.execute(f"create database db3 replica 3 vgroups 1")
        tdSql.execute(f"create database db4 replica 3 vgroups 1")

        tdLog.info(f"=============== step12 wait vgroup2")
        clusterComCheck.checkDbReady("db1")

        tdLog.info(f"=============== step13 wait vgroup3")
        clusterComCheck.checkDbReady("db2")

        tdLog.info(f"=============== step14 wait vgroup4")

        clusterComCheck.checkDbReady("db3")

        tdLog.info(f"=============== step15 wait vgroup5")
        clusterComCheck.checkDbReady("db4")

        tdLog.info(f"=============== step16: create table")
        tdSql.execute(f"create table db1.tb1 (ts timestamp, i int)")
        tdSql.execute(f"create table db2.tb2 (ts timestamp, i int)")
        tdSql.execute(f"create table db3.tb3 (ts timestamp, i int)")
        tdSql.execute(f"create table db4.tb4 (ts timestamp, i int)")
        tdSql.execute(f"insert into db1.tb1 values(now, 1)")
        tdSql.execute(f"insert into db2.tb2 values(now, 1)")
        tdSql.execute(f"insert into db3.tb3 values(now, 1)")
        tdSql.execute(f"insert into db4.tb4 values(now, 1)")

        tdSql.query(f"select count(*) from db1.tb1")
        lastRows1 = tdSql.getRows()
        tdSql.query(f"select count(*) from db2.tb2")
        lastRows2 = tdSql.getRows()
        tdSql.query(f"select count(*) from db3.tb3")
        lastRows3 = tdSql.getRows()
        tdSql.query(f"select count(*) from db4.tb4")
        lastRows4 = tdSql.getRows()

        tdLog.info(f"======== step2")
        self.running = True
        self.threadId = threading.Thread(target=self.threadLoop)
        self.threadId.start()
        time.sleep(2)

        for i in range(2):
            i = i + 1
            sc.dnodeStop(2)
            time.sleep(3)
            clusterComCheck.checkDnodes(3)
            clusterComCheck.checkDbReady("db1")
            clusterComCheck.checkDbReady("db2")
            clusterComCheck.checkDbReady("db3")
            clusterComCheck.checkDbReady("db4")
            tdSql.query(f"select count(*) from db1.tb1")
            tdLog.info(f"rows:{tdSql.getData(0, 0)}")
            tdSql.checkAssert(tdSql.getData(0, 0) >= lastRows1)
            lastRows1 = tdSql.getData(0, 0)

            sc.dnodeStart(2)
            time.sleep(3)
            clusterComCheck.checkDnodes(4)
            clusterComCheck.checkDbReady("db1")
            clusterComCheck.checkDbReady("db2")
            clusterComCheck.checkDbReady("db3")
            clusterComCheck.checkDbReady("db4")
            tdSql.query(f"select count(*) from db1.tb1")
            tdLog.info(f"rows:{tdSql.getData(0, 0)}")
            tdSql.checkAssert(tdSql.getData(0, 0) >= lastRows1)
            lastRows1 = tdSql.getData(0, 0)

            sc.dnodeStop(3)
            time.sleep(3)
            clusterComCheck.checkDnodes(3)
            clusterComCheck.checkDbReady("db1")
            clusterComCheck.checkDbReady("db2")
            clusterComCheck.checkDbReady("db3")
            clusterComCheck.checkDbReady("db4")
            tdSql.query(f"select count(*) from db1.tb1")
            tdLog.info(f"rows:{tdSql.getData(0, 0)}")
            tdSql.checkAssert(tdSql.getData(0, 0) >= lastRows1)
            lastRows1 = tdSql.getData(0, 0)

            sc.dnodeStart(3)
            time.sleep(3)
            clusterComCheck.checkDnodes(4)
            clusterComCheck.checkDbReady("db1")
            clusterComCheck.checkDbReady("db2")
            clusterComCheck.checkDbReady("db3")
            clusterComCheck.checkDbReady("db4")
            tdSql.query(f"select count(*) from db1.tb1")
            tdLog.info(f"rows:{tdSql.getData(0, 0)}")
            tdSql.checkAssert(tdSql.getData(0, 0) >= lastRows1)
            lastRows1 = tdSql.getData(0, 0)

            sc.dnodeStop(2)
            time.sleep(3)
            clusterComCheck.checkDnodes(3)
            clusterComCheck.checkDbReady("db1")
            clusterComCheck.checkDbReady("db2")
            clusterComCheck.checkDbReady("db3")
            clusterComCheck.checkDbReady("db4")
            tdSql.query(f"select count(*) from db1.tb1")
            tdLog.info(f"rows:{tdSql.getData(0, 0)}")
            tdSql.checkAssert(tdSql.getData(0, 0) >= lastRows1)
            lastRows1 = tdSql.getData(0, 0)

            sc.dnodeStart(2)
            time.sleep(3)
            clusterComCheck.checkDnodes(4)
            clusterComCheck.checkDbReady("db1")
            clusterComCheck.checkDbReady("db2")
            clusterComCheck.checkDbReady("db3")
            clusterComCheck.checkDbReady("db4")

            tdSql.query(f"select count(*) from db1.tb1")
            tdSql.checkAssert(tdSql.getData(0, 0) >= lastRows1)
            lastRows1 = tdSql.getData(0, 0)

            tdSql.query(f"select count(*) from db2.tb2")
            tdSql.checkAssert(tdSql.getData(0, 0) >= lastRows2)
            lastRows2 = tdSql.getData(0, 0)

            tdSql.query(f"select count(*) from db3.tb3")
            tdSql.checkAssert(tdSql.getData(0, 0) >= lastRows3)
            lastRows3 = tdSql.getData(0, 0)

            tdSql.query(f"select count(*) from db4.tb4")
            tdSql.checkAssert(tdSql.getData(0, 0) >= lastRows4)
            lastRows4 = tdSql.getData(0, 0)

        self.running = False
        self.threadId.join()

    def threadLoop(self):
        tdLog.info(f"thread is running ")
        x = 1
        while self.running:
            result = tdSql.is_err_sql(f"insert into db1.tb1 values(now, {x}) ")
            result = tdSql.is_err_sql(f"insert into db2.tb2 values(now, {x}) ")
            result = tdSql.is_err_sql(f"insert into db3.tb3 values(now, {x}) ")
            result = tdSql.is_err_sql(f"insert into db4.tb4 values(now, {x}) ")
            
            # tdLog.info(f"execute result:{result}, times:{x}")
            x = x + 1
            time.sleep(0.1)
        tdLog.info(f"thread is stopped ")
