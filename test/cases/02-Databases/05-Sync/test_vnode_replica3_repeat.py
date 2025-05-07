import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestVnodeReplica3Repeat:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_vnode_replica3_repeat(self):
        """vnode replica3 repeat

        1. -

        Catalog:
            - DataBase:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated to new test framework, from tsim/vnode/replica3_repeat.sim

        """

        clusterComCheck.checkDnodes(4)

        tdLog.info(f'========= step1')
        tdSql.execute(f"create database db replica 3 vgroups 1")
        clusterComCheck.checkDbReady("db")

        tdSql.execute(f"create table db.tb (ts timestamp, i int)")
        tdSql.execute(f"insert into db.tb values(now, 1)")
        tdSql.query(f"select count(*) from db.tb")
        lastRows = tdSql.getRows()

        tdLog.info(f'======== step2')
        # run_back tsim/vnode/back_insert.sim
        time.sleep(2)

        tdLog.info(f'======== step3')
        for i in range(2):
            i = i + 1
            sc.dnodeStop(2)
            time.sleep(3)
            clusterComCheck.checkDnodes(3)
            clusterComCheck.checkDbReady("db")

            sc.dnodeStart(2)
            time.sleep(3)
            clusterComCheck.checkDnodes(4)
            clusterComCheck.checkDbReady("db")

            sc.dnodeStop(3)
            time.sleep(3)
            clusterComCheck.checkDnodes(3)
            clusterComCheck.checkDbReady("db")

            sc.dnodeStart(3)
            time.sleep(3)
            clusterComCheck.checkDnodes(4)
            clusterComCheck.checkDbReady("db")

            sc.dnodeStop(2)
            time.sleep(3)
            clusterComCheck.checkDnodes(3)
            clusterComCheck.checkDbReady("db")

            sc.dnodeStart(2)
            time.sleep(3)
            clusterComCheck.checkDnodes(4)
            clusterComCheck.checkDbReady("db")

            tdSql.query(f"select count(*) from db.tb")
            tdSql.checkAssert(tdSql.getData(0, 0) >= lastRows)
            lastRows = tdSql.getData(0, 0)
