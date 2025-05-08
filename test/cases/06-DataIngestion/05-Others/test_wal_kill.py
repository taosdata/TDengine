from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestWalKill:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_wal_kill(self):
        """wal kill

        1. create table
        2. insert data
        3. kill -9 and restart
        4. loop 3 times

        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-29 Simon Guan Migrated from tests/script/tsim/wal/kill.sim

        """
        tdLog.info(f"============== deploy")
        tdSql.execute(f"create database d1")
        tdSql.execute(f"use d1")

        tdSql.execute(f"create table t1 (ts timestamp, i int)")
        tdSql.execute(f"insert into t1 values(now, 1);")

        tdLog.info(f"===============  step3")
        tdSql.query(f"select * from t1;")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(1)

        sc.dnodeForceStop(1)

        tdLog.info(f"===============  step4")
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.query(f"select * from t1;")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(1)

        sc.dnodeForceStop(1)

        tdLog.info(f"===============  step5")
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)
        tdSql.query(f"select * from t1;")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(1)

        sc.dnodeForceStop(1)

        tdLog.info(f"===============  step6")
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)
        tdSql.query(f"select * from t1;")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(1)

        sc.dnodeForceStop(1)

        tdLog.info(f"===============  step7")
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)
        tdSql.query(f"select * from t1;")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(1)

        sc.dnodeForceStop(1)

        tdLog.info(f"===============  step8")
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)
        tdSql.query(f"select * from t1;")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(1)
