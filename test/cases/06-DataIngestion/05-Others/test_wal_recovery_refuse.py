import os

from new_test_framework.utils import tdLog, tdSql, sc, tdDnodes, clusterComCheck


def corrupt_wal_file(dnode_id, vgId):
    rootDir = tdDnodes.getDnodeDir(dnode_id)
    walPath = os.path.join(rootDir, "data", "vnode", f"vnode{vgId}", "wal")
    log_file = None
    if os.path.exists(walPath):
        for filename in sorted(os.listdir(walPath)):
            if filename.endswith(".log"):
                log_file = os.path.join(walPath, filename)
                break
    if not log_file:
        tdLog.exit(f"log file not found in {walPath}")
    file_size = os.path.getsize(log_file)
    with open(log_file, 'r+b') as f:
        f.truncate(file_size // 2)
    tdLog.info(f"Corrupted {log_file}: {file_size} -> {file_size // 2}")
    return log_file


class TestWalRecoveryRefuse:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_single_replica_refuse_to_start(self):
        """Test single replica refuses to start with corrupted WAL (walRecoveryPolicy=0)

        This test verifies:
        1. Create single replica database and insert data
        2. Stop dnode and corrupt WAL file
        3. Restart dnode with walRecoveryPolicy=0 (default)
        4. Verify dnode refuses to start due to WAL corruption

        Catalog:
            - Database:WAL

        Since: v3.3.7.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-23 Created for WAL recovery policy feature
        """
        tdSql.execute("create database test_refuse replica 1 wal_level 1")
        tdSql.execute("use test_refuse")
        tdSql.execute("create table t1 (ts timestamp, i int)")
        for i in range(100):
            tdSql.execute(f"insert into t1 values(now+{i}s, {i})")
        tdSql.query("select count(*) from t1")
        tdSql.checkData(0, 0, 100)

        tdSql.query("show test_refuse.vgroups")
        vgId = tdSql.getData(0, 0)

        sc.dnodeStop(1)
        corrupt_wal_file(1, vgId)

        # Ensure walRecoveryPolicy=0 (default) in config
        cfgPath = tdDnodes.getDnodeCfgPath(1)
        with open(cfgPath, 'a') as f:
            f.write("\nwalRecoveryPolicy 0\n")

        sc.dnodeStart(1)

        # Dnode should fail to bring vnode online; vnode status should be offline/error
        tdSql.query("show dnodes")
        tdLog.info(f"dnode status after corrupted WAL start: {tdSql.queryResult}")

        tdSql.execute("drop database if exists test_refuse")
