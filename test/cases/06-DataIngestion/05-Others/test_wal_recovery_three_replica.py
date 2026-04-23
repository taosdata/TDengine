import os
import time

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


class TestWalRecoveryThreeReplica:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_three_replica_auto_recovery(self):
        """Test three replica auto-recovery with corrupted WAL

        This test verifies:
        1. Create three replica database and insert data
        2. Stop one dnode and corrupt its WAL file
        3. Restart the dnode (no policy change needed)
        4. Verify dnode starts successfully and auto-recovers via Raft

        Catalog:
            - Database:WAL

        Since: v3.3.7.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-23 Created for WAL recovery policy feature
        """
        clusterComCheck.checkDnodes(3)

        tdSql.execute("create database test_three replica 3 wal_level 1")
        tdSql.execute("use test_three")
        tdSql.execute("create table t1 (ts timestamp, i int)")
        for i in range(100):
            tdSql.execute(f"insert into t1 values(now+{i}s, {i})")
        tdSql.query("select count(*) from t1")
        tdSql.checkData(0, 0, 100)

        tdSql.query("show test_three.vgroups")
        vgId = tdSql.getData(0, 0)

        sc.dnodeStop(1)
        corrupt_wal_file(1, vgId)

        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(3)
        time.sleep(5)

        tdSql.query("select count(*) from test_three.t1")
        tdSql.checkData(0, 0, 100)

        tdSql.execute("drop database if exists test_three")
