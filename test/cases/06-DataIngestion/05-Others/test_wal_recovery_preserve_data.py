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
        f.truncate(file_size * 7 // 10)
    tdLog.info(f"Corrupted {log_file}: {file_size} -> {file_size * 7 // 10}")
    return log_file


class TestWalRecoveryPreserveData:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_wal_truncation_preserves_valid_data(self):
        """Test WAL truncation preserves valid data before corruption point

        This test verifies:
        1. Insert first batch and flush to ensure data is persisted
        2. Insert second batch (stays in WAL)
        3. Stop dnode and corrupt WAL file (truncate at 70%)
        4. Restart with walRecoveryPolicy=1
        5. Verify flushed data (first batch) is preserved

        Catalog:
            - Database:WAL

        Since: v3.3.7.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-04-23 Created for WAL recovery policy feature
        """
        tdSql.execute("create database test_preserve replica 1 wal_level 1")
        tdSql.execute("use test_preserve")
        tdSql.execute("create table t1 (ts timestamp, i int)")

        for i in range(50):
            tdSql.execute(f"insert into t1 values(now+{i}s, {i})")
        tdSql.execute("flush database test_preserve")
        time.sleep(2)

        for i in range(50, 100):
            tdSql.execute(f"insert into t1 values(now+{i}s, {i})")

        tdSql.query("show test_preserve.vgroups")
        vgId = tdSql.getData(0, 0)

        sc.dnodeStop(1)
        corrupt_wal_file(1, vgId)

        cfgPath = tdDnodes.getDnodeCfgPath(1)
        with open(cfgPath, 'a') as f:
            f.write("\nwalRecoveryPolicy 1\n")

        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.query("select count(*) from test_preserve.t1")
        count = tdSql.getData(0, 0)
        tdLog.info(f"Preserved data count: {count}")

        assert count >= 50, f"At least 50 flushed rows should be preserved, but got {count}"

        tdSql.execute("drop database if exists test_preserve")
