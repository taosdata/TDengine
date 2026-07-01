import glob
import os
import shutil
import time

from new_test_framework.utils import tdDnodes, tdSql
from test_tsdb_force_repair_base import TsdbForceRepairBase


class TestWalForceRepairE2E(TsdbForceRepairBase):
    def _get_wal_dir(self, vnode_id):
        return os.path.join(
            self._get_primary_data_dir(), "vnode", f"vnode{vnode_id}", "wal"
        )

    def _get_wal_files(self, vnode_id):
        wal_dir = self._get_wal_dir(vnode_id)
        log_files = sorted(glob.glob(os.path.join(wal_dir, "*.log")))
        idx_files = sorted(glob.glob(os.path.join(wal_dir, "*.idx")))
        return wal_dir, log_files, idx_files

    def _remove_wal_pair(self, log_file, idx_file):
        os.remove(log_file)
        os.remove(idx_file)

    def _find_corrupted_wal_dirs(self, vnode_id):
        vnode_dir = os.path.dirname(self._get_wal_dir(vnode_id))
        return sorted(glob.glob(os.path.join(vnode_dir, "wal.corrupted.*")))

    def test_wal_force_repair_recreates_corrupted_wal_dir(self):
        """WAL force repair should rename corrupted wal directory and recover startup.

        1. Create real vnode data with small WAL segment size and confirm multiple wal files exist.
        2. Remove a middle wal log/index pair and run wal force repair for that vnode.
        3. Verify a `wal.corrupted.*` directory is created and the database remains writable after restart.

        Since: v3.4.1.0

        Labels: common,ci
        """
        dbname = f"wal_force_repair_{int(time.time())}"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(
            f"create database {dbname} vgroups 1 "
            f"wal_retention_period 3600 wal_retention_size -1 wal_segment_size 1"
        )
        tdSql.execute(f"create table {dbname}.t1(ts timestamp, v int)")
        for batch in range(6):
            values = []
            base = batch * 200
            for i in range(200):
                values.append(f"(now+{base + i}s, {base + i})")
            tdSql.execute(f"insert into {dbname}.t1 values " + ",".join(values))
        tdSql.execute(f"flush database {dbname}")
        time.sleep(2)

        vnode_id = self._get_vnode_id_for_db(dbname)
        wal_dir, log_files, idx_files = self._get_wal_files(vnode_id)
        tdSql.checkEqual(len(log_files) > 2, True)
        tdSql.checkEqual(len(idx_files) > 2, True)
        target_log = log_files[1]
        target_idx = idx_files[1]

        try:
            tdDnodes.stop(1)
            time.sleep(2)
            self._remove_wal_pair(target_log, target_idx)
            code, output = self._run_taosd_with_cfg(
                f"-r --mode force --node-type vnode --repair-target wal:vnode={vnode_id} --log-output /dev/null",
                timeout_sec=10,
            )
            corrupted_dirs = self._find_corrupted_wal_dirs(vnode_id)
        finally:
            self._restart_taosd_and_wait_ready()

        tdSql.checkEqual(code, 0)
        tdSql.checkEqual(os.path.isdir(wal_dir), True)
        tdSql.checkEqual(len(corrupted_dirs) > 0, True)
        tdSql.query(f"select count(*) from {dbname}.t1")
        tdSql.checkEqual(tdSql.queryResult[0][0] >= 0, True)
        tdSql.execute(f"insert into {dbname}.t1 values(now+2000s, 50000)")
        tdSql.execute(f"flush database {dbname}")
        tdSql.query(f"select count(*) from {dbname}.t1 where v=50000")
        tdSql.checkData(0, 0, 1)
