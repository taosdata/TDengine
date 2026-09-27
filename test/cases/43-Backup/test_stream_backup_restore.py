# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-
import os
import shutil
import time

from new_test_framework.utils import tdLog, tdSql, sc

from backup_restore_base import BackupRestoreTestBase


class TestStreamBackupRestore(BackupRestoreTestBase):
    """Stream (snode) backup and restore on a single node.

    Creates a snode on dnode 1, a source super table and a state-window
    stream (mirroring the working stream test case test_6771200393): the
    stream result table is created by the stream itself on first output.

    Scenario 1: basic backup/restore. Writes data before and after the
    stream is created, takes a full backup, clears the data, restores, and
    verifies that the snode and the stream survive, the result rows are
    preserved, and the stream keeps computing new results.

    Scenario 2: checkpoint backup. The stream writes its checkpoint
    periodically (10 min), but STOP STREAM flushes it immediately. The test
    stops the stream to force a checkpoint file, verifies the backup
    contains it, restores, restarts the stream and verifies it resumes
    (checkpoint loaded) and keeps computing.

    Since: v3.4.3.0

    Labels: common,ci,integration,functional
    """

    backup_root = None

    DB = "st"
    SOURCE_STB = "pk"
    RESULT_TABLE = "pk_result"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.dnode_id = 1
        cls.dnode_nums = 1
        cls.backup_dnode = 1
        cls.backup_root = f"/tmp/backup_stream_{int(time.time() * 1000)}"

    def teardown_class(cls):
        # remove the backup chains
        if cls.backup_root and os.path.exists(cls.backup_root):
            shutil.rmtree(cls.backup_root, ignore_errors=True)

    def _prepare_stream_env(self):
        """Create the database and a snode on dnode 1.

        The source table, the stream and its result table all live in the
        same database, exactly like the working stream test case
        (test_6771200393). The WAL is kept for 1h (wal_retention_period 3600)
        so the stream reader can always pull the written records.
        """
        tdSql.execute(f"DROP DATABASE IF EXISTS {self.DB}")
        tdSql.execute(f"CREATE DATABASE {self.DB} vgroups 1 wal_retention_period 3600")
        tdSql.execute(f"USE {self.DB}")
        # source super table with a primary-key column
        tdSql.execute(f"CREATE TABLE {self.SOURCE_STB} (ts TIMESTAMP, c1 VARCHAR(16) primary key, c2 INT) tags(t INT)")
        # a snode must be created explicitly before any stream can run
        tdSql.execute("CREATE SNODE ON DNODE 1")
        tdSql.query("SHOW SNODES")
        tdSql.checkKeyExist(1)

    def _create_stream(self, stream_name):
        """Create a state-window stream from st.pk into st.pk_result.

        The result table is created by the stream on its first output (no
        pre-created table), matching the working stream test case.
        """
        tdSql.execute(
            f"CREATE STREAM {stream_name} state_window(c2) from {self.DB}.{self.SOURCE_STB} "
            f"partition by tbname into {self.DB}.{self.RESULT_TABLE} "
            f"as select * from {self.DB}.{self.SOURCE_STB} "
            f"where ts >= _twstart and ts <= _twend"
        )

    def _wait_stream_running(self, stream_name, timeout=60):
        """Poll ins_streams until the stream is running."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                tdSql.query("SELECT status FROM information_schema.ins_streams "
                            f"WHERE stream_name = '{stream_name}'")
                if tdSql.queryRows > 0 and tdSql.getData(0, 0) == "Running":
                    return
            except Exception:
                pass
            time.sleep(1)
        tdLog.exit(f"stream {stream_name} did not become running within {timeout}s")

    def _wait_stream_status(self, stream_name, status, timeout=60):
        """Poll ins_streams until the stream reaches the given status."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                tdSql.query("SELECT status FROM information_schema.ins_streams "
                            f"WHERE stream_name = '{stream_name}'")
                if tdSql.queryRows > 0 and tdSql.getData(0, 0) == status:
                    return
            except Exception:
                pass
            time.sleep(1)
        tdLog.exit(f"stream {stream_name} did not reach {status} within {timeout}s")

    def _wait_checkpoint_file(self, timeout=30):
        """Wait until a checkpoint file exists under the local snode dir.

        STOP STREAM marks the stream Stopped immediately but the checkpoint is
        written asynchronously by the task undeploy, so wait for the file.
        """
        ck_dir = os.path.join(sc.clusterRootPath(), "dnode1", "data", "snode", "checkpoint")
        deadline = time.time() + timeout
        while time.time() < deadline:
            ck_files = [f for f in os.listdir(ck_dir) if f.endswith(".ck")] \
                if os.path.isdir(ck_dir) else []
            if ck_files:
                tdLog.info(f"checkpoint file(s) found: {ck_files}")
                return
            time.sleep(1)
        tdLog.exit(f"no checkpoint file appeared under {ck_dir} within {timeout}s")

    def _wait_result_rows(self, table, timeout=60):
        """Poll the result table until it has rows.

        The result table is created by the stream on its first output, and the
        row count jumps directly to the number of state windows, so we only
        check that it is non-empty.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                tdSql.query(f"SELECT COUNT(*) FROM {self.DB}.{table}")
                if tdSql.queryRows > 0 and tdSql.getData(0, 0) > 0:
                    return int(tdSql.getData(0, 0))
            except Exception:
                pass
            time.sleep(1)
        tdLog.exit(f"result table {self.DB}.{table} did not get rows within {timeout}s")

    def _verify_backup_contains_snode(self, chain_dir, require_checkpoint=True):
        """The backup must contain the snode directory with snode.json.

        require_checkpoint: the stream checkpoint is written periodically
        (10 min), but STOP STREAM flushes it immediately; when the test
        stopped the stream before the backup, at least one checkpoint file
        must be in the backup.
        """
        backup_id = os.path.basename(chain_dir)
        snode_dir = os.path.join(chain_dir, backup_id, "snode", f"dnode{self.dnode_id}")
        tdLog.info(f"checking snode backup dir: {snode_dir}")
        snode_json = os.path.join(snode_dir, "snode.json")
        if not os.path.exists(snode_json):
            tdLog.exit(f"snode.json not found in backup: {snode_json}")
        ck_dir = os.path.join(snode_dir, "checkpoint")
        ck_files = [f for f in os.listdir(ck_dir) if f.endswith(".ck")] if os.path.isdir(ck_dir) else []
        if require_checkpoint and not ck_files:
            tdLog.exit(f"expected checkpoint files in backup snode dir, got none: {ck_dir}")
        tdLog.info(f"verified snode backup: snode.json + {len(ck_files)} checkpoint(s)")

    def _result_rows(self):
        tdSql.query(f"SELECT COUNT(*) FROM {self.DB}.{self.RESULT_TABLE}")
        return int(tdSql.getData(0, 0))

    def test_stream_backup_restore(self):
        """Stream (snode) backup and restore on a single node.

        1. Create a snode on dnode 1, a source super table
        2. Insert data, create a state-window stream, wait until running
        3. Insert more data -> the stream computes result rows
        4. STOP STREAM -> the checkpoint is flushed to disk
        5. CREATE FULL BACKUP -> success; verify snode/ + checkpoint in backup
        6. Stop taosd, clear data, restore from the backup
        7. Restart taosd: snode + stream are restored, result rows match
        8. START STREAM -> the stream resumes from the checkpoint and keeps
           computing new result rows

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-08-21 Bomin Zhang created

        """
        tdLog.info("=========== stream backup/restore (single node) ===========")
        self._prepare_stream_env()

        # batch 0: data written before the stream is created.
        # Timestamps are relative to now so the data never ages out of the
        # database keep window (a hard-coded past timestamp would eventually
        # be purged and break the test).
        base_ts = int(time.time() * 1000) - 1000
        tdSql.execute(f"INSERT INTO t1 USING {self.SOURCE_STB} TAGS(1) VALUES ({base_ts}, 'pk1', 1)")

        # create the stream and wait until it is running
        self._create_stream("s1")
        self._wait_stream_running("s1")

        # batch 1: data written after the stream is running -> the stream
        # computes result rows (state_window groups by c2)
        tdSql.execute(f"INSERT INTO t2 USING {self.SOURCE_STB} TAGS(2) VALUES "
                      f"({base_ts + 1}, 'pk2', 11), ({base_ts + 1}, 'pk22', 12)")
        tdSql.execute(f"INSERT INTO t3 USING {self.SOURCE_STB} TAGS(3) VALUES "
                      f"({base_ts + 2}, 'pk3', 13), ({base_ts + 2}, 'pk33', 14)")
        tdSql.execute(f"INSERT INTO t4 USING {self.SOURCE_STB} TAGS(4) VALUES "
                      f"({base_ts + 3}, 'pk4', 15), ({base_ts + 3}, 'pk44', 16)")

        # the stream computes -> result table gets rows
        self._wait_result_rows(self.RESULT_TABLE, timeout=60)
        tdLog.info("stream computed result rows in pk_result")

        # STOP the stream: this flushes the checkpoint to disk immediately
        # (the periodic checkpoint is written every 10 min, too slow for a
        # test), so the backup below contains a real checkpoint file.
        # The STOP command returns as soon as the stream is marked Stopped,
        # but the checkpoint is written asynchronously by the task undeploy,
        # so we must wait for the .ck file to actually appear on disk.
        tdLog.info("stopping stream to flush the checkpoint")
        tdSql.execute(f"STOP STREAM {self.DB}.s1")
        self._wait_stream_status("s1", "Stopped")
        self._wait_checkpoint_file()
        tdLog.info("stream stopped, checkpoint flushed")

        # take the backup; it must contain the snode checkpoint files
        chain = self._start_backup("stream_plain")
        self._verify_backup_contains_snode(chain, require_checkpoint=True)

        # stop, clear, recreate dnode cfg, restore, restart
        self._stop_taosd()
        self._clear_data()
        self._bootstrap_dnode_cfg()
        code, out = self._restore(self.dnode_id, chain)
        if code != 0:
            tdLog.exit(f"restore from {chain} failed with exit code {code}: {out[-500:]}")
        self._start_taosd()

        # the snode and the stream must survive the restore
        tdSql.query("SELECT count(*) FROM information_schema.ins_snodes")
        tdSql.checkData(0, 0, 1)
        tdLog.info("snode survived the restore")

        # the computed result must be preserved
        rows_before = self._result_rows()
        if rows_before < 1:
            tdLog.exit("stream result table is empty after restore")
        tdLog.info(f"stream result preserved after restore ({rows_before} rows)")

        # restart the stream: it resumes from the restored checkpoint
        tdLog.info("restarting stream after restore")
        tdSql.execute(f"START STREAM {self.DB}.s1")
        self._wait_stream_running("s1")
        tdLog.info("stream restarted and resumed from the checkpoint")

        # batch 2: write after the restore -> the stream keeps computing
        tdSql.execute(f"INSERT INTO t5 USING {self.SOURCE_STB} TAGS(5) VALUES "
                      f"({base_ts + 1001}, 'pk5', 21), ({base_ts + 1001}, 'pk55', 22)")
        # the new state windows must add rows to the result; poll until the
        # row count grows (the stream computes asynchronously)
        deadline = time.time() + 60
        while time.time() < deadline:
            rows_now = self._result_rows()
            if rows_now > rows_before:
                tdLog.info(f"stream kept computing after restore ({rows_now} rows)")
                return
            time.sleep(1)
        tdLog.exit(f"stream did not keep computing after restore: {rows_now} <= {rows_before}")
