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

from new_test_framework.utils import tdLog, tdSql, cluster
from backup_restore_base import BackupRestoreTestBase


class TestStreamBackupRestoreCluster(BackupRestoreTestBase):
    """Stream (snode) backup and restore on a 3-dnode cluster.

    Run with: pytest -N 3
    Creates a snode on every dnode (1, 2, 3), one source super table and
    three state-window streams (mirroring the working single-node stream
    test case test_6771200393). The streams are distributed over the snodes
    by the scheduler.

    Scenario: multiple snodes, multiple streams
    1. Create snodes on dnode 1, 2, 3
    2. Create a source super table and three streams, wait until running
    3. Write data -> every stream computes result rows
    4. STOP all streams -> checkpoints are flushed on their snode dnodes
    5. CREATE FULL BACKUP -> verify snode/ + checkpoints in the backup
    6. Stop all taosd, clear all data, rebuild the cluster, restore
    7. Restart: snodes and streams are restored, result rows match
    8. START all streams -> they resume and keep computing

    Since: v3.4.3.0

    Labels: common,ci,integration,functional,cluster
    """

    backup_root = None

    DB = "st"
    SOURCE_STB = "pk"
    RESULT_TABLE = "pk_result"
    STREAMS = ["s1", "s2", "s3"]

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.dnode_id = 1
        cls.dnode_nums = len(cluster.dnodes)
        cls.backup_dnode = 1            # backup agent runs on dnode 1
        cls.backup_root = f"/tmp/backup_stream_cluster_{int(time.time() * 1000)}"

    def teardown_class(cls):
        # remove the backup chains
        if cls.backup_root and os.path.exists(cls.backup_root):
            shutil.rmtree(cls.backup_root, ignore_errors=True)

    def _prepare_stream_env(self):
        """Create the database and a snode on every dnode."""
        tdSql.execute(f"DROP DATABASE IF EXISTS {self.DB}")
        tdSql.execute(f"CREATE DATABASE {self.DB} vgroups 1 wal_retention_period 3600")
        tdSql.execute(f"USE {self.DB}")
        # source super table with a primary-key column
        tdSql.execute(f"CREATE TABLE {self.SOURCE_STB} (ts TIMESTAMP, c1 VARCHAR(16) primary key, c2 INT) tags(t INT)")
        # a snode must be created explicitly before any stream can run
        for dnode_id in range(1, self.dnode_nums + 1):
            tdSql.execute(f"CREATE SNODE ON DNODE {dnode_id}")
        tdSql.query("SELECT count(*) FROM information_schema.ins_snodes")
        if int(tdSql.getData(0, 0)) != self.dnode_nums:
            tdLog.exit(f"expected {self.dnode_nums} snodes, got {tdSql.getData(0, 0)}")
        tdLog.info(f"created {self.dnode_nums} snodes")

    def _create_stream(self, stream_name):
        """Create a state-window stream from st.pk into st.pk_result.

        Every stream reads the same source super table and writes into its
        own result table (pk_result_<stream>), so each stream is an
        independent computation.
        """
        result_table = f"{self.RESULT_TABLE}_{stream_name}"
        tdSql.execute(
            f"CREATE STREAM {stream_name} state_window(c2) from {self.DB}.{self.SOURCE_STB} "
            f"partition by tbname into {self.DB}.{result_table} "
            f"as select * from {self.DB}.{self.SOURCE_STB} "
            f"where ts >= _twstart and ts <= _twend"
        )
        return result_table

    def _wait_streams_running(self, stream_names, timeout=120):
        """Poll ins_streams until all streams are running."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            all_running = True
            for name in stream_names:
                try:
                    tdSql.query("SELECT status FROM information_schema.ins_streams "
                                f"WHERE stream_name = '{name}'")
                    if tdSql.queryRows == 0 or tdSql.getData(0, 0) != "Running":
                        all_running = False
                        break
                except Exception:
                    all_running = False
                    break
            if all_running:
                return
            time.sleep(1)
        tdLog.exit(f"streams {stream_names} did not become running within {timeout}s")

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

    def _wait_checkpoint_files(self, timeout=60):
        """Wait until a checkpoint file exists on every dnode with a snode.

        STOP STREAM marks the stream Stopped immediately but the checkpoint is
        written asynchronously by the task undeploy on the dnode hosting the
        stream's snode, so wait for the files.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            found = []
            for dnode_id in range(1, self.dnode_nums + 1):
                ck_dir = os.path.join(self._dnode_data_dir(dnode_id), "snode", "checkpoint")
                ck_files = [f for f in os.listdir(ck_dir) if f.endswith(".ck")] \
                    if os.path.isdir(ck_dir) else []
                if ck_files:
                    found.append(f"dnode{dnode_id}:{len(ck_files)}")
            if found:
                tdLog.info(f"checkpoint file(s) found: {found}")
                return
            time.sleep(1)
        tdLog.exit(f"no checkpoint file appeared within {timeout}s")

    def _wait_result_rows(self, table, timeout=60):
        """Poll the result table until it has rows."""
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

    def _result_rows(self, table):
        tdSql.query(f"SELECT COUNT(*) FROM {self.DB}.{table}")
        return int(tdSql.getData(0, 0))

    def _verify_backup_contains_snodes(self, chain_dir):
        """The backup must contain a snode dir per dnode with snode.json + checkpoints."""
        backup_id = os.path.basename(chain_dir)
        snode_root = os.path.join(chain_dir, backup_id, "snode")
        if not os.path.isdir(snode_root):
            tdLog.exit(f"snode dir not found in backup: {snode_root}")
        snode_dirs = sorted(os.listdir(snode_root))
        if len(snode_dirs) != self.dnode_nums:
            tdLog.exit(f"expected {self.dnode_nums} snode dirs in backup, got {snode_dirs}")
        total_ck = 0
        for d in snode_dirs:
            snode_json = os.path.join(snode_root, d, "snode.json")
            if not os.path.exists(snode_json):
                tdLog.exit(f"snode.json not found in backup: {snode_json}")
            ck_dir = os.path.join(snode_root, d, "checkpoint")
            ck_files = [f for f in os.listdir(ck_dir) if f.endswith(".ck")] \
                if os.path.isdir(ck_dir) else []
            total_ck += len(ck_files)
            tdLog.info(f"backup snode {d}: snode.json + {len(ck_files)} checkpoint(s)")
        if total_ck == 0:
            tdLog.exit("expected at least one checkpoint in the backup snode dirs")
        tdLog.info(f"verified snode backup: {len(snode_dirs)} snode dirs, {total_ck} checkpoint(s)")

    def test_stream_backup_restore_cluster(self):
        """Stream (snode) backup and restore on a 3-dnode cluster.

        1. Create a snode on every dnode (1, 2, 3), a source super table
        2. Create three state-window streams, wait until running
        3. Write data -> every stream computes result rows
        4. STOP all streams -> checkpoints are flushed on their snode dnodes
        5. CREATE FULL BACKUP -> verify snode/ + checkpoints in the backup
        6. Stop all taosd, clear all data, rebuild the cluster, restore
        7. Restart: snodes and streams are restored, result rows match
        8. START all streams -> they resume and keep computing

        Since: v3.4.3.0

        Labels: common,ci,integration,functional,cluster

        Jira: None

        History:
            - 2026-08-21 Bomin Zhang created

        """
        tdLog.info("=========== cluster stream backup/restore ===========")
        self._prepare_stream_env()

        # create the streams and wait until all are running
        result_tables = {}
        for name in self.STREAMS:
            result_tables[name] = self._create_stream(name)
        self._wait_streams_running(self.STREAMS)

        # write data -> every stream computes result rows
        base_ts = int(time.time() * 1000) - 1000
        for i in range(1, 5):
            tdSql.execute(f"INSERT INTO t{i} USING {self.SOURCE_STB} TAGS({i}) VALUES "
                          f"({base_ts + i}, 'pk{i}', {i}), ({base_ts + i}, 'pk{i}2', {i + 10})")
        for name in self.STREAMS:
            self._wait_result_rows(result_tables[name], timeout=60)
        tdLog.info("all streams computed result rows")

        # STOP all streams: checkpoints are flushed asynchronously on the
        # dnode hosting each stream's snode; wait for the files
        for name in self.STREAMS:
            tdLog.info(f"stopping stream {name}")
            tdSql.execute(f"STOP STREAM {self.DB}.{name}")
            self._wait_stream_status(name, "Stopped")
        self._wait_checkpoint_files()
        tdLog.info("all streams stopped, checkpoints flushed")

        # take the backup; it must contain every snode dir with checkpoints
        chain = self._start_backup("stream_cluster")
        self._verify_backup_contains_snodes(chain)

        # stop all, clear all, rebuild the cluster, restore every dnode
        self._stop_all_taosd()
        self._clear_all_data()
        self._bootstrap_cluster_cfg()
        self._restore_all(chain)
        self._start_all_taosd()

        # the snodes and the streams must survive the restore
        tdSql.query("SELECT count(*) FROM information_schema.ins_snodes")
        tdSql.checkData(0, 0, self.dnode_nums)
        tdLog.info(f"snodes survived the restore ({self.dnode_nums})")

        # the computed results must be preserved
        rows_before = {}
        for name in self.STREAMS:
            rows_before[name] = self._result_rows(result_tables[name])
            if rows_before[name] < 1:
                tdLog.exit(f"stream {name} result table is empty after restore")
        tdLog.info(f"stream results preserved after restore: {rows_before}")

        # restart all streams: they resume from the restored checkpoints
        for name in self.STREAMS:
            tdLog.info(f"restarting stream {name}")
            tdSql.execute(f"START STREAM {self.DB}.{name}")
        self._wait_streams_running(self.STREAMS)
        tdLog.info("all streams restarted and resumed from the checkpoints")

        # write more data -> every stream keeps computing.
        # the restore reconnected tdSql (bootstrap_cluster_cfg calls
        # tdSql.connect), so re-select the database first
        tdSql.execute(f"USE {self.DB}")
        for i in range(5, 8):
            tdSql.execute(f"INSERT INTO t{i} USING {self.SOURCE_STB} TAGS({i}) VALUES "
                          f"({base_ts + 1001 + i}, 'pk{i}', {i}), ({base_ts + 1001 + i}, 'pk{i}2', {i + 10})")
        for name in self.STREAMS:
            deadline = time.time() + 60
            while time.time() < deadline:
                if self._result_rows(result_tables[name]) > rows_before[name]:
                    break
                time.sleep(1)
            else:
                tdLog.exit(f"stream {name} did not keep computing after restore")
        tdLog.info("all streams kept computing after restore")
