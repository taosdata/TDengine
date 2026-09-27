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
import re
import shutil
import subprocess
import time

from new_test_framework.utils import tdLog, tdSql, sc, cluster
from new_test_framework.utils import epath

from backup_restore_base import BackupRestoreTestBase


class TestClusterBackupRestore(BackupRestoreTestBase):
    """Cluster-level backup and restore (3 dnodes).

    Creates two databases with a known number of rows each - db1 is 3-replica
    and db2 is single-replica - takes a full backup (plain and encrypted),
    clears all dnode data, restores every dnode from the backup file, and
    verifies the row counts and the replica counts still match.  Also covers
    the failure cases of restoring an encrypted backup without the password
    and restoring a plain backup with a password.
    """

    backup_root = None
    backup_plain = None     # chain dir of the plain backup
    backup_enc = None       # chain dir of the encrypted backup

    DB_ROWS = 100
    VGROUPS = 2
    REPLICA = 3

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.dnode_id = 1
        cls.dnode_nums = len(cluster.dnodes)
        cls.backup_dnode = 1            # backup agent runs on dnode 1
        cls.backup_root = f"/tmp/backup_cluster_{int(time.time() * 1000)}"
        cls._extra_dnode_proc = None    # handle of the extra dnode subprocess

    def teardown_class(cls):
        # remove any extra dnode (e.g. dnode4) added by the mismatch tests:
        # terminate it directly via the saved handle first (more reliable than
        # matching by cfg path), then pkill as a fallback for any leftovers
        cls._stop_extra_dnode_proc()
        cls._kill_extra_dnode(4)
        # remove the backup chains
        if cls.backup_root and os.path.exists(cls.backup_root):
            shutil.rmtree(cls.backup_root, ignore_errors=True)

    def _prepare_databases(self):
        """Create two replica-3 databases, insert rows, and return the expected row counts."""
        tdSql.execute("DROP DATABASE IF EXISTS db1")
        tdSql.execute("DROP DATABASE IF EXISTS db2")
        # db1 is 3-replica, db2 is single-replica: after restore both must keep
        # their replica counts
        tdSql.execute(f"CREATE DATABASE db1 vgroups {self.VGROUPS} replica 3")
        tdSql.execute(f"CREATE DATABASE db2 vgroups {self.VGROUPS} replica 1")

        # use the current time in ms as the base so the timestamps are always
        # within the valid range and close to "now"
        base_ts = int(time.time() * 1000)

        tdSql.execute("USE db1")
        tdSql.execute("CREATE TABLE t1 (ts TIMESTAMP, v INT)")
        for i in range(self.DB_ROWS):
            tdSql.execute(f"INSERT INTO t1 VALUES ({base_ts + i}, {i})")
        tdSql.execute("USE db2")
        tdSql.execute("CREATE TABLE t2 (ts TIMESTAMP, v INT)")
        for i in range(self.DB_ROWS):
            tdSql.execute(f"INSERT INTO t2 VALUES ({base_ts + i}, {i})")

        tdSql.execute("USE db1")
        tdSql.query("SELECT COUNT(*) FROM t1")
        rows_db1 = tdSql.getData(0, 0)
        tdSql.execute("USE db2")
        tdSql.query("SELECT COUNT(*) FROM t2")
        rows_db2 = tdSql.getData(0, 0)
        tdSql.execute("USE db1")
        tdLog.info(f"prepared rows: db1={rows_db1} db2={rows_db2}")
        return int(rows_db1), int(rows_db2)

    def _rebuild_clean_cluster(self):
        """Rebuild a clean, running 3-dnode cluster from scratch.

        Each dnode-mismatch test prepares its own environment at the start so
        it can run standalone or after any other test in this class (the
        failing-restore tests leave the cluster stopped).
        """
        tdLog.info("rebuilding a clean cluster")
        self._stop_all_taosd()
        self._kill_extra_dnode(4)
        self._clear_all_data()
        self._bootstrap_cluster_cfg()
        self._start_all_taosd()

    @classmethod
    def _stop_extra_dnode_proc(cls):
        """Terminate the extra dnode subprocess directly via its saved handle.

        More reliable than pkill-by-cfg-path for the normal teardown path.
        Returns immediately if no extra dnode is running.
        """
        proc = getattr(cls, "_extra_dnode_proc", None)
        if proc is None or proc.poll() is not None:
            cls._extra_dnode_proc = None
            return
        tdLog.info(f"terminating extra dnode subprocess pid {proc.pid}")
        try:
            proc.terminate()        # SIGTERM -> graceful taosd shutdown
            proc.wait(timeout=10)
        except Exception:
            try:
                proc.kill()         # fallback SIGKILL
                proc.wait(timeout=5)
            except Exception:
                pass
        cls._extra_dnode_proc = None

    @staticmethod
    def _kill_extra_dnode(dnode_id):
        """Kill a leftover extra dnode process by its cfg path.

        The extra dnode is started via subprocess and is NOT managed by the
        framework (which only deploys dnodes 1..dnode_nums), so sc.dnodeForceStop
        cannot stop it. Matching the cfg path avoids killing the framework
        dnodes.
        """
        cfg_marker = os.path.join(sc.clusterRootPath(), f"dnode{dnode_id}", "cfg")
        subprocess.run(
            ["pkill", "-9", "-f", cfg_marker],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        time.sleep(2)

    def _add_dnode(self, dnode_id, port):
        """Dynamically deploy, start and register an extra dnode.

        Copies dnode1's taos.cfg (adjusting serverPort/dataDir/logDir), starts
        the new dnode and registers it via CREATE DNODE. Used to build a
        4-dnode cluster on top of the framework's 3-dnode deployment.

        The subprocess handle is kept on the class so teardown can terminate
        it directly. Note: we deliberately do NOT pass start_new_session=True:
        that would move the child out of the test runner's process group, so a
        Ctrl-C (SIGINT to the whole foreground group) would no longer reach it
        and it would be MORE likely to survive as an orphan. Keeping it in the
        group plus an explicit handle gives us both signal delivery and a
        reliable direct cleanup path.
        """
        base = os.path.join(sc.clusterRootPath(), f"dnode{dnode_id}")
        cfg_dir = os.path.join(base, "cfg")
        data_dir = os.path.join(base, "data")
        log_dir = os.path.join(base, "log")
        for d in (data_dir, log_dir, cfg_dir):
            os.makedirs(d, exist_ok=True)

        # kill a leftover dnode4 process from a previous run: the old process
        # would hold the port and the new one would fail to start (EADDRINUSE)
        self._kill_extra_dnode(dnode_id)
        # drop a leftover SDB record so CREATE DNODE does not conflict
        tdSql.execute_ignore_error(f"DROP DNODE {dnode_id}")
        time.sleep(1)
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
            os.makedirs(data_dir)

        # copy dnode1 cfg, adjusting the per-dnode settings
        with open(os.path.join(self._dnode_cfg_path(1), "taos.cfg")) as f:
            content = f.read()
        content = re.sub(r"(serverPort\s+)\d+", rf"\g<1>{port}", content)
        content = re.sub(r"dataDir\s+\S+", f"dataDir\t{data_dir}", content)
        content = re.sub(r"logDir\s+\S+", f"logDir\t{log_dir}", content)
        with open(os.path.join(cfg_dir, "taos.cfg"), "w") as f:
            f.write(content)

        # start the extra dnode and keep its handle for direct cleanup
        proc = subprocess.Popen(
            [epath.binFile("taosd"), "-c", cfg_dir],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        type(self)._extra_dnode_proc = proc
        time.sleep(2)

        # the process must stay alive (it would have exited if the port was
        # still taken or the cfg was invalid)
        if proc.poll() is not None:
            tdLog.exit(f"dnode{dnode_id} exited early with code {proc.returncode}")

        # register it with the mnode (tolerate a transient failure and retry)
        for attempt in range(10):
            if proc.poll() is not None:
                tdLog.exit(f"dnode{dnode_id} exited early with code {proc.returncode}")
            tdSql.execute_ignore_error(f"CREATE DNODE 'localhost:{port}'")
            tdSql.query("SELECT status FROM information_schema.ins_dnodes "
                        f"WHERE id = {dnode_id}")
            if tdSql.queryRows > 0:
                tdLog.info(f"added dnode{dnode_id} on localhost:{port}")
                break
            time.sleep(1)
        else:
            tdLog.exit(f"dnode{dnode_id} was not registered within 10s")

        # wait until it is ready
        deadline = time.time() + 60
        while time.time() < deadline:
            if proc.poll() is not None:
                tdLog.exit(f"dnode{dnode_id} exited early with code {proc.returncode}")
            try:
                tdSql.query("SELECT status FROM information_schema.ins_dnodes "
                            f"WHERE id = {dnode_id}")
                if tdSql.queryRows > 0 and tdSql.getData(0, 0) == "ready":
                    break
            except Exception:
                pass
            time.sleep(1)
        else:
            tdLog.exit(f"dnode{dnode_id} did not become ready within 60s")

    def _wait_dnode_dropped(self, dnode_id, timeout=120):
        """Wait until the dnode record disappears from ins_dnodes."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                tdSql.query("SELECT count(*) FROM information_schema.ins_dnodes "
                            f"WHERE id = {dnode_id}")
                if tdSql.getData(0, 0) == 0:
                    tdLog.info(f"dnode{dnode_id} dropped from ins_dnodes")
                    return
            except Exception:
                pass
            time.sleep(1)
        tdLog.exit(f"dnode{dnode_id} was not dropped within {timeout}s")

    def _drop_dnode_and_clear(self, dnode_id):
        """DROP DNODE dnode_id, stop its process and clear its data dir.

        After the drop, ensure the dnode process is stopped (it may stop by
        itself) and clear its data dir including dnode.json, so that a later
        re-registration receives a fresh dnode id.
        """
        tdSql.execute(f"DROP DNODE {dnode_id}")
        self._wait_dnode_dropped(dnode_id)
        # the dnode process may have stopped by itself; make sure it is gone
        try:
            sc.dnodeForceStop(dnode_id)
        except Exception:
            pass
        time.sleep(1)
        sc.dnodeClearData(dnode_id)

    def _readd_dnode(self, dnode_id):
        """Restart a dropped dnode and re-register it via CREATE DNODE.

        The dnode has no dnode.json after the data clear, so the mnode assigns
        it a brand-new dnode id (the old one is gone).
        """
        tdLog.info(f"restarting dnode{dnode_id} and re-registering it")
        sc.dnodeStart(dnode_id)
        dnode = cluster.dnodes[dnode_id - 1]
        fqdn = dnode.cfgDict.get("fqdn", "localhost")
        port = dnode.cfgDict.get("serverPort", 6030 + (dnode_id - 1) * 100)
        ep = f"{fqdn}:{port}"
        for _ in range(10):
            tdSql.execute_ignore_error(f"CREATE DNODE '{ep}'")
            tdSql.query("SELECT id, status FROM information_schema.ins_dnodes "
                        f"WHERE endpoint = '{ep}'")
            if tdSql.queryRows > 0:
                new_id = tdSql.getData(0, 0)
                tdLog.info(f"dnode{dnode_id} re-added as dnode{new_id} ({tdSql.getData(0, 1)})")
                break
            time.sleep(1)
        else:
            tdLog.exit(f"dnode{dnode_id} was not re-registered within 10s")

        # wait until it is ready
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                tdSql.query("SELECT status FROM information_schema.ins_dnodes "
                            f"WHERE id = {new_id}")
                if tdSql.queryRows > 0 and tdSql.getData(0, 0) == "ready":
                    break
            except Exception:
                pass
            time.sleep(1)
        else:
            tdLog.exit(f"dnode{new_id} did not become ready within 60s")
        return new_id

    def _full_restore_cycle(self, chain_path, password, rows_db1, rows_db2, parallel=None):
        """Stop all taosd, clear all data, recreate dnode cfgs, restore all, restart, verify."""
        self._stop_all_taosd()
        self._clear_all_data()
        # the restore tool clears data again and needs each dnode.json to exist,
        # which is recreated by a normal taosd start/stop cycle
        self._bootstrap_cluster_cfg()
        self._restore_all(chain_path, password, parallel=parallel)
        self._start_all_taosd()
        self._verify_rows(rows_db1, rows_db2)
        self._verify_replicas()
        self._verify_write_after_restore(rows_db1, rows_db2)

    def test_backup_restore_plain(self):
        """Backup and restore without encryption (cluster), serial vgroup backup (PARALLEL 1)

        1. Create databases db1 and db2 (replica 3) with known row counts
        2. CREATE FULL BACKUP ... PARALLEL 1 (no password) -> success
        3. Stop all taosd, clear all data
        4. Restore every dnode from the plain backup (no password) -> success
        5. Restart all taosd and verify the row counts match
        6. Write new rows into db1 and into a new database db3

        Since: v3.4.3.0

        Labels: common,ci,integration,functional,cluster

        Jira: None

        History:
            - 2026-08-20 Bomin Zhang created

        """
        tdLog.info("=========== cluster backup/restore plain (parallel 1) ===========")
        rows = self._prepare_databases()
        self.backup_plain = self._start_backup("plain", parallel=1)
        self._full_restore_cycle(self.backup_plain, None, rows[0], rows[1])

    def test_backup_restore_encrypted(self):
        """Backup and restore with a password (cluster), parallel vgroup backup (PARALLEL 2)

        1. Create databases db1 and db2 (replica 3) with known row counts
        2. CREATE FULL BACKUP WITH PASSWORD ... PARALLEL 2 -> success
        3. Stop all taosd, clear all data
        4. Restore every dnode from the encrypted backup with the password -> success
        5. Restart all taosd and verify the row counts match
        6. Write new rows into db1 and into a new database db3

        Since: v3.4.3.0

        Labels: common,ci,integration,functional,cluster

        Jira: None

        History:
            - 2026-08-20 Bomin Zhang created

        """
        tdLog.info("=========== cluster backup/restore encrypted (parallel 2) ===========")
        rows = self._prepare_databases()
        self.backup_enc = self._start_backup("enc", password="Taos@123", parallel=2)
        self._full_restore_cycle(self.backup_enc, "Taos@123", rows[0], rows[1], parallel=2)

    def test_restore_wrong_password(self):
        """Restore failures (cluster): encrypted without/wrong password, plain with password

        1. Prepare databases and take an encrypted backup and a plain backup
        2. Restore the encrypted backup WITHOUT the password -> fails
        3. Restore the encrypted backup WITH a WRONG password -> fails
        4. Restore the plain backup WITH a password -> fails

        The restore is expected to fail before deleting any data, so no data
        check is performed afterwards.

        Since: v3.4.3.0

        Labels: common,ci,integration,functional,cluster

        Jira: None

        History:
            - 2026-08-20 Bomin Zhang created

        """
        tdLog.info("=========== cluster restore failures ===========")
        # ensure we have both chain dirs (may reuse from previous tests)
        if not self.backup_enc:
            self._prepare_databases()
            self.backup_enc = self._start_backup("enc", password="Taos@123")
        if not self.backup_plain:
            self._prepare_databases()
            self.backup_plain = self._start_backup("plain")

        # encrypted backup without password -> fail (verified on dnode 1)
        self._stop_all_taosd()
        self._bootstrap_cluster_cfg()
        code, _ = self._restore(self.backup_dnode, self.backup_enc, None)
        if code == 0:
            tdLog.exit("restoring encrypted backup without password should fail")
        self._start_all_taosd()

        # encrypted backup with wrong password -> fail
        self._stop_all_taosd()
        self._bootstrap_cluster_cfg()
        code, _ = self._restore(self.backup_dnode, self.backup_enc, "WrongPass@123")
        if code == 0:
            tdLog.exit("restoring encrypted backup with wrong password should fail")
        self._start_all_taosd()

        # plain backup with password -> fail
        self._stop_all_taosd()
        self._bootstrap_cluster_cfg()
        code, _ = self._restore(self.backup_dnode, self.backup_plain, "Taos@123")
        if code == 0:
            tdLog.exit("restoring plain backup with a password should fail")
        self._start_all_taosd()

    def test_restore_version_mismatch_plain(self):
        """Version mismatch confirmation on a plain backup (cluster).

        1. Prepare databases and take a plain backup
        2. Rewrite tsdbVersion in backup.json to an incompatible version
        3. Restore answering NO to the version warning        -> rejected
        4. Restore answering YES (version) + YES (data clear) -> restored
        5. Restart all taosd and verify the row counts match
        6. Write new rows into db1 and into a new database db3

        Since: v3.4.3.0

        Labels: common,ci,integration,functional,cluster

        Jira: None

        History:
            - 2026-08-20 Bomin Zhang created

        """
        tdLog.info("=========== cluster version mismatch confirmation (plain) ===========")
        rows = self._prepare_databases()
        chain = self._start_backup("ver_plain")
        self._modify_manifest_tsdb_version(chain, None, "1.0.0.0")

        # answer NO to the version-mismatch warning -> restore is rejected
        # (this happens before any data is cleared, so no bootstrap is needed)
        self._stop_all_taosd()
        code, out = self._restore(self.backup_dnode, chain, None, confirmations=("NO",))
        if code == 0:
            tdLog.exit("restore with version mismatch + NO should fail")
        if "Backup version mismatch detected" not in out and "type 'YES'" not in out:
            tdLog.exit(f"expected version-mismatch warning in output, got: {out[-500:]}")

        # answer YES to the version warning and YES to the data-clear warning
        self._clear_all_data()
        self._bootstrap_cluster_cfg()
        self._restore_all(chain, None, confirmations=("YES", "YES"))
        self._start_all_taosd()
        self._verify_rows(rows[0], rows[1])
        self._verify_replicas()
        self._verify_write_after_restore(rows[0], rows[1])

    def test_restore_version_mismatch_encrypted(self):
        """Version mismatch confirmation on an encrypted backup (cluster).

        1. Prepare databases and take an encrypted backup
        2. Rewrite tsdbVersion in backup.json (decrypted with the known
           password, re-encrypted afterwards) to an incompatible version
        3. Restore answering NO to the version warning        -> rejected
        4. Restore answering YES (version) + YES (data clear) -> restored
        5. Restart all taosd and verify the row counts match
        6. Write new rows into db1 and into a new database db3

        Since: v3.4.3.0

        Labels: common,ci,integration,functional,cluster

        Jira: None

        History:
            - 2026-08-20 Bomin Zhang created

        """
        tdLog.info("=========== cluster version mismatch confirmation (encrypted) ===========")
        rows = self._prepare_databases()
        chain = self._start_backup("ver_enc", password="Taos@123")
        self._modify_manifest_tsdb_version(chain, "Taos@123", "1.0.0.0")

        # answer NO -> rejected before any data is cleared
        self._stop_all_taosd()
        code, out = self._restore(self.backup_dnode, chain, "Taos@123", confirmations=("NO",))
        if code == 0:
            tdLog.exit("restore with version mismatch + NO should fail")
        if "Backup version mismatch detected" not in out and "type 'YES'" not in out:
            tdLog.exit(f"expected version-mismatch warning in output, got: {out[-500:]}")

        # answer YES + YES -> restored
        self._clear_all_data()
        self._bootstrap_cluster_cfg()
        self._restore_all(chain, "Taos@123", confirmations=("YES", "YES"))
        self._start_all_taosd()
        self._verify_rows(rows[0], rows[1])
        self._verify_replicas()
        self._verify_write_after_restore(rows[0], rows[1])

    def test_restore_dnode_mismatch_3backup_4restore(self):
        """Restore fails when the cluster has more dnodes than the backup.

        1. Prepare data and take a plain backup on the 3-dnode cluster
        2. Stop all taosd, clear data, rebuild the 3-dnode cluster
        3. Add a 4th dnode (cluster is now 4 dnodes)
        4. taosd --restore -> fails (backup has 3 dnodes, local has 4)

        dmRestoreBuildDnodeMap rejects the restore when the source dnode list
        (from backup.json) does not match the local dnode list size.

        Since: v3.4.3.0

        Labels: common,ci,integration,functional,cluster

        Jira: None

        History:
            - 2026-08-20 Bomin Zhang created

        """
        tdLog.info("=========== dnode mismatch: 3 backup, 4 restore ===========")
        self._prepare_databases()
        chain = self._start_backup("mismatch_3to4")

        # rebuild a 3-dnode cluster
        self._stop_all_taosd()
        self._clear_all_data()
        self._bootstrap_cluster_cfg()

        # add a 4th dnode so the local cluster has 4 dnodes
        self._start_all_taosd()
        self._add_dnode(4, 6330)

        # restore must fail (3 backup dnodes vs 4 local dnodes)
        self._stop_all_taosd()
        code, out = self._restore(self.backup_dnode, chain, None)
        if code == 0:
            tdLog.exit("restore with 3-backup/4-restore dnodes should fail")
        if "map source and target dnodes" not in out and "Invalid data format" not in out:
            tdLog.info(f"restore failed as expected (unexpected message): {out[-500:]}")

        # the failed restore already cleared the data dirs, so do not restart
        # the cluster; only stop the extra dnode process. The next test
        # rebuilds its own environment.
        self._kill_extra_dnode(4)
        tdLog.info("dnode mismatch 3->4 restore failed as expected")

    def test_restore_dnode_mismatch_4backup_3restore(self):
        """Restore fails when the cluster has fewer dnodes than the backup.

        1. Prepare data, add a 4th dnode, take a plain backup on 4 dnodes
        2. Stop all taosd, clear data, rebuild only the original 3 dnodes
        3. taosd --restore -> fails (backup has 4 dnodes, local has 3)

        Since: v3.4.3.0

        Labels: common,ci,integration,functional,cluster

        Jira: None

        History:
            - 2026-08-20 Bomin Zhang created

        """
        tdLog.info("=========== dnode mismatch: 4 backup, 3 restore ===========")
        # the previous mismatch test (3->4) stops the whole cluster after the
        # failed restore; rebuild a clean running cluster before adding dnode4
        self._rebuild_clean_cluster()
        # grow to 4 dnodes and take the backup on the 4-dnode cluster
        self._add_dnode(4, 6330)
        rows = self._prepare_databases()

        chain = self._start_backup("mismatch_4to3")

        # rebuild only the original 3 dnodes (dnode4 is not re-added): the
        # extra dnode process must be killed too, otherwise it keeps running
        # against the rebuilt mnode and holds its old data dir
        self._stop_all_taosd()
        self._kill_extra_dnode(4)
        self._clear_all_data()
        self._bootstrap_cluster_cfg()

        # restore must fail (4 backup dnodes vs 3 local dnodes)
        code, out = self._restore(self.backup_dnode, chain, None)
        if code == 0:
            tdLog.exit("restore with 4-backup/3-restore dnodes should fail")
        if "map source and target dnodes" not in out and "Invalid data format" not in out:
            tdLog.info(f"restore failed as expected (unexpected message): {out[-500:]}")

        # the failed restore already cleared the data dirs; no cluster restart.
        # The next test rebuilds its own environment.
        tdLog.info("dnode mismatch 4->3 restore failed as expected")

    def test_restore_dnode_id_mismatch(self):
        """Restore succeeds with the same dnode count but different dnode ids.

        1. Create data on the 3-dnode cluster (dnode ids 1, 2, 3) and take a backup
        2. Stop all taosd, clear all data, rebuild a fresh 3-dnode cluster
        3. DROP DNODE 2 (its process stops), clear its data dir
        4. Restart dnode 2 and re-add it via CREATE DNODE -> it gets id 4
        5. The cluster now has dnode ids 1, 3, 4 (same count, different ids)
        6. Stop all taosd, restore every dnode, restart, verify rows/replicas
           and write new rows into db1 and a new database db3

        dmRestoreBuildDnodeMap maps the backup dnodes to the local dnodes by
        position, so a same-count/different-id cluster restores successfully.

        Since: v3.4.3.0

        Labels: common,ci,integration,functional,cluster

        Jira: None

        History:
            - 2026-08-20 Bomin Zhang created

        """
        tdLog.info("=========== dnode id mismatch: same count, different ids ===========")
        # the previous mismatch test (4->3) stops the whole cluster after the
        # failed restore; rebuild a clean running cluster before the backup
        self._rebuild_clean_cluster()
        rows = self._prepare_databases()
        chain = self._start_backup("id_mismatch")

        # rebuild a fresh 3-dnode cluster (ids 1, 2, 3)
        self._stop_all_taosd()
        self._clear_all_data()
        self._bootstrap_cluster_cfg()
        self._start_all_taosd()

        # drop dnode 2 and clear its data so it re-registers with a new id
        self._drop_dnode_and_clear(2)
        # restart dnode 2 and re-add it -> it gets a new dnode id (4)
        self._readd_dnode(2)

        # verify the cluster now has dnode ids 1, 3, 4 (same count, different ids)
        tdSql.query("SELECT id FROM information_schema.ins_dnodes ORDER BY id")
        ids = [tdSql.getData(r, 0) for r in range(tdSql.queryRows)]
        tdLog.info(f"cluster dnode ids before restore: {ids}")
        if ids != [1, 3, 4]:
            tdLog.exit(f"expected dnode ids [1, 3, 4], got {ids}")

        # stop, restore every dnode, restart, verify and write new data
        self._stop_all_taosd()
        self._restore_all(chain, None)
        self._start_all_taosd()
        self._verify_rows(rows[0], rows[1])
        self._verify_replicas()
        self._verify_write_after_restore(rows[0], rows[1])
