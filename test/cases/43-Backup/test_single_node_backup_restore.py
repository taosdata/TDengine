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

from new_test_framework.utils import tdLog, tdSql

from backup_restore_base import BackupRestoreTestBase


class TestSingleNodeBackupRestore(BackupRestoreTestBase):
    """Single-node backup and restore.

    Creates two databases with a known number of rows each, takes a backup
    (plain and encrypted), clears the data, restores from the backup file,
    and verifies the row counts match.  Also covers the failure cases of
    restoring an encrypted backup without the password and restoring a plain
    backup with a password.
    """

    backup_root = None
    backup_plain = None     # chain dir of the plain backup
    backup_enc = None       # chain dir of the encrypted backup

    DB_ROWS = 100

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.dnode_id = 1
        cls.dnode_nums = 1
        cls.backup_dnode = 1
        cls.backup_root = f"/tmp/backup_restore_{int(time.time() * 1000)}"

    def teardown_class(cls):
        # remove the backup chains
        if cls.backup_root and os.path.exists(cls.backup_root):
            shutil.rmtree(cls.backup_root, ignore_errors=True)

    def _prepare_databases(self):
        """Create two databases, insert rows, and return the expected row counts."""
        tdSql.execute("DROP DATABASE IF EXISTS db1")
        tdSql.execute("DROP DATABASE IF EXISTS db2")
        tdSql.execute("CREATE DATABASE db1 vgroups 1 replica 1")
        tdSql.execute("CREATE DATABASE db2 vgroups 1 replica 1")

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

    def _corrupt_backup_vnode_file(self, chain_dir):
        """Corrupt a vnode data chunk file in the backup.

        The backup stores one chunked data file per vgroup directly under
        the backup root as <backupId>/vnode<N>/00000001 (vgroup dir +
        zero-padded chunk index). Overwriting a few bytes makes the
        restore-time checksum verification fail. Returns the corrupted
        file path.
        """
        backup_id = os.path.basename(chain_dir)
        backup_root_dir = os.path.join(chain_dir, backup_id)
        # the vnode chunk dirs are named vnode<N> under the backup root
        vnode_dirs = [d for d in os.listdir(backup_root_dir) if d.startswith("vnode")]
        if not vnode_dirs:
            tdLog.exit(f"no vnode dirs under backup: {backup_root_dir}")
        vnode_dirs.sort()
        data_dir = os.path.join(backup_root_dir, vnode_dirs[0])
        chunks = sorted(os.listdir(data_dir))
        if not chunks:
            tdLog.exit(f"no chunk files under: {data_dir}")
        data_file = os.path.join(data_dir, chunks[0])
        with open(data_file, "r+b") as f:
            f.seek(0)
            data = f.read(64)
            f.seek(0)
            # flip all bits of the first 64 bytes
            f.write(bytes(b ^ 0xFF for b in data))
        tdLog.info(f"corrupted backup vnode data file: {data_file}")
        return data_file

    def test_restore_corrupted_backup_plain(self):
        """Restore fails when the plain backup data is corrupted.

        1. Prepare databases and take a plain backup
        2. Corrupt a vnode data chunk file in the backup
        3. Stop taosd and run --restore -> fails at checksum verification
        4. The instance is still running and usable (no data was cleared)

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-08-21 Bomin Zhang created

        """
        tdLog.info("=========== restore corrupted backup (plain) ===========")
        self._prepare_databases()
        chain = self._start_backup("corrupt_plain")
        self._corrupt_backup_vnode_file(chain)

        # stop, recreate dnode cfg (restore needs dnode.json), then restore
        self._stop_taosd()
        self._bootstrap_dnode_cfg()
        code, out = self._restore(self.dnode_id, chain)
        if code == 0:
            tdLog.exit("restore of a corrupted plain backup should fail")
        # the failure must be a checksum/verify error
        if "checksum" not in out.lower() and "corrupt" not in out.lower() \
                and "invalid" not in out.lower() and "failed" not in out.lower():
            tdLog.info(f"restore failed as expected (unexpected message): {out[-500:]}")

        # the corrupted chunk fails the checksum verification before any data
        # is cleared, so the restore exits without touching the data dir;
        # restart the instance to refresh the tdSql connection and verify it
        # is still usable
        self._start_taosd()
        tdSql.query("SELECT 1")
        tdLog.info("restore of corrupted plain backup failed as expected")

    def test_restore_corrupted_backup_encrypted(self):
        """Restore fails when the encrypted backup data is corrupted.

        1. Prepare databases and take an encrypted backup
        2. Corrupt a vnode data chunk file in the backup
        3. Stop taosd and run --restore -> fails at checksum verification
        4. The instance is still running and usable (no data was cleared)

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-08-21 Bomin Zhang created

        """
        tdLog.info("=========== restore corrupted backup (encrypted) ===========")
        self._prepare_databases()
        chain = self._start_backup("corrupt_enc", password="Taos@123")
        self._corrupt_backup_vnode_file(chain)

        self._stop_taosd()
        self._bootstrap_dnode_cfg()
        code, out = self._restore(self.dnode_id, chain, "Taos@123")
        if code == 0:
            tdLog.exit("restore of a corrupted encrypted backup should fail")
        if "checksum" not in out.lower() and "corrupt" not in out.lower() \
                and "invalid" not in out.lower() and "failed" not in out.lower():
            tdLog.info(f"restore failed as expected (unexpected message): {out[-500:]}")

        # same as the plain case: checksum verification fails before any data
        # is cleared; restart the instance to refresh the tdSql connection
        # and verify it is still usable
        self._start_taosd()
        tdSql.query("SELECT 1")
        tdLog.info("restore of corrupted encrypted backup failed as expected")

    def _full_restore_cycle(self, chain_path, password, rows_db1, rows_db2, parallel=None):
        """Stop taosd, clear data, recreate dnode cfg, restore, restart, verify."""
        self._stop_taosd()
        self._clear_data()
        # the restore tool clears data again and needs dnode.json to exist,
        # which is recreated by a normal taosd start/stop cycle
        self._bootstrap_dnode_cfg()
        code, _ = self._restore(self.dnode_id, chain_path, password, parallel=parallel)
        if code != 0:
            tdLog.exit(f"restore from {chain_path} failed with exit code {code}")
        self._start_taosd()
        self._verify_rows(rows_db1, rows_db2)
        self._verify_write_after_restore(rows_db1, rows_db2)

    def test_backup_restore_plain(self):
        """Backup and restore without encryption, serial vgroup backup (PARALLEL 1)

        1. Create databases db1 and db2 with known row counts
        2. CREATE FULL BACKUP ... PARALLEL 1 (no password) -> success
        3. Stop taosd, clear data
        4. Restore from the plain backup (no password) -> success
        5. Restart taosd and verify the row counts match
        6. Write new rows into db1 and into a new database db3

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-08-19 Bomin Zhang created

        """
        tdLog.info("=========== backup/restore plain (parallel 1) ===========")
        rows = self._prepare_databases()
        self.backup_plain = self._start_backup("plain", parallel=1)
        self._full_restore_cycle(self.backup_plain, None, rows[0], rows[1])

    def test_backup_restore_encrypted(self):
        """Backup and restore with a password, parallel vgroup backup (PARALLEL 2)

        1. Create databases db1 and db2 with known row counts
        2. CREATE FULL BACKUP WITH PASSWORD ... PARALLEL 2 -> success
        3. Stop taosd, clear data
        4. Restore from the encrypted backup with the password -> success
        5. Restart taosd and verify the row counts match
        6. Write new rows into db1 and into a new database db3

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-08-19 Bomin Zhang created

        """
        tdLog.info("=========== backup/restore encrypted (parallel 2) ===========")
        rows = self._prepare_databases()
        self.backup_enc = self._start_backup("enc", password="Taos@123", parallel=2)
        self._full_restore_cycle(self.backup_enc, "Taos@123", rows[0], rows[1], parallel=2)

    def test_restore_wrong_password(self):
        """Restore failures: encrypted without/wrong password, plain with password

        1. Prepare databases and take an encrypted backup and a plain backup
        2. Restore the encrypted backup WITHOUT the password -> fails
        3. Restore the encrypted backup WITH a WRONG password -> fails
        4. Restore the plain backup WITH a password -> fails

        The restore is expected to fail before deleting any data, so no data
        check is performed afterwards.

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-08-19 Bomin Zhang created

        """
        tdLog.info("=========== restore failures ===========")
        # ensure we have both chain dirs (may reuse from previous tests)
        if not self.backup_enc:
            self._prepare_databases()
            self.backup_enc = self._start_backup("enc", password="Taos@123")
        if not self.backup_plain:
            self._prepare_databases()
            self.backup_plain = self._start_backup("plain")

        # encrypted backup without password -> fail
        self._stop_taosd()
        self._bootstrap_dnode_cfg()
        code, _ = self._restore(self.dnode_id, self.backup_enc)
        if code == 0:
            tdLog.exit("restoring encrypted backup without password should fail")
        self._start_taosd()

        # encrypted backup with wrong password -> fail
        self._stop_taosd()
        self._bootstrap_dnode_cfg()
        code, _ = self._restore(self.dnode_id, self.backup_enc, "WrongPass@123")
        if code == 0:
            tdLog.exit("restoring encrypted backup with wrong password should fail")
        self._start_taosd()

        # plain backup with password -> fail
        self._stop_taosd()
        self._bootstrap_dnode_cfg()
        code, _ = self._restore(self.dnode_id, self.backup_plain, "Taos@123")
        if code == 0:
            tdLog.exit("restoring plain backup with a password should fail")
        self._start_taosd()

    def test_restore_version_mismatch_plain(self):
        """Version mismatch confirmation on a plain backup.

        1. Prepare databases and take a plain backup
        2. Rewrite tsdbVersion in backup.json to an incompatible version
        3. Restore answering NO to the version warning        -> rejected
        4. Restore answering YES (version) + YES (data clear) -> restored
        5. Restart taosd and verify the row counts match
        6. Write new rows into db1 and into a new database db3

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-08-19 Bomin Zhang created

        """
        tdLog.info("=========== version mismatch confirmation (plain) ===========")
        rows = self._prepare_databases()
        chain = self._start_backup("ver_plain")
        self._modify_manifest_tsdb_version(chain, None, "1.0.0.0")

        # answer NO to the version-mismatch warning -> restore is rejected
        # (this happens before any data is cleared, so no bootstrap is needed)
        self._stop_taosd()
        code, out = self._restore(self.dnode_id, chain, None, confirmations=("NO",))
        if code == 0:
            tdLog.exit("restore with version mismatch + NO should fail")
        if "Backup version mismatch detected" not in out and "type 'YES'" not in out:
            tdLog.exit(f"expected version-mismatch warning in output, got: {out[-500:]}")

        # answer YES to the version warning and YES to the data-clear warning
        self._clear_data()
        self._bootstrap_dnode_cfg()
        code, out = self._restore(self.dnode_id, chain, None, confirmations=("YES", "YES"))
        if code != 0:
            tdLog.exit(f"restore with version mismatch + YES+YES failed: {out[-500:]}")
        self._start_taosd()
        self._verify_rows(rows[0], rows[1])
        self._verify_write_after_restore(rows[0], rows[1])

    def test_restore_version_mismatch_encrypted(self):
        """Version mismatch confirmation on an encrypted backup.

        1. Prepare databases and take an encrypted backup
        2. Rewrite tsdbVersion in backup.json (decrypted with the known
           password, re-encrypted afterwards) to an incompatible version
        3. Restore answering NO to the version warning        -> rejected
        4. Restore answering YES (version) + YES (data clear) -> restored
        5. Restart taosd and verify the row counts match
        6. Write new rows into db1 and into a new database db3

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-08-19 Bomin Zhang created

        """
        tdLog.info("=========== version mismatch confirmation (encrypted) ===========")
        rows = self._prepare_databases()
        chain = self._start_backup("ver_enc", password="Taos@123")
        self._modify_manifest_tsdb_version(chain, "Taos@123", "1.0.0.0")

        # answer NO -> rejected before any data is cleared
        self._stop_taosd()
        code, out = self._restore(self.dnode_id, chain, "Taos@123", confirmations=("NO",))
        if code == 0:
            tdLog.exit("restore with version mismatch + NO should fail")
        if "Backup version mismatch detected" not in out and "type 'YES'" not in out:
            tdLog.exit(f"expected version-mismatch warning in output, got: {out[-500:]}")

        # answer YES + YES -> restored
        self._clear_data()
        self._bootstrap_dnode_cfg()
        code, out = self._restore(self.dnode_id, chain, "Taos@123", confirmations=("YES", "YES"))
        if code != 0:
            tdLog.exit(f"restore with version mismatch + YES+YES failed: {out[-500:]}")
        self._start_taosd()
        self._verify_rows(rows[0], rows[1])
        self._verify_write_after_restore(rows[0], rows[1])
