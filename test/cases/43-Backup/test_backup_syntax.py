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

# SHOW BACKUPS column indexes (see userBackupsSchema in systable.c)
COL_BACKUP_ID = 0
COL_STATUS = 1


class TestBackupSyntax:
    """Backup statement parse / execution behavior.

    Covers the cluster-level backup statements introduced with the
    backup/restore feature:

        CREATE [FULL|INCREMENTAL] BACKUP ON DNODE <id> TO '<path>'
            [<chainId>] [FOR DATABASE <db>] [WITH PASSWORD '<pw>']
            [PARALLEL <n>]
        SHOW BACKUPS
        KILL BACKUP <id>

    Phase 1 only accepts a cluster-level FULL backup without a target chain
    id and without FOR DATABASE; the parser itself rejects those variants.
    Only one backup can run at a time. PARALLEL <n> controls the vgroup
    backup parallelism (1 = serial, >1 = parallel, 0 is normalized to 1).
    """

    backup_dirs = None

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        # single-node cluster: dnode 1 is the local dnode
        cls.dnode_id = 1
        cls.missing_dnode_id = 9999
        cls.backup_dirs = []

    def teardown_class(cls):
        # remove the backup chains created by the success-path cases
        for d in cls.backup_dirs:
            if d and os.path.exists(d):
                shutil.rmtree(d, ignore_errors=True)

    def _unique_backup_dir(self, tag):
        d = f"/tmp/backup_syntax_{tag}_{int(time.time() * 1000)}"
        self.backup_dirs.append(d)
        return d

    def _find_backup_row(self, backup_id):
        """Return the row index of the given backup in the last SHOW BACKUPS query, or None."""
        for r in range(tdSql.queryRows):
            if tdSql.getData(r, COL_BACKUP_ID) == backup_id:
                return r
        return None

    def _max_backup_id(self):
        """Return the maximum backup id in the last SHOW BACKUPS query (0 if empty)."""
        max_id = 0
        for r in range(tdSql.queryRows):
            bid = tdSql.getData(r, COL_BACKUP_ID)
            if bid > max_id:
                max_id = bid
        return max_id

    def _start_backup(self, backup_dir, extra_sql=""):
        """Start a backup and return its backup id.

        Records the current max backup id as the baseline first, then starts the
        backup and polls SHOW BACKUPS until a record with a strictly larger id
        appears (backup ids are timestamp-based and monotonically increasing).
        This is robust to SHOW BACKUPS returning rows in unordered hash order and
        to leftover backup records from previous runs.
        """
        tdSql.query("SHOW BACKUPS")
        baseline = self._max_backup_id()

        tdSql.execute(f"CREATE FULL BACKUP ON DNODE {self.dnode_id} TO '{backup_dir}' {extra_sql}")

        deadline = time.time() + 10
        while time.time() < deadline:
            tdSql.query("SHOW BACKUPS")
            for r in range(tdSql.queryRows):
                bid = tdSql.getData(r, COL_BACKUP_ID)
                if bid > baseline:
                    return bid
            time.sleep(1)
        tdLog.exit(f"backup record did not appear in SHOW BACKUPS after CREATE, baseline {baseline}")

    def _wait_backup_finished(self, backup_id, timeout=120):
        """Poll SHOW BACKUPS until the given backup is no longer running."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            tdSql.query("SHOW BACKUPS")
            row = self._find_backup_row(backup_id)
            if row is not None:
                status = tdSql.getData(row, COL_STATUS)
                if status in ("succeeded", "failed"):
                    return status
            time.sleep(1)
        tdLog.exit(f"backup {backup_id} did not finish within {timeout}s")

    def test_backup_full_flow(self):
        """Full backup lifecycle

        1. CREATE FULL BACKUP ON DNODE 1 TO '<path1>'   -> success (backup A starts)
        2. CREATE FULL BACKUP ON DNODE 1 TO '<path2>'   -> rejected
           (only one backup can run at a time)
        3. poll SHOW BACKUPS until backup A finishes (succeeded)
        4. CREATE FULL BACKUP ... WITH PASSWORD -> success (backup B)
        5. poll SHOW BACKUPS until backup B finishes
        6. SHOW BACKUPS returns both records, both succeeded

        Since: v3.4.3.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2026-08-19 Bomin Zhang created

        """
        tdLog.info("=========== full backup lifecycle ===========")

        # 1. start backup A and capture its id
        dir_a = self._unique_backup_dir("full_a")
        backup_a = self._start_backup(dir_a)
        tdLog.info(f"backup A id: {backup_a}")

        # 2. a concurrent start must be rejected while A is still running
        dir_b = self._unique_backup_dir("full_b")
        tdSql.error(f"CREATE FULL BACKUP ON DNODE {self.dnode_id} TO '{dir_b}'")

        # 3. wait for A to finish
        status_a = self._wait_backup_finished(backup_a)
        if status_a != "succeeded":
            tdLog.exit(f"backup A expected succeeded, got {status_a}")

        # 4. start backup B with a password
        backup_b = self._start_backup(dir_b, "WITH PASSWORD 'Taos@123'")
        tdLog.info(f"backup B id: {backup_b}")

        # 5. wait for B to finish
        status_b = self._wait_backup_finished(backup_b)
        if status_b != "succeeded":
            tdLog.exit(f"backup B expected succeeded, got {status_b}")

        # 6. PARALLEL 1 (explicit serial) and PARALLEL 0 (normalized to 1) are
        # accepted; PARALLEL > 1 is accepted as well. Each backup is waited to
        # finish before the next one starts (only one backup can run at a time).
        backup_par1 = self._start_backup(self._unique_backup_dir('par1'), "PARALLEL 1")
        if self._wait_backup_finished(backup_par1) != "succeeded":
            tdLog.exit(f"backup {backup_par1} (PARALLEL 1) expected succeeded")
        backup_par0 = self._start_backup(self._unique_backup_dir('par0'), "PARALLEL 0")
        if self._wait_backup_finished(backup_par0) != "succeeded":
            tdLog.exit(f"backup {backup_par0} (PARALLEL 0) expected succeeded")
        backup_par2 = self._start_backup(self._unique_backup_dir('par2'), "PARALLEL 2")
        if self._wait_backup_finished(backup_par2) != "succeeded":
            tdLog.exit(f"backup {backup_par2} (PARALLEL 2) expected succeeded")

        # 7. both records are present in SHOW BACKUPS
        tdSql.query("SHOW BACKUPS")
        if self._find_backup_row(backup_a) is None or self._find_backup_row(backup_b) is None:
            tdLog.exit("SHOW BACKUPS should contain both backup records")

    def test_kill_backup(self):
        """Kill backup

        1. CREATE FULL BACKUP ON DNODE 1 TO '<path>'  -> success (backup C starts)
        2. KILL BACKUP <id>                           -> success (the kill txn commits)
        3. poll SHOW BACKUPS until backup C shows status 'cancelled'
        4. CREATE FULL BACKUP ... (D), wait until it succeeds
        5. KILL BACKUP <id D>                         -> rejected (backup not running)

        The killed backup record is kept in SDB with state = CANCELLED
        (and code = BACKUP_CANCELLED), so it stays visible in SHOW BACKUPS with a
        'cancelled' status instead of being dropped.

        Since: v3.4.3.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2026-08-19 Bomin Zhang created

        """
        tdLog.info("=========== kill backup ===========")

        # 1. start a backup and capture its id
        dir_c = self._unique_backup_dir("kill")
        backup_c = self._start_backup(dir_c)
        tdLog.info(f"backup C id: {backup_c}")

        # 2. kill it; the statement must return without hanging
        tdSql.execute(f"KILL BACKUP {backup_c}")

        # 3. poll until the killed backup shows status 'cancelled'
        deadline = time.time() + 30
        cancelled = False
        while time.time() < deadline:
            tdSql.query("SHOW BACKUPS")
            row = self._find_backup_row(backup_c)
            if row is not None and tdSql.getData(row, COL_STATUS) == "cancelled":
                cancelled = True
                break
            time.sleep(1)
        if not cancelled:
            tdLog.exit(f"killed backup {backup_c} should show status 'cancelled' in SHOW BACKUPS")

        # 4. start another backup and wait until it finishes successfully
        backup_d = self._start_backup(self._unique_backup_dir("kill-done"))
        if self._wait_backup_finished(backup_d) != "succeeded":
            tdLog.exit(f"backup {backup_d} expected to succeed before KILL BACKUP")

        # 5. a backup that already finished must not be killed again
        tdSql.error(f"KILL BACKUP {backup_d}")

    def test_backup_rejected_variants(self):
        """Backup statement error variants

        1. CREATE INCREMENTAL BACKUP ...             -> rejected (not supported yet)
        2. CREATE FULL BACKUP ... 12345              -> rejected (chain id not supported yet)
        3. CREATE FULL BACKUP ... FOR DATABASE db1   -> rejected (db-level not supported yet)
        4. CREATE FULL BACKUP ON DNODE 9999 ...      -> rejected (dnode does not exist)
        5. KILL BACKUP 12345                         -> rejected (backup does not exist)

        Since: v3.4.3.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2026-08-19 Bomin Zhang created

        """
        tdLog.info("=========== backup rejected variants ===========")

        # incremental backup not supported yet
        tdSql.error(f"CREATE INCREMENTAL BACKUP ON DNODE {self.dnode_id} TO '{self._unique_backup_dir('incr')}'")

        # specifying a target backup chain id not supported yet
        tdSql.error(f"CREATE FULL BACKUP ON DNODE {self.dnode_id} TO '{self._unique_backup_dir('chain')}' 12345")

        # database-level backup not supported yet
        tdSql.error(f"CREATE FULL BACKUP ON DNODE {self.dnode_id} TO '{self._unique_backup_dir('db')}' FOR DATABASE db1")

        # backup on a non-existent dnode fails at the mnode layer
        tdSql.error(f"CREATE FULL BACKUP ON DNODE {self.missing_dnode_id} TO '{self._unique_backup_dir('node')}'")

        # PARALLEL must be >= 1; 0 is normalized to 1 in the parser but a
        # negative value is rejected
        tdSql.error(f"CREATE FULL BACKUP ON DNODE {self.dnode_id} TO '{self._unique_backup_dir('par_neg')}' PARALLEL -1")

        # kill a non-existent backup
        tdSql.error("KILL BACKUP 12345")
