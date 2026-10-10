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
from new_test_framework.utils import tdLog

from cluster_backup_restore_common import ClusterBackupRestoreCase


class TestClusterBackupRestoreNegative(ClusterBackupRestoreCase):
    """Restore failures: wrong password and version mismatch."""


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

