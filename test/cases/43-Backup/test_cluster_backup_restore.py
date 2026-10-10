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


class TestClusterBackupRestore(ClusterBackupRestoreCase):
    """Plain and encrypted cluster backup/restore."""


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

