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
from new_test_framework.utils import tdLog, tdSql

from cluster_backup_restore_common import ClusterBackupRestoreCase


class TestClusterBackupRestoreDnodeMismatch(ClusterBackupRestoreCase):
    """Restore when the dnode set differs from the backup."""


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
