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

class TestClusterBackupRestore3Mnode(BackupRestoreTestBase):
    """Cluster-level backup and restore with 3 mnodes.

    Run with: pytest -N 3 -M 3
    All 3 dnodes host both a mnode and vnodes (mnode and vnode colocated,
    NOT independent mnodes, so no -I flag). db1 is replica 3 (one copy on
    each dnode), db2 is single-replica. Takes a full backup, clears all
    data, rebuilds the 3-dnode cluster with the mnode group, restores every
    dnode and verifies the row counts, the replica counts, the 3 mnodes and
    post-restore writes.

    Since: v3.4.3.0

    Labels: common,ci,integration,functional,cluster
    """

    backup_root = None

    DB_ROWS = 100
    VGROUPS = 2
    REPLICA = 3

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.dnode_id = 1
        cls.dnode_nums = len(cluster.dnodes)
        cls.mnode_nums = 3              # run with -M 3
        cls.backup_dnode = 1            # backup agent runs on dnode 1
        cls.backup_root = f"/tmp/backup_cluster_3m_{int(time.time() * 1000)}"

    def teardown_class(cls):
        # remove the backup chains
        if cls.backup_root and os.path.exists(cls.backup_root):
            shutil.rmtree(cls.backup_root, ignore_errors=True)

    def _prepare_databases(self):
        """Create two databases (db1 replica 3, db2 replica 1) and insert rows."""
        tdSql.execute("DROP DATABASE IF EXISTS db1")
        tdSql.execute("DROP DATABASE IF EXISTS db2")
        tdSql.execute(f"CREATE DATABASE db1 vgroups {self.VGROUPS} replica 3")
        tdSql.execute(f"CREATE DATABASE db2 vgroups {self.VGROUPS} replica 1")

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

    def _verify_mnodes(self):
        """Verify the cluster has 3 mnodes after restore."""
        tdSql.query("SELECT count(*) FROM information_schema.ins_mnodes")
        tdSql.checkData(0, 0, self.mnode_nums)
        tdLog.info(f"verified mnodes: {self.mnode_nums}")

    def test_backup_restore_3mnodes(self):
        """Backup and restore on a 3-mnode cluster.

        1. Create databases db1 (replica 3) and db2 (replica 1) on the
           6-dnode/3-mnode cluster and insert rows
        2. CREATE FULL BACKUP -> success
        3. Stop all taosd, clear all data
        4. Rebuild the cluster: re-add the 6 dnodes and recreate the
           mnode group (CREATE MNODE ON DNODE 2, 3)
        5. Restore every dnode -> success
        6. Restart all taosd, verify row counts, replica counts, 3 mnodes
        7. Write new rows into db1 and into a new database db3

        Since: v3.4.3.0

        Labels: common,ci,integration,functional,cluster
        Jira: None

        History:
            - 2026-08-20 Bomin Zhang created

        """
        tdLog.info("=========== 3-mnode cluster backup/restore ===========")
        # verify the initial cluster really has 3 mnodes before the backup
        self._verify_mnodes()

        rows = self._prepare_databases()
        chain = self._start_backup("plain_3mnode")

        self._stop_all_taosd()
        self._clear_all_data()
        self._bootstrap_cluster_cfg()
        self._restore_all(chain)
        self._start_all_taosd()

        self._verify_rows(rows[0], rows[1])
        self._verify_replicas()
        self._verify_mnodes()
        self._verify_write_after_restore(rows[0], rows[1])
