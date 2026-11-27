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
import subprocess
import time

from new_test_framework.utils import tdLog, tdSql

from cluster_backup_restore_common import ClusterBackupRestoreCase


class TestClusterBackupRestore(ClusterBackupRestoreCase):
    """Plain and encrypted cluster backup/restore."""

    def do_blob_redistribute(self):
        tdSql.execute("DROP DATABASE IF EXISTS sync_blob")
        tdSql.execute("CREATE DATABASE sync_blob VGROUPS 1 REPLICA 1 KEEP 3650")
        tdSql.execute("CREATE TABLE sync_blob.t (ts TIMESTAMP, b BLOB)")
        tdSql.execute("INSERT INTO sync_blob.t VALUES (1735689600000, 'first')")
        tdSql.execute("FLUSH DATABASE sync_blob")
        tdSql.execute("INSERT INTO sync_blob.t VALUES (1735689601000, 'second')")
        tdSql.execute("FLUSH DATABASE sync_blob")
        tdSql.query("SELECT vgroup_id, v1_dnode FROM information_schema.ins_vgroups WHERE db_name='sync_blob'")
        vg_id, source_id = tdSql.queryResult[0]
        target_id = next(node for node in (1, 2, 3) if node != source_id)
        tdSql.execute(f"REDISTRIBUTE VGROUP {vg_id} DNODE {target_id}")
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            tdSql.query("SELECT v1_dnode, v1_status FROM information_schema.ins_vgroups "
                        f"WHERE vgroup_id={vg_id}")
            if tdSql.queryRows and tdSql.getData(0, 0) == target_id and tdSql.getData(0, 1) == "leader":
                break
            time.sleep(1)
        else:
            raise AssertionError(f"vgroup {vg_id} was not moved to dnode {target_id}")
        tdSql.query("SELECT b FROM sync_blob.t ORDER BY ts")
        assert [tuple(row) for row in tdSql.queryResult] == [(b"first",), (b"second",)]
        tdSql.execute("INSERT INTO sync_blob.t VALUES (1735689602000, 'third')")
        tdSql.execute("FLUSH DATABASE sync_blob")
        tdSql.query("SELECT b FROM sync_blob.t ORDER BY ts")
        assert [tuple(row) for row in tdSql.queryResult] == [(b"first",), (b"second",), (b"third",)]
        print("BSE vnode redistribution ......................... [ passed ]")

    def test_blob_redistribute(self):
        """Verify BLOB values through vnode redistribution.

        1. Flush BLOB data in a single-replica vgroup.
        2. Move the vgroup to another dnode and verify the existing values.
        3. Insert another BLOB and verify old and new values.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-09 Verify BSE vnode redistribution
        """
        self.do_blob_redistribute()

    def do_incremental_cluster_restore(self, password):
        for db, replica in (("inc_r3", 3), ("inc_r1", 1)):
            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
            tdSql.execute(f"CREATE DATABASE {db} VGROUPS 2 REPLICA {replica} KEEP 3650 DURATION 1")
            tdSql.execute(f"CREATE TABLE {db}.t (ts TIMESTAMP, v INT)")
            tdSql.execute(f"INSERT INTO {db}.t VALUES (1735689600000, 1) (1738368000000, 2)")
        tag = "inc_encrypted" if password else "inc_plain"
        chain = self._start_backup(tag, password, parallel=2)
        ids = [int(os.path.basename(chain))]
        for db in ("inc_r3", "inc_r1"):
            tdSql.execute(f"INSERT INTO {db}.t VALUES (1735689600000, 9) (1740787200000, 3)")
            tdSql.execute(f"DELETE FROM {db}.t WHERE ts = 1738368000000")
            tdSql.execute(f"CREATE TABLE {db}.new_t (ts TIMESTAMP, v INT)")
            tdSql.execute(f"INSERT INTO {db}.new_t VALUES (1740787200000, 7)")
        ids.append(self._start_incremental_backup(chain, password))
        for db in ("inc_r3", "inc_r1"):
            tdSql.execute(f"INSERT INTO {db}.t VALUES (1743465600000, 4)")
        ids.append(self._start_incremental_backup(chain, password))
        expected = ([(1,), (2,)], [(9,), (3,)], [(9,), (3,), (4,)])

        if password:
            self._stop_all_taosd()
            marker = os.path.join(self._dnode_data_dir(1), "incremental-password-marker")
            with open(marker, "w", encoding="utf-8") as file:
                file.write("unchanged")
            try:
                for password_args in ([], ["--password", "WrongPass@123"]):
                    args = [self._taosd_path(), "-c", self._dnode_cfg_path(1), "--restore", chain] + password_args
                    result = subprocess.run(args, input="YES\nYES\n", text=True, capture_output=True, timeout=300)
                    assert result.returncode != 0, result.stdout + result.stderr
                    with open(marker, encoding="utf-8") as file:
                        assert file.read() == "unchanged"
            finally:
                if os.path.exists(marker):
                    os.remove(marker)
            self._start_all_taosd()
        for index, backup_id in enumerate(ids):
            self._stop_all_taosd()
            for dnode_id in range(1, self.dnode_nums + 1):
                args = [self._taosd_path(), "-c", self._dnode_cfg_path(dnode_id),
                        "--restore", chain, "--last-backup", str(backup_id), "--parallel", "2"]
                if password:
                    args += ["--password", password]
                result = subprocess.run(args, input="YES\nYES\n", text=True, capture_output=True, timeout=300)
                assert result.returncode == 0, result.stdout + result.stderr
            self._start_all_taosd()
            for db in ("inc_r3", "inc_r1"):
                tdSql.query(f"SELECT v FROM {db}.t ORDER BY ts")
                assert [tuple(row) for row in tdSql.queryResult] == expected[index]
                tdSql.query(f"SHOW {db}.TABLES")
                names = [tdSql.getData(row, 0) for row in range(tdSql.queryRows)]
                assert ("new_t" in names) == (index > 0), names
                if index > 0:
                    tdSql.query(f"SELECT v FROM {db}.new_t")
                    tdSql.checkRows(1)
                    tdSql.checkData(0, 0, 7)
            deadline = time.monotonic() + 120
            groups = []
            while time.monotonic() < deadline:
                tdSql.query("SELECT vgroup_id, v1_dnode, v1_status, v2_dnode, v2_status, v3_dnode, v3_status "
                            "FROM information_schema.ins_vgroups WHERE db_name='inc_r3'")
                groups = [tuple(row) for row in tdSql.queryResult]
                if len(groups) == 2 and all(
                    sorted(str(group[i]).lower() for i in (2, 4, 6)) == ["follower", "follower", "leader"]
                    for group in groups
                ):
                    break
                time.sleep(1)
            else:
                raise AssertionError(f"replica vgroups did not become ready: {groups}")
            vg_id = groups[0][0]
            old_leader = next(groups[0][i] for i in (1, 3, 5) if str(groups[0][i + 1]).lower() == "leader")
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                tdSql.execute(f"BALANCE VGROUP LEADER ON {vg_id} FORCE")
                time.sleep(6)
                tdSql.query("SELECT v1_dnode, v1_status, v2_dnode, v2_status, v3_dnode, v3_status "
                            f"FROM information_schema.ins_vgroups WHERE vgroup_id={vg_id}")
                row = tdSql.queryResult[0]
                leaders = [row[i] for i in (0, 2, 4) if str(row[i + 1]).lower() == "leader"]
                if leaders and leaders[0] != old_leader:
                    break
            else:
                raise AssertionError(f"leader did not change for vgroup {vg_id}")
            tdSql.query("SELECT v FROM inc_r3.t ORDER BY ts")
            assert [tuple(row) for row in tdSql.queryResult] == expected[index]
        print(f"cluster incremental restore ({tag}) ............... [ passed ]")

    def test_incremental_restore_plain(self):
        """Restore three-node incremental backup points without encryption.

        1. Capture a full and two incremental backups for replica 1 and 3 databases.
        2. Restore each point on every dnode and verify data and table changes.
        3. Switch a replica leader and verify its recovered data.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-09 Verify cluster incremental restore
        """
        self.do_incremental_cluster_restore(None)

    def test_incremental_restore_encrypted(self):
        """Restore three-node incremental backup points with encryption.

        1. Capture a full and two encrypted incremental backups.
        2. Restore each point on every dnode and verify data and table changes.
        3. Reject missing or wrong passwords without clearing existing data.
        4. Switch a replica leader and verify its recovered data.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-09 Verify encrypted cluster incremental restore
        """
        self.do_incremental_cluster_restore("Taos@123")

    def do_incremental_topology_changes(self):
        tdSql.execute("DROP DATABASE IF EXISTS inc_topology")
        tdSql.execute("CREATE DATABASE inc_topology VGROUPS 1 REPLICA 1")
        tdSql.execute("CREATE TABLE inc_topology.t (ts TIMESTAMP, v INT)")
        chain = self._start_backup("incremental_real_topology")
        chain_id = os.path.basename(chain)
        backup_dir = os.path.dirname(chain)
        sql = f"CREATE INCREMENTAL BACKUP ON DNODE {self.backup_dnode} TO '{backup_dir}' {chain_id}"
        baseline_entries = set(os.listdir(chain))
        self._add_dnode(4, 6330)
        try:
            tdSql.error(sql, expectedErrno=0x1104)
            assert set(os.listdir(chain)) == baseline_entries
            four_node_chain = self._start_backup("incremental_four_node_topology")
            four_node_entries = set(os.listdir(four_node_chain))
            tdSql.execute("DROP DNODE 4")
            self._wait_dnode_dropped(4)
            self._stop_extra_dnode_proc()
            four_node_sql = (f"CREATE INCREMENTAL BACKUP ON DNODE {self.backup_dnode} "
                             f"TO '{os.path.dirname(four_node_chain)}' {os.path.basename(four_node_chain)}")
            tdSql.error(four_node_sql, expectedErrno=0x1104)
            assert set(os.listdir(four_node_chain)) == four_node_entries
            next_id = self._start_incremental_backup(chain)
            assert os.path.isfile(os.path.join(chain, str(next_id), "backup.json"))
            tdSql.query("SELECT vgroup_id, v1_dnode FROM information_schema.ins_vgroups "
                        "WHERE db_name='inc_topology'")
            vg_id, source_id = tdSql.queryResult[0]
            target_id = next(node for node in (1, 2, 3) if node != source_id)
            tdSql.execute(f"REDISTRIBUTE VGROUP {vg_id} DNODE {target_id}")
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                tdSql.query("SELECT v1_dnode, v1_status FROM information_schema.ins_vgroups "
                            f"WHERE vgroup_id={vg_id}")
                if tdSql.queryRows and tdSql.getData(0, 0) == target_id and tdSql.getData(0, 1) == "leader":
                    break
                time.sleep(1)
            else:
                raise AssertionError(f"vgroup {vg_id} was not moved to dnode {target_id}")
            before = set(os.listdir(chain))
            tdSql.error(sql, expectedErrno=0x1104)
            assert set(os.listdir(chain)) == before
            moved_chain = self._start_backup("incremental_moved_topology")
            tdSql.execute("CREATE DATABASE inc_topology_extra VGROUPS 1")
            try:
                moved_sql = (f"CREATE INCREMENTAL BACKUP ON DNODE {self.backup_dnode} "
                             f"TO '{os.path.dirname(moved_chain)}' {os.path.basename(moved_chain)}")
                tdSql.error(moved_sql, expectedErrno=0x1104)
                assert os.listdir(moved_chain) == [os.path.basename(moved_chain)]
            finally:
                tdSql.execute("DROP DATABASE inc_topology_extra")
        finally:
            self._stop_extra_dnode_proc()
        print("actual incremental topology changes ............... [ passed ]")

    def test_incremental_topology_changes(self):
        """Reject real cluster topology changes before incremental backup.

        1. Add and remove a dnode, reject both old baselines and retry the original chain.
        2. Move a replica to another dnode and reject the previous baseline.
        3. Add a vnode and verify no partial incremental backup is published.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-09 Verify actual incremental topology changes
        """
        self.do_incremental_topology_changes()


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
