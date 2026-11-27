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
import json
import hashlib
import os
import shutil
import subprocess
import time
import tempfile

from new_test_framework.utils import tdLog, tdSql
from backup_restore_base import BackupRestoreTestBase

# SHOW BACKUPS column indexes (see userBackupsSchema in systable.c)
COL_BACKUP_ID = 0
COL_STATUS = 1


class TestBackupSyntax:
    """Backup statement parse / execution behavior.

    Covers the cluster-level backup statements introduced with the
    backup/restore feature:

        CREATE <FULL|INCREMENTAL> BACKUP ON DNODE <id> TO '<path>'
            [<chainId>] [FOR DATABASE <db>] [WITH PASSWORD '<pw>']
            [PARALLEL <n>]
        SHOW BACKUPS
        KILL BACKUP <id>

    Cluster-level FULL and INCREMENTAL are parsed into backup requests.
    A chain id is allowed only for INCREMENTAL; omitted or 0 means the latest
    chain. FOR DATABASE is still rejected by the parser. Incremental requests
    with a valid baseline write and publish a new backup in the chain.
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
        d = os.path.join(tempfile.gettempdir(), f"backup_syntax_{tag}_{int(time.time() * 1000)}").replace("\\", "/")
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

        1. CREATE INCREMENTAL BACKUP ...             -> rejected (no baseline)
        2. CREATE FULL BACKUP ... 12345              -> rejected (chain id only allowed for incremental)
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

        # A missing baseline is rejected by the dnode.
        tdSql.error(f"CREATE INCREMENTAL BACKUP ON DNODE {self.dnode_id} TO '{self._unique_backup_dir('incr')}'")

        # A full backup starts a new chain and cannot specify an existing chain.
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

    def _start_incremental_backup(self, backup_dir, full_id, password_sql=""):
        # IDs have one-second resolution.
        time.sleep(1.1)
        tdSql.query("SHOW BACKUPS")
        baseline = self._max_backup_id()
        tdSql.execute(
            f"CREATE INCREMENTAL BACKUP ON DNODE {self.dnode_id} "
            f"TO '{backup_dir}' {full_id} {password_sql}"
        )
        deadline = time.time() + 10
        backup_id = None
        while time.time() < deadline and backup_id is None:
            tdSql.query("SHOW BACKUPS")
            for row in range(tdSql.queryRows):
                candidate = tdSql.getData(row, COL_BACKUP_ID)
                if candidate > baseline:
                    backup_id = candidate
                    break
            if backup_id is None:
                time.sleep(0.1)
        assert backup_id is not None
        return backup_id

    def do_incremental_published(self, password, numeric_version=False):
        backup_dir = self._unique_backup_dir("incremental_guard")
        password_sql = f"WITH PASSWORD '{password}'" if password else ""
        full_id = self._start_backup(backup_dir, password_sql)
        assert self._wait_backup_finished(full_id) == "succeeded"
        chain_dir = os.path.join(backup_dir, str(full_id))
        full_dir = os.path.join(chain_dir, str(full_id))
        manifest_path = os.path.join(full_dir, "backup.json")
        assert os.path.isfile(manifest_path)
        if numeric_version:
            with open(manifest_path, encoding="utf-8") as file:
                manifest = json.load(file)
            assert manifest["version"] == "2"
            manifest["version"] = 2
            with open(manifest_path, "w", encoding="utf-8") as file:
                json.dump(manifest, file)

        backup_ids = []
        for _ in range(2):
            backup_id = self._start_incremental_backup(backup_dir, full_id, password_sql)
            assert self._wait_backup_finished(backup_id) == "succeeded"
            backup_ids.append(backup_id)
        tdSql.query("SHOW BACKUPS")
        full_row = self._find_backup_row(full_id)
        incremental_row = self._find_backup_row(backup_id)
        assert full_row is not None and incremental_row is not None
        assert tdSql.getData(full_row, COL_STATUS) == "succeeded"
        assert tdSql.getData(incremental_row, 2) == full_id
        assert tdSql.getData(incremental_row, 4) == 0
        assert os.path.isfile(os.path.join(full_dir, "backup.json"))
        for backup_id in backup_ids:
            assert os.path.isfile(os.path.join(chain_dir, str(backup_id), "backup.json"))
            assert not os.path.exists(os.path.join(chain_dir, f"{backup_id}.tmp"))

    def do_incremental_publication_scenarios(self):
        db = f"backup_publish_{int(time.time() * 1000)}"
        tdSql.execute(f"CREATE DATABASE {db} VGROUPS 1")
        try:
            tdSql.execute(f"CREATE TABLE {db}.t (ts TIMESTAMP, v INT)")
            self.do_incremental_published(None)
            time.sleep(1.1)
            self.do_incremental_published(None, numeric_version=True)
            time.sleep(1.1)
            self.do_incremental_published("Taos@123")
        finally:
            tdSql.execute(f"DROP DATABASE {db}")

    def test_incremental_published(self):
        """Publish incremental backups after vnode snapshot reading.

        1. Create a plain and an encrypted full backup.
        2. Create two successive incremental backups on each chain.
        3. Check that all backups succeed and leave no temporary directory.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-02 Verify incomplete incremental backups are not published
        """
        self.do_incremental_publication_scenarios()

    def do_incremental_snapshot_read(self):
        db = f"backup_read_{int(time.time() * 1000)}"
        backup_dir = self._unique_backup_dir("incremental_read")
        tdSql.execute(f"CREATE DATABASE {db} VGROUPS 1 KEEP 3650 DURATION 1")
        try:
            tdSql.execute(f"CREATE TABLE {db}.t (ts TIMESTAMP, v INT)")
            tdSql.execute(f"INSERT INTO {db}.t VALUES (1735689600000, 1) (1738368000000, 2)")
            full_id = self._start_backup(backup_dir)
            assert self._wait_backup_finished(full_id) == "succeeded"
            chain_dir = os.path.join(backup_dir, str(full_id))
            with open(os.path.join(chain_dir, str(full_id), "backup.json"), encoding="utf-8") as file:
                manifest = json.load(file)
            database = next(item for item in manifest["databases"] if item["name"].split(".")[-1] == db)
            vg_id = int(database["vnodes"][0])
            previous_id = full_id
            previous_vnode = next(item for item in manifest["vnodes"] if int(item["id"]) == vg_id)

            for changed in (False, True):
                if changed:
                    tdSql.execute(f"INSERT INTO {db}.t VALUES (1735689600000, 99) (1740787200000, 3)")
                    tdSql.execute(f"DELETE FROM {db}.t WHERE ts = 1738368000000")
                backup_id = self._start_incremental_backup(backup_dir, full_id)
                assert self._wait_backup_finished(backup_id) == "succeeded"
                output_dir = os.path.join(chain_dir, str(backup_id))
                assert not os.path.exists(os.path.join(chain_dir, f"{backup_id}.tmp"))
                with open(os.path.join(output_dir, "backup.json"), encoding="utf-8") as file:
                    incremental = json.load(file)
                assert int(incremental["id"]) == backup_id
                assert int(incremental["chain"]) == full_id
                assert int(incremental["previous"]) == previous_id
                vnode = next(item for item in incremental["vnodes"] if int(item["id"]) == vg_id)
                assert int(vnode["dbUid"]) == int(previous_vnode["dbUid"])
                assert int(vnode["baseVersion"]) == int(previous_vnode["endVersion"])
                assert int(vnode["startVersion"]) == int(vnode["baseVersion"]) + 1
                assert int(vnode["endVersion"]) >= int(vnode["baseVersion"])
                assert vnode["fileSets"]["version"] == "1"
                digest = hashlib.sha256()
                for chunk in range(1, int(vnode["numOfChunks"]) + 1):
                    with open(os.path.join(output_dir, f"vnode{vg_id}", f"{chunk:08d}"), "rb") as file:
                        digest.update(file.read())
                assert vnode["checksum"] == f"sha256:{digest.hexdigest()}"
                previous_id = backup_id
                previous_vnode = vnode
        finally:
            tdSql.execute(f"DROP DATABASE {db}")

    def test_incremental_snapshot_read(self):
        """Publish consecutive incremental snapshot backups.

        1. Read an empty increment from a full backup.
        2. Read updates, inserts and deletes from the previous backup.
        3. Verify chain links, vnode versions and chunk checksums.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-03 Exercise incremental vnode snapshot reading
        """
        self.do_incremental_snapshot_read()

    def do_full_backup_filesets(self):
        suffix = int(time.time() * 1000)
        populated = f"backup_filesets_{suffix}"
        empty = f"backup_empty_{suffix}"
        backup_dir = self._unique_backup_dir("filesets")
        tdSql.execute(f"CREATE DATABASE {populated} VGROUPS 1 DURATION 1 KEEP 3650")
        tdSql.execute(f"CREATE DATABASE {empty} VGROUPS 1")
        try:
            tdSql.execute(f"CREATE TABLE {populated}.t (ts TIMESTAMP, v INT)")
            tdSql.execute(
                f"INSERT INTO {populated}.t VALUES "
                "('2025-01-01 00:00:00', 1) ('2025-02-01 00:00:00', 2)"
            )
            full_id = self._start_backup(backup_dir, "PARALLEL 2")
            assert self._wait_backup_finished(full_id) == "succeeded"
            manifest_path = os.path.join(backup_dir, str(full_id), str(full_id), "backup.json")
            with open(manifest_path, encoding="utf-8") as file:
                manifest = json.load(file)
            assert manifest["version"] == "2"
            vnodes = {int(vnode["id"]): vnode for vnode in manifest["vnodes"]}

            def db_vnode(name):
                db = next(db for db in manifest["databases"] if db["name"].split(".")[-1] == name)
                assert len(db["vnodes"]) == 1
                return vnodes[int(db["vnodes"][0])]

            empty_sets = db_vnode(empty)["fileSets"]
            assert empty_sets["version"] == "1"
            assert empty_sets["sets"] == []

            vnode = db_vnode(populated)
            filesets = vnode["fileSets"]
            assert filesets["version"] == "1"
            assert len(filesets["sets"]) >= 2
            assert len({int(item["fid"]) for item in filesets["sets"]}) == len(filesets["sets"])
            for item in filesets["sets"]:
                assert item["ranges"]
                for version_range in item["ranges"]:
                    assert version_range["type"] in ("head", "data", "sma", "tomb", "stt")
                    int(version_range["minVer"])
                    int(version_range["maxVer"])
                assert any(
                    int(r["minVer"]) <= int(r["maxVer"]) <= int(vnode["endVersion"])
                    for r in item["ranges"]
                )
        finally:
            tdSql.execute(f"DROP DATABASE {populated}")
            tdSql.execute(f"DROP DATABASE {empty}")

    def test_full_backup_filesets(self):
        """Record fileset ranges in the full backup manifest.

        1. Back up an empty vnode and a vnode containing multiple filesets.
        2. Verify the per-vnode fileset descriptions and version ranges.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-03 Record vnode fileset descriptions
        """
        self.do_full_backup_filesets()

    def do_incremental_topology_validation(self):
        db = f"backup_topology_{int(time.time() * 1000)}"
        extra_db = f"{db}_extra"
        backup_dir = self._unique_backup_dir("topology")
        tdSql.execute(f"CREATE DATABASE {db} VGROUPS 1")
        try:
            tdSql.execute(f"CREATE TABLE {db}.t (ts TIMESTAMP, v INT)")
            full_id = self._start_backup(backup_dir)
            assert self._wait_backup_finished(full_id) == "succeeded"
            chain_dir = os.path.join(backup_dir, str(full_id))
            manifest_path = os.path.join(chain_dir, str(full_id), "backup.json")
            with open(manifest_path, encoding="utf-8") as file:
                original = file.read()
            manifest = json.loads(original)
            vnode = next(item for item in manifest["vnodes"] if len(item["dnodes"]) == 1)
            vnode_id = int(vnode["id"])
            sql = f"CREATE INCREMENTAL BACKUP ON DNODE {self.dnode_id} TO '{backup_dir}' {full_id}"
            expected_message = "Cluster topology changed since previous backup"
            for scenario in ("dnode_added", "dnode_removed", "dnode_replaced", "vnode_removed",
                             "vnode_added", "vnode_replaced", "replica_added", "replica_replaced"):
                altered = json.loads(original)
                item = next(value for value in altered["vnodes"] if int(value["id"]) == vnode_id)
                if scenario == "dnode_added":
                    altered["dnodes"].append(9999)
                elif scenario == "dnode_removed":
                    altered["dnodes"].pop()
                elif scenario == "dnode_replaced":
                    altered["dnodes"][0] = 9999
                elif scenario == "vnode_removed":
                    altered["vnodes"].remove(item)
                elif scenario == "vnode_added":
                    added = json.loads(json.dumps(item))
                    added["id"] = "2147483647"
                    altered["vnodes"].append(added)
                elif scenario == "vnode_replaced":
                    item["id"] = "2147483647"
                elif scenario == "replica_added":
                    item["dnodes"].append(9999)
                else:
                    item["dnodes"][0] = 9999
                with open(manifest_path, "w", encoding="utf-8") as file:
                    json.dump(altered, file)
                time.sleep(1.1)
                tdSql.error(sql, expectedErrno=0x1104, expectErrInfo=expected_message)
                assert sorted(os.listdir(chain_dir)) == [str(full_id)]

            for bad_dnodes in (None, [0], [1, 1], ["invalid"]):
                altered = json.loads(original)
                altered["dnodes"] = bad_dnodes
                with open(manifest_path, "w", encoding="utf-8") as file:
                    json.dump(altered, file)
                time.sleep(1.1)
                tdSql.error(sql, expectedErrno=0x0132)
            with open(manifest_path, "w", encoding="utf-8") as file:
                file.write(original)

            tdSql.execute(f"CREATE DATABASE {extra_db} VGROUPS 1")
            try:
                time.sleep(1.1)
                tdSql.error(sql, expectedErrno=0x1104, expectErrInfo=expected_message)
                assert sorted(os.listdir(chain_dir)) == [str(full_id)]
            finally:
                tdSql.execute(f"DROP DATABASE {extra_db}")
            backup_id = self._start_incremental_backup(backup_dir, full_id)
            assert self._wait_backup_finished(backup_id) == "succeeded"
            latest_path = os.path.join(chain_dir, str(backup_id), "backup.json")
            with open(latest_path, encoding="utf-8") as file:
                latest_original = file.read()
            latest = json.loads(latest_original)
            latest["dnodes"][0] = 9999
            with open(latest_path, "w", encoding="utf-8") as file:
                json.dump(latest, file)
            time.sleep(1.1)
            tdSql.error(sql, expectedErrno=0x1104, expectErrInfo=expected_message)
            with open(latest_path, "w", encoding="utf-8") as file:
                file.write(latest_original)
            next_id = self._start_incremental_backup(backup_dir, full_id)
            assert self._wait_backup_finished(next_id) == "succeeded"
        finally:
            tdSql.execute(f"DROP DATABASE {db}")

    def do_incremental_cluster_id_validation(self):
        db = f"backup_cluster_id_{int(time.time() * 1000)}"
        backup_dir = self._unique_backup_dir("cluster_id")
        tdSql.execute(f"CREATE DATABASE {db} VGROUPS 1")
        try:
            tdSql.execute(f"CREATE TABLE {db}.t (ts TIMESTAMP, v INT)")
            tdSql.query("SELECT id FROM information_schema.ins_cluster")
            tdSql.checkRows(1)
            cluster_id = int(tdSql.getData(0, 0))
            assert cluster_id > 0
            full_id = self._start_backup(backup_dir)
            assert self._wait_backup_finished(full_id) == "succeeded"
            chain = os.path.join(backup_dir, str(full_id))
            full_path = os.path.join(chain, str(full_id), "backup.json")
            with open(full_path, encoding="utf-8") as file:
                original_full = file.read()
            assert int(json.loads(original_full)["clusterId"]) == cluster_id
            sql = f"CREATE INCREMENTAL BACKUP ON DNODE {self.dnode_id} TO '{backup_dir}' {full_id}"

            def reject(path, original):
                manifest = json.loads(original)
                replacement = cluster_id + 1 if cluster_id < 9223372036854775807 else cluster_id - 1
                manifest["clusterId"] = str(replacement)
                with open(path, "w", encoding="utf-8") as file:
                    json.dump(manifest, file)
                before = set(os.listdir(chain))
                time.sleep(1.1)
                tdSql.error(sql, expectedErrno=0x1105, expectErrInfo="Cluster ID differs from previous backup")
                assert set(os.listdir(chain)) == before
                with open(path, "w", encoding="utf-8") as file:
                    file.write(original)

            reject(full_path, original_full)
            for value in (None, "", "0", "-1", "invalid", "9223372036854775808", 1.5):
                manifest = json.loads(original_full)
                if value is None:
                    manifest.pop("clusterId")
                else:
                    manifest["clusterId"] = value
                with open(full_path, "w", encoding="utf-8") as file:
                    json.dump(manifest, file)
                before = set(os.listdir(chain))
                time.sleep(1.1)
                tdSql.error(sql, expectedErrno=0x0132)
                assert set(os.listdir(chain)) == before
            with open(full_path, "w", encoding="utf-8") as file:
                file.write(original_full)

            delta_id = self._start_incremental_backup(backup_dir, full_id)
            assert self._wait_backup_finished(delta_id) == "succeeded"
            delta_path = os.path.join(chain, str(delta_id), "backup.json")
            with open(delta_path, encoding="utf-8") as file:
                original_delta = file.read()
            assert int(json.loads(original_delta)["clusterId"]) == cluster_id
            reject(delta_path, original_delta)
            next_id = self._start_incremental_backup(backup_dir, full_id)
            assert self._wait_backup_finished(next_id) == "succeeded"
            with open(os.path.join(chain, str(next_id), "backup.json"), encoding="utf-8") as file:
                assert int(json.load(file)["clusterId"]) == cluster_id
        finally:
            tdSql.execute(f"DROP DATABASE {db}")
        print("incremental cluster ID validation ................. [ passed ]")

    def test_incremental_cluster_id_validation(self):
        """Validate cluster identity before creating incremental backups.

        1. Verify the full and incremental manifests record the actual cluster ID.
        2. Reject a different or malformed previous cluster ID without publishing.
        3. Verify the latest increment is checked and retry the corrected chain.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-10 Verify incremental backup cluster identity
        """
        self.do_incremental_cluster_id_validation()

    def do_incremental_cluster_id_encrypted(self):
        password = "Taos@123"
        backup_dir = self._unique_backup_dir("cluster_id_encrypted")
        password_sql = f"WITH PASSWORD '{password}'"
        full_id = self._start_backup(backup_dir, password_sql)
        assert self._wait_backup_finished(full_id) == "succeeded"
        chain = os.path.join(backup_dir, str(full_id))
        path = os.path.join(chain, str(full_id), "backup.json")
        with open(path, "rb") as file:
            original = file.read()
        iv = original[:16]
        key = BackupRestoreTestBase._backup_sm4_key(self, password).hex()
        result = subprocess.run(
            ["openssl", "enc", "-d", "-sm4-cbc", "-K", key, "-iv", iv.hex()],
            input=original[16:], capture_output=True, timeout=60,
        )
        assert result.returncode == 0, result.stderr
        manifest = json.loads(result.stdout)
        tdSql.query("SELECT id FROM information_schema.ins_cluster")
        tdSql.checkRows(1)
        cluster_id = int(tdSql.getData(0, 0))
        assert int(manifest["clusterId"]) == cluster_id
        manifest["clusterId"] = str(cluster_id + 1 if cluster_id < 9223372036854775807 else cluster_id - 1)
        result = subprocess.run(
            ["openssl", "enc", "-sm4-cbc", "-K", key, "-iv", iv.hex()],
            input=json.dumps(manifest).encode("utf-8"), capture_output=True, timeout=60,
        )
        assert result.returncode == 0, result.stderr
        try:
            with open(path, "wb") as file:
                file.write(iv + result.stdout)
            before = set(os.listdir(chain))
            time.sleep(1.1)
            tdSql.error(f"CREATE INCREMENTAL BACKUP ON DNODE {self.dnode_id} TO '{backup_dir}' "
                        f"{full_id} {password_sql}", expectedErrno=0x1105,
                        expectErrInfo="Cluster ID differs from previous backup")
            assert set(os.listdir(chain)) == before
        finally:
            with open(path, "wb") as file:
                file.write(original)
        delta_id = self._start_incremental_backup(backup_dir, full_id, password_sql)
        assert self._wait_backup_finished(delta_id) == "succeeded"
        print("encrypted incremental cluster ID validation ....... [ passed ]")

    def test_incremental_cluster_id_encrypted(self):
        """Validate cluster identity in an encrypted backup chain.

        1. Check the encrypted full manifest contains the cluster ID.
        2. Reject a modified cluster ID without publishing an increment.
        3. Restore the original manifest and retry successfully.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-10 Verify encrypted backup cluster identity
        """
        self.do_incremental_cluster_id_encrypted()

    def test_incremental_topology_validation(self):
        """Reject topology changes before starting an incremental backup.

        1. Reject dnode membership and vnode count or placement changes.
        2. Reject malformed prior topology separately from genuine changes.
        3. Reject an actual added vnode and accept the unchanged topology.
        4. Compare against the latest increment, then accept the corrected chain.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-04 Validate incremental backup topology
        """
        self.do_incremental_topology_validation()

    def do_incremental_manifest_version_validation(self):
        db = f"backup_manifest_{int(time.time() * 1000)}"
        backup_dir = self._unique_backup_dir("manifest_version")
        tdSql.execute(f"CREATE DATABASE {db} VGROUPS 1")
        try:
            tdSql.execute(f"CREATE TABLE {db}.t (ts TIMESTAMP, v INT)")
            full_id = self._start_backup(backup_dir)
            assert self._wait_backup_finished(full_id) == "succeeded"
            manifest_path = os.path.join(backup_dir, str(full_id), str(full_id), "backup.json")
            with open(manifest_path, encoding="utf-8") as file:
                original = file.read()
            manifest = json.loads(original)
            assert manifest["version"] == "2"
            sql = f"CREATE INCREMENTAL BACKUP ON DNODE {self.dnode_id} TO '{backup_dir}' {full_id}"
            for value in (None, 1, "1", "3", "invalid", 2.5):
                manifest = json.loads(original)
                if value is None:
                    manifest.pop("version")
                else:
                    manifest["version"] = value
                if value == 1 or value == "1":
                    for vnode in manifest["vnodes"]:
                        vnode.pop("fileSets", None)
                with open(manifest_path, "w", encoding="utf-8") as file:
                    json.dump(manifest, file)
                time.sleep(1.1)
                expected_errno = 0x011E if value in (1, "1", "3") else None
                tdSql.error(sql, expectedErrno=expected_errno)

            for field in ("fileSets", "sets", "version"):
                manifest = json.loads(original)
                vnode = manifest["vnodes"][0]
                if field == "fileSets":
                    vnode.pop(field)
                else:
                    vnode["fileSets"].pop(field)
                with open(manifest_path, "w", encoding="utf-8") as file:
                    json.dump(manifest, file)
                time.sleep(1.1)
                tdSql.error(sql)
            for field in ("id", "chain", "previous", "dbUid", "dnodes"):
                manifest = json.loads(original)
                if field in ("dbUid", "dnodes"):
                    manifest["vnodes"][0][field] = "0" if field == "dbUid" else []
                else:
                    manifest[field] = "1"
                with open(manifest_path, "w", encoding="utf-8") as file:
                    json.dump(manifest, file)
                time.sleep(1.1)
                tdSql.error(sql)
            manifest = json.loads(original)
            manifest["vnodes"][0]["fileSets"]["sets"] = [
                {"fid": "1", "ranges": [{"type": "invalid", "minVer": "0", "maxVer": "1"}]}
            ]
            with open(manifest_path, "w", encoding="utf-8") as file:
                json.dump(manifest, file)
            backup_id = self._start_incremental_backup(backup_dir, full_id)
            assert self._wait_backup_finished(backup_id) == "failed"
            chain_dir = os.path.join(backup_dir, str(full_id))
            assert not os.path.exists(os.path.join(chain_dir, str(backup_id)))
            assert not os.path.exists(os.path.join(chain_dir, f"{backup_id}.tmp"))
            with open(manifest_path, "w", encoding="utf-8") as file:
                file.write(original)
            retry_id = self._start_incremental_backup(backup_dir, full_id)
            assert self._wait_backup_finished(retry_id) == "succeeded"
            assert os.path.isfile(os.path.join(chain_dir, str(retry_id), "backup.json"))
            assert not os.path.exists(os.path.join(chain_dir, f"{retry_id}.tmp"))
        finally:
            tdSql.execute(f"DROP DATABASE {db}")

    def test_incremental_manifest_version_validation(self):
        """Validate the previous backup manifest format.

        1. Reject version 1 and invalid or unsupported versions.
        2. Reject a manifest missing fileset description fields.
        3. Reject mismatched chain identities and vnode topology.
        4. Reject malformed fileset ranges without publishing partial output, then retry successfully.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-03 Validate incremental backup manifest versions
        """
        self.do_incremental_manifest_version_validation()

    def do_incremental_vnode_progress_validation(self):
        db = f"backup_progress_{int(time.time() * 1000)}"
        backup_dir = self._unique_backup_dir("progress")
        tdSql.execute(f"CREATE DATABASE {db} VGROUPS 1")
        try:
            tdSql.execute(f"CREATE TABLE {db}.t (ts TIMESTAMP, v INT)")
            full_id = self._start_backup(backup_dir)
            assert self._wait_backup_finished(full_id) == "succeeded"
            manifest_path = os.path.join(backup_dir, str(full_id), str(full_id), "backup.json")
            with open(manifest_path, encoding="utf-8") as file:
                manifest = json.load(file)
            vnode = next(item for item in manifest["vnodes"] if int(item["id"]) > 0)
            original = vnode["endVersion"]
            sql = f"CREATE INCREMENTAL BACKUP ON DNODE {self.dnode_id} TO '{backup_dir}' {full_id}"
            for value in (None, "-1", "9223372036854775808", "1.5"):
                if value is None:
                    vnode.pop("endVersion", None)
                else:
                    vnode["endVersion"] = value
                with open(manifest_path, "w", encoding="utf-8") as file:
                    json.dump(manifest, file)
                time.sleep(1.1)
                tdSql.error(sql)
            vnode["endVersion"] = original
            with open(manifest_path, "w", encoding="utf-8") as file:
                json.dump(manifest, file)
        finally:
            tdSql.execute(f"DROP DATABASE {db}")

    def test_incremental_vnode_progress_validation(self):
        """Reject invalid prior vnode backup versions.

        1. Create a full backup containing a vnode.
        2. Reject a missing, negative, overflowing or nonintegral endVersion.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-02 Add vnode backup progress validation
        """
        self.do_incremental_vnode_progress_validation()
