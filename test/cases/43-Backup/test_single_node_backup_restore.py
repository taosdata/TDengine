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
import tempfile
import json
import hashlib
import struct
import subprocess
import threading

from new_test_framework.utils import tdLog, tdSql

from backup_restore_base import SingleNodeBackupRestoreBase


class TestSingleNodeBackupRestore(SingleNodeBackupRestoreBase):
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
        3. Verify --verify=0 reaches the destructive confirmation despite the corruption
        4. Verify --verify=1/2 reject the corruption without clearing data
        5. Restart the instance and verify it is usable

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
        args = [self._taosd_path(), "-c", self._cfg_path(), "--restore", chain, "--password", "Taos@123"]
        result = subprocess.run(args + ["--verify=0"], input="NO\n", text=True, capture_output=True, timeout=120)
        assert result.returncode != 0 and "restore target configuration validated" in result.stdout
        assert "clearing existing data directories" not in result.stdout
        result = subprocess.run(args + ["--verify=2"], text=True, capture_output=True, timeout=120)
        assert result.returncode != 0 and "backup integrity check failed" in result.stdout
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








    def do_incremental_different_cluster(self):
        tdSql.execute("CREATE DATABASE restore_cluster_identity VGROUPS 1")
        chain = self._start_backup("different_cluster")
        tdSql.query("SELECT id FROM information_schema.ins_cluster")
        tdSql.checkRows(1)
        original_id = int(tdSql.getData(0, 0))
        self._stop_taosd()
        self._clear_data()
        self._bootstrap_dnode_cfg()
        self._start_taosd()
        tdSql.query("SELECT id FROM information_schema.ins_cluster")
        tdSql.checkRows(1)
        new_id = int(tdSql.getData(0, 0))
        assert new_id > 0 and new_id != original_id, (original_id, new_id)
        before = set(os.listdir(chain))
        tdSql.error(f"CREATE INCREMENTAL BACKUP ON DNODE {self.backup_dnode} "
                    f"TO '{os.path.dirname(chain)}' {os.path.basename(chain)}",
                    expectedErrno=0x1105, expectErrInfo="Cluster ID differs from previous backup")
        assert set(os.listdir(chain)) == before
        self._stop_taosd()
        code, output = self._restore(self.dnode_id, chain)
        assert code == 0, output
        self._start_taosd()
        tdSql.query("SHOW DATABASES")
        assert any(row[0] == "restore_cluster_identity" for row in tdSql.queryResult)
        print("different cluster backup validation ............... [ passed ]")

    def test_incremental_different_cluster(self):
        """Reject an increment from a genuinely different cluster.

        1. Create a full backup and record its cluster ID.
        2. Bootstrap a new cluster and reject using the old backup as a baseline.
        3. Restore the original backup successfully to the new cluster.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-10 Verify cross-cluster backup rejection and restore
        """
        self.do_incremental_different_cluster()

    def _set_full_manifest_version_one(self, chain, password):
        path = os.path.join(chain, os.path.basename(chain), "backup.json")
        with open(path, "rb") as file:
            raw = file.read()
        if password:
            iv = raw[:16]
            key = self._backup_sm4_key(password)
            result = subprocess.run(
                ["openssl", "enc", "-d", "-sm4-cbc", "-K", key.hex(), "-iv", iv.hex()],
                input=raw[16:], capture_output=True, timeout=60,
            )
            assert result.returncode == 0, result.stderr
            raw = result.stdout
        manifest = json.loads(raw)
        manifest["version"] = "1"
        manifest.pop("clusterId", None)
        for vnode in manifest["vnodes"]:
            for key in ("dbUid", "startVersion", "fileSets"):
                vnode.pop(key, None)
        raw = json.dumps(manifest).encode("utf-8")
        if password:
            result = subprocess.run(
                ["openssl", "enc", "-sm4-cbc", "-K", self._backup_sm4_key(password).hex(), "-iv", iv.hex()],
                input=raw, capture_output=True, timeout=60,
            )
            assert result.returncode == 0, result.stderr
            raw = iv + result.stdout
        with open(path, "wb") as file:
            file.write(raw)

    def do_legacy_full_restore(self, password):
        tdSql.execute("DROP DATABASE IF EXISTS restore_legacy")
        tdSql.execute("CREATE DATABASE restore_legacy VGROUPS 1 KEEP 3650")
        tdSql.execute("CREATE TABLE restore_legacy.t (ts TIMESTAMP, v INT)")
        tdSql.execute("INSERT INTO restore_legacy.t VALUES (1735689600000, 7)")
        chain = self._start_backup("legacy_enc" if password else "legacy_plain", password)
        self._set_full_manifest_version_one(chain, password)
        password_sql = f" WITH PASSWORD '{password}'" if password else ""
        tdSql.error(f"CREATE INCREMENTAL BACKUP ON DNODE {self.backup_dnode} "
                    f"TO '{os.path.dirname(chain)}' {os.path.basename(chain)}{password_sql}",
                    expectedErrno=0x011E)
        self._stop_taosd()
        code, output = self._restore(self.dnode_id, chain, password)
        assert code == 0, output
        self._start_taosd()
        tdSql.query("SELECT v FROM restore_legacy.t")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 7)
        print("legacy full backup restore ......................... [ passed ]")

    def test_legacy_full_restore_plain(self):
        """Restore a legacy version 1 full backup without encryption.

        1. Convert a full backup manifest to the version 1 field shape.
        2. Reject it as an incremental baseline.
        3. Restore the full backup and verify data.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-09 Verify legacy full backup restore
        """
        self.do_legacy_full_restore(None)

    def test_legacy_full_restore_encrypted(self):
        """Restore a legacy version 1 full backup with encryption.

        1. Convert an encrypted full backup manifest to the version 1 field shape.
        2. Reject it as an incremental baseline.
        3. Restore the full backup and verify data.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-09 Verify encrypted legacy full backup restore
        """
        self.do_legacy_full_restore("Taos@123")

    def do_incremental_chain_mixed_manifest_format(self):
        tdSql.execute("DROP DATABASE IF EXISTS restore_mixed_fmt")
        tdSql.execute("CREATE DATABASE restore_mixed_fmt VGROUPS 1 KEEP 3650")
        tdSql.execute("CREATE TABLE restore_mixed_fmt.t (ts TIMESTAMP, v INT)")
        tdSql.execute("INSERT INTO restore_mixed_fmt.t VALUES (1735689600000, 11)")
        chain = self._start_backup("mixed_fmt")
        tdSql.execute("INSERT INTO restore_mixed_fmt.t VALUES (1738368000000, 12)")
        delta_id = self._start_incremental_backup(chain)

        # Downgrade only the version field of the incremental manifest so the
        # chain mixes format 1 and format 2; the incremental backup is only
        # restorable on top of a format 2 parent, so the chain must be rejected.
        path = os.path.join(chain, str(delta_id), "backup.json")
        with open(path, encoding="utf-8") as file:
            manifest = json.load(file)
        manifest["version"] = "1"
        with open(path, "w", encoding="utf-8") as file:
            json.dump(manifest, file, indent=4)

        self._stop_taosd()
        code, out = self._restore(self.dnode_id, chain)
        if code == 0:
            tdLog.exit("restore of a chain with mixed manifest formats should fail")
        if "invalid backup identity, root or format" not in out:
            tdLog.exit(f"expected a manifest format error, got: {out[-500:]}")

        # the chain is verified before any data is cleared, so the rows survive
        self._start_taosd()
        tdSql.query("SELECT v FROM restore_mixed_fmt.t ORDER BY ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 11)
        tdSql.checkData(1, 0, 12)
        print("mixed manifest formats ............................. [ passed ]")

    def test_incremental_chain_mixed_manifest_format(self):
        """Reject an incremental chain whose manifests mix format versions.

        1. Take a full backup and one incremental backup on the same chain.
        2. Rewrite the version field of the incremental manifest to 1.
        3. Restore the chain -> rejected while verifying, before any data is cleared.
        4. Restart taosd and verify the rows are intact.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-11 Reject mixed manifest formats in one chain
        """
        self.do_incremental_chain_mixed_manifest_format()












    def do_restore_target_preparation(self):
        rows = self._prepare_databases()
        chain = self._start_backup("target_preparation")
        self._stop_taosd()
        data_dir = self._dnode_data_dir(self.dnode_id)
        config_path = os.path.join(data_dir, "dnode", "dnode.json")
        with open(config_path, encoding="utf-8") as file:
            original_config = file.read()
        marker = os.path.join(data_dir, "restore-preflight-marker")
        with open(marker, "w", encoding="utf-8") as file:
            file.write("unchanged")
        overlap_dir = None
        try:
            config = json.loads(original_config)
            extra = dict(config["dnodes"][0])
            extra["id"] = max(int(dnode["id"]) for dnode in config["dnodes"]) + 1
            config["dnodes"].append(extra)
            with open(config_path, "w", encoding="utf-8") as file:
                json.dump(config, file)
            code, out = self._restore(self.dnode_id, chain, None)
            assert code != 0 and "failed to map source and target dnodes" in out
            with open(marker, encoding="utf-8") as file:
                assert file.read() == "unchanged"
            with open(config_path, "w", encoding="utf-8") as file:
                file.write(original_config)

            manifest_path = os.path.join(chain, os.path.basename(chain), "backup.json")
            with open(manifest_path, encoding="utf-8") as file:
                original_manifest = file.read()
            manifest = json.loads(original_manifest)
            vnode = manifest["vnodes"][0]
            assert int(vnode["numOfChunks"]) == 1
            chunk = os.path.join(chain, os.path.basename(chain), f"vnode{vnode['id']}", "00000001")
            with open(chunk, "rb") as file:
                original_chunk = file.read()
            try:
                snap_type, flag, index, length = struct.unpack_from("@bb6xqq", original_chunk, 16)
                assert snap_type == 0 and index == 1
                vnode_info = json.loads(original_chunk[40:40 + length - 1])
                vnode_info["config"]["vgId"] = "2147483647"
                payload = json.dumps(vnode_info).encode("utf-8") + b"\0"
                modified = (original_chunk[:16] + struct.pack("@bb6xqq", snap_type, flag, index, len(payload)) +
                            payload + original_chunk[40 + length:])
                with open(chunk, "wb") as file:
                    file.write(modified)
                args = [self._taosd_path(), "-c", self._cfg_path(), "--restore", chain]
                result = subprocess.run(
                    args + ["--verify=0"],
                    input="NO\n", text=True, capture_output=True, timeout=120,
                )
                assert result.returncode != 0 and "restore target configuration validated" in result.stdout
                assert "clearing existing data directories" not in result.stdout
                assert os.path.isfile(marker)
                for mode in ("--verify=1", "--verify=2"):
                    result = subprocess.run(args + [mode], input="NO\n", text=True, capture_output=True, timeout=120)
                    assert result.returncode != 0 and "backup integrity check failed" in result.stdout
                    assert os.path.isfile(marker)
                vnode["checksum"] = "sha256:" + hashlib.sha256(modified).hexdigest()
                with open(manifest_path, "w", encoding="utf-8") as file:
                    json.dump(manifest, file)
                result = subprocess.run(
                    args + ["--verify=2"], text=True, capture_output=True, timeout=120,
                )
                assert result.returncode == 0 and "verified successfully" in result.stdout
                assert os.path.isfile(marker)
                result = subprocess.run(
                    args + ["--verify=1"],
                    input="NO\n", text=True, capture_output=True, timeout=120,
                )
                assert result.returncode != 0 and "restore target configuration validated" in result.stdout
                assert "clearing existing data directories" not in result.stdout
                assert os.path.isfile(marker)
            finally:
                with open(chunk, "wb") as file:
                    file.write(original_chunk)
                with open(manifest_path, "w", encoding="utf-8") as file:
                    file.write(original_manifest)

            overlap_dir = tempfile.mkdtemp(prefix="restore-overlap-", dir=data_dir)
            overlap_chain = os.path.join(overlap_dir, os.path.basename(chain))
            shutil.copytree(chain, overlap_chain)
            code, out = self._restore(self.dnode_id, overlap_chain, None)
            assert code != 0 and "backup chain and data directory overlap" in out
            assert os.path.isfile(marker) and os.path.isdir(overlap_chain)

            args = [self._taosd_path(), "-c", self._cfg_path(), "--restore", chain]
            process = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT, text=True)
            ready = threading.Event()
            output = []

            def wait_for_prompt():
                for line in process.stdout:
                    output.append(line)
                    if "to continue" in line.lower():
                        ready.set()
                        return

            thread = threading.Thread(target=wait_for_prompt, daemon=True)
            thread.start()
            try:
                assert ready.wait(30), "".join(output)
                contender = subprocess.run(args + ["--verify=2"], input="", text=True,
                                           capture_output=True, timeout=30)
                assert contender.returncode != 0
                assert "taosd is running" in contender.stdout
                process.stdin.write("NO\n")
                process.stdin.flush()
                assert process.wait(timeout=30) != 0
            finally:
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=30)
            assert os.path.isfile(marker)
        finally:
            with open(config_path, "w", encoding="utf-8") as file:
                file.write(original_config)
            if overlap_dir is not None:
                shutil.rmtree(overlap_dir)
            if os.path.exists(marker):
                os.remove(marker)
            self._start_taosd()
        self._verify_rows(rows[0], rows[1])
        print("restore target preparation ......................... [ passed ]")

    def test_restore_target_preparation(self):
        """Validate restore targets before clearing existing data.

        1. Reject a dnode mapping mismatch and overlapping paths.
        2. Verify backup corruption is checked only when verification is enabled.
        3. Hold the restore lock while awaiting confirmation.
        4. Verify failed or cancelled preparation preserves existing data.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-04 Validate restore targets before clearing data
        """
        self.do_restore_target_preparation()

    def do_incremental_chain_validation(self):
        rows = self._prepare_databases()
        chain = self._start_backup("chain_validation")
        chain_id = int(os.path.basename(chain))
        backup_dir = os.path.dirname(chain)
        ids = [chain_id]
        for _ in range(2):
            tdSql.query("SHOW BACKUPS")
            baseline = max((tdSql.getData(r, 0) for r in range(tdSql.queryRows)), default=0)
            time.sleep(1.1)
            tdSql.execute(f"CREATE INCREMENTAL BACKUP ON DNODE {self.backup_dnode} TO '{backup_dir}' {chain_id}")
            backup_id = self._wait_backup_record(baseline)
            self._wait_backup_done(backup_id)
            ids.append(backup_id)

        self._stop_taosd()
        try:
            marker = os.path.join(self._dnode_data_dir(self.dnode_id), "chain-identity-marker")
            with open(marker, "w", encoding="utf-8") as file:
                file.write("unchanged")

            def verify(extra, success):
                result = subprocess.run(
                    [self._taosd_path(), "-c", self._cfg_path(), "--restore", chain] + extra,
                    input="YES\nYES\n", text=True, capture_output=True, timeout=120,
                )
                assert (result.returncode == 0) == success, result.stdout + result.stderr
                return result.stdout

            assert "verified successfully" in verify(["--verify=2"], True)
            verify(["--verify=2", "--last-backup", str(ids[0])], True)
            verify(["--verify=2", "--last-backup", str(ids[2] + 1)], False)
            for index in (0, 1, 2):
                path = os.path.join(chain, str(ids[index]), "backup.json")
                with open(path, encoding="utf-8") as file:
                    original = file.read()
                try:
                    original_manifest = json.loads(original)
                    cluster_id = int(original_manifest["clusterId"])
                    other_id = cluster_id + 1 if cluster_id < 9223372036854775807 else cluster_id - 1
                    for field, value in (("clusterId", str(other_id)), ("clusterId", None),
                                         ("clusterId", "0"), ("clusterId", "-1"),
                                         ("clusterId", "invalid"), ("clusterId", "9223372036854775808"),
                                         ("clusterId", 1.5), ("chain", str(chain_id + 1)),
                                         ("chain", None)):
                        manifest = json.loads(original)
                        if value is None:
                            manifest.pop(field)
                        else:
                            manifest[field] = value
                        with open(path, "w", encoding="utf-8") as file:
                            json.dump(manifest, file)
                        expected = "cluster ID" if field == "clusterId" else "backup identity"
                        for mode in ("--verify=0", "--verify=1", "--verify=2"):
                            assert expected in verify([mode], False)
                        with open(marker, encoding="utf-8") as file:
                            assert file.read() == "unchanged"
                        if index > 0:
                            verify(["--verify=2", "--last-backup", str(ids[index - 1])], True)

                    for field in ("id", "chain", "previous", "version", "endVersion",
                                  "dbUid", "dnodes", "fileSets", "baseVersion", "startVersion"):
                        if index == 0 and field in ("baseVersion",):
                            continue
                        manifest = json.loads(original)
                        vnode = manifest["vnodes"][0]
                        if field in ("id", "chain", "previous", "version"):
                            manifest[field] = "0"
                        elif field == "dnodes":
                            vnode[field] = [9999]
                        elif field == "fileSets":
                            vnode[field] = {"version": "1", "sets": [{"fid": "1", "ranges": "bad"}]}
                        else:
                            vnode[field] = "9223372036854775808"
                        with open(path, "w", encoding="utf-8") as file:
                            json.dump(manifest, file)
                        mode = "--verify=0" if field in ("id", "chain", "previous", "version") else "--verify=1"
                        verify([mode], False)
                finally:
                    with open(path, "w", encoding="utf-8") as file:
                        file.write(original)

            missing = os.path.join(chain, str(ids[1]))
            hidden = missing + ".hidden"
            os.rename(missing, hidden)
            try:
                verify(["--verify=0"], False)
            finally:
                os.rename(hidden, missing)
            path = os.path.join(chain, str(ids[2]), "backup.json")
            with open(path, encoding="utf-8") as file:
                latest = json.load(file)
            vnode = latest["vnodes"][0]
            chunk = os.path.join(chain, str(ids[2]), f"vnode{vnode['id']}", "00000001")
            with open(chunk, "rb") as file:
                original_chunk = file.read()
            try:
                corrupted = bytearray(original_chunk)
                corrupted[-1] ^= 1
                with open(chunk, "wb") as file:
                    file.write(corrupted)
                verify(["--verify=2"], False)
            finally:
                with open(chunk, "wb") as file:
                    file.write(original_chunk)
            verify(["--verify=2"], True)
        finally:
            if os.path.exists(marker):
                os.remove(marker)
            self._start_taosd()
        self._verify_rows(rows[0], rows[1])
        print("incremental chain validation ......................... [ passed ]")

    def test_incremental_chain_validation(self):
        """Validate a full and two-incremental backup chain before restore.

        1. Verify the complete chain and an exact full-backup target.
        2. Reject inconsistent or invalid cluster and chain IDs in every link.
        3. Verify target cutoffs, topology, versions and fileset descriptions.
        4. Reject a missing link and preserve the existing data.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-04 Validate incremental restore chains
        """
        self.do_incremental_chain_validation()

    def do_encrypted_chain_identity_validation(self):
        password = "Taos@123"
        tdSql.execute("DROP DATABASE IF EXISTS restore_chain_identity")
        tdSql.execute("CREATE DATABASE restore_chain_identity VGROUPS 1 KEEP 3650")
        tdSql.execute("CREATE TABLE restore_chain_identity.t (ts TIMESTAMP, v INT)")
        tdSql.execute("INSERT INTO restore_chain_identity.t VALUES (1735689600000, 1)")
        chain = self._start_backup("encrypted_chain_identity", password)
        ids = [int(os.path.basename(chain))]
        for value in (2, 3):
            tdSql.execute(f"INSERT INTO restore_chain_identity.t VALUES (1735689600000, {value})")
            ids.append(self._start_incremental_backup(chain, password))
        key = self._backup_sm4_key(password).hex()
        self._stop_taosd()
        marker = os.path.join(self._dnode_data_dir(self.dnode_id), "encrypted-identity-marker")
        try:
            with open(marker, "w", encoding="utf-8") as file:
                file.write("unchanged")

            def verify(extra, success):
                result = subprocess.run(
                    [self._taosd_path(), "-c", self._cfg_path(), "--restore", chain,
                     "--password", password] + extra,
                    input="YES\nYES\n", text=True, capture_output=True, timeout=120,
                )
                assert (result.returncode == 0) == success, result.stdout + result.stderr
                return result.stdout

            verify(["--verify=2"], True)
            for index, backup_id in enumerate(ids):
                path = os.path.join(chain, str(backup_id), "backup.json")
                with open(path, "rb") as file:
                    original = file.read()
                iv = original[:16]
                result = subprocess.run(
                    ["openssl", "enc", "-d", "-sm4-cbc", "-K", key, "-iv", iv.hex()],
                    input=original[16:], capture_output=True, timeout=60,
                )
                assert result.returncode == 0, result.stderr
                plain = json.loads(result.stdout)
                try:
                    for field in ("clusterId", "chain"):
                        manifest = dict(plain)
                        number = int(manifest[field])
                        manifest[field] = str(number + 1 if number < 9223372036854775807 else number - 1)
                        result = subprocess.run(
                            ["openssl", "enc", "-sm4-cbc", "-K", key, "-iv", iv.hex()],
                            input=json.dumps(manifest).encode("utf-8"), capture_output=True, timeout=60,
                        )
                        assert result.returncode == 0, result.stderr
                        with open(path, "wb") as file:
                            file.write(iv + result.stdout)
                        expected = "cluster ID" if field == "clusterId" else "backup identity"
                        for mode in ("--verify=0", "--verify=1", "--verify=2"):
                            assert expected in verify([mode], False)
                        with open(marker, encoding="utf-8") as file:
                            assert file.read() == "unchanged"
                        if index > 0:
                            verify(["--verify=2", "--last-backup", str(ids[index - 1])], True)
                finally:
                    with open(path, "wb") as file:
                        file.write(original)
            verify(["--verify=2"], True)
        finally:
            if os.path.exists(marker):
                os.remove(marker)
            self._start_taosd()
        tdSql.query("SELECT v FROM restore_chain_identity.t")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)
        print("encrypted restore chain identity .................. [ passed ]")

    def test_encrypted_chain_identity_validation(self):
        """Validate cluster and chain IDs in every encrypted backup.

        1. Create a full and two incremental backups with a password.
        2. Reject a mismatched cluster or chain ID in each backup at all verify levels.
        3. Check earlier restore targets and preserve the existing data.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-10 Verify encrypted restore chain identities
        """
        self.do_encrypted_chain_identity_validation()


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
