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
"""Shared helpers for the backup/restore test suite (cases/43-Backup).

Subclasses must provide at least:
  - backup_root : the root directory for backup chains
  - dnode_id    : the local dnode id (single-node tests)
  - dnode_nums  : the number of dnodes (cluster tests)
  - backup_dnode: the dnode that runs the backup agent
"""

import os
import subprocess
import time

from new_test_framework.utils import tdLog, tdSql, sc, cluster, clusterComCheck
from new_test_framework.utils import epath


class BackupRestoreTestBase:
    """Common backup/restore helpers shared by the 43-Backup test cases."""

    # ------------------------------------------------------------------
    # path helpers
    # ------------------------------------------------------------------

    def _taosd_path(self):
        return epath.binFile("taosd")

    def _dnode_cfg_path(self, dnode_id):
        return os.path.join(sc.clusterRootPath(), f"dnode{dnode_id}", "cfg")

    def _dnode_data_dir(self, dnode_id):
        return os.path.join(sc.clusterRootPath(), f"dnode{dnode_id}", "data")

    def _cfg_path(self):
        return self._dnode_cfg_path(self.dnode_id)

    # ------------------------------------------------------------------
    # backup lifecycle
    # ------------------------------------------------------------------

    def _start_backup(self, tag, password=None, parallel=None):
        """Take a full backup on backup_dnode and return the chain directory.

        The backup lands at <dir>/<chainId>/<backupId>/ where <dir> is the
        value of the TO clause; the restore tool expects the chain directory
        (<dir>/<chainId>) as its --restore argument.

        When parallel is not None, appends " PARALLEL <n>" to the statement to
        exercise the vgroup backup parallelism (1 = serial, >1 = parallel).
        """
        backup_dir = os.path.join(self.backup_root, tag)
        os.makedirs(self.backup_root, exist_ok=True)
        tdSql.query("SHOW BACKUPS")
        baseline = max((tdSql.getData(r, 0) for r in range(tdSql.queryRows)), default=0)
        pwd_sql = f" WITH PASSWORD '{password}'" if password else ""
        parallel_sql = f" PARALLEL {parallel}" if parallel is not None else ""
        tdSql.execute(f"CREATE FULL BACKUP ON DNODE {self.backup_dnode} TO '{backup_dir}'{pwd_sql}{parallel_sql}")
        backup_id = self._wait_backup_record(baseline)
        self._wait_backup_done(backup_id)
        # chain id == backup id for a full backup; take the latest record
        tdSql.query("SHOW BACKUPS")
        max_id = 0
        total_size = 0
        for r in range(tdSql.queryRows):
            bid = tdSql.getData(r, 0)
            if bid > max_id:
                max_id = bid
                total_size = tdSql.getData(r, 6)  # size column
        if max_id == 0:
            tdLog.exit("no backup record found after CREATE FULL BACKUP")
        tdLog.info(f"backup total_size: {total_size} bytes "
                   f"({total_size / 1024 / 1024:.2f} MiB)")
        return os.path.join(backup_dir, str(max_id))

    def _wait_backup_record(self, baseline, timeout=120):
        """Wait until a backup newer than baseline appears in SHOW BACKUPS."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            tdSql.query("SHOW BACKUPS")
            for r in range(tdSql.queryRows):
                backup_id = tdSql.getData(r, 0)
                if backup_id > baseline:
                    return backup_id
            time.sleep(1)
        tdLog.exit(f"backup record did not appear within {timeout}s")

    def _wait_backup_done(self, backup_id, timeout=None):
        """Poll SHOW BACKUPS until the given backup is no longer running.

        Subclasses with very large backups can override the default wait via
        the BACKUP_WAIT_TIMEOUT class attribute.
        """
        if timeout is None:
            timeout = getattr(self, "BACKUP_WAIT_TIMEOUT", 120)
        deadline = time.time() + timeout
        while time.time() < deadline:
            tdSql.query("SHOW BACKUPS")
            for r in range(tdSql.queryRows):
                if tdSql.getData(r, 0) == backup_id and tdSql.getData(r, 1) != "running":
                    return
            time.sleep(1)
        tdLog.exit("backup did not finish within %ds" % timeout)

    # ------------------------------------------------------------------
    # cluster lifecycle
    # ------------------------------------------------------------------

    def _stop_all_taosd(self):
        tdLog.info("stopping all taosd (SIGINT)")
        for i in range(1, self.dnode_nums + 1):
            sc.dnodeStop(i)

    def _start_all_taosd(self, wait_ready=True):
        """Start all taosd; if wait_ready, wait for the full cluster to be ready."""
        tdLog.info("starting all taosd")
        sc.dnodeStartAll()
        if not wait_ready:
            return
        deadline = time.time() + 120
        while time.time() < deadline:
            try:
                tdSql.query("SELECT 1")
                if clusterComCheck.checkDnodes(self.dnode_nums):
                    return
            except Exception:
                pass
            time.sleep(1)
        tdLog.exit("cluster did not become ready after restart")

    def _clear_all_data(self):
        """Clear the whole data directory (including dnode/) on every dnode."""
        tdLog.info("clearing all dnode data (whole data dirs)")
        for i in range(1, self.dnode_nums + 1):
            sc.dnodeClearData(i)

    def _bootstrap_cluster_cfg(self):
        """Recreate the cluster topology after a full data clear.

        1. Start all dnodes (only dnode 1, the firstEP/mnode, registers itself
           and gets a fresh dnode.json).
        2. Re-add the other dnodes via CREATE DNODE.
        3. Optionally recreate the mnode group (when mnode_nums > 1).
        4. Wait for all dnode.json files to appear, then stop the cluster.
        """
        self._start_all_taosd(wait_ready=False)

        dnode1_json = os.path.join(self._dnode_data_dir(1), "dnode", "dnode.json")
        deadline = time.time() + 120
        while time.time() < deadline:
            if os.path.exists(dnode1_json):
                tdLog.info("dnode1.json recreated")
                break
            time.sleep(1)
        else:
            tdLog.exit("dnode1.json was not recreated within 120s")

        tdSql.connect()  # ensure connected to the cluster (dnode1)
        for i in range(2, self.dnode_nums + 1):
            dnode = cluster.dnodes[i - 1]
            fqdn = dnode.cfgDict.get("fqdn", "localhost")
            port = dnode.cfgDict.get("serverPort", 6030 + (i - 1) * 100)
            ep = f"{fqdn}:{port}"
            tdSql.execute_ignore_error(f"CREATE DNODE '{ep}'")
            tdLog.info(f"create dnode {ep}")

        # recreate the mnode group on dnode 2..mnode_nums if configured
        for i in range(2, getattr(self, "mnode_nums", 1) + 1):
            tdSql.execute_ignore_error(f"CREATE MNODE ON DNODE {i}")
            tdLog.info(f"create mnode on dnode {i}")

        # wait for every dnode.json to appear
        deadline = time.time() + 120
        while time.time() < deadline:
            all_present = True
            for i in range(1, self.dnode_nums + 1):
                dnode_json = os.path.join(self._dnode_data_dir(i), "dnode", "dnode.json")
                if not os.path.exists(dnode_json):
                    all_present = False
                    break
            if all_present:
                tdLog.info("all dnode.json files recreated")
                break
            time.sleep(1)
        else:
            tdLog.exit("not all dnode.json files were recreated within 120s")

        self._stop_all_taosd()

    def _restore(self, dnode_id, chain_path, password=None, confirmations=("YES",), parallel=None):
        """Run taosd --restore on one dnode, feeding the confirmation lines.

        The restore tool may prompt for two confirmations: the incompatible-
        version warning and the destructive data-clear warning.

        When parallel is not None, appends "--parallel <n>" to restore the
        local vnodes concurrently (1 = serial, >1 = parallel).

        Returns (exit_code, output).
        """
        cfg = self._dnode_cfg_path(dnode_id)
        args = [self._taosd_path(), "-c", cfg, "--restore", chain_path]
        if password:
            args += ["--password", password]
        if parallel is not None:
            args += ["--parallel", str(parallel)]
        confirm_str = "\n".join(confirmations) + "\n"
        tdLog.info(f"restore dnode{dnode_id} cmd: " + " ".join(args) + f" | stdin={confirmations}")
        proc = subprocess.run(args, input=confirm_str, capture_output=True, text=True, timeout=300)
        tdLog.info(f"restore dnode{dnode_id} exit code: {proc.returncode}")
        tdLog.info(f"restore dnode{dnode_id} stdout: {proc.stdout[-2000:]}")
        if proc.stderr:
            tdLog.info(f"restore dnode{dnode_id} stderr: {proc.stderr[-2000:]}")
        return proc.returncode, proc.stdout

    def _restore_all(self, chain_path, password=None, confirmations=("YES",), parallel=None):
        """Run taosd --restore on every dnode. dnode 1 also restores the mnode."""
        for i in range(1, self.dnode_nums + 1):
            code, out = self._restore(i, chain_path, password, confirmations, parallel)
            if code != 0:
                tdLog.exit(f"restore dnode{i} from {chain_path} failed with exit code {code}: {out[-500:]}")
        tdLog.info("all dnodes restored")

    # ------------------------------------------------------------------
    # single-node lifecycle (dnode_id based)
    # ------------------------------------------------------------------

    def _stop_taosd(self):
        tdLog.info("stopping taosd (SIGINT)")
        sc.dnodeStop(self.dnode_id)

    def _start_taosd(self):
        tdLog.info("starting taosd")
        sc.dnodeStartAll()
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                tdSql.query("SELECT 1")
                return
            except Exception:
                time.sleep(1)
        tdLog.exit("taosd did not become ready after restart")

    def _clear_data(self):
        """Clear the whole dnode data directory (including dnode/)."""
        tdLog.info("clearing dnode data (whole data dir)")
        sc.dnodeClearData(self.dnode_id)

    def _bootstrap_dnode_cfg(self):
        """Start taosd once to recreate dnode.json, then stop it (SIGINT)."""
        self._start_taosd()
        dnode_json = os.path.join(self._dnode_data_dir(self.dnode_id), "dnode", "dnode.json")
        deadline = time.time() + 60
        while time.time() < deadline:
            if os.path.exists(dnode_json):
                tdLog.info(f"dnode.json recreated: {dnode_json}")
                break
            time.sleep(1)
        else:
            tdLog.exit(f"dnode.json was not recreated within 60s: {dnode_json}")
        self._stop_taosd()

    # ------------------------------------------------------------------
    # verification helpers
    # ------------------------------------------------------------------

    def _verify_rows(self, rows_db1, rows_db2):
        tdSql.execute("USE db1")
        tdSql.query("SELECT COUNT(*) FROM t1")
        tdSql.checkData(0, 0, rows_db1)
        tdSql.execute("USE db2")
        tdSql.query("SELECT COUNT(*) FROM t2")
        tdSql.checkData(0, 0, rows_db2)
        tdSql.execute("USE db1")
        tdLog.info(f"verified rows: db1={rows_db1} db2={rows_db2}")

    def _verify_write_after_restore(self, rows_db1, rows_db2):
        """After a successful restore the cluster must accept new writes.

        1. insert more rows into the restored database db1
        2. create a new database db3 and insert rows into it

        Both cases use a timestamp offset so the new rows cannot collide
        with the restored ones. db3 uses REPLICA/VGROUPS when the subclass
        defines them (cluster tests), else replica 1 / 1 vgroup.
        """
        extra = getattr(self, "DB_ROWS", 100)
        replica = getattr(self, "REPLICA", 1)
        vgroups = getattr(self, "VGROUPS", 1)
        base_ts = int(time.time() * 1000) + 100000

        tdSql.execute("USE db1")
        for i in range(extra):
            tdSql.execute(f"INSERT INTO t1 VALUES ({base_ts + i}, {10000 + i})")
        tdSql.query("SELECT COUNT(*) FROM t1")
        tdSql.checkData(0, 0, rows_db1 + extra)

        tdSql.execute("DROP DATABASE IF EXISTS db3")
        tdSql.execute(f"CREATE DATABASE db3 vgroups {vgroups} replica {replica}")
        tdSql.execute("USE db3")
        tdSql.execute("CREATE TABLE t3 (ts TIMESTAMP, v INT)")
        for i in range(extra):
            tdSql.execute(f"INSERT INTO t3 VALUES ({base_ts + i}, {i})")
        tdSql.query("SELECT COUNT(*) FROM t3")
        tdSql.checkData(0, 0, extra)

        tdSql.execute("USE db1")
        tdLog.info(f"verified write-after-restore: db1={rows_db1 + extra}, db3={extra}")

    def _verify_replicas(self):
        """Verify db1 keeps replica REPLICA and db2 keeps replica 1 after restore.

        information_schema.ins_vgroups exposes one vN_dnode column per replica
        slot (v1_dnode..v4_dnode); the first `replica` slots must be filled and
        the remaining slots must be empty.
        """
        replica = getattr(self, "REPLICA", 3)
        vgroups = getattr(self, "VGROUPS", 2)

        tdSql.query("SELECT vgroup_id, v1_dnode, v2_dnode, v3_dnode, v4_dnode "
                    "FROM information_schema.ins_vgroups WHERE db_name = 'db1'")
        if tdSql.queryRows != vgroups:
            tdLog.exit(f"db1 should have {vgroups} vgroups, got {tdSql.queryRows}")
        for r in range(tdSql.queryRows):
            slots = [tdSql.getData(r, c) for c in range(1, 5)]
            filled = [s for s in slots[:replica] if s is not None]
            extra = [s for s in slots[replica:] if s is not None]
            if len(filled) != replica or extra:
                tdLog.exit(f"db1 vgroup {tdSql.getData(r, 0)} should be replica {replica}, "
                           f"got {slots}")

        tdSql.query("SELECT vgroup_id, v1_dnode, v2_dnode, v3_dnode, v4_dnode "
                    "FROM information_schema.ins_vgroups WHERE db_name = 'db2'")
        if tdSql.queryRows != vgroups:
            tdLog.exit(f"db2 should have {vgroups} vgroups, got {tdSql.queryRows}")
        for r in range(tdSql.queryRows):
            slots = [tdSql.getData(r, c) for c in range(1, 5)]
            filled = [s for s in slots[:1] if s is not None]
            extra = [s for s in slots[1:] if s is not None]
            if len(filled) != 1 or extra:
                tdLog.exit(f"db2 vgroup {tdSql.getData(r, 0)} should be replica 1, "
                           f"got {slots}")
        tdLog.info(f"verified replicas: db1={replica} db2=1 ({vgroups} vgroups each)")

    # ------------------------------------------------------------------
    # backup encryption / manifest helpers
    # ------------------------------------------------------------------

    def _backup_sm4_key(self, password):
        """Derive the SM4 key used by the backup encryption.

        The backup key is taosEncryptPass_c(password) (MD5 hex, 32 chars),
        truncated to the first ENCRYPT_KEY_LEN(16) bytes.
        """
        import hashlib
        md5_hex = hashlib.md5(password.encode("utf-8")).hexdigest()
        return md5_hex[:16].encode("utf-8")

    def _modify_manifest_tsdb_version(self, chain_dir, password, new_version):
        """Rewrite tsdbVersion in backup.json of the given chain directory.

        chain_dir is the chain directory (<to>/<chainId>) returned by
        _start_backup; the manifest lives at <chain_dir>/<backupId>/backup.json
        (backupId == chainId for a full backup). For an encrypted backup the
        manifest is SM4-CBC encrypted (16-byte IV header + PKCS7 padding) and
        is decrypted/re-encrypted with the known password.
        """
        import json
        backup_id = os.path.basename(chain_dir)
        manifest_path = os.path.join(chain_dir, backup_id, "backup.json")
        tdLog.info(f"modifying tsdbVersion in {manifest_path} -> {new_version}")

        if password is None:
            with open(manifest_path) as f:
                manifest = json.load(f)
            manifest["tsdbVersion"] = new_version
            with open(manifest_path, "w") as f:
                json.dump(manifest, f, indent=4)
            return

        # encrypted: SM4-CBC, IV(16) header, PKCS7 padding
        key = self._backup_sm4_key(password)
        with open(manifest_path, "rb") as f:
            raw = f.read()
        iv = raw[:16]
        ciphertext = raw[16:]

        dec = subprocess.run(
            ["openssl", "enc", "-d", "-sm4-cbc", "-K", key.hex(), "-iv", iv.hex()],
            input=ciphertext, capture_output=True, timeout=60)
        if dec.returncode != 0:
            tdLog.exit(f"failed to decrypt backup.json: {dec.stderr[-500:]}")
        manifest = json.loads(dec.stdout)
        manifest["tsdbVersion"] = new_version
        new_plain = json.dumps(manifest, indent=4).encode("utf-8")

        enc = subprocess.run(
            ["openssl", "enc", "-sm4-cbc", "-K", key.hex(), "-iv", iv.hex()],
            input=new_plain, capture_output=True, timeout=60)
        if enc.returncode != 0:
            tdLog.exit(f"failed to re-encrypt backup.json: {enc.stderr[-500:]}")
        with open(manifest_path, "wb") as f:
            f.write(iv + enc.stdout)
