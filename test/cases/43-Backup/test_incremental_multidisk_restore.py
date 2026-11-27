# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

import json
import os
import shutil
import struct
import subprocess
import tempfile
import threading

from new_test_framework.utils import tdLog, tdSql

from backup_restore_base import BackupRestoreTestBase


class TestIncrementalMultidiskRestore(BackupRestoreTestBase):
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.dnode_id = 1
        cls.dnode_nums = 1
        cls.backup_dnode = 1
        cls.backup_root = tempfile.mkdtemp(prefix="incremental_multidisk_").replace("\\", "/")

    def teardown_class(cls):
        if cls.backup_root and os.path.exists(cls.backup_root):
            shutil.rmtree(cls.backup_root)

    def _data_dirs(self):
        with open(os.path.join(self._cfg_path(), "taos.cfg"), encoding="utf-8") as file:
            dirs = [line.split()[1] for line in file if line.split() and line.split()[0].lower() == "datadir"]
        assert len(dirs) >= 2, dirs
        return dirs

    def do_incremental_multidisk_restore(self):
        tdSql.execute("DROP DATABASE IF EXISTS restore_disks")
        tdSql.execute("CREATE DATABASE restore_disks VGROUPS 2 KEEP 3650")
        tdSql.execute("CREATE TABLE restore_disks.t (ts TIMESTAMP, v INT)")
        tdSql.execute("INSERT INTO restore_disks.t VALUES (1735689600000, 1)")
        chain = self._start_backup("chain")
        tdSql.execute("INSERT INTO restore_disks.t VALUES (1738368000000, 2)")
        delta_id = self._start_incremental_backup(chain)
        dirs = self._data_dirs()
        self._stop_taosd()
        markers = [os.path.join(path, "incremental-restore-marker") for path in dirs]
        for marker in markers:
            with open(marker, "w", encoding="utf-8") as file:
                file.write("unchanged")
        overlap = tempfile.mkdtemp(prefix="restore-overlap-", dir=dirs[0])
        try:
            overlap_chain = os.path.join(overlap, os.path.basename(chain))
            shutil.copytree(chain, overlap_chain)
            code, output = self._restore(self.dnode_id, overlap_chain)
            assert code != 0 and "backup chain and data directory overlap" in output, output
            assert all(os.path.isfile(marker) for marker in markers)

            args = [self._taosd_path(), "-c", self._cfg_path(), "--restore", chain]
            process = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT, text=True)
            ready = threading.Event()
            lines = []

            def read_prompt():
                for line in process.stdout:
                    lines.append(line)
                    if "to continue" in line.lower():
                        ready.set()
                        return

            thread = threading.Thread(target=read_prompt, daemon=True)
            thread.start()
            try:
                assert ready.wait(30), "".join(lines)
                contender = subprocess.run(args + ["--verify=2"], text=True, capture_output=True, timeout=30)
                assert contender.returncode != 0 and "taosd is running" in contender.stdout
                startup = subprocess.Popen([self._taosd_path(), "-c", self._cfg_path()],
                                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                try:
                    startup_output, _ = startup.communicate(timeout=30)
                    assert startup.returncode != 0, startup_output
                finally:
                    if startup.poll() is None:
                        startup.terminate()
                        startup.wait(timeout=30)
                process.stdin.write("NO\n")
                process.stdin.flush()
                assert process.wait(timeout=30) != 0
            finally:
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=30)
            assert all(os.path.isfile(marker) for marker in markers)
        finally:
            shutil.rmtree(overlap)

        delta_dir = os.path.join(chain, str(delta_id))
        with open(os.path.join(delta_dir, "backup.json"), encoding="utf-8") as file:
            manifest = json.load(file)
        injected = False
        for vnode in manifest["vnodes"]:
            assert int(vnode["numOfChunks"]) == 1
            chunk = os.path.join(delta_dir, f"vnode{vnode['id']}", "00000001")
            with open(chunk, "rb") as file:
                original = file.read()
            damaged = bytearray(original)
            offset = 16
            while offset < len(damaged):
                kind, _, index, size = struct.unpack_from("@bb6xqq", damaged, offset)
                if kind == 2:
                    struct.pack_into("@q", damaged, offset + 8, index + 5)
                    injected = True
                    break
                offset += 24 + size
            if not injected:
                continue
            try:
                with open(chunk, "wb") as file:
                    file.write(damaged)
                failed = subprocess.run(
                    [self._taosd_path(), "-c", self._cfg_path(), "--restore", chain, "--verify=0"],
                    input="YES\nYES\n", text=True, capture_output=True, timeout=180,
                )
                assert failed.returncode != 0 and "failed to restore vnode" in failed.stdout, failed.stdout
                assert all(not os.path.exists(marker) for marker in markers)
            finally:
                with open(chunk, "wb") as file:
                    file.write(original)
            break
        assert injected, "no TSDB snapshot block found in incremental backup"

        code, output = self._restore(self.dnode_id, chain)
        assert code == 0, output
        assert all(not os.path.exists(marker) for marker in markers)
        self._start_taosd()
        tdSql.query("SELECT v FROM restore_disks.t ORDER BY ts")
        assert [tuple(row) for row in tdSql.queryResult] == [(1,), (2,)]
        print("incremental multidisk restore ...................... [ passed ]")

    def test_incremental_multidisk_restore(self):
        """Restore an incremental chain on multiple data directories.

        1. Reject overlapping backup paths without clearing either data directory.
        2. Reject a concurrent restore and startup while the restore lock is held.
        3. Inject a restore failure after clearing both data directories.
        4. Retry successfully and verify the restored data.

        Catalog:
            - Backup

        Since: v3.4.3.0

        Labels: common,ci,integration

        Jira: None

        History:
            - 2026-11-09 Verify incremental multidisk restore
        """
        self.do_incremental_multidisk_restore()
