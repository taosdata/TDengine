# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

from new_test_framework.utils import tdLog, tdSql, epath, sc, etool, AutoGen, tdDnodes, eutil
import os
import subprocess
import time
import psutil


class TestBasic:
    updatecfgDict = {'dDebugFlag': 131}

    # Pre-generate encryption keys so taosd boots with encryption enabled
    # and the key-verification file gets created on the first successful start.
    encryptConfig = {
        "svrKey": "orderchk123",
        "dbKey": "orderchkdb45",
        "dataKey": "orderchkdt1",
        "generateConfig": True,
        "generateMeta": True,
        "generateData": True
    }

    def setup_class(cls):
        cls.init(cls, replicaVar=1, checkColName="c1")
        cls.valgrind = 0
        cls.db = "encrypt_order_db"

    def _is_taosd_running(self, dnodeIndex=1):
        for proc in psutil.process_iter(["pid", "name", "cmdline"]):
            try:
                if proc.info["name"] and "taosd" in proc.info["name"]:
                    if proc.info["cmdline"] and any(
                        f"dnode{dnodeIndex}" in str(arg) for arg in proc.info["cmdline"]
                    ):
                        return True
            except Exception:
                continue
        return False

    def _wait_taosd_running(self, expected, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self._is_taosd_running() == expected:
                return True
            time.sleep(1)
        return self._is_taosd_running() == expected

    def test_encryption_key_check_order(self):
        """Ensure a wrong encryption key is rejected before vnode is opened.

        Regression test: the encryption key correctness check
        (dmVerifyAndInitEncryptionKeys) used to run only after every node
        type -- including vnode -- had already been opened, since it lived
        in the dnode's "start" phase while all nodes' "open" phase runs
        first. With a wrong key, OpenVnode failed first with an opaque
        decrypt error and the dedicated key-verification error was never
        reached. The fix moves the check into the dnode's own "open" phase,
        which always runs before any other node (vnode included) opens.

        1. Start a dnode with encryption enabled and create a database so
           at least one vnode exists.
        2. Stop taosd, then change SVR_KEY via taosk without touching the
           key verification file -- effectively substituting a wrong key.
        3. Restart taosd and verify:
           - the process fails to stay up
           - the log clearly reports the dedicated key verification failure
           - the log does NOT show a vnode being opened for this start,
             proving the check happened before any vnode open attempt.
        4. Restore the original key so taosd (and the environment teardown)
           can come back up cleanly.

        Since: v3.4.0.0

        Labels: common,ci,integration,functional,security

        History:
            - 2026-09-28 Added as a regression test for the vnode
              encryption-key check ordering fix.

        """
        cfg_path = tdDnodes.dnodes[0].cfgDir
        data_dir = tdDnodes.dnodes[0].dataDir
        if isinstance(data_dir, list):
            data_dir = data_dir[0]

        key_dir = os.path.join(data_dir, "dnode", "config")
        master_file = os.path.join(key_dir, "master.bin")
        assert os.path.exists(master_file), f"master.bin missing at {master_file}"

        taosk_bin = etool.taoskFile()
        tdLog.info(f"taosk binary: {taosk_bin}")

        # Test 1: create a database/table so at least one vnode exists --
        # the old bug's failure point (OpenVnode) must be reachable in
        # principle for this regression test to mean anything.
        tdLog.info("=" * 80)
        tdLog.info("Test 1: Preparing an encrypted database with data")
        tdLog.info("=" * 80)

        tdSql.execute(f"CREATE DATABASE IF NOT EXISTS {self.db} ENCRYPT_ALGORITHM 'SM4-CBC'")
        tdSql.execute(f"USE {self.db}")
        tdSql.execute("CREATE TABLE IF NOT EXISTS t (ts TIMESTAMP, v INT)")
        tdSql.execute("INSERT INTO t VALUES (now, 1)")
        tdSql.query(f"SELECT * FROM {self.db}.t")
        assert tdSql.queryRows == 1

        # Test 2: restart with a wrong SVR_KEY and confirm the dedicated
        # key-verification error is reported before any vnode is opened.
        tdLog.info("=" * 80)
        tdLog.info("Test 2: Restarting taosd with a wrong encryption key")
        tdLog.info("=" * 80)

        tdDnodes.dnodes[0].stoptaosd()
        assert self._wait_taosd_running(False), "taosd did not stop"

        # Swap in a different SVR_KEY. key_verify.dat (written against the
        # ORIGINAL key on first successful boot) is left untouched, so the
        # new key is now "wrong" from taosd's point of view.
        cmd = [taosk_bin, '-c', cfg_path, '--update-svrkey', 'wrongchk4567']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=True)
        tdLog.info(f"taosk update-svrkey output: {result.stdout}")

        tdDnodes.dnodes[0].starttaosd()

        assert self._wait_taosd_running(False, timeout=30), \
            "taosd should exit when the encryption key verification fails, but it kept running"
        tdLog.info("taosd correctly exited after rejecting the wrong encryption key")

        cnt = eutil.findTaosdLog("failed to verify encryption keys", retry=5)
        assert cnt > 0, "expected the dedicated encryption key verification error in the log"
        tdLog.info("found the dedicated encryption key verification error in the log")

        cnt_vnode_open = eutil.findTaosdLog("failed to open vnode", retry=1)
        assert cnt_vnode_open == 0, \
            "vnode open must never be attempted once the encryption key check has failed"
        tdLog.info("confirmed vnode open was never attempted with the wrong key")

        # Test 3: restore the original key and confirm normal startup and
        # data access resume, so the environment tears down cleanly.
        tdLog.info("=" * 80)
        tdLog.info("Test 3: Restoring the original key and verifying recovery")
        tdLog.info("=" * 80)

        cmd = [taosk_bin, '-c', cfg_path, '--update-svrkey', 'orderchk123']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=True)
        tdLog.info(f"taosk restore-svrkey output: {result.stdout}")

        tdDnodes.dnodes[0].starttaosd()
        assert self._wait_taosd_running(True, timeout=30), "taosd failed to restart after restoring the correct key"

        # The previous connection died along with the old taosd process; give
        # the RPC service a moment to come up before reconnecting.
        connected = False
        for _ in range(10):
            try:
                tdSql.connect()
                connected = True
                break
            except Exception as e:
                tdLog.info(f"waiting for taosd to accept connections: {e}")
                time.sleep(1)
        assert connected, "failed to reconnect to taosd after restoring the correct key"

        tdSql.query(f"SELECT * FROM {self.db}.t")
        assert tdSql.queryRows == 1

        tdLog.success(f"{__file__} successfully executed")
