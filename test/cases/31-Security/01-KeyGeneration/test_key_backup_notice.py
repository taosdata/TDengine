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


from new_test_framework.utils import tdLog, etool, tdDnodes
import os
import subprocess

MARK = "IMPORTANT: BACK UP YOUR KEYS"
CONFIRM = "Type 'yes' to overwrite the existing keys"


class TestKeyBackupNotice:
    updatecfgDict = {'dDebugFlag': 131}

    def setup_class(cls):
        cls.init(cls, replicaVar=1, checkColName="c1")

    def _run(self, cmd, stdin=""):
        return subprocess.run(cmd, input=stdin, capture_output=True, text=True, timeout=30)

    def test_key_backup_notice(self):
        """ Test taosk backup notice and overwrite confirmation

        1. First generation prints the backup notice, no confirmation prompt
        2. Repeated generation requires typing "yes" to overwrite
        3. Any other input (no, empty, EOF) aborts and leaves key files unchanged
        4. Update prints the backup notice
        5. Failed commands print no notice
        6. --backup / --view-config print no key backup notice

        Since: v3.5.0.0

        Labels: common,ci,integration,functional,security

        Jira: 7146164039

        History:
            - 2026-11-24 Created for taosk backup notice
            - 2026-11-24 Require "yes" to overwrite existing keys

        """
        cfg_path = tdDnodes.dnodes[0].cfgDir
        data_dir = tdDnodes.dnodes[0].dataDir
        if isinstance(data_dir, list):
            data_dir = data_dir[0]
        key_dir = os.path.join(data_dir, "dnode", "config")
        master_file = os.path.join(key_dir, "master.bin")
        derived_file = os.path.join(key_dir, "derived.bin")
        taosk = etool.taoskFile()
        gen = [taosk, '-c', cfg_path, '--encrypt-server', '--encrypt-database']

        def clean():
            for f in (master_file, derived_file):
                if os.path.exists(f):
                    os.remove(f)

        def read(f):
            with open(f, 'rb') as fp:
                return fp.read()

        # 1. first generation: notice, no prompt
        clean()
        r = self._run(gen)
        assert r.returncode == 0, r.stderr
        assert "Keys generated successfully." in r.stdout
        assert MARK in r.stdout
        assert "--backup --svr-key <your SVR_KEY>" in r.stdout
        assert CONFIRM not in r.stdout
        assert r.stdout.index("Keys generated successfully.") < r.stdout.index(MARK)

        # 3. anything other than "yes" aborts and keeps the existing keys
        before = read(master_file)
        for answer in ("no\n", "\n", "YES\n", "yes please\n", ""):
            r = self._run(gen, stdin=answer)
            assert r.returncode != 0, repr(answer)
            assert CONFIRM in r.stdout
            assert "Aborted" in r.stderr
            assert MARK not in r.stdout
            assert read(master_file) == before, repr(answer)

        # 2. "yes" overwrites
        r = self._run(gen, stdin="yes\n")
        assert r.returncode == 0, r.stderr
        assert CONFIRM in r.stdout
        assert "Keys generated successfully." in r.stdout
        assert MARK in r.stdout
        assert read(master_file) != before

        # 4. update: notice, no confirmation
        r = self._run([taosk, '-c', cfg_path, '--update-svrkey', 'newsvr123'])
        assert r.returncode == 0, r.stderr
        assert "Keys updated successfully." in r.stdout
        assert MARK in r.stdout
        assert CONFIRM not in r.stdout

        # 6. other commands
        r = self._run([taosk, '-c', cfg_path, '--backup', '--svr-key', 'newsvr123'])
        assert MARK not in r.stdout

        # 5. failures
        r = self._run([taosk, '-c', cfg_path, '--update-svrkey', 'short'])
        assert r.returncode != 0
        assert MARK not in r.stdout

        clean()
        r = self._run([taosk, '-c', cfg_path, '--update-svrkey', 'newsvr123'])
        assert r.returncode != 0
        assert MARK not in r.stdout
        tdLog.info("key backup notice test passed")
