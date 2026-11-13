# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3 or
# later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.

"""
Integration test for the custom symmetric encryption algorithm.

The custom cipher is shipped as an OpenSSL provider module (libcustomcrypt.so,
built only under -DBUILD_TEST in an enterprise build). taosd loads it through
the `encryptExtDir` server config. This test registers the algorithm, creates an
encrypted database that uses it, inserts rows, and verifies the data can be read
back. It also checks that the on-disk vnode files do NOT contain plaintext,
proving the custom algorithm is actually applied (and is not a no-op).

If the provider .so is not present (e.g. community build, or BUILD_TEST off),
the test is skipped rather than failing.
"""

from new_test_framework.utils import tdLog, tdSql, epath, tdDnodes
import os
import pytest


class TestCustomEncryptAlgorithm:
    # The custom cipher provider .so is built next to the taosd binary by the
    # enterprise BUILD_TEST build. cryptLoadProviders() in taosd only loads it at
    # *startup*, when encryptExtDir is already part of the server config. The
    # deploy fixture (new_test_framework) writes updatecfgDict into taos.cfg
    # before taosd starts, so encryptExtDir MUST be a class attribute resolved
    # at import time -- setting it from setup_class would be too late and the
    # provider would never load (leading to "cipher does not exist" at runtime).
    @staticmethod
    def _resolve_provider_so():
        """Locate libcustomcrypt.so.

        Probe the framework-resolved bin dir first, then common install and
        in-tree build layouts, so the path is correct both for installed runs
        and for in-tree builds.
        """
        candidates = []
        try:
            candidates.append(epath.binPath())
        except Exception:
            pass
        candidates += ["/usr/bin", "/usr/local/taos/bin", "/usr/local/bin"]
        # In-tree build layout: <repo-root>/debug/build/bin
        here = os.path.dirname(os.path.abspath(__file__))
        root = here
        for _ in range(10):
            candidates.append(os.path.join(root, "debug", "build", "bin"))
            if os.path.exists(os.path.join(root, "CMakeLists.txt")):
                break
            parent = os.path.dirname(root)
            if parent == root:
                break
            root = parent
        seen = set()
        for d in candidates:
            if not d or d in seen:
                continue
            seen.add(d)
            p = os.path.join(d, "libcustomcrypt.so")
            if os.path.exists(p):
                return p
        return None

    _PROVIDER_SO = _resolve_provider_so()

    # Per-class taosd configuration. encryptExtDir (when the .so is present) is
    # injected here so the deploy fixture can write it to taos.cfg before taosd
    # starts.
    if _PROVIDER_SO is not None:
        updatecfgDict = {'dDebugFlag': 131, 'encryptExtDir': _PROVIDER_SO}
    else:
        updatecfgDict = {'dDebugFlag': 131}

    # Key material required by the transparent-encryption path.
    encryptConfig = {
        "svrKey": "customsvr1",
        "dbKey": "customdb45",
        "dataKey": "customdat1",
        "generateConfig": True,
        "generateMeta": True,
        "generateData": True,
    }

    @classmethod
    def setup_class(cls):
        # If the provider .so was not built, skip rather than fail.
        if cls._PROVIDER_SO is None:
            pytest.skip(
                "custom cipher provider not built (need enterprise build with "
                "-DBUILD_TEST=true); searched next to taosd and common "
                "install/build layouts"
            )
        tdLog.info(f"using custom cipher provider: {cls._PROVIDER_SO}")

        cls.init(cls, replicaVar=1, checkColName="c1")
        cls.db = "custom_enc_db"

    def test_custom_algorithm(self):
        """Register a custom algorithm, create an encrypted DB, insert/read.

        1. Register the custom cipher algorithm with CREATE ENCRYPT_ALGR
        2. Create an encrypted database that uses the custom algorithm
        3. Insert rows and verify they can be read back after decryption
        4. Verify the on-disk vnode files do NOT contain plaintext, proving
           the custom algorithm is actually applied (not a no-op); the test
           is skipped when the provider .so is unavailable

        Since: v3.4.4.0

        Labels: common,ci,integration,functional,security
        """
        tdSql.execute(
            "CREATE ENCRYPT_ALGR 'TD-CUSTOM-TEST' "
            "ALGR_NAME 'customtest' "
            "DESC 'customtest' "
            "ALGR_TYPE 'Symmetric_Ciphers_CBC_mode' "
            "OSSL_ALGR_NAME 'TD-CUSTOM-TEST'"
        )
        tdSql.execute(
            f"CREATE DATABASE IF NOT EXISTS {self.db} ENCRYPT_ALGORITHM 'TD-CUSTOM-TEST'"
        )
        tdSql.execute(f"USE {self.db}")
        tdSql.execute(
            "CREATE TABLE meters (ts TIMESTAMP, voltage INT, "
            "current FLOAT, phase FLOAT, note BINARY(64))"
        )
        tdSql.execute(
            "INSERT INTO meters VALUES (now, 220, 10.5, 0.95, 'SecretData123')"
        )
        tdSql.execute(
            "INSERT INTO meters VALUES (now+1s, 221, 10.6, 0.96, 'SecretData456')"
        )
        tdSql.execute(
            "INSERT INTO meters VALUES (now+2s, 222, 10.7, 0.97, 'SecretData789')"
        )

        # Verify the rows are readable after transparent decryption.
        tdSql.query("SELECT * FROM meters")
        tdSql.checkRows(3)

        tdSql.query("SELECT note FROM meters ORDER BY ts")
        tdSql.checkData(0, 0, "SecretData123")
        tdSql.checkData(1, 0, "SecretData456")
        tdSql.checkData(2, 0, "SecretData789")

        # Verify on-disk data is NOT plaintext -> the custom algorithm really
        # applied. (If it were a no-op, the secret strings would appear verbatim.)
        data_dir = tdDnodes.dnodes[0].dataDir
        if isinstance(data_dir, list):
            data_dir = data_dir[0]
        vnode_dir = os.path.join(data_dir, "vnode")
        found_plaintext = False
        if os.path.exists(vnode_dir):
            for root, _dirs, files in os.walk(vnode_dir):
                for f in files:
                    fp = os.path.join(root, f)
                    try:
                        with open(fp, 'rb') as fh:
                            blob = fh.read()
                    except Exception:
                        continue
                    if (b'SecretData123' in blob or b'SecretData456' in blob
                            or b'SecretData789' in blob):
                        found_plaintext = True
                        break
                if found_plaintext:
                    break
        assert not found_plaintext, (
            "vnode data files contain plaintext -> custom encryption not applied"
        )

        tdLog.success(f"{__file__} successfully executed")
