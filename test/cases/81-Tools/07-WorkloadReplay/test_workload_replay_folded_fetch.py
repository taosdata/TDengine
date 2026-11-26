# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL-3.0-or-later") as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""Workload replay coverage for folded fetch sessions.

The recorder folds every synchronous fetch call on one result into a single
BEGIN/RETURN pair and records the call count on the RETURN. Replay executes the
BEGIN, so it has to read the count from the terminal event; reading it from the
BEGIN yields one call and makes a multi-block result replay as a single row.

This file is self-contained: it records with the CLI, then replays, so it needs
only a reachable cluster plus a BUILD_TEST tree that produced taosreplay.
"""

import os
import shutil
import subprocess
import sys
import tempfile
import time

import pytest

from new_test_framework.utils import tdCom, tdLog, tdSql

RECORD_CFG = "workloadRecord"
DIR_CFG = "workloadRecordDir"
BUF_CFG = "workloadRecordBufferSize"

EXIT_OK = 0
EXIT_PARTIAL = 4


class TestWorkloadReplayFoldedFetch:
    """Fold one result's fetch calls into one recorded pair, then replay it."""

    db = "wlr_folded_fetch"

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.buildPath = tdCom.getBuildPath()
        cls.binPath = os.path.join(cls.buildPath, "build", "bin")
        cls.taos = os.path.join(cls.binPath, "taos")
        cls.replay = os.path.join(cls.binPath, "taosreplay")
        cls.tmpDirs = []
        cls.host = "localhost"
        cls.port = 6030
        cls.user = os.environ.get("TAOS_TEST_USER", "root")
        cls.password = os.environ.get("TAOS_TEST_PASSWORD")

        sharedCfg = os.path.join(tdCom.getClientCfgPath(), "taos.cfg")
        if not os.path.isfile(sharedCfg):
            pytest.fail(f"shared client configuration is not a regular file: {sharedCfg}")
        cls.clientCfgDir = tempfile.mkdtemp(prefix="wlr-folded-cfg-")
        cls.tmpDirs.append(cls.clientCfgDir)
        cls.clientCfg = os.path.join(cls.clientCfgDir, "taos.cfg")
        shutil.copy2(sharedCfg, cls.clientCfg)
        with open(cls.clientCfg, "rb") as f:
            cls.originalClientCfg = f.read()

    @classmethod
    def teardown_class(cls):
        cls._restore_cfg()
        cls._drop()
        for d in getattr(cls, "tmpDirs", []):
            shutil.rmtree(d, ignore_errors=True)

    # ------------------------------------------------------------------ helpers

    @classmethod
    def _restore_cfg(cls):
        if not getattr(cls, "originalClientCfg", None):
            return
        with open(cls.clientCfg, "wb") as f:
            f.write(cls.originalClientCfg)

    @classmethod
    def _drop(cls):
        try:
            tdSql.execute(f"drop database if exists {cls.db};")
        except Exception as e:  # pragma: no cover
            tdLog.debug(f"drop database failed: {e}")

    def _new_dir(self):
        d = tempfile.mkdtemp(prefix="wlr-folded-")
        self.tmpDirs.append(d)
        return d

    def _write_cfg(self, **kv):
        """Rewrite the client config with the recorder keys appended."""
        managed = {RECORD_CFG, DIR_CFG, BUF_CFG}
        with open(self.clientCfg, "r", encoding="utf-8", errors="ignore") as f:
            lines = [line for line in f.readlines()
                     if line.split(" ", 1)[0].strip() not in managed]
        with open(self.clientCfg, "wb") as f:
            f.writelines(line.encode("utf-8") for line in lines)
        with open(self.clientCfg, "a", encoding="utf-8") as f:
            f.write("\n")
            for k, v in kv.items():
                f.write(f"{k} {v}\n")

    def _run_client(self, sql, timeout=300):
        fd, sql_path = tempfile.mkstemp(prefix="wlr-folded-sql-", suffix=".sql",
                                        dir=self.clientCfgDir, text=True)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as sql_file:
                sql_file.write(sql)
            cmd = [self.taos, "-c", self.clientCfgDir, "-f", sql_path]
            try:
                p = subprocess.run(cmd, capture_output=True, timeout=timeout)
            except subprocess.TimeoutExpired as exc:
                pytest.fail(f"recording client timed out after {timeout}s: {exc}")
        finally:
            try:
                os.unlink(sql_path)
            except FileNotFoundError:
                pass
        return (p.returncode, p.stdout.decode("utf-8", errors="ignore"),
                p.stderr.decode("utf-8", errors="ignore"))

    def _record(self, sql):
        d = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: d, BUF_CFG: 256})
        try:
            rc, out, err = self._run_client(sql)
        finally:
            self._restore_cfg()
        assert rc == 0, f"recording client failed: {out}{err}"
        for _ in range(100):
            if any(f.endswith(".wlr") for f in os.listdir(d)):
                return d
            time.sleep(0.1)
        pytest.fail(f"recording produced no segment after client exit: {out}{err}")

    def _replay_to_target(self, session, *extra, timeout=900):
        cmd = [self.replay, "replay", "--input", session,
               "--target-host", self.host, "--target-port", str(self.port), *extra]
        env = os.environ.copy()
        env["TAOSREPLAY_USER_PASSWORD"] = f"{self.user}={self.password}"
        try:
            p = subprocess.run(cmd, capture_output=True, timeout=timeout, env=env)
        except subprocess.TimeoutExpired as exc:
            pytest.fail(f"taosreplay timed out after {timeout}s: {exc}")
        return (p.returncode,
                p.stdout.decode("utf-8", errors="ignore") + p.stderr.decode("utf-8", errors="ignore"))

    def _require(self):
        if not self.password:
            pytest.skip("TAOS_TEST_PASSWORD is not set")
        if not os.path.isfile(self.taos):
            pytest.fail(f"taos client not found at expected build path: {self.taos}")
        if not os.path.isfile(self.replay):
            pytest.skip("taosreplay not built (dev tool, excluded from product package)")

    # --------------------------------------------------------------------- test

    def test_replay_folded_fetch(self):
        """Tools: replay a folded multi-row fetch

        1. Record a query whose result needs several fetch calls, so the recorder
           folds them into one BEGIN/RETURN pair carrying the call count
        2. Replay it: the whole fetch loop runs, not a single row
        3. Replay exits 0 and the target holds every recorded row

        A pair replayed as one call fetches one row, frees the result and reports
        a row-count mismatch, which turns the replay exit code into 4.

        Catalog:
            - Tools

        Since: v3.5.0

        Labels: common,replay,functional

        Jira: None

        History:
            - 2026-11-16 Alex Duan created (review follow-up)
        """
        self._require()
        # Several result blocks are required: a single-block result is fetched once,
        # so the folded count is 1 and the defect stays invisible.
        rows = 20000
        per_insert = 4000
        chunks = []
        for start in range(0, rows, per_insert):
            values = ",".join(f"({start + i}, {start + i})" for i in range(per_insert))
            chunks.append(f"insert into t1 values {values};")
        workload = (
            f"drop database if exists {self.db};"
            f"create database {self.db};"
            f"use {self.db};"
            f"create table t1 (ts timestamp, c1 int);"
            + "".join(chunks)
            + "select * from t1;"
        )
        d = self._record(workload)

        rc, out = self._replay_to_target(d)
        assert rc == EXIT_OK, (
            f"folded fetch replay should exit {EXIT_OK}, got {rc} "
            f"(a row-count mismatch reports {EXIT_PARTIAL}): {out}"
        )

        tdSql.query(f"select count(*) from {self.db}.t1;")
        assert tdSql.queryResult[0][0] == rows, "replayed row count mismatch"
        self._drop()
        tdLog.success(f"{__file__} folded fetch replay passed")
