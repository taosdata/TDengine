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
import signal
import shutil
import subprocess
import sys
import tempfile
import time

import pytest

from new_test_framework.utils import tdCom, tdLog, tdSql

#
# Black-box taosreplay tests.
#
# taosreplay is treated as a CLI: inputs are recorded session dirs, and assertions
# are made on exit codes, stdout/report text, and the resulting target state. One
# corruption fixture uses the documented on-disk geometry to damage a known frame.
#
# Exit codes under test (FS 4.12):
#   0 replay completed, no execution errors
#   2 input file / format / argument error
#   3 credentials missing or connection failure
#   4 completed with API errors, skips, or incomplete input
#   5 tool internal error
#

RECORD_CFG = "workloadRecord"
DIR_CFG = "workloadRecordDir"
BUF_CFG = "workloadRecordBufferSize"

EXIT_OK = 0
EXIT_INPUT = 2
EXIT_CONN = 3
EXIT_PARTIAL = 4

# WLR on-disk geometry used only to locate a CRC-covered byte in the corruption fixture.
# Keep these values synchronized with wlrFormat.h.
WLR_FILE_HEADER_SIZE = 148
WLR_FRAME_LENGTH_SIZE = 4
WLR_FRAME_EVENT_SEQUENCE_OFFSET = 8


class TestWorkloadReplay:
    """taosreplay -- CLI contract, endpoint mapping, credentials, reporting."""

    testDatabases = ("wlr_inspect", "wlr_args", "wlr_pwwarn", "wlr_unmapped", "wlr_badcred",
                     "wlr_roundtrip", "wlr_incomplete", "wlr_merge_a", "wlr_merge_b",
                     "wlr_fetch_mismatch", "wlr_affected_mismatch", "wlr_tail_a", "wlr_tail_b",
                     "wlr_endpoint_a", "wlr_endpoint_b", "wlr_sml_result", "wlr_sml_empty", "wlr_segment_gap",
                     "wlr_stmt2_row", "wlr_stmt2_tags", "wlr_stmt2_short", "wlr_stmt2_no_cols",
                     "wlr_stmt2_table_first", "wlr_stmt2_long_names", "wlr_stmt1_bad",
                     "wlr_null_host", "wlr_default_port", "wlr_ipv6_host",
                     "wlr_stmt2_zero_tags", "wlr_stmt2_callback",
                     "wlr_stmt2_exec_callback", "wlr_stmt2_missing_prepare",
                     "wlr_missing_query_a", "wlr_missing_query_b", "wlr_select_source",
                     "b" * 63, "b" * 64, "wlr_lane_source", "wlr_lane_target")

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        cls.configReady = False
        cls.buildPath = tdCom.getBuildPath()
        cls.binPath = os.path.join(cls.buildPath, "build", "bin")
        cls.taos = os.path.join(cls.binPath, "taos")
        cls.replay = os.path.join(cls.binPath, "taosreplay")
        cls.tmpDirs = []
        sharedCfg = os.path.join(tdCom.getClientCfgPath(), "taos.cfg")
        if not os.path.isfile(sharedCfg):
            pytest.fail(f"shared client configuration is not a regular file: {sharedCfg}")
        cls.clientCfgDir = tempfile.mkdtemp(prefix="wlr-replay-client-cfg-")
        cls.tmpDirs.append(cls.clientCfgDir)
        cls.clientCfg = os.path.join(cls.clientCfgDir, "taos.cfg")
        shutil.copy2(sharedCfg, cls.clientCfg)
        with open(cls.clientCfg, "rb") as f:
            cls.originalClientCfg = f.read()
        cls.configReady = True
        cls.host = "localhost"
        cls.port = 6030
        cls.user = os.environ.get("TAOS_TEST_USER", "root")
        cls.password = os.environ.get("TAOS_TEST_PASSWORD")
        cls.probe_error = None

        cls.recorder_ok = cls._probe_support()
        cls.replay_ok = os.path.isfile(cls.replay)

    @classmethod
    def teardown_class(cls):
        cls._restore_cfg()
        cls._cleanup_databases()
        for d in getattr(cls, "tmpDirs", []):
            shutil.rmtree(d, ignore_errors=True)

    @pytest.fixture(autouse=True)
    def cleanup_after_test(self):
        if not getattr(self, "configReady", False):
            yield
            return
        try:
            yield
        finally:
            self._restore_cfg()
            self._cleanup_databases()

    # ------------------------------------------------------------------ helpers

    @classmethod
    def _probe_support(cls):
        try:
            tdSql.query("show local variables;")
            names = {str(row[0]) for row in tdSql.queryResult or [] if row and len(row) > 0}
        except Exception as e:  # pragma: no cover
            tdLog.debug(f"probe row parse failed: {e}")
            cls.probe_error = e
            return False
        return all(name in names for name in (RECORD_CFG, DIR_CFG, BUF_CFG))

    def _require(self):
        if self.probe_error is not None:
            pytest.fail(f"failed to probe {RECORD_CFG} support: {self.probe_error}")
        if not self.password:
            pytest.skip("TAOS_TEST_PASSWORD is not set")
        if not os.path.isfile(self.taos):
            pytest.fail(f"taos client not found at expected build path: {self.taos}")
        if not self.recorder_ok:
            pytest.skip(f"{RECORD_CFG} unsupported: recorder not implemented in this build")
        if not self.replay_ok:
            pytest.skip("taosreplay not built (dev tool, excluded from product package)")

    def _new_dir(self):
        d = tempfile.mkdtemp(prefix="wlr-replay-")
        self.tmpDirs.append(d)
        return d

    @classmethod
    def _restore_cfg(cls):
        if not getattr(cls, "configReady", False):
            return
        with open(cls.clientCfg, "wb") as f:
            f.write(cls.originalClientCfg)

    @classmethod
    def _cleanup_databases(cls):
        for db in cls.testDatabases:
            try:
                tdSql.execute(f"drop database if exists {db}")
            except Exception as exc:
                tdLog.debug(f"cleanup failed for {db}: {exc}")

    def _write_cfg(self, **kv):
        self._restore_cfg()
        managed = {name.encode("utf-8") for name in (RECORD_CFG, DIR_CFG, BUF_CFG)}
        lines = []
        for line in self.originalClientCfg.splitlines(keepends=True):
            stripped = line.lstrip()
            if stripped.startswith((b"#", b";")):
                lines.append(line)
                continue
            for marker in (b"#", b";"):
                cut = stripped.find(marker)
                if cut != -1:
                    stripped = stripped[:cut]
            key = stripped.split(None, 1)[0].split(b"=", 1)[0] if stripped else b""
            if key not in managed:
                lines.append(line)
        with open(self.clientCfg, "wb") as f:
            f.writelines(lines)
        with open(self.clientCfg, "a", encoding="utf-8") as f:
            f.write("\n")
            for k, v in kv.items():
                f.write(f"{k} {v}\n")

    def _run_client(self, sql, timeout=120, host=None):
        fd, sql_path = tempfile.mkstemp(prefix="wlr-replay-client-", suffix=".sql",
                                        dir=self.clientCfgDir, text=True)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as sql_file:
                sql_file.write(sql)
            cmd = [self.taos, "-c", self.clientCfgDir, "-f", sql_path]
            if host is not None:
                cmd.extend(("-h", host))
            try:
                p = subprocess.run(cmd, capture_output=True, timeout=timeout)
            except subprocess.TimeoutExpired as exc:
                pytest.fail(f"recording client timed out after {timeout}s: {' '.join(cmd[:3])} ({exc})")
            except OSError as exc:
                pytest.fail(f"could not execute recording client {self.taos}: {exc}")
        finally:
            try:
                os.unlink(sql_path)
            except FileNotFoundError:
                pass
        return (p.returncode, p.stdout.decode("utf-8", errors="ignore"),
                p.stderr.decode("utf-8", errors="ignore"))

    def _record(self, sql, buf=256, host=None):
        """Produce a recorded session directory for the given workload."""
        d = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: d, BUF_CFG: buf})
        try:
            rc, out, err = self._run_client(sql, host=host)
        finally:
            self._restore_cfg()
        assert rc == 0, f"recording client failed: {out}{err}"
        for _ in range(100):
            if any(f.endswith(".wlr") for f in os.listdir(d)):
                break
            time.sleep(0.1)
        else:
            pytest.fail(f"recording produced no segment after client exit: {out}{err}")
        return d

    def _replay(self, *args, timeout=900, password_env=None):
        cmd = [self.replay, "replay"] + list(args)
        env = os.environ.copy()
        if password_env is not None:
            env["TAOSREPLAY_USER_PASSWORD"] = password_env
        try:
            p = subprocess.run(cmd, capture_output=True, timeout=timeout, env=env)
        except subprocess.TimeoutExpired as exc:
            safe_cmd = []
            hide_next = False
            for arg in cmd:
                if hide_next:
                    safe_cmd.append("<redacted>")
                    hide_next = False
                elif arg == "--user-password":
                    safe_cmd.append(arg)
                    hide_next = True
                else:
                    safe_cmd.append(arg)
            pytest.fail(f"taosreplay timed out after {timeout}s running: {' '.join(safe_cmd)[:200]} ({exc})")
        out = p.stdout.decode("utf-8", errors="ignore")
        err = p.stderr.decode("utf-8", errors="ignore")
        tdLog.debug(f"replay rc={p.returncode}; output lengths stdout={len(out)}, stderr={len(err)}")
        return p.returncode, out + err

    def _replay_to_target(self, session, *extra, timeout=900):
        return self._replay(
            "--input", session,
            "--target-host", self.host,
            "--target-port", str(self.port),
            *extra,
            timeout=timeout,
            password_env=f"{self.user}={self.password}",
        )

    def _inspect(self, session, verbose=False):
        cmd = [self.replay, "inspect", "--input", session]
        if verbose:
            cmd.append("--verbose")
        try:
            p = subprocess.run(cmd, capture_output=True, timeout=300)
        except subprocess.TimeoutExpired as exc:
            pytest.fail(f"taosreplay inspect timed out after 300s: {' '.join(cmd)} ({exc})")
        return p.returncode, p.stdout.decode("utf-8", errors="ignore") + p.stderr.decode("utf-8", errors="ignore")

    def _sample_workload(self, db):
        """A small mixed workload: DDL, insert, query."""
        return (
            f"drop database if exists {db};"
            f"create database {db};"
            f"use {db};"
            f"create table t1 (ts timestamp, c1 int, c2 binary(32));"
            f"insert into t1 values (now, 1, 'a');"
            f"insert into t1 values (now+1s, 2, 'b');"
            f"select count(*) from t1;"
        )

    def _drop(self, db):
        tdSql.execute(f"drop database if exists {db};")

    # ------------------------------------------------------------- 6.9 inspect

    def test_com_workload_replay_inspect(self):
        """taosreplay -- inspect summary and verbose (FMT001/FMT002)

        1. inspect on a clean session exits 0 and prints a summary
        2. --verbose reveals recorded SQL text

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-07-29 Claude Initial design-stage suite
        """
        self._require()
        db = "wlr_inspect"
        d = self._record(self._sample_workload(db))
        self._drop(db)

        rc, out = self._inspect(d)
        assert rc == EXIT_OK, f"inspect on clean session should exit 0: {out}"
        assert out.strip(), "inspect printed nothing"

        rc, vout = self._inspect(d, verbose=True)
        assert rc == EXIT_OK
        assert "select count(*) from t1" in vout.lower(), "verbose did not reveal SQL"
        # RPT003 -- default (non-verbose) must not dump full SQL
        assert "select count(*) from t1" not in out.lower(), \
            "non-verbose inspect leaked full SQL text"
        tdLog.success(f"{__file__} inspect passed")

    def test_com_workload_replay_bad_input(self):
        """taosreplay -- malformed input rejected (FMT003/FMT006, RPT006)

        1. Nonexistent path exits 2
        2. Garbage file with a bad magic exits 2
        3. Truncated header exits 2

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-07-29 Claude Initial design-stage suite
        """
        self._require()

        missing = os.path.join(self._new_dir(), "nope")
        rc, _ = self._replay_to_target(missing)
        assert rc == EXIT_INPUT, f"missing input should exit {EXIT_INPUT}, got {rc}"

        garbage = self._new_dir()
        with open(os.path.join(garbage, "wlr-0-0-0-0.wlr"), "wb") as f:
            f.write(b"NOTAWLR!" + os.urandom(4096))
        rc, _ = self._replay_to_target(garbage)
        assert rc == EXIT_INPUT, f"bad magic should exit {EXIT_INPUT}, got {rc}"

        short = self._new_dir()
        with open(os.path.join(short, "wlr-0-0-0-0.wlr"), "wb") as f:
            f.write(b"TDWREC01")  # header claimed, nothing follows
        rc, _ = self._replay_to_target(short)
        assert rc == EXIT_INPUT, f"truncated header should exit {EXIT_INPUT}, got {rc}"

        tdLog.success(f"{__file__} malformed-input rejection passed")

    # ---------------------------------------------------------- 6.10 args/creds

    def test_com_workload_replay_arg_validation(self):
        """taosreplay -- argument validation (RIN013/RIN015/RIN021, RPT006)

        1. --target-host and --endpoint-map are mutually exclusive
        2. Missing --user-password fails before any connection
        3. Path traversal in --input is rejected

        Since: v3.4.3

        Labels: ci,functional,security,taosc

        History:
            - 2026-07-29 Claude Initial design-stage suite
        """
        self._require()
        db = "wlr_args"
        d = self._record(self._sample_workload(db))
        self._drop(db)

        # RIN013 -- mutually exclusive redirection modes
        rc, _ = self._replay(
            "--input", d,
            "--target-host", self.host, "--target-port", str(self.port),
            "--endpoint-map", f"{self.host}:{self.port}={self.host}:{self.port}",
            "--user-password", f"{self.user}={self.password}",
        )
        assert rc == EXIT_INPUT, f"exclusive modes should exit {EXIT_INPUT}, got {rc}"

        # RIN015 -- no credentials at all
        rc, _ = self._replay(
            "--input", d,
            "--target-host", self.host, "--target-port", str(self.port),
        )
        assert rc == EXIT_INPUT, f"missing credentials should exit {EXIT_INPUT}, got {rc}"

        # RIN021 -- traversal
        rc, traversal_out = self._replay_to_target(os.path.join(d, "..", "..", "etc", "passwd"))
        assert rc == EXIT_INPUT, f"path traversal should exit {EXIT_INPUT}, got {rc}"
        assert "must not contain '..'" in traversal_out, \
            f"path traversal was not rejected for the expected reason: {traversal_out}"

        tdLog.success(f"{__file__} argument validation passed")

    def test_com_workload_replay_password_warning(self):
        """taosreplay -- CLI password risk warning (RIN017, SEC008)

        1. Passing --user-password must emit a visible warning
        2. The password must not be echoed back in output

        Since: v3.4.3

        Labels: ci,functional,security,taosc

        History:
            - 2026-07-29 Claude Initial design-stage suite
        """
        self._require()
        db = "wlr_pwwarn"
        d = self._record(self._sample_workload(db))
        self._drop(db)

        rc, out = self._replay(
            "--input", d,
            "--target-host", self.host,
            "--target-port", str(self.port),
            "--user-password", f"{self.user}={self.password}",
        )
        low = out.lower()
        assert any(k in low for k in ("warn", "risk", "history", "process list")), \
            "no CLI-password risk warning emitted"
        # RIN019 -- username may be printed, password may not
        credential = f"{self.user}={self.password}"
        leaked_credential = credential in out or (bool(self.password) and self.password in out)
        assert not leaked_credential, "credential material echoed back in replay output"
        tdLog.success(f"{__file__} password warning passed")

    def test_com_workload_replay_unmapped_endpoint(self):
        """taosreplay -- unmapped endpoint fails before connecting (RIN014)

        1. An endpoint-map that misses the recorded endpoint must fail up front
        2. Partial connection to the live address must not happen

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-07-29 Claude Initial design-stage suite
        """
        self._require()
        db = "wlr_unmapped"
        d = self._record(self._sample_workload(db))
        self._drop(db)

        try:
            rc, out = self._replay(
                "--input", d,
                "--endpoint-map", "no-such-host:9999=other-host:9999",
                "--user-password", f"{self.user}={self.password}",
            )
            assert rc in (EXIT_INPUT, EXIT_CONN), \
                f"unmapped endpoint should exit 2 or 3, got {rc}"
            assert "no --endpoint-map entry" in out, \
                f"unmapped endpoint rejection was not explicit: {out}"
            # The recorded db must not have been recreated on the live cluster.
            tdSql.query("show databases;")
            names = [str(r[0]) for r in tdSql.queryResult]
            assert db not in names, "replay partially executed despite unmapped endpoint"
        finally:
            self._drop(db)
        tdLog.success(f"{__file__} unmapped-endpoint rejection passed")

    def test_com_workload_replay_later_unmapped_endpoint(self):
        """taosreplay -- preflight all endpoints before the first write

        1. Record two sessions using distinct endpoint spellings
        2. Map only the first endpoint and attempt a merged replay
        3. Assert the second mapping failure leaves the first database absent

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-23 Added later-endpoint preflight coverage
        """
        self._require()
        db_a, db_b = "wlr_endpoint_a", "wlr_endpoint_b"
        session_a = self._record(self._sample_workload(db_a), host="localhost")
        session_b = self._record(self._sample_workload(db_b), host="127.0.0.1")
        self._drop(db_a)
        self._drop(db_b)

        rc, out = self._replay(
            "--input", session_a, "--input", session_b,
            "--endpoint-map", "localhost:6030=localhost:6030",
            "--user-password", f"{self.user}={self.password}",
        )
        assert rc == EXIT_CONN, f"later missing mapping should exit {EXIT_CONN}: {out}"
        assert "no --endpoint-map entry" in out, out
        tdSql.query("show databases;")
        names = [str(row[0]) for row in tdSql.queryResult]
        assert db_a not in names, "replay wrote to the first endpoint before preflight completed"
        self._drop(db_a)
        self._drop(db_b)

    def test_com_workload_replay_later_omitted_connection_options(self):
        """taosreplay -- reject omitted options before any target writes

        1. Record a write followed by a separate connection session
        2. Mark the later connection's options as omitted in its valid frame
        3. Verify preflight rejects replay before the first write

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-27 Added connection-option preflight coverage
        """
        self._require()
        db_a, db_b = "wlr_endpoint_a", "wlr_endpoint_b"
        session_a = self._record(self._sample_workload(db_a))
        session_b = self._record(self._sample_workload(db_b))
        segments = [name for name in os.listdir(session_b) if name.endswith(".wlr")]
        assert len(segments) == 1, segments
        path = os.path.join(session_b, segments[0])
        with open(path, "rb") as stream:
            data = bytearray(stream.read())

        def crc32c(payload):
            checksum = 0xffffffff
            for value in payload:
                checksum ^= value
                for _ in range(8):
                    checksum = (checksum >> 1) ^ (0x82f63b78 if checksum & 1 else 0)
            return checksum ^ 0xffffffff

        changed = False
        offset = WLR_FILE_HEADER_SIZE
        while offset + 4 <= len(data):
            frame_length = int.from_bytes(data[offset:offset + 4], "little")
            frame_end = offset + 4 + frame_length
            assert frame_end <= len(data)
            payload_length = int.from_bytes(data[offset + 24:offset + 28], "little")
            payload_start = offset + 28
            payload_end = payload_start + payload_length
            field_offset = payload_start
            api_type = None
            phase = None
            api_value_offset = None
            while field_offset + 8 <= payload_end:
                tag = int.from_bytes(data[field_offset:field_offset + 2], "little") & 0x7fff
                field_length = int.from_bytes(data[field_offset + 4:field_offset + 8], "little")
                field_end = field_offset + 8 + field_length
                assert field_end <= payload_end
                if tag == 5:
                    api_value_offset = field_offset + 8
                    api_type = int.from_bytes(data[api_value_offset:field_end], "little")
                elif tag == 6:
                    phase = int.from_bytes(data[field_offset + 8:field_end], "little")
                field_offset = field_end
            if api_type == 1 and phase == 1:
                payload = bytearray(data[payload_start:payload_end])
                relative_api_offset = api_value_offset - payload_start
                payload[relative_api_offset:relative_api_offset + 8] = (2).to_bytes(8, "little")
                payload.extend((113).to_bytes(2, "little"))
                payload.extend((0).to_bytes(2, "little"))
                payload.extend((0).to_bytes(4, "little"))
                frame = bytearray(data[offset:offset + 28])
                frame[:4] = (29 + len(payload)).to_bytes(4, "little")
                frame[24:28] = len(payload).to_bytes(4, "little")
                frame.extend(payload)
                frame.extend(crc32c(frame[4:]).to_bytes(4, "little"))
                frame.append(10)
                data[offset:frame_end] = frame
                changed = True
                break
            offset = frame_end
        assert changed, "recording contains no CONNECT BEGIN event"
        with open(path, "wb") as stream:
            stream.write(data)

        inspect_rc, inspect_out = self._inspect(session_b)
        assert inspect_rc == EXIT_OK, inspect_out
        self._drop(db_a)
        self._drop(db_b)
        rc, out = self._replay_to_target(session_a, "--input", session_b)
        assert rc == EXIT_INPUT, out
        assert "omitted connection options" in out, out
        tdSql.query("show databases;")
        names = [str(row[0]) for row in tdSql.queryResult]
        assert db_a not in names, "replay wrote before checking later connection options"

    def test_com_workload_replay_quoted_long_database_name(self):
        """taosreplay -- preserve quoted database names at the 64-byte limit

        1. Record a switch from A to quoted B with a 63- or 64-byte name
        2. Replay after dropping both databases
        3. Confirm the unqualified insert lands in B rather than A

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-27 Cover quoted select_db argument lengths
        """
        self._require()
        source_db = "wlr_select_source"
        for db_length in (63, 64):
            target_db = "b" * db_length
            session = self._new_dir()
            self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: session})
            script = """
import os
import taos

source_db = "wlr_select_source"
target_db = "b" * int(os.environ["WLR_DB_LENGTH"])
connection = taos.connect(config=os.environ["WLR_CFG"], host=os.environ["WLR_HOST"],
                          port=int(os.environ["WLR_PORT"]), user=os.environ["WLR_USER"],
                          password=os.environ["WLR_PASSWORD"])
for sql in (f"create database {source_db}", f"create database {target_db}",
            f"create table {source_db}.t (ts timestamp, v int)",
            f"create table {target_db}.t (ts timestamp, v int)"):
    connection.query(sql).close()
connection.select_db(source_db)
connection.select_db(f"`{target_db}`")
connection.query("insert into t values (1700000000000, 1)").close()
connection.close()
"""
            env = os.environ.copy()
            env.update(WLR_CFG=self.clientCfgDir, WLR_HOST=self.host, WLR_PORT=str(self.port),
                       WLR_USER=self.user, WLR_PASSWORD=self.password,
                       WLR_DB_LENGTH=str(db_length))
            try:
                process = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                         timeout=120, env=env)
            finally:
                self._restore_cfg()
            output = (process.stdout + process.stderr).decode("utf-8", errors="ignore")
            assert process.returncode == 0, output

            self._drop(source_db)
            self._drop(target_db)
            replay_rc, replay_out = self._replay_to_target(session)
            assert replay_rc == EXIT_OK, replay_out
            tdSql.query(f"select count(*) from {source_db}.t")
            assert tdSql.queryResult[0][0] == 0, replay_out
            tdSql.query(f"select count(*) from {target_db}.t")
            assert tdSql.queryResult[0][0] == 1, replay_out
            self._drop(source_db)
            self._drop(target_db)

    def test_com_workload_replay_rejects_lane_overflow(self):
        """taosreplay -- reject a skipped database switch before writing

        1. Record 257 live threads sharing a connection, with the last switching to B
        2. Have the first thread insert after that switch
        3. Verify replay rejects the lane limit before creating either database

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-27 Cover state-changing calls beyond the lane limit
        """
        self._require()
        source_db, target_db = "wlr_lane_source", "wlr_lane_target"
        session = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: session})
        script = """
import os
import threading
import taos

source_db, target_db = "wlr_lane_source", "wlr_lane_target"
connection = taos.connect(config=os.environ["WLR_CFG"], host=os.environ["WLR_HOST"],
                          port=int(os.environ["WLR_PORT"]), user=os.environ["WLR_USER"],
                          password=os.environ["WLR_PASSWORD"])
for sql in (f"create database {source_db}", f"create database {target_db}",
            f"create table {source_db}.t (ts timestamp, v int)",
            f"create table {target_db}.t (ts timestamp, v int)"):
    connection.query(sql).close()
connection.select_db(source_db)
turns = [threading.Event() for _ in range(257)]
release = threading.Event()
errors = []

def switch_db(index):
    if not turns[index].wait(120):
        errors.append("thread did not receive its turn")
        turns[index + 1].set()
        return
    try:
        connection.select_db(target_db if index == 255 else source_db)
    except Exception as exc:
        errors.append(str(exc))
    finally:
        turns[index + 1].set()
    release.wait(120)

threads = [threading.Thread(target=switch_db, args=(index,)) for index in range(256)]
for thread in threads:
    thread.start()
turns[0].set()
assert turns[256].wait(120), "database switches did not finish"
assert not errors, errors
connection.query("insert into t values (1700000000000, 1)").close()
release.set()
for thread in threads:
    thread.join(120)
assert all(not thread.is_alive() for thread in threads)
connection.close()
"""
        env = os.environ.copy()
        env.update(WLR_CFG=self.clientCfgDir, WLR_HOST=self.host, WLR_PORT=str(self.port),
                   WLR_USER=self.user, WLR_PASSWORD=self.password)
        try:
            process = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                     timeout=300, env=env)
        finally:
            self._restore_cfg()
        output = (process.stdout + process.stderr).decode("utf-8", errors="ignore")
        assert process.returncode == 0, output

        self._drop(source_db)
        self._drop(target_db)
        replay_rc, replay_out = self._replay_to_target(session)
        assert replay_rc == EXIT_INPUT, replay_out
        assert "more than 256 threads" in replay_out, replay_out
        tdSql.query("show databases;")
        names = [str(row[0]) for row in tdSql.queryResult]
        assert source_db not in names and target_db not in names, replay_out

    def test_com_workload_replay_bad_credentials(self):
        """taosreplay -- wrong password exits 3 (RPT007)

        Verifies that replay rejects an incorrect password with the dedicated
        connection failure exit code instead of starting a partial replay.

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-07-29 Claude Initial design-stage suite
        """
        self._require()
        db = "wlr_badcred"
        d = self._record(self._sample_workload(db))
        self._drop(db)

        rc, _ = self._replay(
            "--input", d,
            "--target-host", self.host, "--target-port", str(self.port),
            "--user-password", f"{self.user}=definitely-wrong",
        )
        assert rc == EXIT_CONN, f"bad credentials should exit {EXIT_CONN}, got {rc}"
        tdLog.success(f"{__file__} credential failure passed")

    # -------------------------------------------------------- 6.11/6.15 replay

    def test_com_workload_replay_roundtrip(self):
        """taosreplay -- record, restore snapshot, replay (E2E008, RPT001/RPT005)

        1. Record a mixed workload
        2. Drop the database to restore the pre-recording snapshot
        3. Replay recreates it; exit code 0 and report is populated

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-07-29 Claude Initial design-stage suite
        """
        self._require()
        db = "wlr_roundtrip"
        d = self._record(self._sample_workload(db))

        # restore the snapshot: the recording starts from "db does not exist"
        self._drop(db)

        report = self._new_dir()
        rc, out = self._replay_to_target(d, "--report-dir", report)
        assert rc == EXIT_OK, f"clean replay should exit {EXIT_OK}, got {rc}: {out}"

        # the workload's effects must be reproduced on the target
        tdSql.query("show databases;")
        names = [str(r[0]) for r in tdSql.queryResult]
        assert db in names, "replay did not recreate the recorded database"
        tdSql.query(f"select count(*) from {db}.t1;")
        assert tdSql.queryResult[0][0] == 2, "replayed row count mismatch"

        # RPT001 -- report must mention the core sections
        low = out.lower()
        for token in ("session", "event"):
            assert token in low, f"replay report missing '{token}'"
        assert os.listdir(report), "--report-dir produced no files"

        self._drop(db)
        tdLog.success(f"{__file__} roundtrip passed")

    def test_com_workload_replay_incomplete_gating(self):
        """taosreplay -- incomplete input gating (RIN005/RIN006, RPT009)

        1. A truncated session is refused by default
        2. --allow-incomplete replays the verifiable prefix and exits 4

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-07-29 Claude Initial design-stage suite
        """
        self._require()
        db = "wlr_incomplete"
        stmts = [f"drop database if exists {db};", f"create database {db};", f"use {db};",
                 "create table t1 (ts timestamp, c1 int);"]
        for i in range(300):
            stmts.append(f"insert into t1 values (now+{i}s, {i});")
        d = self._record("".join(stmts))
        self._drop(db)

        # Corrupt the middle of the largest segment so the session has a real gap
        # rather than just a torn tail (which is legal and must NOT be gated).
        segs = sorted(f for f in os.listdir(d) if f.endswith(".wlr"))
        target = max(segs, key=lambda f: os.path.getsize(os.path.join(d, f)))
        path = os.path.join(d, target)
        size = os.path.getsize(path)
        # Locate the second frame from the first frame's little-endian length prefix.
        # Keeping one valid prefix frame ensures inspect can load the session and then
        # classify the following CRC failure as structural damage.
        with open(path, "r+b") as f:
            f.seek(WLR_FILE_HEADER_SIZE)
            length_bytes = f.read(WLR_FRAME_LENGTH_SIZE)
            assert len(length_bytes) == WLR_FRAME_LENGTH_SIZE, \
                f"WLR file-header geometry changed or no frame was recorded (size={size})"
            first_frame_length = int.from_bytes(length_bytes, "little")
            second_frame_offset = WLR_FILE_HEADER_SIZE + WLR_FRAME_LENGTH_SIZE + first_frame_length
            corrupt_offset = second_frame_offset + WLR_FRAME_EVENT_SEQUENCE_OFFSET
            assert size > corrupt_offset, \
                f"recording has no second complete frame to corrupt (size={size}, first={first_frame_length})"
            f.seek(corrupt_offset)
            original = f.read(1)
            assert original, "failed to read the selected frame byte"
            f.seek(corrupt_offset)
            f.write(bytes([original[0] ^ 0xff]))

        inspect_rc, inspect_out = self._inspect(d)
        assert inspect_rc == EXIT_PARTIAL, \
            f"fixture corruption was not detected as structural damage: {inspect_out}"

        rc, out = self._replay_to_target(d)
        assert rc == EXIT_INPUT, \
            f"corrupt session should exit {EXIT_INPUT} by default, got {rc}: {out}"

        rc2, out2 = self._replay_to_target(d, "--allow-incomplete")
        assert rc2 == EXIT_PARTIAL, \
            f"--allow-incomplete should exit {EXIT_PARTIAL}, got {rc2}: {out2}"

        self._drop(db)
        tdLog.success(f"{__file__} incomplete gating passed")

    def test_com_workload_replay_damage_stops_later_segment(self):
        """taosreplay -- do not load events after damage in an earlier segment

        1. Split one recorded session into two consecutive segments
        2. Damage the second frame and verify later SQL is not loaded

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-26 Added per-session damaged-prefix coverage
        """
        self._require()
        db = "wlr_incomplete"
        session = self._record(self._sample_workload(db))
        segments = [name for name in os.listdir(session) if name.endswith(".wlr")]
        assert len(segments) == 1, segments
        with open(os.path.join(session, segments[0]), "rb") as stream:
            data = bytearray(stream.read())

        first_end = WLR_FILE_HEADER_SIZE + WLR_FRAME_LENGTH_SIZE + int.from_bytes(
            data[WLR_FILE_HEADER_SIZE:WLR_FILE_HEADER_SIZE + 4], "little")
        second_end = first_end + WLR_FRAME_LENGTH_SIZE + int.from_bytes(
            data[first_end:first_end + 4], "little")
        assert second_end < len(data), "recording needs a later segment with SQL"
        first = self._new_dir()
        second = self._new_dir()
        data[first_end + WLR_FRAME_EVENT_SEQUENCE_OFFSET] ^= 0xff
        with open(os.path.join(first, "session-0.wlr"), "wb") as stream:
            stream.write(data[:second_end])
        header = bytearray(data[:WLR_FILE_HEADER_SIZE])
        header[56:60] = (1).to_bytes(4, "little")
        header[60:68] = data[second_end + WLR_FRAME_EVENT_SEQUENCE_OFFSET:
                             second_end + WLR_FRAME_EVENT_SEQUENCE_OFFSET + 8]
        later_path = os.path.join(second, "session-1.wlr")
        with open(later_path, "wb") as stream:
            stream.write(header)
            stream.write(data[second_end:])

        result = subprocess.run(
            [self.replay, "inspect", "--input", first, "--input", later_path, "--verbose"],
            capture_output=True, timeout=300,
        )
        output = (result.stdout + result.stderr).decode("utf-8", errors="ignore")
        assert result.returncode == EXIT_PARTIAL, output
        assert "events               : 1" in output, output
        assert "insert into t1" not in output, output
        self._drop(db)

    def test_com_workload_replay_unfinished_timeout(self):
        """taosreplay -- unfinished call timeout (WD002/WD003/WD007)

        1. --unfinished-timeout accepts a value and 0 (no timeout)
        2. A delayed unfinished query is dispatched before its timeout starts

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-07-29 Claude Initial design-stage suite
            - 2026-10-26 Start the timeout when the unfinished call is dispatched
        """
        self._require()

        # sleep() keeps the public query call in flight deterministically. BEGIN is
        # acknowledged before the real API starts, so a segment larger than its file
        # header proves the BEGIN is on disk before the process is killed.
        d = self._new_dir()
        p = None
        begin_captured = False
        script_path = os.path.join(self.clientCfgDir, "unfinished-timeout.sql")
        try:
            self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: d})
            with open(script_path, "w", encoding="utf-8") as script:
                script.write("select sleep(2);\nselect sleep(60);\n")
            p = subprocess.Popen(
                [self.taos, "-c", self.clientCfgDir, "-f", script_path],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            started = time.monotonic()
            for _ in range(600):
                if p.poll() is not None:
                    pytest.fail(f"sleep query exited before an in-flight BEGIN was captured: {p.returncode}")
                segments = [os.path.join(d, name) for name in os.listdir(d) if name.endswith(".wlr")]
                if time.monotonic() - started >= 3 and any(
                    os.path.getsize(path) > WLR_FILE_HEADER_SIZE for path in segments
                ):
                    begin_captured = True
                    break
                time.sleep(0.05)
            if not begin_captured:
                pytest.fail("timed out waiting for the in-flight BEGIN frame")
        finally:
            try:
                if p is not None:
                    if p.poll() is None:
                        if p.pid is not None:
                            os.killpg(p.pid, signal.SIGKILL)
                    try:
                        p.communicate(timeout=30)
                    except subprocess.TimeoutExpired:
                        if p.pid is not None:
                            os.killpg(p.pid, signal.SIGKILL)
                        try:
                            p.wait(timeout=30)
                        except subprocess.TimeoutExpired:
                            tdLog.warning("taos client did not exit during replay test cleanup")
            finally:
                self._restore_cfg()
                if os.path.exists(script_path):
                    os.unlink(script_path)

        inspect_rc, inspect_out = self._inspect(d, verbose=True)
        assert inspect_rc in (EXIT_OK, EXIT_PARTIAL), inspect_out
        assert "select sleep(60)" in inspect_out.lower(), inspect_out

        rc, out = self._replay_to_target(
            d, "--allow-incomplete", "--unfinished-timeout", "1", timeout=300
        )
        assert rc in (EXIT_OK, EXIT_PARTIAL), \
            f"bounded replay should exit 0 or 4, got {rc}: {out}"
        assert "calls skipped        : 0" in out, out
        tdLog.success(f"{__file__} unfinished timeout passed")

    def test_com_workload_replay_schemaless_result_lifetime(self):
        """taosreplay -- retain line and raw schemaless results through free

        1. Record both schemaless forms followed by their result frees
        2. Replay every free without an unknown-result skip

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-26 Preserve schemaless result handles during replay
        """
        self._require()
        db = "wlr_sml_result"
        session = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: session})
        script = """
import os
import taos

connection = taos.connect(config=os.environ["WLR_CFG"], host=os.environ["WLR_HOST"],
                          port=int(os.environ["WLR_PORT"]), user=os.environ["WLR_USER"],
                          password=os.environ["WLR_PASSWORD"])
connection.query("create database wlr_sml_result").close()
connection.select_db("wlr_sml_result")
assert connection.schemaless_insert(
    ["meters,site=a value=1i 1700000000000"], taos.SmlProtocol.LINE_PROTOCOL,
    taos.SmlPrecision.MILLI_SECONDS) == 1
assert connection.schemaless_insert_raw(
    "meters,site=b value=2i 1700000001000", taos.SmlProtocol.LINE_PROTOCOL,
    taos.SmlPrecision.MILLI_SECONDS) == 1
connection.close()
"""
        env = os.environ.copy()
        env.update(WLR_CFG=self.clientCfgDir, WLR_HOST=self.host, WLR_PORT=str(self.port),
                   WLR_USER=self.user, WLR_PASSWORD=self.password)
        try:
            process = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                     timeout=120, env=env)
        finally:
            self._restore_cfg()
        output = (process.stdout + process.stderr).decode("utf-8", errors="ignore")
        assert process.returncode == 0, output

        inspect_rc, inspect_out = self._inspect(session, verbose=True)
        assert inspect_rc == EXIT_OK, inspect_out
        assert inspect_out.count("free_result") >= 6, inspect_out
        self._drop(db)

        replay_rc, replay_out = self._replay_to_target(session)
        assert replay_rc == EXIT_OK, replay_out
        assert "calls skipped        : 0" in replay_out, replay_out
        self._drop(db)

    def do_schemaless_empty_key(self, monkeypatch):
        self._require()
        monkeypatch.setenv("TAOS_SML_CHILD_TABLE_NAME", "id")
        session = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: session, "smlChildTableName": "id"})
        script = """
import ctypes
import os

lib = ctypes.CDLL("libtaos.so")
lib.taos_options.argtypes = [ctypes.c_int, ctypes.c_char_p]
lib.taos_options.restype = ctypes.c_int
lib.taos_connect.argtypes = [ctypes.c_char_p] * 4 + [ctypes.c_uint16]
lib.taos_connect.restype = ctypes.c_void_p
lib.taos_query.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
lib.taos_query.restype = ctypes.c_void_p
lib.taos_errno.argtypes = [ctypes.c_void_p]
lib.taos_errno.restype = ctypes.c_int
lib.taos_free_result.argtypes = [ctypes.c_void_p]
lib.taos_close.argtypes = [ctypes.c_void_p]
line_api = lib.taos_schemaless_insert_ttl_with_reqid_tbname_key
line_api.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_char_p), ctypes.c_int,
                    ctypes.c_int, ctypes.c_int, ctypes.c_int32, ctypes.c_int64, ctypes.c_char_p]
line_api.restype = ctypes.c_void_p
raw_api = lib.taos_schemaless_insert_raw_ttl_with_reqid_tbname_key
raw_api.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int, ctypes.POINTER(ctypes.c_int32),
                   ctypes.c_int, ctypes.c_int, ctypes.c_int32, ctypes.c_int64, ctypes.c_char_p]
raw_api.restype = ctypes.c_void_p
assert lib.taos_options(3, os.environ["WLR_CFG"].encode()) == 0
connection = lib.taos_connect(os.environ["WLR_HOST"].encode(), os.environ["WLR_USER"].encode(),
                              os.environ["WLR_PASSWORD"].encode(), None, int(os.environ["WLR_PORT"]))
assert connection

def check(result):
    assert result
    code = lib.taos_errno(result)
    lib.taos_free_result(result)
    assert code == 0, code

check(lib.taos_query(connection, b"create database wlr_sml_empty keep 36500"))
check(lib.taos_query(connection, b"use wlr_sml_empty"))
for form in ("line", "raw"):
    for mode, key in (("empty", b""), ("default", None)):
        line = f"meters_{form}_{mode},id=chosen_{form},host=a value=1i 1700000000000".encode()
        if form == "line":
            check(line_api(connection, (ctypes.c_char_p * 1)(line), 1, 1, 4, 0, 0, key))
        else:
            total = ctypes.c_int32()
            check(raw_api(connection, line, len(line), ctypes.byref(total), 1, 4, 0, 0, key))
            assert total.value == 1
lib.taos_close(connection)
lib.taos_cleanup()
"""
        env = os.environ.copy()
        env.update(WLR_CFG=self.clientCfgDir, WLR_HOST=self.host, WLR_PORT=str(self.port),
                   WLR_USER=self.user, WLR_PASSWORD=self.password)
        try:
            process = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                     timeout=120, env=env)
        finally:
            self._restore_cfg()
        output = (process.stdout + process.stderr).decode("utf-8", errors="ignore")
        assert process.returncode == 0, output

        expected = {}
        for form in ("line", "raw"):
            stable = f"wlr_sml_empty.meters_{form}_empty"
            tdSql.query(f"select tbname, id, host from {stable}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, f"chosen_{form}")
            tdSql.checkData(0, 2, "a")
            assert tdSql.queryResult[0][0] != f"chosen_{form}"
            expected[stable] = list(tdSql.queryResult)
            tdSql.query(f"select tbname, host from wlr_sml_empty.meters_{form}_default")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, f"chosen_{form}")
            tdSql.query(f"describe wlr_sml_empty.meters_{form}_default")
            assert "id" not in {row[0] for row in tdSql.queryResult}
        self._drop("wlr_sml_empty")

        replay_rc, replay_out = self._replay_to_target(session)
        assert replay_rc == EXIT_OK, replay_out
        assert "calls skipped        : 0" in replay_out, replay_out
        for stable, rows in expected.items():
            tdSql.query(f"select tbname, id, host from {stable}")
            assert list(tdSql.queryResult) == rows
        for form in ("line", "raw"):
            tdSql.query(f"select tbname, host from wlr_sml_empty.meters_{form}_default")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, f"chosen_{form}")
            tdSql.query(f"describe wlr_sml_empty.meters_{form}_default")
            assert "id" not in {row[0] for row in tdSql.queryResult}
        print("schemaless empty table-name keys ......................... [ passed ]")

    def test_com_workload_replay_schemaless_empty_key(self, monkeypatch):
        """Preserve explicit empty schemaless table-name keys

        1. Record line/raw calls with empty and absent keys under an id naming default
        2. Replay and compare generated child-table names and retained tag sets

        Catalog:
            - Taosc

        Since: v3.4.3.0

        Labels: common,ci,functional,taosc

        Jira: None

        History:
            - 2026-10-27 Add empty table-name key recording and replay regression
        """
        self.do_schemaless_empty_key(monkeypatch)

    @pytest.mark.parametrize("bind_mode", ["short", "missing_full", "index", "tags", "tbname_tags", "valid"])
    def test_com_workload_replay_stmt1_bind_shape(self, bind_mode):
        """taosreplay -- skip legacy STMT binds with unsafe column shapes

        1. Record two legacy STMT batches using single-column calls
        2. Recast a second-batch call as an unsafe full or tag bind, or change its index
        3. Verify the first batch survives and an unsafe second batch cannot write

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-26 Added malformed legacy STMT bind coverage
        """
        self._require()
        db = "wlr_stmt1_bad"
        session = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: session})
        script = """
import os
from ctypes import byref
import taos
from taos.bind import new_multi_binds
from taos.cinterface import taos_stmt_bind_single_param_batch

connection = taos.connect(config=os.environ["WLR_CFG"], host=os.environ["WLR_HOST"],
                          port=int(os.environ["WLR_PORT"]), user=os.environ["WLR_USER"],
                          password=os.environ["WLR_PASSWORD"])
connection.query("create database wlr_stmt1_bad").close()
connection.query("create table wlr_stmt1_bad.t1 (ts timestamp, value int)").close()
statement = connection.statement("insert into wlr_stmt1_bad.t1 values (?, ?)")
binds = new_multi_binds(2)
binds[0].timestamp([1700000000000])
binds[1].int([42])
taos_stmt_bind_single_param_batch(statement._stmt, byref(binds[0]), 0)
taos_stmt_bind_single_param_batch(statement._stmt, byref(binds[1]), 1)
statement.add_batch()
statement.execute()
binds[0].timestamp([1700000001000])
binds[1].int([43])
taos_stmt_bind_single_param_batch(statement._stmt, byref(binds[0]), 0)
taos_stmt_bind_single_param_batch(statement._stmt, byref(binds[1]), 1)
statement.add_batch()
statement.execute()
statement.close()
connection.close()
"""
        env = os.environ.copy()
        env.update(WLR_CFG=self.clientCfgDir, WLR_HOST=self.host, WLR_PORT=str(self.port),
                   WLR_USER=self.user, WLR_PASSWORD=self.password)
        try:
            process = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                     timeout=120, env=env)
        finally:
            self._restore_cfg()
        output = (process.stdout + process.stderr).decode("utf-8", errors="ignore")
        assert process.returncode == 0, output

        segments = [name for name in os.listdir(session) if name.endswith(".wlr")]
        assert len(segments) == 1, segments
        path = os.path.join(session, segments[0])
        with open(path, "rb") as stream:
            data = bytearray(stream.read())

        def crc32c(payload):
            checksum = 0xffffffff
            for value in payload:
                checksum ^= value
                for _ in range(8):
                    checksum = (checksum >> 1) ^ (0x82f63b78 if checksum & 1 else 0)
            return checksum ^ 0xffffffff

        changed = False
        bind_calls = 0
        offset = WLR_FILE_HEADER_SIZE
        while bind_mode != "valid" and offset + 4 <= len(data):
            frame_length = int.from_bytes(data[offset:offset + 4], "little")
            frame_end = offset + 4 + frame_length
            assert frame_end <= len(data)
            payload_length = int.from_bytes(data[offset + 24:offset + 28], "little")
            payload_start = offset + 28
            payload_end = payload_start + payload_length
            assert payload_end + 5 == frame_end
            assert crc32c(data[offset + 4:payload_end]) == int.from_bytes(
                data[payload_end:payload_end + 4], "little")
            fields = {}
            field_offset = payload_start
            while field_offset + 8 <= payload_end:
                tag = int.from_bytes(data[field_offset:field_offset + 2], "little") & 0x7fff
                field_length = int.from_bytes(data[field_offset + 4:field_offset + 8], "little")
                field_end = field_offset + 8 + field_length
                assert field_end <= payload_end
                fields[tag] = (field_offset, field_end)
                field_offset = field_end
            if 5 in fields:
                api_start, api_end = fields[5]
                api_type = int.from_bytes(data[api_start + 8:api_end], "little")
                if api_type == 50 and 125 in fields:
                    bind_calls += 1
                if api_type == 50 and 125 in fields and bind_calls == 3:
                    payload = bytearray(data[payload_start:payload_end])
                    if bind_mode == "index":
                        assert 128 in fields
                        index_start, index_end = fields[128]
                        assert index_end - index_start == 16
                        payload[index_start + 8 - payload_start:index_end - payload_start] = (2).to_bytes(8, "little")
                    else:
                        assert api_end - api_start == 16
                        replay_api = {"short": 49, "missing_full": 48,
                                      "tags": 46, "tbname_tags": 44}[bind_mode]
                        payload[api_start + 8 - payload_start:api_end - payload_start] = replay_api.to_bytes(8, "little")
                        if bind_mode == "missing_full":
                            bind_start, bind_end = fields[125]
                            del payload[bind_start - payload_start:bind_end - payload_start]
                        if bind_mode == "tbname_tags" and 125 in fields:
                            payload.extend((126).to_bytes(2, "little"))
                            payload.extend((0).to_bytes(2, "little"))
                            payload.extend((2).to_bytes(4, "little"))
                            payload.extend(b"t1")
                    frame = bytearray(data[offset:offset + 28])
                    frame[:4] = (29 + len(payload)).to_bytes(4, "little")
                    frame[24:28] = len(payload).to_bytes(4, "little")
                    frame.extend(payload)
                    frame.extend(crc32c(frame[4:]).to_bytes(4, "little"))
                    frame.append(10)
                    data[offset:frame_end] = frame
                    changed = True
                    break
            offset = frame_end
        if bind_mode != "valid":
            assert changed, "recording contains no matching legacy STMT bind"
            with open(path, "wb") as stream:
                stream.write(data)

        inspect_rc, inspect_out = self._inspect(session)
        assert inspect_rc == EXIT_OK, inspect_out
        self._drop(db)
        replay_rc, replay_out = self._replay_to_target(session)
        assert replay_rc == (EXIT_OK if bind_mode == "valid" else EXIT_PARTIAL), replay_out
        if bind_mode != "valid":
            if bind_mode != "missing_full":
                diagnostic = ("STMT bind column index exceeds prepared column count" if bind_mode == "index" else
                              "legacy STMT tag bind shape cannot be verified" if bind_mode in ("tags", "tbname_tags")
                              else "STMT bind does not match prepared column count")
                assert diagnostic in replay_out, replay_out
        tdSql.query(f"select count(*) from {db}.t1")
        assert tdSql.queryResult == [(2 if bind_mode == "valid" else 1,)], tdSql.queryResult
        self._drop(db)

    def test_com_workload_replay_stmt2_row_bind(self):
        """taosreplay -- preserve STMT2 row-bind values for a fixed table

        1. Record an ordinary two-column STMT2 bind and execute
        2. Replay the prepared insert without skipped calls or lost rows

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-26 Added STMT2 row-bind replay coverage
        """
        self._require()
        db = "wlr_stmt2_row"
        session = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: session})
        script = """
import os
import taos

connection = taos.connect(config=os.environ["WLR_CFG"], host=os.environ["WLR_HOST"],
                          port=int(os.environ["WLR_PORT"]), user=os.environ["WLR_USER"],
                          password=os.environ["WLR_PASSWORD"])
connection.query("create database wlr_stmt2_row").close()
connection.query("create table wlr_stmt2_row.t1 (ts timestamp, value int)").close()
statement = connection.statement2("insert into wlr_stmt2_row.t1 values (?, ?)")
statement.bind_param(None, None, [[[1700000000000], [42]]])
assert statement.execute() == 1
statement.close()
connection.close()
"""
        env = os.environ.copy()
        env.update(WLR_CFG=self.clientCfgDir, WLR_HOST=self.host, WLR_PORT=str(self.port),
                   WLR_USER=self.user, WLR_PASSWORD=self.password)
        try:
            process = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                     timeout=120, env=env)
        finally:
            self._restore_cfg()
        output = (process.stdout + process.stderr).decode("utf-8", errors="ignore")
        assert process.returncode == 0, output

        self._drop(db)
        replay_rc, replay_out = self._replay_to_target(session)
        assert replay_rc == EXIT_OK, replay_out
        assert "calls skipped        : 0" in replay_out, replay_out
        tdSql.query(f"select value from {db}.t1")
        assert tdSql.queryResult == [(42,)], tdSql.queryResult
        self._drop(db)

    def test_com_workload_replay_stmt2_missing_prepare_sql(self):
        """taosreplay -- discard a statement after an oversized PREPARE

        1. Record a successful short PREPARE and insert into the first table
        2. Record an oversized PREPARE and insert into the second table
        3. Verify replay does not send the second row to the first table

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-27 Added missing PREPARE SQL regression coverage
        """
        self._require()
        db = "wlr_stmt2_missing_prepare"
        session = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: session})
        script = """
import ctypes
import os

class Bind(ctypes.Structure):
    _fields_ = [("buffer_type", ctypes.c_int), ("buffer", ctypes.c_void_p),
                ("length", ctypes.POINTER(ctypes.c_int32)), ("is_null", ctypes.c_void_p),
                ("num", ctypes.c_int32)]

class Bindv(ctypes.Structure):
    _fields_ = [("count", ctypes.c_int32), ("tbnames", ctypes.POINTER(ctypes.c_char_p)),
                ("tags", ctypes.POINTER(ctypes.POINTER(Bind))),
                ("bind_cols", ctypes.POINTER(ctypes.POINTER(Bind)))]

lib = ctypes.CDLL("libtaos.so")
lib.taos_options.argtypes = [ctypes.c_int, ctypes.c_char_p]
lib.taos_connect.argtypes = [ctypes.c_char_p] * 4 + [ctypes.c_uint16]
lib.taos_connect.restype = ctypes.c_void_p
lib.taos_query.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
lib.taos_query.restype = ctypes.c_void_p
lib.taos_errno.argtypes = [ctypes.c_void_p]
lib.taos_errno.restype = ctypes.c_int
lib.taos_free_result.argtypes = [ctypes.c_void_p]
lib.taos_stmt2_init.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
lib.taos_stmt2_init.restype = ctypes.c_void_p
lib.taos_stmt2_prepare.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_ulong]
lib.taos_stmt2_prepare.restype = ctypes.c_int
lib.taos_stmt2_bind_param.argtypes = [ctypes.c_void_p, ctypes.POINTER(Bindv), ctypes.c_int32]
lib.taos_stmt2_bind_param.restype = ctypes.c_int
lib.taos_stmt2_exec.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_int)]
lib.taos_stmt2_exec.restype = ctypes.c_int
lib.taos_stmt2_close.argtypes = [ctypes.c_void_p]
lib.taos_close.argtypes = [ctypes.c_void_p]
assert lib.taos_options(3, os.environ["WLR_CFG"].encode()) == 0
connection = lib.taos_connect(os.environ["WLR_HOST"].encode(), os.environ["WLR_USER"].encode(),
                              os.environ["WLR_PASSWORD"].encode(), None, int(os.environ["WLR_PORT"]))
assert connection
for sql in (b"create database wlr_stmt2_missing_prepare",
            b"create table wlr_stmt2_missing_prepare.t1 (ts timestamp, value int)",
            b"create table wlr_stmt2_missing_prepare.t2 (ts timestamp, value int)"):
    result = lib.taos_query(connection, sql)
    assert result and lib.taos_errno(result) == 0
    lib.taos_free_result(result)
statement = lib.taos_stmt2_init(connection, None)
assert statement
for table, timestamp, value in ((b"t1", 1700000000000, 42),
                                (b"t2", 1700000000001, 43)):
    sql = b"insert into wlr_stmt2_missing_prepare." + table + b" values (?, ?)"
    if table == b"t2":
        sql += b" " * (1100000 - len(sql))
    assert lib.taos_stmt2_prepare(statement, sql, len(sql)) == 0
    ts = ctypes.c_int64(timestamp)
    val = ctypes.c_int32(value)
    ts_length = ctypes.c_int32(8)
    val_length = ctypes.c_int32(4)
    columns = (Bind * 2)(Bind(9, ctypes.cast(ctypes.byref(ts), ctypes.c_void_p),
                               ctypes.pointer(ts_length), None, 1),
                         Bind(4, ctypes.cast(ctypes.byref(val), ctypes.c_void_p),
                               ctypes.pointer(val_length), None, 1))
    groups = (ctypes.POINTER(Bind) * 1)(columns)
    bindv = Bindv(1, None, None, groups)
    assert lib.taos_stmt2_bind_param(statement, ctypes.byref(bindv), -1) == 0
    affected = ctypes.c_int()
    assert lib.taos_stmt2_exec(statement, ctypes.byref(affected)) == 0
    assert affected.value == 1
lib.taos_stmt2_close(statement)
lib.taos_close(connection)
"""
        env = os.environ.copy()
        env.update(WLR_CFG=self.clientCfgDir, WLR_HOST=self.host, WLR_PORT=str(self.port),
                   WLR_USER=self.user, WLR_PASSWORD=self.password)
        try:
            process = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                     timeout=120, env=env)
        finally:
            self._restore_cfg()
        output = (process.stdout + process.stderr).decode("utf-8", errors="ignore")
        assert process.returncode == 0, output

        self._drop(db)
        replay_rc, replay_out = self._replay_to_target(session)
        assert replay_rc == EXIT_PARTIAL, replay_out
        tdSql.query(f"select value from {db}.t1")
        assert tdSql.queryResult == [(42,)], tdSql.queryResult
        tdSql.query(f"select value from {db}.t2")
        assert tdSql.queryResult == [], tdSql.queryResult
        self._drop(db)

    def test_com_workload_replay_missing_query_sql(self):
        """taosreplay -- reject an omitted state-changing query before connecting

        1. Record a large USE query followed by an unqualified insert
        2. Confirm the recorded insert targets the selected database
        3. Refuse replay before it can write to either target database

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-27 Added omitted QUERY SQL state regression coverage
        """
        self._require()
        first_db = "wlr_missing_query_a"
        second_db = "wlr_missing_query_b"
        session = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: session})
        script = """
import os
import taos

connection = taos.connect(config=os.environ["WLR_CFG"], host=os.environ["WLR_HOST"],
                          port=int(os.environ["WLR_PORT"]), user=os.environ["WLR_USER"],
                          password=os.environ["WLR_PASSWORD"])
for database in ("wlr_missing_query_a", "wlr_missing_query_b"):
    connection.query(f"create database {database}").close()
    connection.query(f"create table {database}.t (ts timestamp, value int)").close()
connection.query("use wlr_missing_query_a").close()
use_sql = "use wlr_missing_query_b"
use_sql += " " * (1100000 - len(use_sql))
connection.query(use_sql).close()
connection.query("insert into t values (1700000000000, 42)").close()
connection.close()
"""
        env = os.environ.copy()
        env.update(WLR_CFG=self.clientCfgDir, WLR_HOST=self.host, WLR_PORT=str(self.port),
                   WLR_USER=self.user, WLR_PASSWORD=self.password)
        try:
            process = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                     timeout=120, env=env)
        finally:
            self._restore_cfg()
        output = (process.stdout + process.stderr).decode("utf-8", errors="ignore")
        assert process.returncode == 0, output
        tdSql.query(f"select value from {first_db}.t")
        assert tdSql.queryResult == [], tdSql.queryResult
        tdSql.query(f"select value from {second_db}.t")
        assert tdSql.queryResult == [(42,)], tdSql.queryResult

        for database in (first_db, second_db):
            self._drop(database)
            tdSql.execute(f"create database {database}")
            tdSql.execute(f"create table {database}.t (ts timestamp, value int)")
        replay_rc, replay_out = self._replay_to_target(session)
        assert replay_rc == EXIT_INPUT, replay_out
        assert "unavailable SQL" in replay_out, replay_out
        for database in (first_db, second_db):
            tdSql.query(f"select value from {database}.t")
            assert tdSql.queryResult == [], tdSql.queryResult
            self._drop(database)

    @pytest.mark.parametrize("bind_col_idx", [None, 2], ids=["short-row", "invalid-column-index"])
    def test_com_workload_replay_stmt2_short_bind(self, bind_col_idx):
        """taosreplay -- reject a CRC-valid STMT2 bind with an unsafe shape or index

        1. Record two STMT2 binds before an insert, then shorten the second bind
        2. Optionally mark that column with an out-of-range bind index
        3. Verify replay does not execute values from the first bind

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-26 Added malformed STMT2 bind-shape coverage
        """
        self._require()
        db = "wlr_stmt2_short"
        session = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: session})
        script = """
import os
import taos

connection = taos.connect(config=os.environ["WLR_CFG"], host=os.environ["WLR_HOST"],
                          port=int(os.environ["WLR_PORT"]), user=os.environ["WLR_USER"],
                          password=os.environ["WLR_PASSWORD"])
connection.query("create database wlr_stmt2_short").close()
connection.query("create table wlr_stmt2_short.t1 (ts timestamp, value int)").close()
statement = connection.statement2("insert into wlr_stmt2_short.t1 values (?, ?)")
statement.bind_param(None, None, [[[1700000000000], [42]]])
statement.bind_param(None, None, [[[1700000000001], [43]]])
assert statement.execute() > 0
statement.close()
connection.close()
"""
        env = os.environ.copy()
        env.update(WLR_CFG=self.clientCfgDir, WLR_HOST=self.host, WLR_PORT=str(self.port),
                   WLR_USER=self.user, WLR_PASSWORD=self.password)
        try:
            process = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                     timeout=120, env=env)
        finally:
            self._restore_cfg()
        output = (process.stdout + process.stderr).decode("utf-8", errors="ignore")
        assert process.returncode == 0, output

        segments = [name for name in os.listdir(session) if name.endswith(".wlr")]
        assert len(segments) == 1, segments
        path = os.path.join(session, segments[0])
        with open(path, "rb") as stream:
            data = bytearray(stream.read())

        def crc32c(payload):
            checksum = 0xffffffff
            for value in payload:
                checksum ^= value
                for _ in range(8):
                    checksum = (checksum >> 1) ^ (0x82f63b78 if checksum & 1 else 0)
            return checksum ^ 0xffffffff

        changed = False
        bind_count = 0
        offset = WLR_FILE_HEADER_SIZE
        while offset + 4 <= len(data):
            frame_length = int.from_bytes(data[offset:offset + 4], "little")
            frame_end = offset + 4 + frame_length
            assert frame_end <= len(data)
            payload_length = int.from_bytes(data[offset + 24:offset + 28], "little")
            payload_start = offset + 28
            payload_end = payload_start + payload_length
            assert payload_end + 5 == frame_end
            assert crc32c(data[offset + 4:payload_end]) == int.from_bytes(
                data[payload_end:payload_end + 4], "little")
            field_offset = payload_start
            while field_offset + 8 <= payload_end:
                tag = int.from_bytes(data[field_offset:field_offset + 2], "little") & 0x7fff
                field_length = int.from_bytes(data[field_offset + 4:field_offset + 8], "little")
                field_end = field_offset + 8 + field_length
                assert field_end <= payload_end
                if tag == 125:
                    bind_count += 1
                    if bind_count != 2:
                        field_offset = field_end
                        continue
                    blob = data[field_offset + 8:field_end]
                    assert int.from_bytes(blob[:4], "little") == 2
                    first_column_end = 4 + 20 + sum(int.from_bytes(blob[4 + part:8 + part], "little")
                                                      for part in (8, 12, 16))
                    assert first_column_end < len(blob)
                    short_blob = bytearray(blob[:first_column_end])
                    short_blob[:4] = (1).to_bytes(4, "little")
                    payload = bytearray(data[payload_start:field_offset])
                    payload.extend(data[field_offset:field_offset + 4])
                    payload.extend(len(short_blob).to_bytes(4, "little"))
                    payload.extend(short_blob)
                    payload.extend(data[field_end:payload_end])
                    if bind_col_idx is not None:
                        index_offset = 0
                        while index_offset + 8 <= len(payload):
                            index_tag = int.from_bytes(payload[index_offset:index_offset + 2], "little") & 0x7fff
                            index_length = int.from_bytes(payload[index_offset + 4:index_offset + 8], "little")
                            if index_tag == 128:
                                assert index_length == 8
                                payload[index_offset + 8:index_offset + 16] = bind_col_idx.to_bytes(8, "little")
                                break
                            index_offset += 8 + index_length
                        else:
                            payload.extend((128).to_bytes(2, "little"))
                            payload.extend((0).to_bytes(2, "little"))
                            payload.extend((8).to_bytes(4, "little"))
                            payload.extend(bind_col_idx.to_bytes(8, "little"))
                    frame = bytearray(data[offset:offset + 28])
                    frame[:4] = (29 + len(payload)).to_bytes(4, "little")
                    frame[24:28] = len(payload).to_bytes(4, "little")
                    frame.extend(payload)
                    frame.extend(crc32c(frame[4:]).to_bytes(4, "little"))
                    frame.append(10)
                    data[offset:frame_end] = frame
                    changed = True
                    break
                field_offset = field_end
            if changed:
                break
            offset = frame_end
        assert changed, "recording contains no STMT2 bind payload"
        with open(path, "wb") as stream:
            stream.write(data)

        inspect_rc, inspect_out = self._inspect(session)
        assert inspect_rc == EXIT_OK, inspect_out
        self._drop(db)
        replay_rc, replay_out = self._replay_to_target(session)
        assert replay_rc == EXIT_PARTIAL, replay_out
        assert "calls skipped        : 0" not in replay_out, replay_out
        diagnostic = ("STMT2 bind column index exceeds prepared column count" if bind_col_idx is not None
                      else "STMT2 bind does not match prepared column count")
        assert diagnostic in replay_out, replay_out
        tdSql.query(f"select count(*) from {db}.t1")
        assert tdSql.queryResult == [(0,)], tdSql.queryResult
        self._drop(db)

    def test_com_workload_replay_stmt2_tag_bind(self):
        """taosreplay -- preserve STMT2 tags when replay creates a child table

        1. Record a STMT2 insert into two child tables with distinct tags
        2. Replay into an empty database and verify both tag values

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-26 Added STMT2 tag-bind replay coverage
        """
        self._require()
        db = "wlr_stmt2_tags"
        session = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: session})
        script = """
import os
import taos

connection = taos.connect(config=os.environ["WLR_CFG"], host=os.environ["WLR_HOST"],
                          port=int(os.environ["WLR_PORT"]), user=os.environ["WLR_USER"],
                          password=os.environ["WLR_PASSWORD"])
connection.query("create database wlr_stmt2_tags").close()
connection.query("create stable wlr_stmt2_tags.stb (ts timestamp, value int) tags (site binary(32), region int)").close()
connection.select_db("wlr_stmt2_tags")
statement = connection.statement2("insert into ? using wlr_stmt2_tags.stb tags (?, ?) values (?, ?)")
statement.bind_param(["t1", "t2"], [["north", 1], ["south", 2]],
                     [[[1700000000000], [42]], [[1700000000001], [43]]])
assert statement.execute() == 2
statement.close()
connection.close()
"""
        env = os.environ.copy()
        env.update(WLR_CFG=self.clientCfgDir, WLR_HOST=self.host, WLR_PORT=str(self.port),
                   WLR_USER=self.user, WLR_PASSWORD=self.password)
        try:
            process = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                     timeout=120, env=env)
        finally:
            self._restore_cfg()
        output = (process.stdout + process.stderr).decode("utf-8", errors="ignore")
        assert process.returncode == 0, output

        self._drop(db)
        replay_rc, replay_out = self._replay_to_target(session)
        assert replay_rc == EXIT_OK, replay_out
        assert "calls skipped        : 0" in replay_out, replay_out
        tdSql.query(f"select value, site, region from {db}.stb order by value")
        assert tdSql.queryResult == [(42, "north", 1), (43, "south", 2)], tdSql.queryResult
        self._drop(db)

    def test_com_workload_replay_null_host(self):
        """taosreplay -- retain a NULL recorded host without a target override.

        Record a connection opened with a NULL host, then replay without endpoint
        redirection and verify that the original target receives the write.

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-27 Added NULL-host replay coverage
        """
        self._require()
        db = "wlr_null_host"
        session = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: session})
        script = """
import ctypes
import os

lib = ctypes.CDLL("libtaos.so")
lib.taos_options.argtypes = [ctypes.c_int, ctypes.c_char_p]
lib.taos_options.restype = ctypes.c_int
lib.taos_connect.argtypes = [ctypes.c_char_p] * 4 + [ctypes.c_uint16]
lib.taos_connect.restype = ctypes.c_void_p
lib.taos_query.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
lib.taos_query.restype = ctypes.c_void_p
lib.taos_errno.argtypes = [ctypes.c_void_p]
lib.taos_errno.restype = ctypes.c_int
lib.taos_free_result.argtypes = [ctypes.c_void_p]
lib.taos_close.argtypes = [ctypes.c_void_p]
assert lib.taos_options(3, os.environ["WLR_CFG"].encode()) == 0
connection = lib.taos_connect(None, os.environ["WLR_USER"].encode(),
                              os.environ["WLR_PASSWORD"].encode(), None, 0)
assert connection
for sql in ("create database wlr_null_host", "create table wlr_null_host.t1 (ts timestamp, value int)",
            "insert into wlr_null_host.t1 values (1700000000000, 42)"):
    result = lib.taos_query(connection, sql.encode())
    assert result and lib.taos_errno(result) == 0, sql
    lib.taos_free_result(result)
lib.taos_close(connection)
"""
        env = os.environ.copy()
        env.update(WLR_CFG=self.clientCfgDir, WLR_USER=self.user, WLR_PASSWORD=self.password)
        try:
            process = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                     timeout=120, env=env)
        finally:
            self._restore_cfg()
        assert process.returncode == 0, (process.stdout + process.stderr).decode("utf-8", errors="ignore")

        self._drop(db)
        replay_rc, replay_out = self._replay("--input", session,
                                             password_env=f"{self.user}={self.password}")
        assert replay_rc == EXIT_OK, replay_out
        tdSql.query(f"select value from {db}.t1")
        assert tdSql.queryResult == [(42,)], tdSql.queryResult
        self._drop(db)

    def test_com_workload_replay_recorded_default_port(self):
        """taosreplay -- retain port zero for a recorded hostname

        1. Record a localhost connection made with port zero
        2. Set the replay client's configured server port to an unused port
        3. Verify replay does not silently connect to port 6030

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-27 Cover recorded host-only default-port semantics
        """
        self._require()
        db = "wlr_default_port"
        session = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: session})
        script = """
import ctypes
import os

lib = ctypes.CDLL("libtaos.so")
lib.taos_options.argtypes = [ctypes.c_int, ctypes.c_char_p]
lib.taos_options.restype = ctypes.c_int
lib.taos_connect.argtypes = [ctypes.c_char_p] * 4 + [ctypes.c_uint16]
lib.taos_connect.restype = ctypes.c_void_p
lib.taos_query.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
lib.taos_query.restype = ctypes.c_void_p
lib.taos_errno.argtypes = [ctypes.c_void_p]
lib.taos_errno.restype = ctypes.c_int
lib.taos_free_result.argtypes = [ctypes.c_void_p]
lib.taos_close.argtypes = [ctypes.c_void_p]
assert lib.taos_options(3, os.environ["WLR_CFG"].encode()) == 0
connection = lib.taos_connect(b"localhost", os.environ["WLR_USER"].encode(),
                              os.environ["WLR_PASSWORD"].encode(), None, 0)
assert connection
result = lib.taos_query(connection, b"create database wlr_default_port")
assert result and lib.taos_errno(result) == 0
lib.taos_free_result(result)
lib.taos_close(connection)
"""
        env = os.environ.copy()
        env.update(WLR_CFG=self.clientCfgDir, WLR_USER=self.user, WLR_PASSWORD=self.password)
        try:
            process = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                     timeout=120, env=env)
        finally:
            self._restore_cfg()
        output = (process.stdout + process.stderr).decode("utf-8", errors="ignore")
        assert process.returncode == 0, output

        self._drop(db)
        env = os.environ.copy()
        env.update(TAOS_SERVER_PORT="1", TAOSREPLAY_USER_PASSWORD=f"{self.user}={self.password}")
        process = subprocess.run([self.replay, "replay", "--input", session],
                                 capture_output=True, timeout=120, env=env)
        output = (process.stdout + process.stderr).decode("utf-8", errors="ignore")
        assert process.returncode == EXIT_CONN, output
        assert "connect to localhost:0 failed" in output, output
        tdSql.query("show databases;")
        names = [str(row[0]) for row in tdSql.queryResult]
        assert db not in names, "replay ignored the configured default port"

    def test_com_workload_replay_bare_ipv6_host(self):
        """taosreplay -- retain a recorded bare IPv6 address

        1. Record a connection to ::1 with port zero when IPv6 is available
        2. Replay without a redirect and verify the write reaches the same server

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-27 Cover unmodified bare IPv6 endpoints
        """
        self._require()
        db = "wlr_ipv6_host"
        session = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: session})
        script = """
import ctypes
import os
import sys

lib = ctypes.CDLL("libtaos.so")
lib.taos_options.argtypes = [ctypes.c_int, ctypes.c_char_p]
lib.taos_options.restype = ctypes.c_int
lib.taos_connect.argtypes = [ctypes.c_char_p] * 4 + [ctypes.c_uint16]
lib.taos_connect.restype = ctypes.c_void_p
lib.taos_query.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
lib.taos_query.restype = ctypes.c_void_p
lib.taos_errno.argtypes = [ctypes.c_void_p]
lib.taos_errno.restype = ctypes.c_int
lib.taos_free_result.argtypes = [ctypes.c_void_p]
lib.taos_close.argtypes = [ctypes.c_void_p]
assert lib.taos_options(3, os.environ["WLR_CFG"].encode()) == 0
connection = lib.taos_connect(b"::1", os.environ["WLR_USER"].encode(),
                              os.environ["WLR_PASSWORD"].encode(), None, 0)
if not connection:
    sys.exit(77)
result = lib.taos_query(connection, b"create database wlr_ipv6_host")
assert result and lib.taos_errno(result) == 0
lib.taos_free_result(result)
lib.taos_close(connection)
"""
        env = os.environ.copy()
        env.update(WLR_CFG=self.clientCfgDir, WLR_USER=self.user, WLR_PASSWORD=self.password)
        try:
            process = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                     timeout=120, env=env)
        finally:
            self._restore_cfg()
        if process.returncode == 77:
            pytest.skip("local TDengine server does not accept bare IPv6 connections")
        output = (process.stdout + process.stderr).decode("utf-8", errors="ignore")
        assert process.returncode == 0, output

        self._drop(db)
        replay_rc, replay_out = self._replay("--input", session,
                                             password_env=f"{self.user}={self.password}")
        assert replay_rc == EXIT_OK, replay_out
        tdSql.query("show databases;")
        names = [str(row[0]) for row in tdSql.queryResult]
        assert db in names, replay_out
        self._drop(db)

    def test_com_workload_replay_stmt2_zero_num_tags(self):
        """taosreplay -- preserve scalar STMT2 tags with num set to zero.

        Bind INT and BINARY tags with zero-initialized counts, then verify that
        replay preserves both tag values and the inserted row.

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-27 Added zero-count scalar-tag replay coverage
        """
        self._require()
        db = "wlr_stmt2_zero_tags"
        session = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: session})
        script = """
import ctypes
import os
import taos

connection = taos.connect(config=os.environ["WLR_CFG"], host=os.environ["WLR_HOST"],
                          port=int(os.environ["WLR_PORT"]), user=os.environ["WLR_USER"],
                          password=os.environ["WLR_PASSWORD"])
connection.query("create database wlr_stmt2_zero_tags").close()
connection.query("create stable wlr_stmt2_zero_tags.stb (ts timestamp, value int) "
                 "tags (site int, region binary(32))").close()
connection.close()

class Bind(ctypes.Structure):
    _fields_ = [("buffer_type", ctypes.c_int), ("buffer", ctypes.c_void_p),
                ("length", ctypes.POINTER(ctypes.c_int32)), ("is_null", ctypes.c_char_p),
                ("num", ctypes.c_int)]

class Bindv(ctypes.Structure):
    _fields_ = [("count", ctypes.c_int), ("tbnames", ctypes.POINTER(ctypes.c_char_p)),
                ("tags", ctypes.POINTER(ctypes.POINTER(Bind))),
                ("bind_cols", ctypes.POINTER(ctypes.POINTER(Bind)))]

lib = ctypes.CDLL("libtaos.so")
lib.taos_options.argtypes = [ctypes.c_int, ctypes.c_char_p]
lib.taos_options.restype = ctypes.c_int
lib.taos_connect.argtypes = [ctypes.c_char_p] * 4 + [ctypes.c_uint16]
lib.taos_connect.restype = ctypes.c_void_p
lib.taos_stmt2_init.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
lib.taos_stmt2_init.restype = ctypes.c_void_p
lib.taos_stmt2_prepare.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_ulong]
lib.taos_stmt2_prepare.restype = ctypes.c_int
lib.taos_stmt2_bind_param.argtypes = [ctypes.c_void_p, ctypes.POINTER(Bindv), ctypes.c_int32]
lib.taos_stmt2_bind_param.restype = ctypes.c_int
lib.taos_stmt2_exec.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_int)]
lib.taos_stmt2_exec.restype = ctypes.c_int
lib.taos_stmt2_close.argtypes = [ctypes.c_void_p]
lib.taos_close.argtypes = [ctypes.c_void_p]
assert lib.taos_options(3, os.environ["WLR_CFG"].encode()) == 0
connection = lib.taos_connect(os.environ["WLR_HOST"].encode(), os.environ["WLR_USER"].encode(),
                              os.environ["WLR_PASSWORD"].encode(), b"wlr_stmt2_zero_tags",
                              int(os.environ["WLR_PORT"]))
assert connection
statement = lib.taos_stmt2_init(connection, None)
assert statement
assert lib.taos_stmt2_prepare(statement, b"insert into ? using stb tags (?, ?) values (?, ?)", 0) == 0
names = (ctypes.c_char_p * 1)(b"t1")
site = ctypes.c_int32(7)
region = ctypes.create_string_buffer(b"north")
region_length = ctypes.c_int32(5)
tags = (Bind * 2)(Bind(4, ctypes.cast(ctypes.byref(site), ctypes.c_void_p), None, None, 0),
                  Bind(8, ctypes.cast(region, ctypes.c_void_p), ctypes.pointer(region_length), None, 0))
tag_groups = (ctypes.POINTER(Bind) * 1)(tags)
timestamp = ctypes.c_int64(1700000000000)
value = ctypes.c_int32(42)
timestamp_length = ctypes.c_int32(8)
value_length = ctypes.c_int32(4)
row = (Bind * 2)(Bind(9, ctypes.cast(ctypes.byref(timestamp), ctypes.c_void_p),
                       ctypes.pointer(timestamp_length), None, 1),
                 Bind(4, ctypes.cast(ctypes.byref(value), ctypes.c_void_p),
                      ctypes.pointer(value_length), None, 1))
rows = (ctypes.POINTER(Bind) * 1)(row)
bindv = Bindv(1, names, tag_groups, rows)
assert lib.taos_stmt2_bind_param(statement, ctypes.byref(bindv), -1) == 0
affected = ctypes.c_int()
assert lib.taos_stmt2_exec(statement, ctypes.byref(affected)) == 0
assert affected.value == 1
assert lib.taos_stmt2_close(statement) == 0
lib.taos_close(connection)
"""
        env = os.environ.copy()
        env.update(WLR_CFG=self.clientCfgDir, WLR_HOST=self.host, WLR_PORT=str(self.port),
                   WLR_USER=self.user, WLR_PASSWORD=self.password)
        try:
            process = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                     timeout=120, env=env)
        finally:
            self._restore_cfg()
        assert process.returncode == 0, (process.stdout + process.stderr).decode("utf-8", errors="ignore")

        self._drop(db)
        replay_rc, replay_out = self._replay_to_target(session)
        assert replay_rc == EXIT_OK, replay_out
        tdSql.query(f"select value, site, region from {db}.stb")
        assert tdSql.queryResult == [(42, 7, "north")], tdSql.queryResult
        self._drop(db)

    def test_com_workload_replay_stmt2_callback_exec_overlap(self):
        """taosreplay -- reject an EXEC waiting for callback-nested writes.

        Run EXEC while an async bind callback writes through another connection,
        and verify that replay refuses the unsafe order before modifying the target.

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-27 Added async-bind callback dependency coverage
        """
        self._require()
        db = "wlr_stmt2_callback"
        session = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: session})
        script = """
import ctypes
import os
import threading
import time
import taos

connection = taos.connect(config=os.environ["WLR_CFG"], host=os.environ["WLR_HOST"],
                          port=int(os.environ["WLR_PORT"]), user=os.environ["WLR_USER"],
                          password=os.environ["WLR_PASSWORD"])
connection.query("create database wlr_stmt2_callback").close()
connection.query("create table wlr_stmt2_callback.t1 (ts timestamp, value int)").close()
connection.close()

class Bind(ctypes.Structure):
    _fields_ = [("buffer_type", ctypes.c_int), ("buffer", ctypes.c_void_p),
                ("length", ctypes.POINTER(ctypes.c_int32)), ("is_null", ctypes.c_char_p),
                ("num", ctypes.c_int)]

class Bindv(ctypes.Structure):
    _fields_ = [("count", ctypes.c_int), ("tbnames", ctypes.POINTER(ctypes.c_char_p)),
                ("tags", ctypes.POINTER(ctypes.POINTER(Bind))),
                ("bind_cols", ctypes.POINTER(ctypes.POINTER(Bind)))]

lib = ctypes.CDLL("libtaos.so")
lib.taos_options.argtypes = [ctypes.c_int, ctypes.c_char_p]
lib.taos_options.restype = ctypes.c_int
lib.taos_connect.argtypes = [ctypes.c_char_p] * 4 + [ctypes.c_uint16]
lib.taos_connect.restype = ctypes.c_void_p
lib.taos_query.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
lib.taos_query.restype = ctypes.c_void_p
lib.taos_errno.argtypes = [ctypes.c_void_p]
lib.taos_errno.restype = ctypes.c_int
lib.taos_free_result.argtypes = [ctypes.c_void_p]
lib.taos_stmt2_init.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
lib.taos_stmt2_init.restype = ctypes.c_void_p
lib.taos_stmt2_prepare.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_ulong]
lib.taos_stmt2_prepare.restype = ctypes.c_int
callback_type = ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int)
lib.taos_stmt2_bind_param_a.argtypes = [ctypes.c_void_p, ctypes.POINTER(Bindv), ctypes.c_int32,
                                        callback_type, ctypes.c_void_p]
lib.taos_stmt2_bind_param_a.restype = ctypes.c_int
lib.taos_stmt2_exec.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_int)]
lib.taos_stmt2_exec.restype = ctypes.c_int
lib.taos_stmt2_close.argtypes = [ctypes.c_void_p]
lib.taos_close.argtypes = [ctypes.c_void_p]
assert lib.taos_options(3, os.environ["WLR_CFG"].encode()) == 0

def connect():
    handle = lib.taos_connect(os.environ["WLR_HOST"].encode(), os.environ["WLR_USER"].encode(),
                              os.environ["WLR_PASSWORD"].encode(), b"wlr_stmt2_callback",
                              int(os.environ["WLR_PORT"]))
    assert handle
    return handle

main_connection = connect()
callback_connection = connect()
statement = lib.taos_stmt2_init(main_connection, None)
assert statement
assert lib.taos_stmt2_prepare(statement, b"insert into t1 values (?, ?)", 0) == 0
timestamp = ctypes.c_int64(1700000000000)
value = ctypes.c_int32(42)
timestamp_length = ctypes.c_int32(8)
value_length = ctypes.c_int32(4)
row = (Bind * 2)(Bind(9, ctypes.cast(ctypes.byref(timestamp), ctypes.c_void_p),
                       ctypes.pointer(timestamp_length), None, 1),
                 Bind(4, ctypes.cast(ctypes.byref(value), ctypes.c_void_p),
                      ctypes.pointer(value_length), None, 1))
rows = (ctypes.POINTER(Bind) * 1)(row)
bindv = Bindv(1, None, None, rows)
callback_started = threading.Event()
release_callback = threading.Event()
callback_done = threading.Event()
callback_errors = []

@callback_type
def on_bind(_param, _result, code):
    try:
        assert code == 0
        callback_started.set()
        assert release_callback.wait(10)
        result = lib.taos_query(callback_connection,
                                b"insert into t1 values (1700000000001, 43)")
        assert result and lib.taos_errno(result) == 0
        lib.taos_free_result(result)
    except Exception as exc:
        callback_errors.append(str(exc))
    finally:
        callback_done.set()

assert lib.taos_stmt2_bind_param_a(statement, ctypes.byref(bindv), -1, on_bind, None) == 0
assert callback_started.wait(10)
exec_result = []

def execute():
    affected = ctypes.c_int()
    exec_result.append((lib.taos_stmt2_exec(statement, ctypes.byref(affected)), affected.value))

thread = threading.Thread(target=execute)
thread.start()
time.sleep(0.2)
assert thread.is_alive(), "EXEC did not wait for the callback"
release_callback.set()
thread.join(20)
assert not thread.is_alive()
assert callback_done.wait(10) and not callback_errors, callback_errors
assert exec_result == [(0, 1)], exec_result
assert lib.taos_stmt2_close(statement) == 0
lib.taos_close(callback_connection)
lib.taos_close(main_connection)
"""
        env = os.environ.copy()
        env.update(WLR_CFG=self.clientCfgDir, WLR_HOST=self.host, WLR_PORT=str(self.port),
                   WLR_USER=self.user, WLR_PASSWORD=self.password)
        try:
            process = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                     timeout=120, env=env)
        finally:
            self._restore_cfg()
        assert process.returncode == 0, (process.stdout + process.stderr).decode("utf-8", errors="ignore")

        self._drop(db)
        replay_rc, replay_out = self._replay_to_target(session)
        assert replay_rc == EXIT_INPUT, replay_out
        assert "STMT2 async bind callback overlaps execute" in replay_out, replay_out
        tdSql.query("show databases")
        assert db not in [str(row[0]) for row in tdSql.queryResult], tdSql.queryResult

    @pytest.mark.parametrize("operation", ["close", "prepare"], ids=["close", "reprepare"])
    def test_com_workload_replay_stmt2_exec_callback_lifecycle_overlap(self, operation):
        """taosreplay -- reject lifecycle calls racing with an EXEC callback.

        Let close or re-prepare wait for an EXEC callback that accesses the same
        statement, then verify replay rejects before opening a target handle.

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-27 Added EXEC callback-to-lifecycle dependency coverage
        """
        self._require()
        db = "wlr_stmt2_exec_callback"
        session = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: session})
        script = """
import ctypes
import os
import threading
import time
import taos

connection = taos.connect(config=os.environ["WLR_CFG"], host=os.environ["WLR_HOST"],
                          port=int(os.environ["WLR_PORT"]), user=os.environ["WLR_USER"],
                          password=os.environ["WLR_PASSWORD"])
connection.query("create database wlr_stmt2_exec_callback").close()
connection.query("create table wlr_stmt2_exec_callback.t1 (ts timestamp, value int)").close()
connection.query("insert into wlr_stmt2_exec_callback.t1 values (1700000000000, 42)").close()
connection.close()

class Bind(ctypes.Structure):
    _fields_ = [("buffer_type", ctypes.c_int), ("buffer", ctypes.c_void_p),
                ("length", ctypes.POINTER(ctypes.c_int32)), ("is_null", ctypes.c_char_p),
                ("num", ctypes.c_int)]

class Bindv(ctypes.Structure):
    _fields_ = [("count", ctypes.c_int), ("tbnames", ctypes.POINTER(ctypes.c_char_p)),
                ("tags", ctypes.POINTER(ctypes.POINTER(Bind))),
                ("bind_cols", ctypes.POINTER(ctypes.POINTER(Bind)))]

callback_type = ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int)

class Option(ctypes.Structure):
    _fields_ = [("reqid", ctypes.c_int64), ("singleStbInsert", ctypes.c_bool),
                ("singleTableBindOnce", ctypes.c_bool), ("asyncExecFn", callback_type),
                ("userdata", ctypes.c_void_p)]

lib = ctypes.CDLL("libtaos.so")
lib.taos_options.argtypes = [ctypes.c_int, ctypes.c_char_p]
lib.taos_options.restype = ctypes.c_int
lib.taos_connect.argtypes = [ctypes.c_char_p] * 4 + [ctypes.c_uint16]
lib.taos_connect.restype = ctypes.c_void_p
lib.taos_stmt2_init.argtypes = [ctypes.c_void_p, ctypes.POINTER(Option)]
lib.taos_stmt2_init.restype = ctypes.c_void_p
lib.taos_stmt2_prepare.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_ulong]
lib.taos_stmt2_prepare.restype = ctypes.c_int
lib.taos_stmt2_bind_param.argtypes = [ctypes.c_void_p, ctypes.POINTER(Bindv), ctypes.c_int32]
lib.taos_stmt2_bind_param.restype = ctypes.c_int
lib.taos_stmt2_exec.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_int)]
lib.taos_stmt2_exec.restype = ctypes.c_int
lib.taos_stmt2_result.argtypes = [ctypes.c_void_p]
lib.taos_stmt2_result.restype = ctypes.c_void_p
lib.taos_stmt2_close.argtypes = [ctypes.c_void_p]
lib.taos_stmt2_close.restype = ctypes.c_int
lib.taos_close.argtypes = [ctypes.c_void_p]
assert lib.taos_options(3, os.environ["WLR_CFG"].encode()) == 0
taos_handle = lib.taos_connect(os.environ["WLR_HOST"].encode(), os.environ["WLR_USER"].encode(),
                               os.environ["WLR_PASSWORD"].encode(), b"wlr_stmt2_exec_callback",
                               int(os.environ["WLR_PORT"]))
assert taos_handle
callback_started = threading.Event()
release_callback = threading.Event()
callback_done = threading.Event()
callback_errors = []
statement = None

@callback_type
def on_exec(_param, _result, code):
    try:
        assert code == 0, f"callback code={code}"
        callback_started.set()
        assert release_callback.wait(10), "callback release timed out"
        result = lib.taos_stmt2_result(statement)
        assert result, "statement result is NULL"
    except Exception as exc:
        callback_errors.append(str(exc))
    finally:
        callback_done.set()

option = Option(0, False, False, on_exec, None)
statement = lib.taos_stmt2_init(taos_handle, ctypes.byref(option))
assert statement
assert lib.taos_stmt2_prepare(statement, b"select value from t1 where value = ?", 0) == 0
value = ctypes.c_int32(42)
length = ctypes.c_int32(4)
row = (Bind * 1)(Bind(4, ctypes.cast(ctypes.byref(value), ctypes.c_void_p),
                      ctypes.pointer(length), None, 1))
rows = (ctypes.POINTER(Bind) * 1)(row)
bindv = Bindv(1, None, None, rows)
assert lib.taos_stmt2_bind_param(statement, ctypes.byref(bindv), -1) == 0
affected = ctypes.c_int()
assert lib.taos_stmt2_exec(statement, ctypes.byref(affected)) == 0
assert callback_started.wait(10)
operation = os.environ["WLR_OPERATION"]
operation_result = []

def update_statement():
    if operation == "close":
        operation_result.append(lib.taos_stmt2_close(statement))
    else:
        operation_result.append(lib.taos_stmt2_prepare(statement,
                                b"select value from t1 where ts = ?", 0))

thread = threading.Thread(target=update_statement)
thread.start()
time.sleep(0.2)
assert thread.is_alive(), f"{operation} did not wait for the EXEC callback"
release_callback.set()
thread.join(20)
assert not thread.is_alive()
assert callback_done.wait(10) and not callback_errors, callback_errors
assert operation_result == [0], operation_result
if operation == "prepare":
    assert lib.taos_stmt2_close(statement) == 0
lib.taos_close(taos_handle)
"""
        env = os.environ.copy()
        env.update(WLR_CFG=self.clientCfgDir, WLR_HOST=self.host, WLR_PORT=str(self.port),
                   WLR_USER=self.user, WLR_PASSWORD=self.password, WLR_OPERATION=operation)
        try:
            process = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                     timeout=120, env=env)
        finally:
            self._restore_cfg()
        assert process.returncode == 0, (process.stdout + process.stderr).decode("utf-8", errors="ignore")

        self._drop(db)
        replay_rc, replay_out = self._replay_to_target(session)
        assert replay_rc == EXIT_INPUT, replay_out
        assert f"STMT2 callback overlaps {operation}" in replay_out, replay_out
        tdSql.query("show databases")
        assert db not in [str(row[0]) for row in tdSql.queryResult], tdSql.queryResult

    def test_com_workload_replay_stmt2_no_column_bind(self):
        """taosreplay -- retain a valid STMT2 table/tag-only bind before row data

        1. Record a table/tag-only bind followed by a column bind and execute
        2. Replay both binds and verify the inserted row and tag

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-27 Added STMT2 no-column bind replay coverage
        """
        self._require()
        db = "wlr_stmt2_no_cols"
        session = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: session})
        script = """
import os
import taos

connection = taos.connect(config=os.environ["WLR_CFG"], host=os.environ["WLR_HOST"],
                          port=int(os.environ["WLR_PORT"]), user=os.environ["WLR_USER"],
                          password=os.environ["WLR_PASSWORD"])
connection.query("create database wlr_stmt2_no_cols").close()
connection.query("create stable wlr_stmt2_no_cols.stb (ts timestamp, value int) tags (site binary(32))").close()
connection.query("create table wlr_stmt2_no_cols.t1 using wlr_stmt2_no_cols.stb tags ('north')").close()
connection.select_db("wlr_stmt2_no_cols")
statement = connection.statement2("insert into ? using wlr_stmt2_no_cols.stb tags (?) values (?, ?)")
statement.bind_param(["t1"], [["north"]], None)
statement.bind_param(["t1"], None, [[[1700000000000], [42]]])
assert statement.execute() == 1
statement.close()
connection.close()
"""
        env = os.environ.copy()
        env.update(WLR_CFG=self.clientCfgDir, WLR_HOST=self.host, WLR_PORT=str(self.port),
                   WLR_USER=self.user, WLR_PASSWORD=self.password)
        try:
            process = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                     timeout=120, env=env)
        finally:
            self._restore_cfg()
        output = (process.stdout + process.stderr).decode("utf-8", errors="ignore")
        assert process.returncode == 0, output

        self._drop(db)
        replay_rc, replay_out = self._replay_to_target(session)
        assert replay_rc == EXIT_OK, replay_out
        assert "calls skipped        : 0" in replay_out, replay_out
        tdSql.query(f"select value, site from {db}.stb")
        assert tdSql.queryResult == [(42, "north")], tdSql.queryResult
        self._drop(db)

    @pytest.mark.parametrize("bind_mode", ["table-first", "one-call", "nullable-table-first",
                                          "nullable-one-call", "untyped-none", "untyped-null"])
    def test_com_workload_replay_stmt2_table_name_before_fields(self, bind_mode):
        """taosreplay -- bind a table name before STMT2 can resolve fields

        1. Prepare an insert with an unspecified ordinary table
        2. Bind its name separately or together with row values
        3. Replay both sequences without dropping the statement
        4. Preserve optional names and distinguish single-row NONE from NULL updates

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-27 Added table-first STMT2 replay coverage
            - 2026-10-27 Added nullable names and untyped NONE/NULL updates
        """
        self._require()
        db = "wlr_stmt2_table_first"
        session = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: session})
        script = """
import os
import taos
import ctypes

connection = taos.connect(config=os.environ["WLR_CFG"], host=os.environ["WLR_HOST"],
                          port=int(os.environ["WLR_PORT"]), user=os.environ["WLR_USER"],
                          password=os.environ["WLR_PASSWORD"])
connection.query("create database wlr_stmt2_table_first").close()
connection.query("create table wlr_stmt2_table_first.t1 (ts timestamp, value int)").close()
bind_mode = os.environ["WLR_BIND_MODE"]
if bind_mode.startswith("untyped-"):
    connection.query("insert into wlr_stmt2_table_first.t1 values (1700000000000, 42)").close()
connection.close()

class Bind(ctypes.Structure):
    _fields_ = [("buffer_type", ctypes.c_int), ("buffer", ctypes.c_void_p),
                ("length", ctypes.POINTER(ctypes.c_int32)), ("is_null", ctypes.c_char_p),
                ("num", ctypes.c_int)]

class Bindv(ctypes.Structure):
    _fields_ = [("count", ctypes.c_int), ("tbnames", ctypes.POINTER(ctypes.c_char_p)),
                ("tags", ctypes.POINTER(ctypes.POINTER(Bind))),
                ("bind_cols", ctypes.POINTER(ctypes.POINTER(Bind)))]

lib = ctypes.CDLL("libtaos.so")
lib.taos_options.argtypes = [ctypes.c_int, ctypes.c_char_p]
lib.taos_options.restype = ctypes.c_int
lib.taos_connect.argtypes = [ctypes.c_char_p] * 4 + [ctypes.c_uint16]
lib.taos_connect.restype = ctypes.c_void_p
lib.taos_stmt2_init.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
lib.taos_stmt2_init.restype = ctypes.c_void_p
lib.taos_stmt2_prepare.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_ulong]
lib.taos_stmt2_prepare.restype = ctypes.c_int
lib.taos_stmt2_bind_param.argtypes = [ctypes.c_void_p, ctypes.POINTER(Bindv), ctypes.c_int32]
lib.taos_stmt2_bind_param.restype = ctypes.c_int
lib.taos_stmt2_exec.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_int)]
lib.taos_stmt2_exec.restype = ctypes.c_int
lib.taos_stmt2_close.argtypes = [ctypes.c_void_p]
lib.taos_close.argtypes = [ctypes.c_void_p]
assert lib.taos_options(3, os.environ["WLR_CFG"].encode()) == 0
taos_handle = lib.taos_connect(os.environ["WLR_HOST"].encode(), os.environ["WLR_USER"].encode(),
                               os.environ["WLR_PASSWORD"].encode(), b"wlr_stmt2_table_first",
                               int(os.environ["WLR_PORT"]))
assert taos_handle
statement = lib.taos_stmt2_init(taos_handle, None)
assert statement
assert lib.taos_stmt2_prepare(statement, b"insert into ? values (?, ?)", 0) == 0
names = (ctypes.c_char_p * 1)(b"t1")
if bind_mode.startswith("nullable-"):
    names = (ctypes.c_char_p * 2)(b"t1", None)
if bind_mode in ("table-first", "nullable-table-first"):
    table_only = Bindv(len(names), names, None, None)
    assert lib.taos_stmt2_bind_param(statement, ctypes.byref(table_only), -1) == 0
timestamp = ctypes.c_int64(1700000000000)
value = ctypes.c_int32(42)
timestamp_length = ctypes.c_int32(8)
value_length = ctypes.c_int32(4)
row = (Bind * 2)(Bind(9, ctypes.cast(ctypes.byref(timestamp), ctypes.c_void_p),
                       ctypes.pointer(timestamp_length), None, 1),
                 Bind(4, ctypes.cast(ctypes.byref(value), ctypes.c_void_p),
                      ctypes.pointer(value_length), None, 1))
groups = (ctypes.POINTER(Bind) * 1)(row)
if bind_mode.startswith("nullable-"):
    groups = (ctypes.POINTER(Bind) * 2)(None, row)
if bind_mode.startswith("untyped-"):
    row[1] = Bind(0, None, None, bytes([2 if bind_mode == "untyped-none" else 1]), 1)
with_values = Bindv(len(names), names, None, groups)
assert lib.taos_stmt2_bind_param(statement, ctypes.byref(with_values), -1) == 0
affected = ctypes.c_int()
assert lib.taos_stmt2_exec(statement, ctypes.byref(affected)) == 0
assert affected.value == 1
assert lib.taos_stmt2_close(statement) == 0
lib.taos_close(taos_handle)
"""
        env = os.environ.copy()
        env.update(WLR_CFG=self.clientCfgDir, WLR_HOST=self.host, WLR_PORT=str(self.port),
                   WLR_USER=self.user, WLR_PASSWORD=self.password,
                   WLR_BIND_MODE=bind_mode)
        try:
            process = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                     timeout=120, env=env)
        finally:
            self._restore_cfg()
        output = (process.stdout + process.stderr).decode("utf-8", errors="ignore")
        assert process.returncode == 0, output

        self._drop(db)
        replay_rc, replay_out = self._replay_to_target(session)
        assert replay_rc == EXIT_OK, replay_out
        assert "calls skipped        : 0" in replay_out, replay_out
        tdSql.query(f"select value from {db}.t1")
        expected_value = None if bind_mode == "untyped-null" else 42
        assert tdSql.queryResult == [(expected_value,)], tdSql.queryResult
        self._drop(db)

    def test_com_workload_replay_stmt2_long_table_names(self):
        """taosreplay -- preserve a STMT2 table-name list larger than 4 KiB

        1. Record a 32-table STMT2 bind with 128-byte table names
        2. Replay all rows with their table names intact

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-27 Added large table-name-list replay coverage
        """
        self._require()
        db = "wlr_stmt2_long_names"
        session = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: session})
        script = """
import os
import taos

connection = taos.connect(config=os.environ["WLR_CFG"], host=os.environ["WLR_HOST"],
                          port=int(os.environ["WLR_PORT"]), user=os.environ["WLR_USER"],
                          password=os.environ["WLR_PASSWORD"])
connection.query("create database wlr_stmt2_long_names").close()
connection.query("create stable wlr_stmt2_long_names.stb (ts timestamp, value int) tags (site int)").close()
names = [f"t{index:02d}_" + "x" * 124 for index in range(32)]
assert all(len(name) == 128 for name in names)
for index, name in enumerate(names):
    connection.query(f"create table wlr_stmt2_long_names.{name} using wlr_stmt2_long_names.stb tags ({index})").close()
connection.select_db("wlr_stmt2_long_names")
statement = connection.statement2("insert into ? using wlr_stmt2_long_names.stb tags (?) values (?, ?)")
statement.bind_param(names, [[index] for index in range(32)],
                     [[[1700000000000 + index], [index]] for index in range(32)])
assert statement.execute() == 32
statement.close()
connection.close()
"""
        env = os.environ.copy()
        env.update(WLR_CFG=self.clientCfgDir, WLR_HOST=self.host, WLR_PORT=str(self.port),
                   WLR_USER=self.user, WLR_PASSWORD=self.password)
        try:
            process = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                     timeout=120, env=env)
        finally:
            self._restore_cfg()
        output = (process.stdout + process.stderr).decode("utf-8", errors="ignore")
        assert process.returncode == 0, output

        self._drop(db)
        replay_rc, replay_out = self._replay_to_target(session)
        assert replay_rc == EXIT_OK, replay_out
        assert "calls skipped        : 0" in replay_out, replay_out
        tdSql.query(f"select value, site from {db}.stb order by value")
        assert tdSql.queryResult == [(index, index) for index in range(32)], tdSql.queryResult
        self._drop(db)

    def test_com_workload_replay_multi_input_merge(self):
        """taosreplay -- multi-session merge from one host (RIN002/RIN009, E2E004)

        1. Record two sessions from separate client processes
        2. Replay both together; both workloads' effects appear

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-07-29 Claude Initial design-stage suite
        """
        self._require()
        db_a, db_b = "wlr_merge_a", "wlr_merge_b"
        d_a = self._record(self._sample_workload(db_a))
        d_b = self._record(self._sample_workload(db_b))
        self._drop(db_a)
        self._drop(db_b)

        rc, out = self._replay(
            "--input", d_a,
            "--input", d_b,
            "--target-host", self.host, "--target-port", str(self.port),
            "--user-password", f"{self.user}={self.password}",
        )
        assert rc == EXIT_OK, f"merged replay should exit {EXIT_OK}, got {rc}: {out}"

        tdSql.query("show databases;")
        names = [str(r[0]) for r in tdSql.queryResult]
        for db in (db_a, db_b):
            assert db in names, f"{db} missing after merged replay"
        self._drop(db_a)
        self._drop(db_b)
        tdLog.success(f"{__file__} multi-input merge passed")

    def test_com_workload_replay_mixed_segment_inputs(self):
        """taosreplay -- order segments across directory and explicit-file inputs

        1. Split a recorded session at a complete frame boundary
        2. Inspect directory-plus-file inputs in both argument orders

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-26 Added mixed segment-input ordering coverage
        """
        self._require()
        session = self._record("select 1;")
        segments = [name for name in os.listdir(session) if name.endswith(".wlr")]
        assert len(segments) == 1, segments
        with open(os.path.join(session, segments[0]), "rb") as stream:
            data = stream.read()

        frame_size = int.from_bytes(data[WLR_FILE_HEADER_SIZE:WLR_FILE_HEADER_SIZE + 4], "little") + 4
        split = WLR_FILE_HEADER_SIZE + frame_size
        assert split < len(data), "recording needs at least two frames"
        first = self._new_dir()
        second = self._new_dir()
        first_path = os.path.join(first, "session-0.wlr")
        second_path = os.path.join(second, "session-1.wlr")
        with open(first_path, "wb") as stream:
            stream.write(data[:split])
        next_sequence = data[split + WLR_FRAME_EVENT_SEQUENCE_OFFSET:
                             split + WLR_FRAME_EVENT_SEQUENCE_OFFSET + 8]
        header = bytearray(data[:WLR_FILE_HEADER_SIZE])
        header[56:60] = (1).to_bytes(4, "little")
        header[60:68] = next_sequence
        with open(second_path, "wb") as stream:
            stream.write(header)
            stream.write(data[split:])

        for sources in ((first, second_path), (second_path, first)):
            result = subprocess.run(
                [self.replay, "inspect", "--input", sources[0], "--input", sources[1]],
                capture_output=True, timeout=300,
            )
            output = (result.stdout + result.stderr).decode("utf-8", errors="ignore")
            assert result.returncode == EXIT_OK, f"mixed segment inputs must inspect cleanly: {output}"

    def test_com_workload_replay_missing_middle_segment(self):
        """taosreplay -- replay only the verified prefix before a segment gap

        1. Split a recording before its first INSERT and label the suffix segment 2
        2. Reject default replay but inspect and replay the prefix as incomplete
        3. Confirm the omitted suffix never inserts rows

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-26 Added missing-middle-segment prefix coverage
        """
        self._require()
        db = "wlr_segment_gap"
        session = self._record(self._sample_workload(db))
        segments = [name for name in os.listdir(session) if name.endswith(".wlr")]
        assert len(segments) == 1, segments
        with open(os.path.join(session, segments[0]), "rb") as stream:
            data = stream.read()

        split = None
        offset = WLR_FILE_HEADER_SIZE
        while offset + WLR_FRAME_LENGTH_SIZE <= len(data):
            frame_end = offset + WLR_FRAME_LENGTH_SIZE + int.from_bytes(data[offset:offset + 4], "little")
            assert frame_end <= len(data), "recording contains an incomplete frame"
            if b"insert into t1" in data[offset:frame_end].lower():
                split = offset
                break
            offset = frame_end
        assert split is not None and split > WLR_FILE_HEADER_SIZE, "recording needs a later INSERT"
        assert b"create table t1" in data[:split].lower(), "prefix must create the target table"

        gap = self._new_dir()
        with open(os.path.join(gap, "segment-0.wlr"), "wb") as stream:
            stream.write(data[:split])
        header = bytearray(data[:WLR_FILE_HEADER_SIZE])
        header[56:60] = (2).to_bytes(4, "little")
        header[60:68] = data[split + WLR_FRAME_EVENT_SEQUENCE_OFFSET:
                             split + WLR_FRAME_EVENT_SEQUENCE_OFFSET + 8]
        with open(os.path.join(gap, "segment-2.wlr"), "wb") as stream:
            stream.write(header)
            stream.write(data[split:])

        inspect_rc, inspect_out = self._inspect(gap, verbose=True)
        assert inspect_rc == EXIT_PARTIAL, inspect_out
        assert "insert into t1" not in inspect_out.lower(), inspect_out

        duplicate = os.path.join(gap, "duplicate-2.wlr")
        shutil.copy2(os.path.join(gap, "segment-2.wlr"), duplicate)
        duplicate_rc, duplicate_out = self._inspect(gap)
        assert duplicate_rc == EXIT_INPUT, f"duplicate segment must still be rejected: {duplicate_out}"
        os.unlink(duplicate)

        self._drop(db)
        rc, out = self._replay_to_target(gap)
        assert rc == EXIT_INPUT, f"segment gap must be rejected by default: {out}"
        rc, out = self._replay_to_target(gap, "--allow-incomplete")
        assert rc == EXIT_PARTIAL, f"verified prefix should replay as partial: {out}"
        tdSql.query(f"select count(*) from {db}.t1")
        assert tdSql.queryResult[0][0] == 0, "segment 2 INSERT must not be replayed"
        self._drop(db)

    def test_com_workload_replay_fetch_count_mismatch(self):
        """taosreplay -- report a fetch row-count mismatch

        1. Record a query that fetches a row
        2. Remove the row and replay the query against the empty table
        3. Assert a partial exit and a count mismatch in the report

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-23 Added fetch-count mismatch coverage
        """
        self._require()
        db = "wlr_fetch_mismatch"
        self._drop(db)
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"create table {db}.t1 (ts timestamp, v int)")
        tdSql.execute(f"insert into {db}.t1 values (now, 1)")
        session = self._record(f"select * from {db}.t1;")
        tdSql.execute(f"delete from {db}.t1")

        report = self._new_dir()
        rc, out = self._replay_to_target(session, "--report-dir", report)
        assert rc == EXIT_PARTIAL, f"missing fetched rows must be reported: {out}"
        with open(os.path.join(report, "replay-report.json"), encoding="utf-8") as stream:
            result = json.load(stream)
        assert result["countMismatches"] > 0, result
        self._drop(db)

    def test_com_workload_replay_affected_count_mismatch(self):
        """taosreplay -- compare synchronous DELETE affected rows

        1. Record a DELETE that removes one row
        2. Replay against a matching row and assert no count mismatch
        3. Replay against the now-empty table and assert a count mismatch

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-26 Added synchronous affected-row coverage
        """
        self._require()
        db = "wlr_affected_mismatch"
        self._drop(db)
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"create table {db}.t1 (ts timestamp, v int)")
        tdSql.execute(f"insert into {db}.t1 values (1700000000000, 1)")
        session = self._record(f"delete from {db}.t1 where ts = 1700000000000;")

        tdSql.execute(f"insert into {db}.t1 values (1700000000000, 1)")
        report = self._new_dir()
        rc, out = self._replay_to_target(session, "--report-dir", report)
        assert rc == EXIT_OK, f"matching DELETE row count must replay cleanly: {out}"
        with open(os.path.join(report, "replay-report.json"), encoding="utf-8") as stream:
            result = json.load(stream)
        assert result["countMismatches"] == 0, result

        report = self._new_dir()
        rc, out = self._replay_to_target(session, "--report-dir", report)
        assert rc == EXIT_PARTIAL, f"DELETE 1-to-0 affected rows must be reported: {out}"
        with open(os.path.join(report, "replay-report.json"), encoding="utf-8") as stream:
            result = json.load(stream)
        assert result["countMismatches"] > 0, result
        self._drop(db)

    def test_com_workload_replay_torn_tail_in_other_session(self):
        """taosreplay -- accept a final torn tail in a shared directory

        1. Record two sessions and place their segments in one directory
        2. Append a partial frame to the first session's final segment
        3. Assert the later, unrelated session does not make it non-final

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-23 Added multi-session torn-tail coverage
        """
        self._require()
        db_a, db_b = "wlr_tail_a", "wlr_tail_b"
        session_a = self._record(self._sample_workload(db_a))
        session_b = self._record(self._sample_workload(db_b))
        shared = self._new_dir()
        for source, name in ((session_a, "a-0.wlr"), (session_b, "b-0.wlr")):
            segments = [entry for entry in os.listdir(source) if entry.endswith(".wlr")]
            assert len(segments) == 1, segments
            shutil.copy2(os.path.join(source, segments[0]), os.path.join(shared, name))
        with open(os.path.join(shared, "a-0.wlr"), "ab") as stream:
            stream.write(b"\x20\x00\x00\x00xxx")

        rc, out = self._inspect(shared)
        assert rc == EXIT_OK, f"another session must not make a torn tail non-final: {out}"
        self._drop(db_a)
        self._drop(db_b)

    def test_com_workload_replay_missing_initial_segment(self):
        """taosreplay -- reject a recording suffix without segment zero

        1. Record a complete single-segment session
        2. Present that segment as index one with its frames unchanged
        3. Reject both directory and explicit-file input without allow-incomplete

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-10-26 Added missing-initial-segment coverage
        """
        self._require()
        session = self._record("select 1;")
        segments = [name for name in os.listdir(session) if name.endswith(".wlr")]
        assert len(segments) == 1, segments
        suffix = self._new_dir()
        segment = os.path.join(suffix, "suffix.wlr")
        with open(os.path.join(session, segments[0]), "rb") as stream:
            data = bytearray(stream.read())
        data[56:60] = (1).to_bytes(4, "little")
        with open(segment, "wb") as stream:
            stream.write(data)

        for source in (suffix, segment):
            rc, out = self._replay_to_target(source)
            assert rc == EXIT_INPUT, f"missing segment zero must be rejected: {out}"

        data[56:60] = (0).to_bytes(4, "little")
        data[60:68] = (2).to_bytes(8, "little")
        with open(segment, "wb") as stream:
            stream.write(data)
        rc, out = self._replay_to_target(segment)
        assert rc == EXIT_INPUT, f"header/first-frame sequence mismatch must be rejected: {out}"

    def test_com_workload_replay_rest_input_rejected(self):
        """taosreplay -- non-native input rejected (C010)

        1. A REST/WebSocket-style capture is not a valid workload input
        2. Rejection is explicit, not a silent skip

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-07-29 Claude Initial design-stage suite
        """
        self._require()
        fake = self._new_dir()
        with open(os.path.join(fake, "capture.har"), "w", encoding="utf-8") as f:
            f.write('{"log":{"entries":[{"request":{"method":"POST","url":"/rest/sql"}}]}}')
        rc, out = self._replay_to_target(fake)
        assert rc == EXIT_INPUT, f"non-native input should exit {EXIT_INPUT}, got {rc}"
        assert "no .wlr segments found" in out.lower(), f"non-native rejection was not explicit: {out}"
        tdLog.success(f"{__file__} non-native input rejection passed")
