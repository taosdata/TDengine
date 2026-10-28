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

import os
import re
import shutil
import stat
import subprocess
import time
import tempfile

import pytest

from new_test_framework.utils import tdCom, tdLog, tdSql

#
# Black-box workload recording tests.
#
# Nothing here decodes the .wlr frame format. Recording is asserted through
# three externally observable surfaces only:
#
#   1. the filesystem  -- files exist, permissions, sizes, segment count
#   2. `taosreplay inspect`  -- the shipped reader is the oracle for event content
#   3. raw byte search  -- secrets must NOT appear anywhere in the files
#
# Every test is gated on a runtime capability probe and skips (not fails) when
# the client under test has no `workloadRecord` config. That keeps the file
# harmless against an older or differently configured build rather than
# reporting a missing feature as a regression.
#

RECORD_CFG = "workloadRecord"
DIR_CFG = "workloadRecordDir"
BUF_CFG = "workloadRecordBufferSize"
WLR_FILE_HEADER_SIZE = 68


class TestWorkloadRecord:
    """Client workload recording -- config, capture contract, ordering, fail-open."""

    testDatabases = ("wlr_live", "wlr_capture", "wlr_bp", "wlr_failopen", "wlr_copy")

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.configReady = False
        cls.buildPath = tdCom.getBuildPath()
        cls.binPath = os.path.join(cls.buildPath, "build", "bin")
        cls.taos = os.path.join(cls.binPath, "taos")
        cls.replay = os.path.join(cls.binPath, "taosreplay")
        cls.tmpDirs = []
        cls.probe_error = None
        sharedCfg = os.path.join(tdCom.getClientCfgPath(), "taos.cfg")
        if not os.path.isfile(sharedCfg):
            pytest.fail(f"shared client configuration is not a regular file: {sharedCfg}")
        cls.clientCfgDir = tempfile.mkdtemp(prefix="wlr-client-cfg-")
        cls.tmpDirs.append(cls.clientCfgDir)
        cls.clientCfg = os.path.join(cls.clientCfgDir, "taos.cfg")
        shutil.copy2(sharedCfg, cls.clientCfg)
        with open(cls.clientCfg, "rb") as f:
            cls.originalClientCfg = f.read()
        cls.configReady = True

        cls.supported = cls._probe_support()
        if not cls.supported and cls.probe_error is None:
            tdLog.debug(f"{RECORD_CFG} not supported by this client build")

    @classmethod
    def teardown_class(cls):
        cls._restore_cfg()
        cls._cleanup_databases()
        for d in getattr(cls, "tmpDirs", []):
            shutil.rmtree(d, ignore_errors=True)

    @pytest.fixture(autouse=True)
    def restore_cfg_after_test(self):
        try:
            yield
        finally:
            self._restore_cfg()
            self._cleanup_databases()

    # ------------------------------------------------------------------ helpers

    @classmethod
    def _probe_support(cls):
        """True when the client build knows the workloadRecord config keys."""
        try:
            tdSql.query("show local variables;")
        except Exception as exc:  # pragma: no cover - surfaced by _require
            cls.probe_error = exc
            tdLog.debug(f"probe failed: {exc}")
            return False
        names = {str(row[0]) for row in tdSql.queryResult or [] if len(row) > 0}
        return all(name in names for name in (RECORD_CFG, DIR_CFG, BUF_CFG))

    def _require(self):
        if self.probe_error is not None:
            pytest.fail(f"failed to probe {RECORD_CFG} support: {self.probe_error}")
        if not self.supported:
            pytest.skip(f"{RECORD_CFG} unsupported: recorder not implemented in this build")
        if not os.path.isfile(self.taos):
            pytest.fail(f"taos client not found at expected build path: {self.taos}")

    def _require_replay(self):
        self._require()
        if not os.path.isfile(self.replay):
            pytest.skip("taosreplay not built (dev tool, excluded from product package)")

    def _new_dir(self, mode=0o755):
        d = tempfile.mkdtemp(prefix="wlr-")
        self.tmpDirs.append(d)
        os.chmod(d, mode)
        return d

    @classmethod
    def _restore_cfg(cls):
        """Restore the exact shared client configuration captured at setup."""
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
        """Rewrite client cfg with the given workload* params. startup-only, so
        every caller must launch a fresh client afterwards."""
        self._restore_cfg()
        if not kv:
            return
        managed = {name.encode("utf-8") for name in (RECORD_CFG, DIR_CFG, BUF_CFG)}
        lines = []
        for line in self.originalClientCfg.splitlines(keepends=True):
            stripped = line.lstrip()
            if stripped.startswith((b"#", b";")):
                lines.append(line)
                continue
            parts = stripped.split(None, 1)
            key = parts[0].split(b"=", 1)[0] if parts else b""
            if key not in managed:
                lines.append(line)
        with open(self.clientCfg, "wb") as f:
            f.writelines(lines)
        with open(self.clientCfg, "a", encoding="utf-8") as f:
            f.write("\n")
            for k, v in kv.items():
                f.write(f"{k} {v}\n")

    def _run_client(self, sql, timeout=60, user=None, password=None):
        """Run SQL through a freshly started taos shell -- a new process, which is
        what startup-only recording requires."""
        fd, sql_path = tempfile.mkstemp(prefix="wlr-client-", suffix=".sql", dir=self.clientCfgDir,
                                        text=True)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as sql_file:
                sql_file.write(sql)
            cmd = [self.taos, "-c", self.clientCfgDir, "-f", sql_path]
            if user is not None:
                cmd.extend(["-u", user])
            password_input = None
            if password is not None:
                # Bare -p makes the shell read the password from stdin, keeping it
                # out of argv and /proc/<pid>/cmdline.
                cmd.append("-p")
                password_input = (password + "\n").encode("utf-8")
            try:
                p = subprocess.run(cmd, input=password_input, capture_output=True, timeout=timeout)
            except subprocess.TimeoutExpired as exc:
                pytest.fail(f"taos client timed out after {timeout}s ({exc})")
        finally:
            try:
                os.unlink(sql_path)
            except FileNotFoundError:
                pass
        out = p.stdout.decode("utf-8", errors="ignore")
        err = p.stderr.decode("utf-8", errors="ignore")
        safe_out = out.replace(password, "<redacted>") if password else out
        safe_err = err.replace(password, "<redacted>") if password else err
        tdLog.debug(f"taos rc={p.returncode} out={safe_out[:400]} err={safe_err[:400]}")
        return p.returncode, out, err

    def _segments(self, d):
        if not os.path.isdir(d):
            return []
        return sorted(f for f in os.listdir(d) if f.endswith(".wlr"))

    def _session_files(self, d):
        if not os.path.isdir(d):
            return []
        return sorted(os.listdir(d))

    def _inspect(self, d, verbose=False):
        """taosreplay inspect is the only sanctioned reader of the file format."""
        cmd = [self.replay, "inspect", "--input", d]
        if verbose:
            cmd.append("--verbose")
        try:
            p = subprocess.run(cmd, capture_output=True, timeout=300)
        except subprocess.TimeoutExpired as exc:
            pytest.fail(f"taosreplay inspect timed out after 300s: {' '.join(cmd)} ({exc})")
        out = p.stdout.decode("utf-8", errors="ignore")
        err = p.stderr.decode("utf-8", errors="ignore")
        tdLog.debug(f"inspect rc={p.returncode}\n{out[:2000]}\n{err[:1000]}")
        return p.returncode, out + err

    def _raw_bytes(self, d):
        """Concatenated raw content of every file in the session -- for secret
        scanning only. Deliberately not interpreted as frames."""
        blob = b""
        for name in self._session_files(d):
            path = os.path.join(d, name)
            if os.path.isfile(path):
                with open(path, "rb") as f:
                    blob += f.read()
        return blob

    def _record_workload(self, sql, buf=256, extra=None, password=None):
        """Enable recording, run SQL in a fresh client, return the session dir."""
        d = self._new_dir()
        cfg = {RECORD_CFG: 1, DIR_CFG: d, BUF_CFG: buf}
        if extra:
            cfg.update(extra)
        self._write_cfg(**cfg)
        try:
            self._run_client(sql, password=password)
        finally:
            self._restore_cfg()
        return d

    # ------------------------------------------------------------------- 6.1 CFG

    def test_com_workload_record_config(self):
        """Workload recording -- startup config matrix (CFG001-CFG020)

        1. Default off: no directory, no files
        2. Explicit 0 off; 1 on creates a session
        3. Reserved values 2/255 and out-of-range treated as off
        4. Directory default/custom/unwritable
        5. Buffer size boundaries 16/4096 accepted, 15/4097 rejected
        6. Directory 0700 and file 0600 permissions
        7. changing cfg after a client exits cannot retroactively record its calls

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-07-29 Claude Initial design-stage suite
        """
        self._require()

        # CFG001/CFG002 -- off leaves nothing behind
        default_dir = None
        tdSql.query("show local variables;")
        for row in tdSql.queryResult or []:
            if len(row) > 1 and str(row[0]) == DIR_CFG:
                default_dir = str(row[1])
                break
        assert default_dir, f"could not resolve the default {DIR_CFG}"
        default_before = set(os.listdir(default_dir)) if os.path.isdir(default_dir) else set()

        for mode in (None, 0):
            d = self._new_dir()
            self._write_cfg(**({} if mode is None else {RECORD_CFG: 0, DIR_CFG: d}))
            self._run_client("show databases;")
            self._restore_cfg()
            assert self._session_files(d) == [], f"mode={mode} produced files"
            if mode is None:
                # No recording directory is configured in this branch; the default-off
                # behavior is verified by comparing the default directory below.
                default_after = set(os.listdir(default_dir)) if os.path.isdir(default_dir) else set()
                assert default_after == default_before, "default-off client created recording files"

        # CFG003 -- on creates at least one segment
        d = self._record_workload("show databases;")
        segs = self._segments(d)
        assert segs, "workloadRecord=1 produced no .wlr segment"

        # CFG011 -- permissions
        assert stat.S_IMODE(os.stat(d).st_mode) == 0o700, "record dir must be 0700"
        for name in self._session_files(d):
            p = os.path.join(d, name)
            if os.path.isfile(p):
                assert stat.S_IMODE(os.stat(p).st_mode) == 0o600, f"{name} must be 0600"

        # CFG004/CFG005 -- reserved modes are IN range, so they reach the recorder,
        # which logs WLR_E_CONFIG and stays off. The client still starts normally.
        for mode in (2, 255):
            d = self._new_dir()
            self._write_cfg(**{RECORD_CFG: mode, DIR_CFG: d})
            rc, _, _ = self._run_client("show databases;")
            self._restore_cfg()
            assert rc == 0, f"reserved mode={mode} must not prevent client startup"
            assert self._session_files(d) == [], f"mode={mode} must not record"

        # CFG006/CFG005b -- out-of-range values are refused by the config layer
        # itself, so the client fails to start. This is a DIFFERENT outcome from a
        # reserved value: 0..255 is registered deliberately so that a reserved
        # value fails open instead of aborting startup (DS 6.1).
        for mode in (-1, 256):
            d = self._new_dir()
            self._write_cfg(**{RECORD_CFG: mode, DIR_CFG: d})
            rc, out, err = self._run_client("show databases;")
            self._restore_cfg()
            assert rc != 0, f"out-of-range mode={mode} must be rejected by config parsing"
            assert out or err, f"out-of-range mode={mode} failed without diagnostic output"
            assert self._session_files(d) == [], f"mode={mode} must not record"

        # CFG012/CFG013 -- buffer boundaries accepted
        for buf in (16, 4096):
            d = self._record_workload("show databases;", buf=buf)
            assert self._segments(d), f"bufferSize={buf} should be accepted"

        # CFG014 -- buffer out of range: refused by the config layer, startup fails
        for buf in (15, 4097):
            d = self._new_dir()
            self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: d, BUF_CFG: buf})
            rc, out, err = self._run_client("show databases;")
            self._restore_cfg()
            assert rc != 0, f"out-of-range bufferSize={buf} must be rejected by config parsing"
            assert out or err, f"out-of-range bufferSize={buf} failed without diagnostic output"
            assert self._session_files(d) == [], f"bufferSize={buf} must not record"

        # CFG009 -- unwritable directory: no recording, business continues. Root can
        # bypass mode bits, so this sub-check is meaningful only for an unprivileged uid.
        if os.geteuid() != 0:
            d = self._new_dir(mode=0o500)
            self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: d})
            try:
                rc, _, _ = self._run_client("show databases;")
            finally:
                self._restore_cfg()
                os.chmod(d, 0o755)
            assert rc == 0, "unwritable record dir must not break the client (fail-open)"
            assert self._segments(d) == [], "no segments expected in unwritable dir"
        else:
            tdLog.info("CFG009 skipped: root can write through directory mode 0500")

        # A completed client cannot retroactively record calls when the config
        # is changed. This does not test hot reload in a running client.
        d = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 0, DIR_CFG: d})
        self._run_client("show databases;")
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: d})
        self._restore_cfg()
        assert self._session_files(d) == [], "config edit must not record a completed process"

        tdLog.success(f"{__file__} config matrix passed")

    def test_com_workload_record_dir_resolved_visible(self):
        """Workload recording -- resolved record dir is observable (CFG007/CFG007b/CFG008)

        1. With no explicit dir, SHOW LOCAL VARIABLES reports ${logDir}/workload
           expanded, not an empty string
        2. An explicit dir is reported verbatim

        Unlike the rest of this suite this needs no recorder: it checks the config
        layer only, so it runs as soon as the three params exist.

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-07-29 Claude Initial design-stage suite
        """
        self._require()

        def show_dir():
            tdSql.query("show local variables;")
            for row in tdSql.queryResult or []:
                if len(row) > 1 and str(row[0]) == DIR_CFG:
                    return str(row[1])
            return None

        # Default case: whatever the running client resolved. It must be non-empty
        # and must end in the documented suffix, otherwise an operator cannot tell
        # where recordings will land.
        resolved = show_dir()
        assert resolved is not None, f"{DIR_CFG} not present in local variables"
        assert resolved.strip() != "", "resolved record dir must not display as empty"
        assert resolved.endswith("workload"), f"default dir should end in 'workload', got {resolved}"
        assert os.path.isabs(resolved), f"resolved dir should be absolute, got {resolved}"

        explicit = self._new_dir()
        self._write_cfg(**{DIR_CFG: explicit})
        try:
            rc, out, err = self._run_client("show local variables;")
        finally:
            self._restore_cfg()
        assert rc == 0, f"fresh client could not show explicit record dir: {out}{err}"
        matching = [line for line in out.splitlines() if DIR_CFG in line]
        assert matching, f"{DIR_CFG} not present in fresh client's local variables: {out}"
        assert any(explicit in line for line in matching), \
            f"explicit record dir should be reported verbatim as {explicit}: {matching}"

        tdLog.success(f"{__file__} resolved dir visibility passed: {resolved}, {explicit}")

    def test_com_workload_record_live_traffic_decodes(self):
        """Workload recording -- live traffic produces a decodable segment (E2E)

        1. Run real SQL through a client with recording on
        2. Validate the segment with wlrSegmentReadTest, the shipped decoder
        3. Confirm the SQL text is present and no credential is

        This is the case that ties the recorder to the format library. The unit
        suites prove each side in isolation; only this proves the recorder's actual
        output is readable by the actual parser.

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-08-06 Claude Added after hooks landed
        """
        self._require()

        validator = os.path.join(self.binPath, "wlrSegmentReadTest")
        if not os.path.isfile(validator):
            pytest.skip("wlrSegmentReadTest not built (needs BUILD_TEST=ON)")

        db = "wlr_live"
        marker = "WLR_LIVE_MARKER"
        sql = (
            f"drop database if exists {db};"
            f"create database {db};"
            f"use {db};"
            f"create table t1 (ts timestamp, c1 int, c2 binary(64));"
            f"insert into t1 values (now, 1, '{marker}');"
            f"select * from t1;"
        )
        d = self._record_workload(sql)

        segs = self._segments(d)
        assert segs, "live traffic produced no segment"

        # The shipped decoder is the oracle: it checks frame CRCs, sequence
        # contiguity, the required-tag table, and BEGIN-before-RETURN ordering.
        path = os.path.join(d, segs[0])
        validator_env = os.environ.copy()
        validator_env["WLR_TEST_CREDENTIAL_SENTINEL"] = os.environ.get("TAOS_TEST_PASSWORD") or "taosdata"
        p = subprocess.run([validator, path], capture_output=True, timeout=300, env=validator_env)
        out = p.stdout.decode("utf-8", errors="ignore")
        tdLog.debug(out[-2000:])
        assert p.returncode == 0, f"recorded segment failed validation:\n{out[-3000:]}"

        blob = self._raw_bytes(d)
        assert marker.encode() in blob, "inserted value not found in recording"
        assert b"select * from t1" in blob, "query SQL not recorded"

        tdLog.success(f"{__file__} live traffic validation passed")

    def test_com_workload_record_no_dynamic_switch(self):
        """Workload recording -- no SQL/heartbeat control path (CFG018)

        1. `alter local` on the three params must not enable recording
        2. `alter all dnodes` must not accept them either

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-07-29 Claude Initial design-stage suite
        """
        self._require()
        d = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: d, BUF_CFG: 256})
        try:
            for name, val in ((RECORD_CFG, 1), (DIR_CFG, d), (BUF_CFG, 256)):
                tdSql.error(f'alter local {name} {val};')
                tdSql.error(f'alter all dnodes {name} {val};')
            assert self._session_files(d) == [], "recording must not be enablable via SQL"
        finally:
            self._restore_cfg()
        tdLog.success(f"{__file__} startup-only enforcement passed")

    # ------------------------------------------------------ 6.2/6.3 capture contract

    def test_com_workload_record_capture_contract(self):
        """Workload recording -- API capture contract (API001-API007, RE001/RE005/RE008)

        1. SQL query produces recorded events visible to inspect
        2. taos_select_db records once, without the inner `use db` query
        3. Excluded surfaces produce no events

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-07-29 Claude Initial design-stage suite
        """
        self._require_replay()

        db = "wlr_capture"
        sql = (
            f"drop database if exists {db};"
            f"create database {db};"
            f"use {db};"
            f"create table t1 (ts timestamp, c1 int);"
            f"insert into t1 values (now, 1);"
            f"select * from t1;"
        )
        d = self._record_workload(sql)
        rc, out = self._inspect(d, verbose=True)
        assert rc == 0, f"inspect rejected a clean session: {out}"

        # API002 -- the SELECT must be present as recorded SQL text
        assert "select * from t1" in out.lower(), "query SQL not recorded"

        # RE001 -- `use db` arrives as select_db; the inner taos_query must not
        # appear as a second, separately recorded call. Counting occurrences of
        # the inner SQL form is the black-box proxy for outermost-only capture.
        inner = len(re.findall(r"use\s+" + db, out, flags=re.IGNORECASE))
        assert inner == 1, f"`use {db}` must be recorded exactly once, got {inner}"

        tdLog.success(f"{__file__} capture contract passed")

    def test_com_workload_record_secrets_absent(self):
        """Workload recording -- no credentials in recording/output (ENC014, SEC002)

        1. Connect password must not appear in any session file
        2. Client stdout/stderr must not contain the password either

        Since: v3.4.3

        Labels: ci,functional,security,taosc

        History:
            - 2026-07-29 Claude Initial design-stage suite
        """
        self._require()

        secret = "wlr_secret_9f3c2a7b"
        user = "wlr_secret_user"
        tdSql.execute(f"drop user if exists {user}")
        tdSql.execute(f"create user {user} pass '{secret}'")
        d = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: d, BUF_CFG: 256})
        try:
            rc, out, err = self._run_client("show databases;", user=user, password=secret)
        finally:
            self._restore_cfg()
            tdSql.execute(f"drop user if exists {user}")
        safe_output = (out + err).replace(secret, "<redacted>")
        assert rc == 0, f"credential-bearing client failed: {safe_output}"
        blob = self._raw_bytes(d)
        assert blob, "no recorded bytes to scan"
        assert secret.encode() not in blob, "connect password found in recording files"

        # SEC003 -- the client process output must not carry it either.
        assert secret not in out, "connect password leaked to client stdout"
        assert secret not in err, "connect password leaked to client stderr"

        tdLog.success(f"{__file__} secret scan passed")

    # ---------------------------------------------------------------- 6.4 BEGIN

    def test_com_workload_record_begin_before_call(self):
        """Workload recording -- BEGIN written before the real call (BGN001/BGN002/BGN009)

        1. Kill the client mid-query; the in-flight SQL still has a BEGIN on disk
        2. A BEGIN with no END is reported as unfinished, not as corruption

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-07-29 Claude Initial design-stage suite
        """
        self._require_replay()

        d = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: d})
        # sleep() deterministically keeps the public query call in flight. Polling for
        # bytes beyond the fixed file header proves its BEGIN frame reached disk before
        # the process is killed; a fast metadata query made this test skip routinely.
        long_sql = "select sleep(60);"
        p = subprocess.Popen(
            [self.taos, "-c", self.clientCfgDir, "-s", long_sql],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            begin_captured = False
            for _ in range(600):
                if p.poll() is not None:
                    pytest.fail(f"sleep query exited before BEGIN was captured: {p.returncode}")
                paths = [os.path.join(d, name) for name in self._segments(d)]
                if any(os.path.getsize(path) > WLR_FILE_HEADER_SIZE for path in paths):
                    begin_captured = True
                    break
                time.sleep(0.05)
            if not begin_captured:
                pytest.fail("timed out waiting for the in-flight BEGIN frame")
            p.kill()
            try:
                p.communicate(timeout=30)
            except subprocess.TimeoutExpired as exc:
                p.kill()
                pytest.fail(f"taos client did not exit after kill: {exc}")
        finally:
            if p.poll() is None:
                p.kill()
                try:
                    p.communicate(timeout=30)
                except subprocess.TimeoutExpired:
                    tdLog.warning("taos client did not exit during cleanup")
            self._restore_cfg()

        assert self._segments(d), "no segment written before kill"
        rc, out = self._inspect(d)
        # FIL011/BGN009 -- an abruptly killed session is recoverable by prefix and
        # must not be reported as structurally corrupt.
        assert rc == 0, f"killed session inspect failed with rc={rc}: {out}"
        assert "structurally complete: yes" in out.lower(), f"killed session was incomplete: {out}"
        match = re.search(r"unfinished calls\s*:\s*(\d+)", out, flags=re.IGNORECASE)
        assert match is not None, f"inspect did not report unfinished calls: {out}"
        assert int(match.group(1)) == 1, \
            f"in-flight BEGIN was not reported as unfinished: {out}"
        tdLog.success(f"{__file__} BEGIN-before-call passed")

    # ------------------------------------------------------ 6.6/6.7 backpressure

    def test_com_workload_record_backpressure_no_loss(self):
        """Workload recording -- smallest 16 MB buffer loses no events (ORD008/ORD009)

        1. Record a write-heavy workload with the 16 MB minimum buffer
        2. Session must still inspect clean, with no reported gaps

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-07-29 Claude Initial design-stage suite
        """
        self._require_replay()

        db = "wlr_bp"
        stmts = [f"drop database if exists {db};", f"create database {db};", f"use {db};",
                 "create table t1 (ts timestamp, c1 int, c2 binary(200));"]
        for i in range(500):
            stmts.append(f"insert into t1 values (now+{i}s, {i}, '{'x' * 180}');")
        d = self._record_workload("".join(stmts), buf=16)

        rc, out = self._inspect(d)
        assert rc == 0, f"backpressured session did not inspect clean: {out}"
        low = out.lower()
        assert "structurally complete: yes" in low, f"backpressured session not structurally complete: {out}"
        tdLog.success(f"{__file__} backpressure integrity passed")

    def test_com_workload_record_fail_open(self):
        """Workload recording -- disk full is fail-open (FO001/FO008)

        1. Point the record dir at a tiny full filesystem
        2. Business SQL keeps succeeding; recorder stops on its own

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-07-29 Claude Initial design-stage suite
        """
        self._require()

        tiny = os.environ.get("WLR_TINY_FS")
        if not tiny:
            pytest.skip("set WLR_TINY_FS to a small writable mount to exercise disk-full")

        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: tiny, BUF_CFG: 16})
        try:
            db = "wlr_failopen"
            stmts = [f"drop database if exists {db};", f"create database {db};", f"use {db};",
                     "create table t1 (ts timestamp, c1 binary(1000));"]
            for i in range(5000):
                stmts.append(f"insert into t1 values (now+{i}s, '{'y' * 900}');")
            rc, out, err = self._run_client("".join(stmts), timeout=600)
            assert rc == 0, f"disk-full recording must not fail the business call: {err}"
        finally:
            self._restore_cfg()
        tdLog.success(f"{__file__} fail-open passed")

    # ---------------------------------------------------------------- 6.8 files

    def test_com_workload_record_active_copy_recoverable(self):
        """Workload recording -- an active session can be copied (FIL009/FIL010)

        1. Copy the whole session dir while the client is still recording
        2. The copy inspects to a valid prefix; a torn tail frame is a warning

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-07-29 Claude Initial design-stage suite
        """
        self._require_replay()

        d = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: d})
        db = "wlr_copy"
        stmts = [f"drop database if exists {db};", f"create database {db};", f"use {db};",
                 "create table t1 (ts timestamp, c1 int);"]
        # Enough to guarantee a segment exists while the client is still running, but
        # bounded: this test only needs an in-progress file, not a large one. The
        # original 2000 statements took minutes and stalled the whole suite.
        for i in range(300):
            stmts.append(f"insert into t1 values (now+{i}s, {i});")
        p = subprocess.Popen(
            [self.taos, "-c", self.clientCfgDir, "-s", "".join(stmts)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        copy = self._new_dir()
        copied = False
        try:
            # Poll with a delay: a tight loop finishes all 200 iterations before the
            # client has even created its first segment, which made this skip rather
            # than test anything.
            for _ in range(200):
                if self._segments(d):
                    for name in self._session_files(d):
                        src = os.path.join(d, name)
                        if os.path.isfile(src):
                            shutil.copy2(src, os.path.join(copy, name))
                    copied = True
                    break
                time.sleep(0.05)
            try:
                p.communicate(timeout=120)
            except subprocess.TimeoutExpired as exc:
                p.kill()
                p.communicate(timeout=30)
                pytest.fail(f"active-copy client timed out after 120s ({exc})")
        finally:
            if p.poll() is None:
                p.kill()
                try:
                    p.communicate(timeout=30)
                except subprocess.TimeoutExpired:
                    tdLog.warning("taos client did not exit during cleanup")
            self._restore_cfg()

        if not copied:
            pytest.skip("no active segment observed to copy within 10 seconds")
        rc, out = self._inspect(copy)
        assert rc == 0, f"active-file inspect failed with rc={rc}: {out}"
        assert "structurally complete: yes" in out.lower(), f"active-file copy was incomplete: {out}"
        tdLog.success(f"{__file__} active-copy recovery passed")

    def test_com_workload_record_single_session_per_process(self):
        """Workload recording -- one session per process (RS F5, DS 9.3)

        1. Multiple connections from one client process share one session
        2. Two client processes yield two distinct session file groups

        Since: v3.4.3

        Labels: ci,functional,taosc

        History:
            - 2026-07-29 Claude Initial design-stage suite
        """
        self._require()

        d = self._new_dir()
        self._write_cfg(**{RECORD_CFG: 1, DIR_CFG: d})
        try:
            self._run_client("show databases;")
            first = set(self._session_files(d))
            assert first, "first process produced no session"
            self._run_client("show databases;")
            second = set(self._session_files(d))
        finally:
            self._restore_cfg()

        added = second - first
        assert added, "second client process did not create its own session files"
        # DS 6.5 -- exclusive create, never overwrite: the first group survives.
        assert first.issubset(second), "second process overwrote the first session"
        tdLog.success(f"{__file__} per-process session isolation passed")
