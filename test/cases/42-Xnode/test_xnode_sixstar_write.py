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

"""
XNode DSN six-star write-protection integration test via direct SQL.

Verifies that with xnodeDsnProtection=1, mnode rejects candidate DSNs whose
sensitive field is a literal six-star or a percent-encoded six-star on the
CREATE XNODE TASK write path (TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED),
and that the rejection happens before the xnoded /task/check connector call
(this environment has no xnoded, yet the protection error is still returned).
With xnodeDsnProtection=0, the same values are treated as ordinary real
credentials and carry no six-star control semantics. With protection enabled,
empty-authority DSNs such as csv/orc (no locatable userinfo password field)
are not rejected by the six-star rule.

ALTER-path KEEP/REPLACE semantics are driven directly against the merge engine
by xnodeSixStarWriteTest (gtest). This file additionally covers SQL-level ALTER
TASK KEEP, the SHOW readback of a merged task, and a protection 0->1 switch
over a task committed while protection was off, using a mock xnoded bound to
the xnoded unix socket. This test does not depend on Explorer UI or Server.
"""

import json
import os
import socketserver
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler

import taos
from new_test_framework.utils import tdDnodes, tdLog, tdSql

TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED = 0x8026

# Test-only RSA-2048 public key (no private counterpart is needed: the write
# path never encrypts, taosd only validates and caches the public key at
# config load when xnodeDsnProtection=1).
XNODE_DSN_TEST_PUBLIC_KEY_PEM = """-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAuMXYY/FYkBOHMPL6H1bh
WON7BamspVhJsc5SRO5Qo8GZjhG4bCms2j9L1EI22iCPrALWAE/bEO2RRkyJqqPZ
C3uZq1z45z07oHnCrmI1MRyLopM+upOQRhQO7OQ8YrJjLlYKhwXZOa6zw8cIREh/
pMBWgeb9L8uOOvfDmXWQjWd4gQhmbnDO+SR9ko8YGwh6iU9JhrK92INUk5pQgI5E
Ja7NZX6ej11shBd3OzLOd11+QNc5ANlPtqzRrZ+3RV22y6s0o/gFJMaECEmh7mgL
PE/P6qV8hzyyFMOBnrBW2zNb2mB+7ITHd6BGAfzoSU2RJ3RhTc2HcdYOSM2RUowE
CwIDAQAB
-----END PUBLIC KEY-----
"""


class _MockXnodedHandler(BaseHTTPRequestHandler):
    """Answers every xnoded connector call with an empty success object."""

    def _ok(self):
        body = b"{}"
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self._ok()

    def do_DELETE(self):
        self._ok()

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length > 0:
            self.rfile.read(length)
        self._ok()

    def log_message(self, *args):
        pass


class _UnixSocketHttpServer(socketserver.UnixStreamServer):
    daemon_threads = True

    def get_request(self):
        request, _ = self.socket.accept()
        return request, ("local", 0)


class MockXnoded:
    """Always-success xnoded stand-in bound to the xnoded unix socket.

    taosd unlinks the socket path whenever its xnoded management (re)spawns or
    shuts down, so a guard thread re-binds the stand-in whenever the path
    disappears.
    """

    def __init__(self, socket_path: str):
        self.socket_path = socket_path
        self._server = None
        self._lock = threading.Lock()
        self._stopped = threading.Event()
        self._guard = None

    def _bind(self):
        with self._lock:
            if os.path.exists(self.socket_path):
                os.unlink(self.socket_path)
            self._server = _UnixSocketHttpServer(self.socket_path, _MockXnodedHandler)
            threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def _unbind(self):
        with self._lock:
            if self._server is not None:
                self._server.shutdown()
                self._server.server_close()
                self._server = None

    def _watch(self):
        while not self._stopped.is_set():
            if not os.path.exists(self.socket_path):
                self._unbind()
                try:
                    self._bind()
                except OSError:
                    pass
            self._stopped.wait(0.1)

    def start(self):
        self._stopped.clear()
        self._bind()
        self._guard = threading.Thread(target=self._watch, daemon=True)
        self._guard.start()
        for _ in range(50):
            if os.path.exists(self.socket_path):
                return
            time.sleep(0.1)
        raise AssertionError(f"mock xnoded socket did not appear at {self.socket_path}")

    def stop(self):
        self._stopped.set()
        if self._guard is not None:
            self._guard.join(timeout=2)
            self._guard = None
        self._unbind()
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)


class TestXnodeSixStarWrite:
    """CREATE XNODE TASK six-star rejection under direct SQL."""

    replicaVar = 1

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.suffix = uuid.uuid4().hex[:6]
        cls.key_path = os.path.join("/tmp", f"xnode_dsn_test_pub_{cls.suffix}.pem")
        with open(cls.key_path, "w") as key_file:
            key_file.write(XNODE_DSN_TEST_PUBLIC_KEY_PEM)

    @classmethod
    def teardown_class(cls):
        # Restore the default (protection off) configuration for any later
        # tests sharing this dnode deployment.
        cls._restart_with_protection(0)
        if os.path.exists(cls.key_path):
            os.remove(cls.key_path)

    @classmethod
    def _restart_with_protection(cls, protection: int):
        updatecfg = {
            "forceReadConfig": "1",
            "xnodeDsnProtection": str(protection),
            "xnodeDsnPublicKeyFile": cls.key_path,
        }
        tdSql.close()
        tdDnodes.stop(1)
        tdDnodes.sim.deploy(updatecfg)
        tdDnodes.deploy(1, updatecfg)
        tdDnodes.starttaosd(1)
        last_err = None
        for _ in range(30):
            try:
                new_conn = taos.connect()
                tdSql.init(new_conn.cursor())
                return
            except Exception as err:  # taosd not ready yet
                last_err = err
                time.sleep(1)
        raise AssertionError(f"taosd did not come back after restart: {last_err}")

    @classmethod
    def _switch_protection_preserving_data(cls, protection: int):
        """Flip xnodeDsnProtection without wiping the dnode data directory.

        The persisted global config is loaded after taos.cfg and therefore
        must be updated together with the external config file when present.
        """
        cfg_path = tdDnodes.dnodes[0].cfgPath
        tdSql.close()
        tdDnodes.stoptaosd(1)
        with open(cfg_path) as cfg_file:
            lines = cfg_file.readlines()
        with open(cfg_path, "w") as cfg_file:
            replaced = False
            for line in lines:
                if line.strip().startswith("xnodeDsnProtection"):
                    cfg_file.write(f"xnodeDsnProtection {protection}\n")
                    replaced = True
                else:
                    cfg_file.write(line)
            if not replaced:
                cfg_file.write(f"xnodeDsnProtection {protection}\n")

        data_dir = tdDnodes.dnodes[0].dataDir[0].split(" ", 1)[0]
        global_cfg_path = os.path.join(data_dir, "dnode", "config", "global.json")
        if os.path.isfile(global_cfg_path):
            with open(global_cfg_path, encoding="utf-8") as global_cfg_file:
                global_cfg = json.load(global_cfg_file)
            persisted_configs = global_cfg.get("configs")
            if not isinstance(persisted_configs, dict) or "xnodeDsnProtection" not in persisted_configs:
                raise AssertionError(f"xnodeDsnProtection is missing from {global_cfg_path}")
            persisted_configs["xnodeDsnProtection"] = protection
            with open(global_cfg_path, "w", encoding="utf-8") as global_cfg_file:
                json.dump(global_cfg, global_cfg_file, separators=(",", ":"))
                global_cfg_file.write("\n")

        tdDnodes.starttaosd(1)
        last_err = None
        for _ in range(30):
            try:
                new_conn = taos.connect()
                tdSql.init(new_conn.cursor())
                break
            except Exception as err:  # taosd not ready yet
                last_err = err
                time.sleep(1)
        else:
            raise AssertionError(f"taosd did not come back after restart: {last_err}")
        tdSql.query("SHOW VARIABLES LIKE 'xnodeDsnProtection'")
        assert len(tdSql.queryResult) == 1, tdSql.queryResult
        assert str(tdSql.queryResult[0][1]) == str(protection), tdSql.queryResult

    def _start_mock_xnoded(self) -> MockXnoded:
        socket_path = os.path.join(tdDnodes.dnodes[0].dataDir[0], ".taosxnoded.sock")
        mock = MockXnoded(socket_path)
        mock.start()
        return mock

    def _expect_protection_reject(self, task_name: str, from_dsn: str, to_dsn: str):
        tdSql.error(
            f"CREATE XNODE TASK '{task_name}' FROM '{from_dsn}' TO '{to_dsn}'",
            expectedErrno=TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED,
        )

    def _assert_no_protection_error(self, task_name: str, from_dsn: str, to_dsn: str, context: str):
        try:
            tdSql.execute(f"CREATE XNODE TASK '{task_name}' FROM '{from_dsn}' TO '{to_dsn}'", queryTimes=1)
            tdLog.notice(f"create succeeded (xnoded reachable), dropping task {task_name}")
            tdSql.execute(f"DROP XNODE TASK '{task_name}'", queryTimes=1)
        except Exception as err:
            actual = getattr(err, "errno", 0) or 0
            assert (actual & 0xFFFF) != TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED, f"{context}: {err}"
            tdLog.notice(f"expected non-protection error tolerated: {err}")

    def test_create_task_rejects_sixstar_when_protected(self):
        """Literal and percent-encoded six-star are rejected before the connector.

        1. Restart taosd with xnodeDsnProtection=1 and a valid public key file.
        2. CREATE XNODE TASK with a literal six-star password is rejected with
           TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED (not an xnoded connectivity
           error, proving the rejection happens before the /task/check call).
        3. Same for a percent-encoded six-star in the sink DSN.

        Since: v3.4.2.8

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-09-10 mnode six-star write semantics
        """
        self._restart_with_protection(1)
        self._expect_protection_reject(
            f"t8_lit_{self.suffix}", "taos://u:******@h/db", "taos://u:x@h2/db"
        )
        self._expect_protection_reject(
            f"t8_enc_{self.suffix}", "taos://u:x@h/db", "taos://u:%2A%2A%2A%2A%2A%2A@h2/db"
        )

    def test_create_task_sixstar_is_plain_credential_when_unprotected(self):
        """With protection off, six-star is an ordinary credential.

        1. Restart taosd with xnodeDsnProtection=0.
        2. The same CREATE XNODE TASK statements must NOT fail with the
           protection error; they proceed to the xnoded /task/check connector
           call (which fails in this environment because xnoded is absent) or
           succeed if xnoded is reachable. Any other error is acceptable.

        Since: v3.4.2.8

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-09-10 mnode six-star write semantics
        """
        self._restart_with_protection(0)
        for name, from_dsn, to_dsn in [
            (f"t8_off_lit_{self.suffix}", "taos://u:******@h/db", "taos://u:x@h2/db"),
            (f"t8_off_enc_{self.suffix}", "taos://u:x@h/db", "taos://u:%2A%2A%2A%2A%2A%2A@h2/db"),
        ]:
            self._assert_no_protection_error(
                name, from_dsn, to_dsn, "six-star must not be a control marker with protection off"
            )

    def test_create_task_accepts_empty_authority_dsn_when_protected(self):
        """csv/orc empty-authority DSNs are not six-star rejection targets.

        1. Restart taosd with xnodeDsnProtection=1 and a valid public key file.
        2. CREATE XNODE TASK with a csv:/// or orc:/// source DSN has no
           locatable userinfo password field, so it must NOT fail with
           TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED; the request proceeds to
           the xnoded /task/check connector call (which fails in this
           environment because xnoded is absent) or succeeds if reachable.

        Since: v3.4.2.8

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-09-11 accept empty-authority DSNs under protection
        """
        self._restart_with_protection(1)
        for name, from_dsn in [
            (f"t8_csv_{self.suffix}", "csv:///tmp/t8_demo.csv"),
            (f"t8_orc_{self.suffix}", "orc:///tmp/t8_demo.orc"),
        ]:
            self._assert_no_protection_error(
                name, from_dsn, "taos://u:x@h2/db", "empty-authority DSN must not trigger six-star protection"
            )

    def test_alter_task_keep_merges_committed_secret_when_protected(self):
        """SQL-level ALTER TASK KEEP merges against the committed DSN.

        1. Restart taosd with xnodeDsnProtection=1 and a valid public key file.
        2. Bind a mock xnoded to the xnoded unix socket so /task/check succeeds
           and CREATE XNODE TASK commits a task with real credentials.
        3. ALTER XNODE TASK with a "******" userinfo password and the
           committed query token succeeds; the SHOW readback returns
           the merged candidate shape with masked sensitive values; a KEEP
           without a matching old sensitive key fails with
           TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED.
        4. ALTER XNODE TASK with a fresh credential (REPLACE) also succeeds.

        Since: v3.4.2.8

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-09-16 SQL-level ALTER TASK KEEP coverage
        """
        self._restart_with_protection(1)
        mock = self._start_mock_xnoded()
        try:
            name = f"t8_keep_{self.suffix}"
            tdSql.execute(
                f"CREATE XNODE TASK '{name}' FROM 'taos://user1:oldsecret@h1:6030/db1?token=tok123' "
                f"TO 'taos://sinku:sinkpw@h2:6030/db2'",
                queryTimes=1,
            )
            # KEEP the password while retaining the committed connection identity and token.
            tdSql.execute(
                f"ALTER XNODE TASK '{name}' FROM 'taos://user1:******@h1:6030/db1?token=tok123' "
                f"TO 'taos://sinku:******@h2:6030/db2'",
                queryTimes=1,
            )
            rs = tdSql.query(f"SHOW XNODE TASKS WHERE name = '{name}'", row_tag=True)
            assert len(rs) == 1, f"expected one task row, got {rs}"
            from_shown, to_shown = str(rs[0][2]), str(rs[0][3])
            assert "user1" in from_shown and "h1" in from_shown and "db1" in from_shown, from_shown
            assert "oldsecret" not in from_shown and "sinkpw" not in to_shown, (from_shown, to_shown)
            assert "******" in from_shown and "******" in to_shown, (from_shown, to_shown)
            # KEEP for a sensitive key the committed DSN does not carry is rejected.
            tdSql.error(
                f"ALTER XNODE TASK '{name}' FROM 'taos://user1:******@h1:6030/db1?password=******' "
                f"TO 'taos://sinku:******@h2:6030/db2'",
                expectedErrno=TSDB_CODE_MND_XNODE_DSN_PROTECTION_FAILED,
            )
            # REPLACE with fresh credentials succeeds and stays readable.
            tdSql.execute(
                f"ALTER XNODE TASK '{name}' FROM 'taos://userX:newpw@hX:6030/dbX?token=tok999' "
                f"TO 'taos://sinku:newsink@h2:6030/db2'",
                queryTimes=1,
            )
            rs = tdSql.query(f"SHOW XNODE TASKS WHERE name = '{name}'", row_tag=True)
            assert len(rs) == 1, f"expected one task row, got {rs}"
            assert "newpw" not in str(rs[0][2]) and "newsink" not in str(rs[0][3]), rs
            tdSql.execute(f"DROP XNODE TASK '{name}'", queryTimes=1)
        finally:
            mock.stop()

    def test_protection_switch_keep_on_preexisting_plaintext(self):
        """KEEP works on a task committed while protection was off.

        1. Restart taosd with xnodeDsnProtection=0 and a mock xnoded so CREATE
           XNODE TASK commits a plaintext real credential.
        2. Flip xnodeDsnProtection to 1 without wiping the data directory.
        3. ALTER XNODE TASK with "******" KEEPs the pre-protection committed
           secret, SHOW reads the task back masked, and a fresh CREATE with a
           six-star marker is now rejected.

        Since: v3.4.2.8

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-09-16 protection switch coverage
        """
        self._restart_with_protection(0)
        mock = self._start_mock_xnoded()
        try:
            name = f"t8_sw_{self.suffix}"
            tdSql.execute(
                f"CREATE XNODE TASK '{name}' FROM 'taos://u:legacy_pw@h:6030/db' "
                f"TO 'taos://v:w@h2:6030/db2'",
                queryTimes=1,
            )
            self._switch_protection_preserving_data(1)
            # KEEP finds the pre-protection committed value.
            tdSql.execute(
                f"ALTER XNODE TASK '{name}' FROM 'taos://u:******@h:6030/db' "
                f"TO 'taos://v:******@h2:6030/db2'",
                queryTimes=1,
            )
            rs = tdSql.query(f"SHOW XNODE TASKS WHERE name = '{name}'", row_tag=True)
            assert len(rs) == 1, f"expected one task row, got {rs}"
            assert "legacy_pw" not in str(rs[0][2]) and "******" in str(rs[0][2]), rs
            self._expect_protection_reject(
                f"t8_sw2_{self.suffix}", "taos://u:******@h/db", "taos://v:w@h2/db2"
            )
            tdSql.execute(f"DROP XNODE TASK '{name}'", queryTimes=1)
        finally:
            mock.stop()
