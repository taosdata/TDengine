# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

import glob
import json
import os
import platform
import re
import shutil
import subprocess
import time

import pytest

from new_test_framework.utils import tdDnodes, tdLog, tdSql

CERT_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "certs")

# Test-only self-signed material generated once for this case.  Bundling it
# keeps the case runnable on Windows, where no openssl command line tool is
# guaranteed to be installed.
def _certFile(name):
    # taosd/taosc read the value literally; forward slashes work on all platforms.
    return os.path.join(CERT_DIR, name).replace("\\", "/")


def _appendCfg(cfgPath, option, value):
    # The framework's cfg() helpers append lines through an unquoted shell
    # echo, which splits values on spaces (e.g. C:\Users\John Doe\...).  Write
    # the file directly instead and quote the value the way taos.cfg expects:
    # its tokenizer keeps everything between two '@' characters verbatim,
    # including spaces.
    with open(cfgPath, "a", encoding="utf-8") as cfg:
        cfg.write(f"{option} @{value}@\n")


def _cliEnv():
    # Environment of a short-lived ``taos`` child.  pytest.sh leaves leak
    # detection on so checkAsan.sh still sees taosd, and the CLI inherits it.
    # After a TLS handshake the CLI exits through LeakSanitizer, which reports
    # OpenSSL's per-thread state on the transport worker thread that is still
    # running at exit().  That exit code says nothing about the query, and it
    # would also make the "plaintext client is rejected" check pass for the
    # wrong reason, so keep the other options but turn leak detection off here.
    environment = os.environ.copy()
    for name in ("ASAN_OPTIONS", "LSAN_OPTIONS"):
        options = [
            option for option in environment.get(name, "").split(":")
            if option and not option.startswith("detect_leaks=")
        ]
        environment[name] = ":".join(options + ["detect_leaks=0"])
    return environment


class TestTls:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.prepare(dbname="tls_demo", drop=True, replica=cls.replicaVar)

    # --- util ---
    def _requireTlsSupport(self):
        system = platform.system().lower()
        if system not in ("linux", "windows"):
            reason = f"native TLS is not supported on {platform.system()}"
            # pytest.skip already reports the reason, but print it as well so
            # a zero-coverage run is visible in the CI summary.
            print(f"SKIPPED: {reason}", flush=True)
            pytest.skip(reason)
        version = str(tdSql.getFirstValue("select server_version()"))
        if "enterprise" not in version.lower():
            reason = f"native TLS requires the enterprise edition, server version is {version}"
            print(f"SKIPPED: {reason}", flush=True)
            pytest.skip(reason)

    def _serverTlsCfg(self):
        return {
            "enableTLS": "1",
            "tlsCaPath": _certFile("ca.pem"),
            "tlsSvrCertPath": _certFile("server.pem"),
            "tlsSvrKeyPath": _certFile("server.key"),
            "tlsCliCertPath": _certFile("client.pem"),
            "tlsCliKeyPath": _certFile("client.key"),
        }

    def _dnodes(self):
        # tdDnodes always instantiates ten nodes; only the first
        # session.denodes_num are deployed and have a cfgPath.
        return [dnode.index for dnode in tdDnodes.dnodes if dnode.deployed]

    def _taosCli(self):
        suffix = ".exe" if platform.system().lower() == "windows" else ""
        return os.path.join(os.path.dirname(tdDnodes.dnodes[0].binPath), "taos" + suffix)

    def _runCli(self, sql, timeoutSec=30):
        cmd = [self._taosCli(), "-c", tdDnodes.sim.getCfgDir(), "-s", sql]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeoutSec, env=_cliEnv())
        except subprocess.TimeoutExpired as err:
            # taosd can still be coming up when the first CLI attempt runs
            # (much slower under the ASAN CI build), and taos_connect then
            # blocks past this attempt's timeout.  Return it as a failed
            # attempt so _cliSucceeded keeps retrying; letting the exception
            # escape would abort the whole case.
            stdout = err.stdout or ""
            stderr = err.stderr or ""
            if isinstance(stdout, bytes):
                stdout = stdout.decode(errors="replace")
            if isinstance(stderr, bytes):
                stderr = stderr.decode(errors="replace")
            tdLog.info(f"{' '.join(cmd)} timed out after {timeoutSec}s")
            return subprocess.CompletedProcess(cmd, -1, stdout, stderr)
        tdLog.info(f"{' '.join(cmd)} rc={result.returncode} out={result.stdout} err={result.stderr}")
        return result

    def _plaintextClientCfgDir(self):
        # A client config that keeps TLS off.  It is a copy of the taosc config
        # the TLS case uses, so every non-TLS setting (endpoint, logDir, ...)
        # stays identical, with enableTLS=0 appended last to win during
        # parsing.  Keeping it outside the psim cfg dir avoids interfering
        # with the server/TLS settings written there.
        dirPath = os.path.join(tdDnodes.sim.path, "psim", "cfg-plaintext")
        os.makedirs(dirPath, exist_ok=True)
        cfgPath = os.path.join(dirPath, "taos.cfg")
        shutil.copyfile(tdDnodes.sim.cfgPath, cfgPath)
        _appendCfg(cfgPath, "enableTLS", "0")
        return dirPath

    def _plaintextClientMustFail(self, sql, timeoutSec=60):
        # Negative check: while the server has enableTLS=1, a client with
        # enableTLS=0 must not be able to talk to it.  Without this, a
        # regression that keeps accepting plaintext connections would still
        # pass the positive assertions above.
        cmd = [self._taosCli(), "-c", self._plaintextClientCfgDir(), "-s", sql]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeoutSec, env=_cliEnv())
        except subprocess.TimeoutExpired:
            # The TLS-only server never answers the plaintext client; the CLI
            # was still retrying when the timeout fired, which is rejection.
            tdLog.info(f"{' '.join(cmd)} did not connect within {timeoutSec}s, as expected")
            return
        output = (result.stdout + result.stderr).lower()
        tdLog.info(f"{' '.join(cmd)} rc={result.returncode} out={result.stdout} err={result.stderr}")
        assert result.returncode != 0 or "failed to connect" in output, (
            "a client with enableTLS=0 reached a TLS-only server: "
            f"rc={result.returncode} output={result.stdout}{result.stderr}"
        )

    # ``taos -s`` reports SQL errors as "DB error: ..." and still exits with 0
    # (shellExecute returns 0 once the one-shot command has run), so the exit
    # code alone cannot tell success from failure.  A successful statement
    # always ends with one of the "<verb> OK[, N row(s) ...]" lines, which is
    # the positive signal used instead.
    _CLI_ERROR = re.compile(r"DB error:|failed to connect", re.IGNORECASE)
    _CLI_OK = re.compile(r"\b(?:Query|Insert|Create|Delete|Drop) OK\b", re.IGNORECASE)

    @classmethod
    def _cliResultOk(cls, result):
        output = result.stdout + result.stderr
        return (result.returncode == 0 and not cls._CLI_ERROR.search(output)
                and cls._CLI_OK.search(result.stdout) is not None), output

    @staticmethod
    def _resultRows(stdout):
        # Data rows of the horizontal table the shell prints: after the line
        # of '=' that separates the column header, each line holds the cells
        # separated by '|' and terminated by one.  Only those cells are
        # returned, so a value can never be confused with the surrounding
        # "Query OK, N row(s)" text.
        rows = []
        inTable = False
        for line in stdout.splitlines():
            text = line.strip()
            if not text:
                continue
            if set(text) == {"="}:
                inTable = True
                continue
            if text.startswith("Query OK"):
                break
            if inTable:
                cells = [cell.strip() for cell in text.split("|")]
                if cells and cells[-1] == "":
                    cells.pop()
                rows.append(cells)
        return rows

    @classmethod
    def _parseCliCount(cls, result):
        # Value of a successful single-cell query, or None when the run failed
        # (so polling callers can tell a transient failure from a real value).
        ok, output = cls._cliResultOk(result)
        if not ok:
            return None, output
        rows = cls._resultRows(result.stdout)
        if len(rows) != 1 or len(rows[0]) != 1:
            return None, output
        return rows[0][0], output

    def _assertCliCount(self, sql, expected, timeoutSec=120):
        deadline = time.time() + timeoutSec
        output = ""
        while time.time() < deadline:
            result = self._runCli(sql)
            value, output = self._parseCliCount(result)
            if value is not None:
                assert value == str(expected), \
                    f"'{sql}' returned {value!r}, expected {expected}:\n{result.stdout}"
                return
            time.sleep(2)
        raise AssertionError(f"taos CLI did not run '{sql}' within {timeoutSec}s, last output:\n{output}")

    def _cliSucceeded(self, sql, timeoutSec=120):
        deadline = time.time() + timeoutSec
        output = ""
        while time.time() < deadline:
            result = self._runCli(sql)
            ok, output = self._cliResultOk(result)
            if ok:
                return result
            time.sleep(2)
        raise AssertionError(f"taos CLI did not run '{sql}' within {timeoutSec}s, last output:\n{output}")

    def _taosdLogContains(self, text, timeoutSec=30, dnodeIndex=1):
        logDir = tdDnodes.dnodes[dnodeIndex - 1].logDir
        deadline = time.time() + timeoutSec
        while time.time() < deadline:
            for path in sorted(glob.glob(os.path.join(logDir, "taosdlog*"))):
                try:
                    with open(path, "r", errors="ignore") as log:
                        if text in log.read():
                            return path
                except OSError:
                    pass
            time.sleep(1)
        return None

    def _globalCfgPath(self, dnodeIndex):
        # taosd pre-loads <dataDir>/dnode/config/global.json after taos.cfg and
        # uses it to overwrite every global item it contains.  The file is
        # written from the mnode config response on the first startup, so a
        # plain restart after appending to taos.cfg would silently restore
        # enableTLS=0 and the previous certificate paths.
        dataDir = tdDnodes.dnodes[dnodeIndex - 1].dataDir[0].split(" ")[0]
        return os.path.join(dataDir, "dnode", "config", "global.json")

    def _persistGlobalCfg(self, dnodeIndex, cfgDict):
        path = self._globalCfgPath(dnodeIndex)
        if not os.path.exists(path):
            # No persisted config yet: taos.cfg is authoritative on startup.
            tdLog.info(f"{path} does not exist, skip updating it")
            return
        with open(path, "r", encoding="utf-8") as cfgFile:
            content = json.load(cfgFile)
        configs = content.setdefault("configs", {})
        for option, value in cfgDict.items():
            if option == "enableTLS":
                # globalConfigSerialize writes BOOL items as JSON booleans.
                configs[option] = str(value).strip().lower() in ("1", "true", "yes", "on")
            else:
                configs[option] = value
        # Keep "version" and "file_version" untouched so the dnode still
        # considers this the config accepted from the mnode.
        with open(path, "w", encoding="utf-8") as cfgFile:
            json.dump(content, cfgFile)
        tdLog.info(f"updated persisted global config: {path}")

    def _restartWithCfg(self, cfgDict):
        for index in self._dnodes():
            tdDnodes.stop(index)
        time.sleep(2)
        # The psim config is also the taosc config the CLI child process loads.
        for option, value in cfgDict.items():
            for index in self._dnodes():
                _appendCfg(tdDnodes.dnodes[index - 1].cfgPath, option, value)
            _appendCfg(tdDnodes.sim.cfgPath, option, value)
        for index in self._dnodes():
            self._persistGlobalCfg(index, cfgDict)
        for index in self._dnodes():
            tdDnodes.starttaosd(index)
        # taosd only starts listening a moment after the process is spawned;
        # give it a head start before the first CLI probe (which retries).
        time.sleep(2)

    def _waitAllDnodesReady(self, timeoutSec=120):
        expected = len(self._dnodes())
        sql = "select count(*) from information_schema.ins_dnodes where status='ready'"
        deadline = time.time() + timeoutSec
        output = ""
        while time.time() < deadline:
            value, output = self._parseCliCount(self._runCli(sql))
            if value == str(expected):
                return
            time.sleep(2)
        raise AssertionError(f"not all {expected} dnodes reached ready within {timeoutSec}s:\n{output}")

    # --- impl ---
    def do_plaintextBaseline(self):
        # TLS off: the native connection keeps working in plaintext.
        tdSql.execute("create table if not exists tls_plain(ts timestamp, v int)")
        tdSql.execute("insert into tls_plain values (1750046400000, 1)")
        tdSql.query("select v from tls_plain")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        result = self._cliSucceeded("select server_version()")
        assert "enterprise" in result.stdout.lower(), result.stdout

        print("plaintext baseline ......................... [ passed ]")

    def do_enableTls(self):
        self._restartWithCfg(self._serverTlsCfg())

        # A fresh CLI process reads the TLS client settings from the psim config
        # and must reach the TLS-only server.
        self._cliSucceeded("select server_version()")
        # With more than one deployed dnode the cluster only reports every
        # node ready if dnode-to-dnode traffic (mnode/status/sync) completed
        # its TLS handshakes too.
        self._waitAllDnodesReady()
        self._cliSucceeded("create table tls_demo.tls_tls(ts timestamp, v int)")
        self._cliSucceeded("insert into tls_demo.tls_tls values (1750046400000, 2)")
        self._assertCliCount("select count(*) from tls_demo.tls_tls", 1)
        self._cliSucceeded("show databases")

        # Guard the checks themselves, so a failure cannot pass as success:
        # a SQL error must be recognised although the shell still exits with
        # 0, and the count must be read from the result cell instead of the
        # "1 row(s)" text that every single-row result prints.
        errorResult = self._runCli("select count(*) from tls_demo.tls_no_such_table")
        errorOk, errorOutput = self._cliResultOk(errorResult)
        assert not errorOk and "DB error:" in errorOutput, \
            f"a SQL error was not detected as a failure:\n{errorOutput}"
        self._assertCliCount("select count(*) from tls_demo.tls_tls where ts < 0", 0)

        if len(self._dnodes()) > 1:
            # replica 2 forces the write and the query to cross dnodes over TLS.
            self._cliSucceeded("create database if not exists tls_repl replica 2")
            self._cliSucceeded("create table if not exists tls_repl.tls_repl(ts timestamp, v int)")
            self._cliSucceeded("insert into tls_repl.tls_repl values (1750046400000, 3)")
            self._assertCliCount("select count(*) from tls_repl.tls_repl", 1)

        for index in self._dnodes():
            log = self._taosdLogContains("TLS is enabled for", dnodeIndex=index)
            assert log is not None, f"dnode {index} did not report TLS enabled in {tdDnodes.dnodes[index - 1].logDir}"

        # TLS must be mandatory on the server side, not merely supported:
        # a client that leaves enableTLS=0 has to be rejected.
        self._plaintextClientMustFail("select server_version()")

        print("tls enabled ................................ [ passed ]")

    def do_disableTls(self):
        # enableTLS=0 is appended last in taos.cfg and written to the persisted
        # global config, so the node comes back in plaintext mode.
        self._restartWithCfg({"enableTLS": "0"})
        result = self._cliSucceeded("select server_version()")
        assert "enterprise" in result.stdout.lower(), result.stdout
        # The rows written over TLS must survive the plaintext restart.
        self._assertCliCount("select count(*) from tls_demo.tls_tls", 1)
        if len(self._dnodes()) > 1:
            self._assertCliCount("select count(*) from tls_repl.tls_repl", 1)

        print("tls disabled ............................... [ passed ]")

    def test_tls(self):
        """Native TLS for taosc/taosd connections

        1. Plaintext native connections keep working while enableTLS is off.
        2. With enableTLS=1 and the CA/server/client certificate paths set,
           taosd starts with TLS and a fresh taosc CLI process completes a
           native query, DDL and DML over TLS; taosd logs that TLS is enabled
           and a client using enableTLS=0 is refused.  When the case is
           deployed with more than one dnode (-N 2), every dnode must stay
           ready and a replica-2 database must accept writes, which exercises
           dnode-to-dnode traffic over TLS.
        3. Disabling enableTLS restores plaintext connections without losing
           the data written while TLS was on.

        Runs on Linux and Windows enterprise builds; Windows is the platform
        covered by this change, and its CI registration deploys two dnodes so
        client-server and node-node TLS are both covered.

        Catalog:
            - Security:TLS

        Since: v3.4.4.0

        Labels: common,ci,integration,functional,security

        Jira: 7171724883

        History:
            - 2026-11-18 yihaoDeng Cover Windows OpenSSL-backed native TLS
        """
        self._requireTlsSupport()
        self.do_plaintextBaseline()
        try:
            self.do_enableTls()
        except Exception:
            # Restore plaintext mode, but never let a cleanup failure hide the
            # TLS failure that brought us here.
            try:
                self.do_disableTls()
            except Exception:
                tdLog.error("failed to restore plaintext mode after the TLS test")
            raise
        else:
            self.do_disableTls()
