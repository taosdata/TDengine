###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved
#
#  This file is proprietary and confidential to TAOS Technologies, Inc.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission of Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
"""Rolling-upgrade compatibility for tag-ref on virtual tables.

Feature under test: tag-ref sources extended to virtual-normal (vntb) and normal
(ntb) tables, plus recursive chain resolution through virtual-child hops.

Rolling-upgrade contract verified here:
  1. Pre-existing virtual-child tag-ref queries (SELECT / WHERE / aggregate /
     PARTITION BY) stay correct while the cluster runs mixed versions — plans
     from a NEW coordinator must execute correctly on OLD executors.
  2. The new capability (a vntb/ntb table as a tag-ref source) is rejected
     loudly with VERSION_NOT_COMPATIBLE when the translating client is connected
     to an OLD server, instead of being silently accepted and then mis-executed
     by old executors (which skip the new physical-plan TLVs).
  3. After all dnodes are upgraded, the new capability works end to end,
     including chains that pass through a virtual-child hop (whose stored
     literal must not shadow the active tag-ref).
"""

import os
import pytest
import re
import subprocess
import time

from new_test_framework.utils import tdLog, tdSql, tdDnodes, tdCom, tdCb

DB = "td_trvru"
TS0 = 1700000000000


class TestVtableTagRefRollingUpgrade:
    """tag-ref rolling upgrade: mixed-window correctness, version gate, post-upgrade feature."""

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def teardown_method(self):
        # Restore the cluster shape no matter how the case ended — pass, skip, or an
        # assertion failure mid-window: drop the fixture DB (frees the vgroups placed
        # on 6130/6230) and de-register the two extra dnodes. Otherwise a failed run
        # leaves "dnode1 upgraded + 6130/6230 registered" behind, polluting the next
        # case in the same session, and a rerun trips over CREATE DNODE name clashes
        # (DROP DATABASE does not remove dnode registrations). Best-effort throughout:
        # teardown must never mask the real test outcome with its own error.
        try:
            tdSql.execute(f"DROP DATABASE IF EXISTS {DB};")
        except Exception as e:
            tdLog.info(f"teardown: drop database skipped: {e}")
        for port in (6130, 6230):
            try:
                tdSql.execute(f"DROP DNODE '{self.host}:{port}' FORCE;")
            except Exception as e:
                tdLog.info(f"teardown: drop dnode {port} skipped (never registered?): {e}")

    # ------------------------------------------------------------------
    # Helpers (skeleton mirrors test_compatibility_rolling_upgrade.py)
    # ------------------------------------------------------------------
    def getLastBigVersion(self):
        tdSql.query("SELECT SERVER_VERSION();")
        nowServerVersion = tdSql.queryResult[0][0]
        tdLog.info(f"Now server version is {nowServerVersion}")
        self.newServerVersion = nowServerVersion  # stash for the port/version mapping check
        parts = nowServerVersion.split(".")
        assert len(parts) >= 3, \
            f"SERVER_VERSION() returned '{nowServerVersion}', expected at least 3 components"
        lastBigVersion = parts[0] + "." + parts[1] + "." + parts[2] + "." + "0"
        tdLog.info(f"Last big version is {lastBigVersion}")
        return lastBigVersion

    def getDnodePath(self):
        buildPath = tdCom.getBuildPath()
        return [buildPath + "/../sim/dnode1/", buildPath + "/../sim/dnode2/", buildPath + "/../sim/dnode3/"]

    def _wait_cluster_ready(self, timeout=90):
        """Server process up does not imply vgroups recovered — accept readiness only once a
        real read on a fixture table succeeds."""
        deadline = time.time() + timeout
        last_state = "no response"
        while time.time() < deadline:
            try:
                tdSql.query("SELECT SERVER_VERSION();")
                if tdSql.queryResult and tdSql.queryResult[0][0]:
                    # non-aggregate read: COUNT(*) always returns exactly one row, which
                    # would make any row-count check vacuously true; a plain SELECT only
                    # returns rows once the fixture's vgroup has recovered and elected a
                    # leader (before that the query throws and lands in the except branch)
                    tdSql.query(f"SELECT * FROM {DB}.c1 LIMIT 1;")
                    if tdSql.queryRows >= 1:
                        return
                    last_state = "fixture read returned no rows (vgroups not recovered)"
                else:
                    last_state = "server_version empty"
            except Exception as e:
                last_state = f"{type(e).__name__}: {e}"
            time.sleep(1)
        raise AssertionError(f"cluster not ready in {timeout}s, last state: {last_state}")

    def _rows(self, sql):
        tdSql.query(sql)
        return [tuple(str(tdSql.getData(i, j)) for j in range(tdSql.queryCols))
                for i in range(tdSql.queryRows)]

    def _one(self, sql, desc=""):
        rows = self._rows(sql)
        assert len(rows) >= 1, f"{desc}: no rows"
        first = rows[0]
        for r in rows[1:]:
            assert r == first, f"{desc}: rows disagree: {rows}"
        tdLog.info(f"  PASS: {desc} -> {first} (x{len(rows)})")
        return first

    def _check_vctb_ref_queries(self, phase):
        """SELECT / WHERE / aggregate / PARTITION BY over a virtual-child tag-ref — a
        pre-existing feature that must stay correct in every upgrade phase."""
        row = self._one(f"SELECT ref_city FROM {DB}.v0;", f"{phase}: vctb projection")
        assert row[0] == "beijing", f"{phase}: v0.ref_city wrong: {row[0]}"
        tdSql.query(f"SELECT COUNT(*) FROM {DB}.v0 WHERE ref_city='beijing';")
        assert int(tdSql.getData(0, 0)) == 3, f"{phase}: vctb WHERE hit wrong"
        tdSql.query(f"SELECT COUNT(*) FROM {DB}.v0 WHERE ref_city='nowhere';")
        assert int(tdSql.getData(0, 0)) == 0, f"{phase}: vctb WHERE miss wrong"
        rows = self._rows(f"SELECT ref_city, COUNT(*) FROM {DB}.v0 PARTITION BY ref_city;")
        assert rows == [("beijing", "3")], f"{phase}: vctb PARTITION BY wrong: {rows}"
        tdLog.info(f"  PASS: {phase}: vctb tag-ref SELECT/WHERE/COUNT/PARTITION BY")

    def _shell_env(self):
        """Shared env for build-tree taos invocations (no shell string, list-args only)."""
        bPath = tdCom.getBuildPath()
        return bPath, dict(os.environ, LD_LIBRARY_PATH=f"{bPath}/build/lib")

    def _server_version_via_shell(self, port):
        """SERVER_VERSION() reported by the dnode at `port`, probed with the build-tree shell."""
        bPath, env = self._shell_env()
        # list-args invocation: no shell interpolation of the SQL, stderr kept for diagnosis
        r = subprocess.run([f"{bPath}/build/bin/taos", "-h", self.host, "-P", str(port),
                            "-s", "SELECT SERVER_VERSION();"],
                           capture_output=True, text=True, env=env, timeout=60)
        output = r.stdout + r.stderr
        for line in output.splitlines():
            line = line.strip().strip('|').strip()
            if re.match(r"^\d+\.\d+\.\d+", line):
                return line
        return None

    def _create_gate_table_via_shell(self, port):
        """CREATE a vntb whose tag-ref source is another vntb (the NEW capability) via the
        build-tree taos shell connected to the given dnode port. Returns (created, output)."""
        bPath, env = self._shell_env()
        sql = (f"CREATE VTABLE {DB}.a_gate (ts TIMESTAMP, val INT FROM {DB}.c1.val) "
               f"TAGS (t NCHAR(20) FROM {DB}.b0.loc);")
        # list-args invocation: the SQL (quotes, identifiers) is never shell-interpreted,
        # and stderr is folded into the returned output instead of discarded
        r = subprocess.run([f"{bPath}/build/bin/taos", "-h", self.host, "-P", str(port),
                            "-s", sql],
                           capture_output=True, text=True, env=env, timeout=60)
        output = (r.stdout + r.stderr).strip()
        tdLog.info(f"gate create via port {port}: rc={r.returncode}, output={output}")
        # Positive signal only: rc alone is unreliable (some shells exit 0 on server
        # errors), and the "Create OK" banner is printed only on success -- rejections
        # print "DB error: ..." with no banner. No negative substring filtering: broad
        # markers like "error"/"failed" could misfire on unrelated output content.
        created = (r.returncode == 0 and "Create OK" in r.stdout)
        return created, output

    # ------------------------------------------------------------------
    # Main test
    # ------------------------------------------------------------------
    def test_vtable_tag_ref_rolling_upgrade(self):
        """Comp: tag-ref rolling upgrade — mixed-window correctness, version gate, post-upgrade feature

        1. Test [Fixtures on old version]
            1.1 Install the last big version on 3 dnodes and form a cluster
            1.2 Create physical child tables, a virtual stable and a virtual child
                with a tag-ref (all supported by the old build)

        2. Test [Mixed-version window after upgrading dnode1 only]
            2.1 vctb tag-ref SELECT / WHERE / COUNT / PARTITION BY stay correct with
                a NEW coordinator and OLD executors
            2.2 The new capability (vntb as tag-ref source) is rejected with
                VERSION_NOT_COMPATIBLE when the client translates against an OLD server

        3. Test [After all dnodes upgraded]
            3.1 vctb tag-ref queries still correct
            3.2 vntb-source const-ref chain: SELECT / WHERE / DISTINCT / PARTITION BY
            3.3 Chain through a virtual-child hop follows the active tag-ref, never
                the stale literal left in ctbEntry.pTags

        Catalog:
            - VirtualTable:Compatibility:RollingUpgrade

        Since: v3.4.3.0

        Labels: common,ci,integration,functional,compatibility

        Jira: None

        History:
            - 2026-09-14 initial version for vntb/ntb tag-ref sources
        """
        hostname = self.host
        tdLog.info(f"hostname: {hostname}")
        lastBigVersion = self.getLastBigVersion()

        tdDnodes.stopAll()
        baseVersionExist = tdCb.installTaosdForRollingUpgrade(self.getDnodePath(), lastBigVersion)
        if not baseVersionExist:
            # visible skip, not a silent green pass: the case's whole value (version
            # gate + post-upgrade E2E) would otherwise never run while CI shows passed
            tdDnodes.starttaosd(1)  # stopAll already ran: bring the dnode back first
                                    # (TDDnodes has no startAll; srvCtl.dnodeStartAll's
                                    # non-cluster path is exactly starttaosd(1))
            pytest.skip(f"base version {lastBigVersion} package not found; "
                        f"rolling-upgrade contract not executed this run")

        # fresh fixtures: leftovers from a previously failed run (b0/b1/a_ref/vc_stb in
        # td_trvru) would make the CREATEs below fail with name clashes
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB};")

        # idempotent re-entry: a previously hard-killed run (where teardown_method never
        # got to drop them) leaves 6130/6230 registered, and CREATE DNODE would fail on
        # the name clash — clear any stale registration first, best-effort
        for port in (6130, 6230):
            try:
                tdSql.execute(f"DROP DNODE '{hostname}:{port}' FORCE;")
            except Exception as e:
                tdLog.info(f"no stale dnode {port} to drop before create: {e}")

        tdSql.execute(f"CREATE DNODE '{hostname}:6130'")
        tdSql.execute(f"CREATE DNODE '{hostname}:6230'")
        time.sleep(10)

        # ---- phase 1: fixtures on the OLD version — only features the old build supports
        tdSql.execute(f"CREATE DATABASE {DB} BUFFER 16 VGROUPS 2;")
        tdSql.execute(f"CREATE STABLE {DB}.src_stb (ts TIMESTAMP, val INT) TAGS (city NCHAR(20), code INT);")
        tdSql.execute(f"CREATE TABLE {DB}.c1 USING {DB}.src_stb TAGS ('beijing', 100);")
        tdSql.execute(f"CREATE TABLE {DB}.c2 USING {DB}.src_stb TAGS ('shanghai', 200);")
        for j, v in enumerate([1, 2, 3]):
            tdSql.execute(f"INSERT INTO {DB}.c1 VALUES ({TS0 + j * 1000}, {v});")
            tdSql.execute(f"INSERT INTO {DB}.c2 VALUES ({TS0 + j * 1000}, {v + 10});")
        tdSql.execute(f"CREATE STABLE {DB}.vstb (ts TIMESTAMP, val INT) "
                      f"TAGS (local_tag NCHAR(20), ref_city NCHAR(20)) VIRTUAL 1;")
        tdSql.execute(f"CREATE VTABLE {DB}.v0 (val FROM {DB}.c1.val) USING {DB}.vstb "
                      f"TAGS ('local0', ref_city FROM {DB}.c1.city);")
        self._check_vctb_ref_queries("old version")

        # ---- phase 2: rolling upgrade dnode1 only -> mixed-version window
        tdCb.updateNewVersion(tdCom.getBuildPath(), self.getDnodePath(), 0)
        time.sleep(10)
        self._wait_cluster_ready()
        self._check_vctb_ref_queries("mixed window")

        # The new capability must fail loudly (never silently mis-execute) when the
        # translating client is connected to an OLD server. The gate lives in the
        # client-side translate path and keys off the CONNECTED server's version.
        # Port/version mapping this relies on: firstEp is 6030 (dnode1, the mode-0
        # upgrade target, NEW); 6130/6230 are dnode2/dnode3 added via CREATE DNODE,
        # still OLD in the window. updateNewVersion(mode=0) picks the first taosd pid,
        # so VERIFY the mapping explicitly instead of assuming it — a wrong premise
        # must fail the case, never silently skip the gate assertion built on it.
        ver6030 = self._server_version_via_shell(6030)
        ver6130 = self._server_version_via_shell(6130)
        mapping_ok = (ver6030 == self.newServerVersion and
                      ver6130 is not None and ver6130 != self.newServerVersion)
        tdLog.info(f"mixed window port/version mapping: 6030={ver6030}, 6130={ver6130}, "
                   f"new={self.newServerVersion}, mapping_ok={mapping_ok}")
        # mapping_ok is the PREMISE of the version-gate assertion below — this test's
        # unique value over the other vtable cases. If it does not hold, the environment
        # deviates from the documented upgrade mapping (updateNewVersion mode-0 picked a
        # different dnode, or 6130 failed to come up), and the gate would silently
        # degenerate into a no-op pass. Fail loudly instead of skipping: a vacuous pass
        # here is strictly worse than a red CI signal.
        assert mapping_ok, (f"mixed window port/version mapping unexpected: 6030={ver6030}, "
                            f"6130={ver6130}, new={self.newServerVersion}; "
                            f"cannot validate version gate on a wrong premise")
        # Gate source for a_gate below: a BARE vntb (no tag-ref -- old builds support bare
        # vntbs too, and parCheckConstRefSourceSupported only gates tag-refs whose source
        # is an ntb/vntb). It is created via tdSql -> firstEp 6030, the UPGRADED dnode,
        # so a version-gate rejection is impossible here: any failure is a real
        # environment/regression signal and must fail the test -- no skip branch.
        tdSql.execute(f"CREATE VTABLE {DB}.b0 (ts TIMESTAMP, val INT FROM {DB}.c1.val) "
                      f"TAGS (loc NCHAR(20) = 'oldtown');")
        created, out = self._create_gate_table_via_shell(6130)
        # mapping_ok above already asserted ver6130 is not None, i.e. 6130 was reachable
        # moments ago -- an unreachable old dnode here is a torn-down race, not a state
        # to silently skip the gate assertion on (same fail-loudly contract as mapping_ok)
        assert "Unable to establish connection" not in out, \
            f"old dnode 6130 unreachable though mapping_ok confirmed it reachable: {out}"
        # reject REASON check kept on wording, not an exact string: the gate message
        # ("mixed-version cluster" / "Version not compatible") may be rephrased -- accept
        # either family case-insensitively so a text tweak upstream does not false-red
        # this case, while still distinguishing a version-gate rejection from unrelated
        # failures (name clash, syntax, ...)
        low = out.lower()
        assert not created and ("mixed-version" in low or "not compatible" in low), \
            f"old-connected create must be rejected by the version gate, got: {out}"
        tdLog.info("  PASS: mixed window: new-capability DDL rejected via old server")
        # checked only after a real rejection attempt -- otherwise the line below
        # would vacuously "pass" on a table that was never even attempted
        tdSql.error(f"SELECT t FROM {DB}.a_gate;")  # rejected DDL must leave no table behind
        try:
            tdSql.execute(f"DROP VTABLE {DB}.a_gate;")
        except Exception as e:
            tdLog.info(f"drop a_gate skipped (rejected DDL created nothing): {e}")

        # ---- phase 3: upgrade ALL dnodes; the feature then works end to end
        tdCb.updateNewVersion(tdCom.getBuildPath(), self.getDnodePath(), 1)
        time.sleep(10)
        self._wait_cluster_ready()
        self._check_vctb_ref_queries("post upgrade")

        # vntb-source const-ref chain: a_ref -> b1(vntb) -> c1(physical child)
        tdSql.execute(f"CREATE VTABLE {DB}.b1 (ts TIMESTAMP, val INT FROM {DB}.c1.val) "
                      f"TAGS (loc NCHAR(20) FROM {DB}.c1.city);")
        tdSql.execute(f"CREATE VTABLE {DB}.a_ref (ts TIMESTAMP, vv INT FROM {DB}.b1.val) "
                      f"TAGS (t_loc NCHAR(20) FROM {DB}.b1.loc);")
        row = self._one(f"SELECT t_loc FROM {DB}.a_ref;", "post upgrade: const-ref projection")
        assert row[0] == "beijing", f"post upgrade: a_ref.t_loc wrong: {row[0]}"
        tdSql.query(f"SELECT COUNT(*) FROM {DB}.a_ref WHERE t_loc='beijing';")
        assert int(tdSql.getData(0, 0)) == 3, "post upgrade: const-ref WHERE wrong"
        rows = self._rows(f"SELECT DISTINCT t_loc FROM {DB}.a_ref;")
        assert rows == [("beijing",)], f"post upgrade: const-ref DISTINCT wrong: {rows}"
        rows = self._rows(f"SELECT t_loc, COUNT(*) FROM {DB}.a_ref PARTITION BY t_loc;")
        assert rows == [("beijing", "3")], f"post upgrade: const-ref PARTITION BY wrong: {rows}"
        tdLog.info("  PASS: post upgrade: vntb-source chain SELECT/WHERE/DISTINCT/PARTITION BY")

        # chain through a virtual-child hop: the active tag-ref on the vctb must beat the
        # stale literal metaAlterTagRef leaves in ctbEntry.pTags
        tdSql.execute(f"CREATE STABLE {DB}.vc_stb (ts TIMESTAMP, val INT) TAGS (g NCHAR(20)) VIRTUAL 1;")
        tdSql.execute(f"CREATE VTABLE {DB}.vc_mid (val FROM {DB}.c1.val) USING {DB}.vc_stb TAGS ('stale_city');")
        tdSql.execute(f"ALTER TABLE {DB}.vc_mid SET TAG g = {DB}.c1.city;")
        tdSql.execute(f"CREATE VTABLE {DB}.b_vc (ts TIMESTAMP, val INT FROM {DB}.c1.val) "
                      f"TAGS (loc NCHAR(20) FROM {DB}.vc_mid.g);")
        tdSql.execute(f"CREATE VTABLE {DB}.a_vc (ts TIMESTAMP, vv INT FROM {DB}.b_vc.val) "
                      f"TAGS (t_loc NCHAR(20) FROM {DB}.b_vc.loc);")
        row = self._one(f"SELECT t_loc FROM {DB}.a_vc;", "post upgrade: vctb-hop chain")
        assert row[0] == "beijing", f"must not read vc_mid's stale literal: {row[0]}"
        tdSql.query(f"SELECT COUNT(*) FROM {DB}.a_vc WHERE t_loc='beijing';")
        assert int(tdSql.getData(0, 0)) == 3, "post upgrade: vctb-hop WHERE wrong"
        tdLog.info("  PASS: post upgrade: vctb-hop follows active ref, not the stale literal")

        tdLog.printNoPrefix("========== Tag-Ref Rolling Upgrade Compatibility Test Completed ==========")
