###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import os
import shlex
import shutil
import subprocess
import time

from new_test_framework.utils import tdLog, tdSql, tdDnodes


class TestFmnodeOfflineWalSurvival:
    """Coverage for `taosd -fMnode` (FS "支持手动修改 sdb" §4.2), the offline
    counterpart to `FLUSH MNODE`: taosd must already be stopped, it replays
    the WAL into memory, writes sdb.data, and truncates the WAL by calling
    the WAL API directly (no sync/raft involved).

    Same methodology as test_flush_mnode_wal_survival.py's online case: a
    marker created before the offline flush must survive total WAL loss
    afterwards; a marker created later and never flushed again must not --
    proving `-fMnode`'s truncation is the thing keeping the pre-flush state
    safe, not some incidental side effect.
    """

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.index = 1

    def _get_taosd_bin(self):
        candidates = []
        if tdDnodes.binPath:
            candidates.append(tdDnodes.binPath)

        taosd_bin = os.getenv("TAOSD_BIN")
        if taosd_bin:
            candidates.append(taosd_bin)

        taos_bin_path = os.getenv("TAOS_BIN_PATH")
        if taos_bin_path:
            candidates.append(os.path.join(taos_bin_path, "taosd"))

        for bin_path in candidates:
            if os.path.isfile(bin_path) and os.access(bin_path, os.X_OK):
                tdDnodes.binPath = bin_path
                tdLog.info("taosd found in %s" % bin_path)
                return bin_path

        tdLog.exit(
            "taosd not found! set TAOSD_BIN or TAOS_BIN_PATH when running this case standalone."
        )

    def _get_cfg_dir(self):
        return tdDnodes.dnodes[0].cfgDir

    def _get_data_dir(self):
        return os.path.join(os.path.dirname(self._get_cfg_dir()), "data")

    def _mnode_wal_dir(self):
        return os.path.join(self._get_data_dir(), "mnode", "wal")

    def _run_taosd(self, args, cwd=None, timeout_sec=30):
        bin_path = self._get_taosd_bin()
        cmd = [bin_path] + shlex.split(args)
        tdLog.info("run cmd: %s (cwd=%s)" % (" ".join(cmd), cwd))
        env = os.environ.copy()
        asan_options = env.get("ASAN_OPTIONS", "")
        if "detect_leaks=" not in asan_options:
            env["ASAN_OPTIONS"] = (
                "detect_leaks=0" if not asan_options else asan_options + ":detect_leaks=0"
            )
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            env=env,
            cwd=cwd,
            timeout=timeout_sec,
        )
        output = proc.stdout or ""
        tdLog.info("ret=%s output=%s" % (proc.returncode, output[:500].replace("\n", "\\n")))
        return proc.returncode, output

    def _user_exists(self, name):
        tdSql.query(f"select * from information_schema.ins_users where name='{name}';")
        return tdSql.queryRows > 0

    def test_fmnode_offline_wal_survival(self):
        """`taosd -fMnode` makes prior mnode state durable enough to survive total
        WAL loss; anything committed afterwards and never re-flushed does not.

        1. Create marker_before while taosd is up, then gracefully stop taosd
           (`-fMnode`'s documented precondition per FS 4.2 is that taosd is
           already stopped -- a graceful stop, not a mid-write crash, so this
           is the realistic operator workflow, not a race against WAL fsync).
        2. Run `taosd -fMnode`.
        3. Delete the mnode WAL directory entirely (on top of -fMnode's own
           truncation, to prove there is truly nothing left to lose from it).
        4. Restart taosd: marker_before must still be there.
        5. Create marker_after (never flushed), force-kill taosd (SIGKILL,
           simulating a genuine crash with nothing durably flushed), delete
           the WAL again, restart: marker_after must be gone (negative
           control -- this is the scenario `-fMnode` protects you FROM).

        Since: v3.4.3.0

        Labels: common,ci,integration,functional
        """
        cfg_dir = self._get_cfg_dir()
        marker_before = "fmn_wal_before"
        marker_after = "fmn_wal_after"

        tdSql.execute(f"create user {marker_before} pass 'Test1234!';")
        tdSql.checkEqual(self._user_exists(marker_before), True)

        tdLog.info("gracefully stopping taosd to run -fMnode offline flush")
        tdDnodes.stop(self.index)

        code, output = self._run_taosd(f"-fMnode -c {cfg_dir}", cwd=cfg_dir)
        tdSql.checkEqual(code, 0)
        tdLog.info("-fMnode output: %s" % output[:500].replace("\n", "\\n"))

        wal_dir = self._mnode_wal_dir()
        tdLog.info("deleting mnode WAL entirely (on top of -fMnode's own truncation): %s" % wal_dir)
        shutil.rmtree(wal_dir, ignore_errors=True)
        tdSql.checkEqual(os.path.isdir(wal_dir), False)

        tdLog.info("restarting taosd after -fMnode + WAL deletion")
        tdDnodes.start(self.index)
        time.sleep(2)

        tdSql.checkEqual(self._user_exists(marker_before), True)

        # Negative control: a change made *after* the offline flush and never
        # re-flushed must NOT survive a WAL wipe -- proving the previous
        # survival was really due to -fMnode's truncation, not luck.
        tdSql.execute(f"create user {marker_after} pass 'Test1234!';")
        tdSql.checkEqual(self._user_exists(marker_after), True)

        tdLog.info("force-killing taosd (SIGKILL) without a further flush")
        tdDnodes.forcestop(self.index)

        shutil.rmtree(wal_dir, ignore_errors=True)
        tdSql.checkEqual(os.path.isdir(wal_dir), False)

        tdDnodes.start(self.index)
        time.sleep(2)

        tdSql.checkEqual(self._user_exists(marker_before), True)
        after_survived = self._user_exists(marker_after)
        tdLog.info("marker_after present after WAL loss (expected False): %s" % after_survived)
        tdSql.checkEqual(after_survived, False)

        if self._user_exists(marker_before):
            tdSql.execute(f"drop user {marker_before};")
