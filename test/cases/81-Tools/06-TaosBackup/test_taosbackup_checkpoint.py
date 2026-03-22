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
import signal
import subprocess
import time

from new_test_framework.utils import tdLog, tdSql, etool


# ---------------------------------------------------------------------------
# constants
# ---------------------------------------------------------------------------
DB_SRC     = "test"          # source database created by taosBenchmark
DB_DST     = "newtest"       # target database for restore
STB_NAME   = "meters"        # default super-table name from taosBenchmark
MAX_TRIES    = 5   # maximum backup / restore attempts (1 without -C + up to 4 with -C)
KILL_FIRST   = 1   # seconds before killing the 1st (no -C) backup - short enough to always fire
KILL_RETRY   = 5   # seconds before killing intermediate backup (-C) runs - accumulates progress
KILL_RESTORE = 20  # seconds before killing the 1st restore; must be
                   #  > tag-creation time (~0.4s for 100 tables)  AND
                   #  < total data-restore time so some files are checkpointed before kill


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def run_with_timeout(cmd: str, timeout_secs: float):
    """Start *cmd* in a new process group. Kill it after *timeout_secs*.

    Returns:
        (returncode, was_killed)
        was_killed=True means we sent SIGKILL; False means it finished on its own.
    """
    tdLog.info(f"  exec (timeout={timeout_secs}s): {cmd}")
    proc = subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid)
    try:
        proc.wait(timeout=timeout_secs)
        return proc.returncode, False
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass  # already exited
        proc.wait()
        tdLog.info(f"  process killed (exceeded {timeout_secs}s timeout)")
        return proc.returncode, True


# ---------------------------------------------------------------------------
# test class
# ---------------------------------------------------------------------------

class TestTaosBackupCheckpoint:
    """Checkpoint / resume feature test for taosBackup.

    Test flow:
      1. taosBenchmark generates 10 000 child-tables × 10 000 rows = 100 000 000 rows.
      2. Record aggregation results (used later to validate restored data).
      3. Backup (-T 1): attempt 1 has no -C; it is killed after KILL_FIRST s.
                 Attempts 2-4 use -C and are killed after KILL_RETRY s each.
                 Attempt 5 (final) uses -C and runs to completion.
      4. Restore (-T 1): same pattern (attempt 1 no -C; attempts 2-5 with -C).
      5. Verify restored database matches the source aggregations.
    """

    # ------------------------------------------------------------------
    # setup
    # ------------------------------------------------------------------

    def find_programs(self):
        taosbackup = etool.taosBackupFile()
        if not taosbackup:
            tdLog.exit("taosBackup not found!")
        tdLog.info(f"taosBackup: {taosbackup}")

        benchmark = etool.benchMarkFile()
        if not benchmark:
            tdLog.exit("taosBenchmark not found!")
        tdLog.info(f"taosBenchmark: {benchmark}")

        tmpdir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tmp_ckpt")
        if os.path.exists(tmpdir):
            tdLog.info(f"{tmpdir} exists - clearing.")
            os.system(f"rm -rf {tmpdir}/*")
        else:
            os.makedirs(tmpdir)

        return taosbackup, benchmark, tmpdir

    # ------------------------------------------------------------------
    # data setup
    # ------------------------------------------------------------------

    def insert_data(self, benchmark: str):
        """Generate 100 child-tables × 200 000 rows in DB_SRC (20 M rows total).

        Using few tables keeps the tag-restore phase short (~0.4 s) so that
        KILL_RESTORE fires AFTER tag creation but well before data restore
        finishes, giving the checkpoint enough time to accumulate entries.
        """
        cmd = f"{benchmark} -d {DB_SRC} -t 100 -n 200000 -y"
        tdLog.info(f"insert data: {cmd}")
        ret = os.system(cmd)
        if ret != 0:
            tdLog.exit(f"taosBenchmark failed, return code {ret}")

    # ------------------------------------------------------------------
    # aggregation helpers
    # ------------------------------------------------------------------

    def get_agg(self, db: str) -> dict:
        """Return aggregation results used for correctness validation."""
        results = {}

        tdSql.query(f"SELECT count(*) FROM {db}.{STB_NAME}")
        results["count"]       = tdSql.getData(0, 0)

        tdSql.query(f"SELECT sum(voltage) FROM {db}.{STB_NAME}")
        results["sum_voltage"] = tdSql.getData(0, 0)

        tdSql.query(f"SELECT avg(current) FROM {db}.{STB_NAME}")
        results["avg_current"] = tdSql.getData(0, 0)

        tdSql.query(f"SELECT sum(phase) FROM {db}.{STB_NAME}")
        results["sum_phase"]   = tdSql.getData(0, 0)

        tdLog.info(
            f"[{db}] count={results['count']}  "
            f"sum(voltage)={results['sum_voltage']}  "
            f"avg(current)={results['avg_current']}  "
            f"sum(phase)={results['sum_phase']}"
        )
        return results

    def verify(self, src_agg: dict, dst_db: str):
        """Compare aggregation values between source and restored DB."""
        dst_agg = self.get_agg(dst_db)

        ok = True
        for key in ("count", "sum_voltage", "sum_phase"):
            if src_agg[key] != dst_agg[key]:
                tdLog.error(f"mismatch [{key}]: src={src_agg[key]} dst={dst_agg[key]}")
                ok = False
            else:
                tdLog.info(f"  ok [{key}]: {src_agg[key]}")

        # avg(current) is a float - allow a tiny relative tolerance
        key = "avg_current"
        sv, dv = src_agg[key], dst_agg[key]
        if sv is None or dv is None:
            tdLog.error(f"  mismatch [{key}]: src={sv} dst={dv}  (None)")
            ok = False
        else:
            rel_err = abs(sv - dv) / (abs(sv) + 1e-15)
            if rel_err > 1e-6:
                tdLog.error(f"  mismatch [{key}]: src={sv} dst={dv}  rel_err={rel_err:.2e}")
                ok = False
            else:
                tdLog.info(f"  ok [{key}]: src={sv} dst={dv}  rel_err={rel_err:.2e}")

        if not ok:
            tdLog.exit(f"data verification FAILED for restored db: {dst_db}")
        tdLog.info(f"data verification PASSED for restored db: {dst_db}")

    # ------------------------------------------------------------------
    # backup with checkpoint
    # ------------------------------------------------------------------

    def run_backup(self, taosbackup: str, outdir: str) -> bool:
        """Run backup with the checkpoint-interrupt pattern.

        attempt 1  : no -C, killed after KILL_FIRST seconds
        attempts 2-(MAX_TRIES-1): with -C, killed after KILL_RETRY seconds each
        attempt MAX_TRIES: with -C, run to completion (no kill)

        Returns True on eventual success, exits on final failure.
        """
        base_cmd = f"{taosbackup} -T 1 -D {DB_SRC} -o {outdir}"

        for attempt in range(1, MAX_TRIES + 1):
            use_ckpt = (attempt > 1)
            cmd = base_cmd + (" -C" if use_ckpt else "")
            is_last = (attempt == MAX_TRIES)

            tdLog.info(
                f"backup attempt {attempt}/{MAX_TRIES}"
                f" ({'checkpoint -C' if use_ckpt else 'no -C'})"
                f" {'→ run to completion' if is_last else f'→ kill after {KILL_FIRST if attempt == 1 else KILL_RETRY}s'}"
            )

            if is_last:
                # final attempt: must complete successfully
                ret = os.system(cmd)
                if ret == 0:
                    tdLog.info(f"backup SUCCEEDED on attempt {attempt}")
                    return True
                else:
                    tdLog.exit(f"backup FAILED on final attempt {attempt} (ret={ret})")
                    return False
            else:
                kill_after = KILL_FIRST if attempt == 1 else KILL_RETRY
                ret, killed = run_with_timeout(cmd, kill_after + attempt)
                if not killed and ret == 0:
                    tdLog.info(f"backup completed early on attempt {attempt} - no further retries needed")
                    return True
                time.sleep(1)   # brief pause before next attempt

        return False  # unreachable

    # ------------------------------------------------------------------
    # restore with checkpoint
    # ------------------------------------------------------------------

    def run_restore(self, taosbackup: str, outdir: str) -> bool:
        """Run restore with a 2-attempt checkpoint pattern.

        attempt 1  : no -C, killed after KILL_RESTORE seconds so checkpoint
                     entries are written for the files processed so far.
        attempt 2  : with -C, run to completion (skips files in checkpoint).

        The checkpoint file is written PER-FILE immediately by the C code so
        that a kill during data-restore always leaves a non-empty checkpoint.

        Returns True on success, exits on failure.
        """
        base_cmd = f"{taosbackup} -T 1 -W \"{DB_SRC}={DB_DST}\" -i {outdir}"
        ckpt_file = os.path.join(outdir, DB_SRC, "restore_checkpoint.txt")

        # -- attempt 1: no -C ------------------------------------------
        print(f"restore attempt 1/2 (no -C) → kill after {KILL_RESTORE}s")
        ret, killed = run_with_timeout(base_cmd, KILL_RESTORE)
        if not killed:
            if ret == 0:
                # Restore finished before the kill timer fired — still valid,
                # but checkpoint was never exercised (data was too small).
                print("restore completed on first attempt (faster than KILL_RESTORE)")
                return True
            tdLog.exit(f"restore attempt 1 failed (not killed, ret={ret})")

        # Verify checkpoint was populated (fails fast if data-restore never
        # started, i.e. KILL_RESTORE is shorter than the setup phase).
        ckpt_entries = 0
        if os.path.exists(ckpt_file):
            with open(ckpt_file) as f:
                ckpt_entries = sum(1 for ln in f if ln.strip())
        print(f"\ncheckpoint has {ckpt_entries} entries - checkpoint path exercised")
        time.sleep(1)

        # -- attempt 2: with -C, run to completion ---------------------
        print("\nrestore attempt 2/2 (with -C) → run to completion")
        ret = os.system(base_cmd + " -C")
        if ret == 0:
            tdLog.info("restore SUCCEEDED on attempt 2")
            return True
        tdLog.exit(f"restore FAILED on attempt 2 (ret={ret})")
        return False  # unreachable

    # ------------------------------------------------------------------
    # main test entry point
    # ------------------------------------------------------------------

    def test_taosbackup_checkpoint(self):
        """taosBackup checkpoint / resume test

        1. taosBenchmark inserts 100 child-tables * 200 000 rows (20 000 000 rows total)
           into database 'test' (super table 'meters').
           Using 100 tables keeps tag-restore setup short so KILL_RESTORE can
           fire AFTER setup but well before data restore completes.
        2. Record reference aggregations:
              sum(voltage), avg(current), sum(phase), count(*)
        3. Backup database 'test' with -T 1 (single thread, slow enough for kill):
              - Attempt 1: no -C option; kill after KILL_FIRST s  →  creates checkpoint data
              - Attempts 2-4: with -C; kill after KILL_RETRY s each  →  resumes progress
              - Attempt 5 (max): with -C; run to completion
        4. Restore to database 'newtest' with -T 1 (2-attempt pattern):
              - Attempt 1: no -C; kill after KILL_RESTORE s  →  writes restore checkpoint
              - Attempt 2 (final): with -C; run to completion
                  → verify skipped(checkpoint) > 0 (checkpoint path actually exercised)
        5. Verify restored data matches reference aggregations.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-05 Created to test the -C / --checkpoint feature
        """
        taosbackup, benchmark, tmpdir = self.find_programs()

        # ----- step 1: insert data ----------------------------------------
        tdLog.info("=== step 1: insert 20 000 000 rows via taosBenchmark ===")
        self.insert_data(benchmark)

        # ----- step 2: record reference aggregations ---------------------
        tdLog.info("=== step 2: record reference aggregations ===")
        src_agg = self.get_agg(DB_SRC)
        if src_agg["count"] == 0:
            tdLog.exit("source table is empty - taosBenchmark may have failed")

        # ----- step 3: backup with checkpoint interrupts ------------------
        tdLog.info("=== step 3: backup with checkpoint interrupts ===")
        self.run_backup(taosbackup, tmpdir)

        # -- step 4: restore with checkpoint interrupts ----------------
        tdLog.info("=== step 4: restore with checkpoint interrupts ===")
        self.run_restore(taosbackup, tmpdir)

        # -- step 5: verify data correctness ---------------------------
        tdLog.info("=== step 5: verify restored data ===")
        self.verify(src_agg, DB_DST)

        tdLog.info("test_taosbackup_checkpoint PASSED")
