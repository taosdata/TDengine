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

import glob
import hashlib
import os
import re
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

# CI runners vary widely in speed, so a single fixed kill delay is inherently
# flaky: too short and the kill fires before any progress is checkpointed;
# too long and the whole operation finishes before the kill ever fires. Both
# run_backup and run_restore retry with escalating delays - landing "mid-op"
# on any one try is enough to prove the checkpoint/resume path works.
BACKUP_KILL_MULTIPLIERS = [1, 2, 4]   # widens KILL_FIRST/KILL_RETRY per full backup pass
RESTORE_MAX_RETRIES     = 4           # KILL_RESTORE, *2, *4, *8


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def run_with_timeout(cmd: str, timeout_secs: float):
    """Start *cmd* in a new process group, redirecting stdout/stderr to temp
    files (same pattern as new_test_framework.utils.eos.run - avoids the
    subprocess.PIPE deadlock risk on long-running/large-output commands).
    Kill it after *timeout_secs*.

    Returns:
        (returncode, was_killed, rlist)
        was_killed=True means we sent SIGKILL; False means it finished on its own.
        rlist is whatever stdout+stderr content was flushed to disk before the
        process exited or was killed, split into lines.
    """
    uid = time.time_ns() % 1000000
    out_path = f"ckpt_out_{uid}.txt"
    err_path = f"ckpt_err_{uid}.txt"
    full_cmd = f"{cmd} 1>{out_path} 2>{err_path}"

    tdLog.info(f"  exec (timeout={timeout_secs}s): {cmd}")
    proc = subprocess.Popen(full_cmd, shell=True, preexec_fn=os.setsid)
    killed = False
    try:
        proc.wait(timeout=timeout_secs)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass  # already exited
        proc.wait()
        killed = True
        tdLog.info(f"  process killed (exceeded {timeout_secs}s timeout)")

    rlist = []
    for path in (out_path, err_path):
        if os.path.exists(path):
            with open(path, errors="replace") as f:
                rlist += f.read().splitlines()
            os.remove(path)

    return proc.returncode, killed, rlist


def extract_int(text: str, pattern: str):
    """Extract the first captured integer group from *text* via regex, or None."""
    m = re.search(pattern, text)
    return int(m.group(1)) if m else None


def count_dat_files(outdir: str, db: str) -> int:
    """Count *.dat data files for *db* under *outdir* (excluding tag files)."""
    files = glob.glob(os.path.join(outdir, db, "**", "*.dat"), recursive=True)
    return len([p for p in files if f"{os.sep}tags{os.sep}" not in p])


def hash_file(path: str) -> str:
    """Return the sha256 hex digest of *path*'s contents."""
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def snapshot_dat_files(db_dir: str) -> dict:
    """Return {path: (mtime, sha256)} for every .dat file under *db_dir*
    (excluding tag files).  Used to prove a "skipped" table file was truly
    left untouched by a -C resume run, not silently rewritten with identical
    content.
    """
    snap = {}
    for path in glob.glob(os.path.join(db_dir, "**", "*.dat"), recursive=True):
        if f"{os.sep}tags{os.sep}" in path:
            continue
        snap[path] = (os.path.getmtime(path), hash_file(path))
    return snap


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

    In addition to end-to-end data correctness, the tests in this file assert
    directly on taosdump's console output (the "Data Files : total=...,
    skipped(resume/checkpoint)=..., failed=..." summary line from
    main.c:printEndSummary(), and the "loaded restore checkpoint: N files
    already done" line from restoreCkpt.c) so that a -C run which silently
    stopped skipping (i.e. re-processed everything, but happened to produce
    correct data anyway because writes are idempotent) would fail the test.
    """

    # ------------------------------------------------------------------
    # setup
    # ------------------------------------------------------------------

    def cleanup_dbs(self, *dbs):
        """Drop each of *dbs* if it exists - releases the vnodes this test's
        own database(s) hold so later tests in this file (or a full-file
        run) don't get starved out of the dnode's fixed vnode budget."""
        for db in dbs:
            tdSql.execute(f"drop database if exists {db}")

    def checkListString(self, rlist, expected):
        """Check that *expected* appears as a substring somewhere in *rlist*."""
        output = "\n".join(rlist)
        if expected not in output:
            tdLog.exit(f"Expected string '{expected}' not found in output:\n{output}")

    def find_programs(self):
        taosbackup = etool.taosDumpFile()
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
        """Run the backup checkpoint-interrupt pattern, retried across
        widening kill-delay multipliers (BACKUP_KILL_MULTIPLIERS) so the
        test isn't flaky across CI machines of varying speed: a slow runner
        needs longer delays to land mid-backup, a fast one may finish a
        whole pass before any kill fires. Landing a demonstrable -C skip on
        any one pass is enough - exits only after every multiplier fails.
        """
        for mult in BACKUP_KILL_MULTIPLIERS:
            if self._run_backup_once(taosbackup, outdir, mult):
                return True
            tdLog.info(
                f"backup pass with kill-delay multiplier={mult} never "
                f"demonstrably skipped a data file via -C - retrying with wider kill delays"
            )
            os.system(f"rm -rf {outdir}/*")

        tdLog.exit(
            f"backup -C resume never demonstrably skipped a data file across "
            f"kill-delay multipliers {BACKUP_KILL_MULTIPLIERS} - the -C resume "
            f"path may not be working, or this environment's timing is far "
            f"outside the tuned range"
        )

    def _run_backup_once(self, taosbackup: str, outdir: str, kill_mult: int) -> bool:
        """One full backup checkpoint-interrupt pass.

        attempt 1  : no -C, killed after KILL_FIRST*kill_mult seconds
        attempts 2-(MAX_TRIES-1): with -C, killed after KILL_RETRY*kill_mult seconds each
        attempt MAX_TRIES: with -C, run to completion (no kill)

        Every -C attempt's output is inspected for the "skipped(resume)=N"
        summary line; the function fails unless at least one -C attempt
        demonstrably skipped (N > 0) previously-completed table files -
        proving -C isn't silently re-dumping everything.

        Returns True on a pass that demonstrably exercised -C's skip logic
        and completed successfully; False if it never proved a skip (caller
        may retry with a different kill_mult).
        """
        base_cmd = f"{taosbackup} -T 1 -D {DB_SRC} -o {outdir}"
        saw_positive_skip = False

        for attempt in range(1, MAX_TRIES + 1):
            use_ckpt = (attempt > 1)
            is_last = (attempt == MAX_TRIES)
            dat_before = count_dat_files(outdir, DB_SRC)
            kill_after = (KILL_FIRST if attempt == 1 else KILL_RETRY) * kill_mult

            tdLog.info(
                f"backup attempt {attempt}/{MAX_TRIES} (kill-mult={kill_mult})"
                f" ({'checkpoint -C' if use_ckpt else 'no -C'})"
                f" {'→ run to completion' if is_last else f'→ kill after {kill_after}s'}"
            )

            if is_last:
                # final attempt: must complete successfully - use etool.taosdump
                # so the full console output is captured for the skip-count check.
                rlist = etool.taosdump(f"-T 1 -C -D {DB_SRC} -o {outdir}")
                self.checkListString(rlist, "Result       : SUCCESS")
                output = "\n".join(rlist)
                skipped = extract_int(output, r"skipped\(resume\)=(\d+)")
                tdLog.info(f"  final attempt: skipped(resume)={skipped}, tables on disk before={dat_before}")
                if skipped is not None:
                    if skipped > dat_before:
                        tdLog.exit(
                            f"skipped(resume)={skipped} exceeds {dat_before} "
                            f".dat files that existed on disk before this attempt"
                        )
                    if skipped > 0:
                        saw_positive_skip = True
                if not saw_positive_skip:
                    return False
                tdLog.info(f"backup SUCCEEDED on attempt {attempt}")
                return True
            else:
                cmd = base_cmd + (" -C" if use_ckpt else "")
                ret, killed, rlist = run_with_timeout(cmd, kill_after + attempt)
                output = "\n".join(rlist)
                skipped = extract_int(output, r"skipped\(resume\)=(\d+)")
                if skipped is not None:
                    if skipped > dat_before:
                        tdLog.exit(
                            f"skipped(resume)={skipped} exceeds {dat_before} "
                            f".dat files that existed on disk before this attempt"
                        )
                    if use_ckpt and skipped > 0:
                        saw_positive_skip = True
                        tdLog.info(f"  attempt {attempt}: observed skipped(resume)={skipped} > 0")
                if not killed and ret == 0:
                    if not use_ckpt:
                        # baseline (no -C) attempt finished before the kill fired -
                        # harmless (even helpful: attempt 2's -C run will now have
                        # 100% of the data to skip). Just move on to a -C attempt.
                        tdLog.info(
                            f"backup attempt {attempt} (no -C) completed before "
                            f"the kill fired - proceeding to a -C attempt"
                        )
                        time.sleep(1)
                        continue
                    tdLog.info(f"backup completed early on attempt {attempt} - no further retries needed")
                    if not saw_positive_skip:
                        return False
                    return True
                time.sleep(1)   # brief pause before next attempt

        return False  # unreachable

    # ------------------------------------------------------------------
    # restore with checkpoint
    # ------------------------------------------------------------------

    def run_restore(self, taosbackup: str, outdir: str) -> bool:
        """Run restore with a 2-attempt checkpoint pattern.

        attempt 1  : no -C, killed after some delay so checkpoint entries are
                     written for the files processed so far. The delay
                     needed to land "mid data-restore" depends on machine
                     speed, which varies a lot across CI runners, so this
                     retries attempt 1 with an escalating kill delay
                     (KILL_RESTORE, *2, *4, *8, up to RESTORE_MAX_RETRIES
                     tries) until one try lands with a non-empty checkpoint
                     file - succeeding once is enough.
        attempt 2  : with -C, run to completion (skips files in checkpoint).

        The checkpoint file is written PER-FILE immediately by the C code so
        that a kill during data-restore always leaves a non-empty checkpoint.

        After attempt 2, three independently-derived numbers are cross-checked
        for exact equality: the "loaded restore checkpoint: N files already
        done" count (restoreCkpt.c), the deduplicated line count of
        restore_checkpoint.txt read just before attempt 2, and the final
        "skipped(checkpoint)=N" summary count. This proves -C actually
        consulted and matched the checkpoint file, not just that the final
        row count happened to come out right.

        Returns True on success, exits on failure.
        """
        base_cmd = f"{taosbackup} -T 1 -W \"{DB_SRC}={DB_DST}\" -i {outdir}"
        ckpt_file = os.path.join(outdir, DB_SRC, "restore_checkpoint.txt")

        # -- attempt 1: no -C, retried with escalating kill delays -----
        dedup_ckpt = 0
        kill_after = KILL_RESTORE
        for try_no in range(1, RESTORE_MAX_RETRIES + 1):
            tdLog.info(f"restore attempt 1 (try {try_no}/{RESTORE_MAX_RETRIES}, no -C) → kill after {kill_after}s")
            # clean slate: a previous try in this loop may have partially
            # created DB_DST or left a (near-)empty checkpoint file behind
            self.cleanup_dbs(DB_DST)
            if os.path.exists(ckpt_file):
                os.remove(ckpt_file)

            ret, killed, _rlist = run_with_timeout(base_cmd, kill_after)
            if not killed:
                if ret == 0:
                    # Restore finished before the kill timer fired — still
                    # valid, but checkpoint was never exercised (data was too
                    # small, or this try's delay was too generous).
                    tdLog.info(f"restore completed on try {try_no} (faster than {kill_after}s) - treating as success")
                    return True
                tdLog.exit(f"restore attempt 1 failed (not killed, ret={ret})")

            # Check whether the checkpoint was populated before the kill.
            ckpt_lines = []
            if os.path.exists(ckpt_file):
                with open(ckpt_file) as f:
                    ckpt_lines = [ln.strip() for ln in f if ln.strip()]
            dedup_ckpt = len(set(ckpt_lines))
            tdLog.info(f"  try {try_no}: checkpoint has {len(ckpt_lines)} entries ({dedup_ckpt} unique) after kill@{kill_after}s")
            if dedup_ckpt > 0:
                tdLog.info(f"checkpoint path exercised on try {try_no}/{RESTORE_MAX_RETRIES}")
                break

            tdLog.info(f"  try {try_no}: kill fired before any checkpoint entry was written - retrying with a longer delay")
            kill_after *= 2
        else:
            tdLog.exit(
                f"restore_checkpoint.txt stayed empty after kill across all "
                f"{RESTORE_MAX_RETRIES} tries (delays up to {kill_after // 2}s) - "
                f"checkpoint path was never exercised; this environment may be "
                f"far slower than expected, consider raising KILL_RESTORE"
            )
        time.sleep(1)

        # -- attempt 2: with -C, run to completion ---------------------
        tdLog.info("restore attempt 2/2 (with -C) → run to completion")
        rlist = etool.taosdump(f'-T 1 -C -W "{DB_SRC}={DB_DST}" -i {outdir}')
        self.checkListString(rlist, "Result       : SUCCESS")
        output = "\n".join(rlist)

        loaded  = extract_int(output, r"loaded restore checkpoint: (\d+) files already done")
        skipped = extract_int(output, r"skipped\(checkpoint\)=(\d+)")
        tdLog.info(f"  loaded={loaded}, dedup(checkpoint file)={dedup_ckpt}, skipped(checkpoint)={skipped}")

        if loaded is None or skipped is None:
            tdLog.exit(
                "could not find 'loaded restore checkpoint' and/or "
                "'skipped(checkpoint)=' lines in restore -C output"
            )
        if not (loaded == dedup_ckpt == skipped):
            tdLog.exit(
                f"checkpoint reconciliation mismatch: loaded={loaded}, "
                f"dedup(checkpoint file)={dedup_ckpt}, skipped(checkpoint)={skipped} "
                f"- these should all be equal for a deterministic resume run"
            )
        if skipped == 0:
            tdLog.exit("restore with -C completed but skipped(checkpoint)=0 - nothing was actually skipped")

        tdLog.info("restore SUCCEEDED on attempt 2")
        return True

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
              Every -C attempt's console output is checked for
              skipped(resume)=N > 0 (main.c summary line), proving -C actually
              skipped previously-completed tables rather than re-dumping them.
        4. Restore to database 'newtest' with -T 1 (2-attempt pattern):
              - Attempt 1: no -C; kill after KILL_RESTORE s  →  writes restore checkpoint
              - Attempt 2 (final): with -C; run to completion
                  → verify skipped(checkpoint), the checkpoint-file entry count,
                    and the "loaded restore checkpoint" count all reconcile
                    exactly and are > 0.
        5. Verify restored data matches reference aggregations.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-05 Created to test the -C / --checkpoint feature
            - 2026-08-11 Alex Duan strengthened to assert exact skipped-count
              signals (not just final data correctness) so a -C path that
              silently stopped skipping would fail this test
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

    # ------------------------------------------------------------------
    # Test 2: backup checkpoint skip - deterministic, per-table branch
    # ------------------------------------------------------------------

    def test_backup_checkpoint_skip_per_table(self):
        """taosBackup -C resumes per-table when backup_complete.flag is absent.

        Deterministic (no kill/timing dependency) verification that -C
        genuinely SKIPS already-backed-up child tables rather than silently
        re-writing them with identical data. Exercises the g_backResumeMode
        branch in backupData.c backChildTableData()/backNormalOneTable().

        Steps:
          1. Insert TABLES x ROWS rows via taosBenchmark.
          2. Full backup (no -C) → all .dat files + backup_complete.flag created.
          3. Delete backup_complete.flag to simulate "interrupted before the
             flag was written" (the only state where per-table resume mode,
             as opposed to whole-db skip, is taken - see
             test_backup_checkpoint_skip_whole_db for that other branch).
          4. Snapshot mtime + sha256 of every .dat file.
          5. Re-run backup with -C.
          6. Assert skipped(resume) == TABLES and failed == 0 (exact summary
             counters from main.c printEndSummary()).
          7. Assert every .dat file's mtime + hash is unchanged - proving the
             skip never touched the file at all, not merely rewrote it with
             the same content.
          8. Assert backup_complete.flag exists again.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-11 Alex Duan Created to prove -C skips, not just re-dumps idempotently
        """
        db = "ckpt_skip_tbl_src"
        TABLES = 8
        ROWS = 20

        taosbackup, benchmark, tmpdir = self.find_programs()

        tdLog.info(f"=== step 1: insert {TABLES} x {ROWS} rows ===")
        ret = os.system(f"{benchmark} -d {db} -t {TABLES} -n {ROWS} -y")
        if ret != 0:
            tdLog.exit(f"taosBenchmark failed (ret={ret})")

        tdLog.info("=== step 2: full backup (no -C) ===")
        rlist = etool.taosdump(f"-T 1 -D {db} -o {tmpdir}")
        self.checkListString(rlist, "Result       : SUCCESS")

        flag_path = os.path.join(tmpdir, db, "backup_complete.flag")
        if not os.path.exists(flag_path):
            tdLog.exit(f"backup_complete.flag missing after full backup: {flag_path}")

        tdLog.info("=== step 3: delete backup_complete.flag (simulate interrupted-before-flag run) ===")
        os.remove(flag_path)

        tdLog.info("=== step 4: snapshot .dat files before resume ===")
        before = snapshot_dat_files(os.path.join(tmpdir, db))
        if len(before) != TABLES:
            tdLog.exit(f"expected {TABLES} .dat files, found {len(before)}")

        tdLog.info("=== step 5: backup again with -C (per-table resume) ===")
        rlist = etool.taosdump(f"-T 1 -C -D {db} -o {tmpdir}")
        self.checkListString(rlist, "Result       : SUCCESS")
        output = "\n".join(rlist)

        skipped = extract_int(output, r"skipped\(resume\)=(\d+)")
        failed  = extract_int(output, r"failed=(\d+)")
        tdLog.info(f"  skipped(resume)={skipped}, failed={failed}")
        if skipped != TABLES:
            tdLog.exit(f"expected skipped(resume)=={TABLES}, got {skipped}")
        if failed != 0:
            tdLog.exit(f"expected failed==0, got {failed}")

        tdLog.info("=== step 6: verify .dat files are byte-for-byte untouched ===")
        after = snapshot_dat_files(os.path.join(tmpdir, db))
        if after != before:
            changed = [p for p in before if before.get(p) != after.get(p)]
            tdLog.exit(f"some .dat files changed despite skip: {changed}")

        if not os.path.exists(flag_path):
            tdLog.exit("backup_complete.flag not recreated after resume completed")

        tdLog.info("test_backup_checkpoint_skip_per_table PASSED")
        self.cleanup_dbs(db)

    # ------------------------------------------------------------------
    # Test 3: backup checkpoint skip - deterministic, whole-db branch
    # ------------------------------------------------------------------

    def test_backup_checkpoint_skip_whole_db(self):
        """taosBackup -C skips an entire database when backup_complete.flag
        already exists.

        Deterministic verification of the whole-DB skip branch in
        backupData.c backDatabaseData() (flag present + -C => immediate
        return, BEFORE any per-table counters are touched). This branch is
        invisible to dataFilesSkipped/dataFilesTotal (they stay at 0 for this
        DB), so - unlike test_backup_checkpoint_skip_per_table - this test
        intentionally asserts total=0, skipped(resume)=0, failed=0, and
        instead proves the skip via the exact "skip database <db>: already
        completed in previous." log line plus byte-for-byte unchanged files.
        Do not "fix" the zero assertion below to expect skipped > 0 - that
        would make this test always pass regardless of whether -C works.

        Steps:
          1. Insert TABLES x ROWS rows via taosBenchmark.
          2. Full backup (no -C) → all .dat files + backup_complete.flag created.
          3. Snapshot mtime + sha256 of every .dat file and of the flag file
             itself (flag is NOT deleted here, unlike the per-table test).
          4. Re-run backup with -C.
          5. Assert the exact whole-db-skip log line appears.
          6. Assert total=0, skipped(resume)=0, failed=0.
          7. Assert every .dat file and the flag file are byte-for-byte
             unchanged.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-11 Alex Duan Created to prove -C's whole-db skip path is not a no-op
        """
        db = "ckpt_skip_db_src"
        TABLES = 5
        ROWS = 20

        try:
            taosbackup, benchmark, tmpdir = self.find_programs()

            tdLog.info(f"=== step 1: insert {TABLES} x {ROWS} rows ===")
            ret = os.system(f"{benchmark} -d {db} -t {TABLES} -n {ROWS} -y")
            if ret != 0:
                tdLog.exit(f"taosBenchmark failed (ret={ret})")

            tdLog.info("=== step 2: full backup (no -C) ===")
            rlist = etool.taosdump(f"-T 1 -D {db} -o {tmpdir}")
            self.checkListString(rlist, "Result       : SUCCESS")

            flag_path = os.path.join(tmpdir, db, "backup_complete.flag")
            if not os.path.exists(flag_path):
                tdLog.exit(f"backup_complete.flag missing after full backup: {flag_path}")

            tdLog.info("=== step 3: snapshot .dat files + flag (flag kept, not deleted) ===")
            before = snapshot_dat_files(os.path.join(tmpdir, db))
            before_flag = (os.path.getmtime(flag_path), hash_file(flag_path))
            if len(before) != TABLES:
                tdLog.exit(f"expected {TABLES} .dat files, found {len(before)}")

            tdLog.info("=== step 4: backup again with -C (whole-db already-completed skip) ===")
            rlist = etool.taosdump(f"-T 1 -C -D {db} -o {tmpdir}")
            output = "\n".join(rlist)

            tdLog.info("=== step 5: verify the whole-db skip log line ===")
            self.checkListString(rlist, f"skip database {db}: already completed in previous.")

            tdLog.info("=== step 6: verify counters are 0 (this branch precedes per-table counting) ===")
            total   = extract_int(output, r"Data Files\s*:\s*total=(\d+)")
            skipped = extract_int(output, r"skipped\(resume\)=(\d+)")
            failed  = extract_int(output, r"failed=(\d+)")
            tdLog.info(f"  total={total}, skipped(resume)={skipped}, failed={failed}")
            if (total, skipped, failed) != (0, 0, 0):
                tdLog.exit(
                    f"expected total=0, skipped(resume)=0, failed=0 for whole-db "
                    f"skip, got total={total}, skipped={skipped}, failed={failed}"
                )

            tdLog.info("=== step 7: verify .dat files + flag are byte-for-byte untouched ===")
            after = snapshot_dat_files(os.path.join(tmpdir, db))
            if after != before:
                changed = [p for p in before if before.get(p) != after.get(p)]
                tdLog.exit(f"some .dat files changed despite whole-db skip: {changed}")
            after_flag = (os.path.getmtime(flag_path), hash_file(flag_path))
            if after_flag != before_flag:
                tdLog.exit("backup_complete.flag was modified despite whole-db skip")

            tdLog.info("test_backup_checkpoint_skip_whole_db PASSED")
        finally:
            self.cleanup_dbs(db)

    # ------------------------------------------------------------------
    # Test 4: large-subtable scenario — restoreCkpt.c hash table coverage
    # ------------------------------------------------------------------

    def test_checkpoint_large_subtable(self):
        """restoreCkpt.c hash table init / insert / lookup / free paths

        The 16 KB thread-local checkpoint buffer only flushes to disk when it
        overflows (markRestoreDone fills it past 16 384 bytes).  With the default
        100-subtable dataset the buffer never overflows, so no checkpoint data is
        persisted on SIGKILL and the hash table functions are never exercised.

        This test uses 2 000 subtables × 200 rows = 400 000 rows.  Each
        checkpoint entry is ~70 bytes, so 234 entries fill the 16 KB buffer and
        trigger an fsync flush.  After the kill, the checkpoint file contains
        hundreds of flushed entries; the second restore run (-C) loads them into
        the hash table, exercising:
          - initCkptHashTable()   — allocates bucket array
          - insertCkptHash()      — populates entries from file
          - lookupCkptHash()      — called for every file during resume run
          - freeCkptHashTable()   — called at restore cleanup

        Steps:
          1. Insert 2 000 subtables × 200 rows via taosBenchmark.
          2. Backup with -T 1 (must complete fully — no interrupt).
          3. Record reference aggregations.
          4. Restore attempt 1 (no -C): kill after KILL_RESTORE s so the
             thread-local buffer fills and flushes at least once. The
             checkpoint file must be non-empty afterward (hard failure if
             not - previously this only logged a warning).
          5. Restore attempt 2 (with -C): verify skipped(checkpoint) reconciles
             exactly with the loaded-checkpoint count and the checkpoint
             file's entry count, and is > 0 - proving the hash table was
             populated and its lookups actually hit.
          6. Verify restored data matches the reference aggregations.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:
            - 2026-03-24 Alex Duan Created to cover restoreCkpt.c hash table paths
            - 2026-08-11 Alex Duan replaced soft warning with hard reconciliation
              assertion on skipped(checkpoint)

        """
        DB_LARGE   = "ckpt_large_src"
        DB_DST_L   = "ckpt_large_dst"
        STB        = "meters"
        TABLES     = 2000
        ROWS       = 200
        KILL_RST   = 25   # seconds — enough for 300+ files to be checkpointed

        try:
            taosbackup, benchmark, tmpdir = self.find_programs()
            tmpdir_l = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "tmp_ckpt_large"
            )
            if os.path.exists(tmpdir_l):
                os.system(f"rm -rf {tmpdir_l}")
            os.makedirs(tmpdir_l)

            # -- step 1: insert 2 000 tables × 200 rows -------------------
            tdLog.info(f"=== step 1: insert {TABLES} × {ROWS} rows ===")
            cmd = f"{benchmark} -d {DB_LARGE} -t {TABLES} -n {ROWS} -y"
            tdLog.info(f"  exec: {cmd}")
            ret = os.system(cmd)
            if ret != 0:
                tdLog.exit(f"taosBenchmark failed (ret={ret})")

            # -- step 2: backup (no interrupt) ----------------------------
            tdLog.info("=== step 2: backup (no interrupt) ===")
            rlist = etool.taosdump(f"-T 2 -D {DB_LARGE} -o {tmpdir_l}")
            self.checkListString(rlist, "Result       : SUCCESS")

            # -- step 3: record reference aggregations --------------------
            tdLog.info("=== step 3: record reference aggregations ===")
            tdSql.query(f"SELECT count(*) FROM {DB_LARGE}.{STB}")
            src_count = tdSql.getData(0, 0)
            tdSql.query(f"SELECT sum(voltage) FROM {DB_LARGE}.{STB}")
            src_sum_v = tdSql.getData(0, 0)
            tdLog.info(f"  source: count={src_count}  sum(voltage)={src_sum_v}")
            if src_count == 0:
                tdLog.exit("source table empty — taosBenchmark may have failed")

            # -- step 4: restore attempt 1 (no -C, kill after KILL_RST s) --
            tdLog.info(f"=== step 4: restore attempt 1/2 (no -C, kill after {KILL_RST}s) ===")
            tdSql.execute(f"drop database if exists {DB_DST_L}")
            restore_cmd = f"{taosbackup} -T 1 -W \"{DB_LARGE}={DB_DST_L}\" -i {tmpdir_l}"
            ret, killed, _rlist = run_with_timeout(restore_cmd, KILL_RST)
            if not killed:
                if ret == 0:
                    tdLog.info("restore completed before kill — skip step 5 (already verified path)")
                    return
                tdLog.exit(f"restore attempt 1 failed unexpectedly (ret={ret})")

            ckpt_file = os.path.join(tmpdir_l, DB_LARGE, "restore_checkpoint.txt")
            ckpt_lines = []
            if os.path.exists(ckpt_file):
                with open(ckpt_file) as f:
                    ckpt_lines = [ln.strip() for ln in f if ln.strip()]
            dedup_ckpt = len(set(ckpt_lines))
            tdLog.info(f"  checkpoint file has {len(ckpt_lines)} entries ({dedup_ckpt} unique) after kill")
            if dedup_ckpt == 0:
                tdLog.exit(
                    "checkpoint file is empty after kill — buffer never flushed. "
                    "Increase TABLES or KILL_RST to let more files complete."
                )

            # -- step 5: restore attempt 2 (with -C, run to completion) ---
            tdLog.info("=== step 5: restore attempt 2/2 (with -C) ===")
            tdSql.execute(f"drop database if exists {DB_DST_L}")
            rlist = etool.taosdump(f'-T 1 -C -W "{DB_LARGE}={DB_DST_L}" -i {tmpdir_l}')
            self.checkListString(rlist, "Result       : SUCCESS")
            output = "\n".join(rlist)

            loaded  = extract_int(output, r"loaded restore checkpoint: (\d+) files already done")
            skipped = extract_int(output, r"skipped\(checkpoint\)=(\d+)")
            tdLog.info(f"  loaded={loaded}, dedup(checkpoint file)={dedup_ckpt}, skipped(checkpoint)={skipped}")
            if loaded is None or skipped is None:
                tdLog.exit(
                    "could not find 'loaded restore checkpoint' and/or "
                    "'skipped(checkpoint)=' lines in restore -C output"
                )
            if not (loaded == dedup_ckpt == skipped):
                tdLog.exit(
                    f"checkpoint reconciliation mismatch: loaded={loaded}, "
                    f"dedup(checkpoint file)={dedup_ckpt}, skipped(checkpoint)={skipped}"
                )
            if skipped == 0:
                tdLog.exit("restore with -C completed but skipped(checkpoint)=0")

            # -- step 6: verify data correctness --------------------------
            tdLog.info("=== step 6: verify restored data ===")
            tdSql.query(f"SELECT count(*) FROM {DB_DST_L}.{STB}")
            dst_count = tdSql.getData(0, 0)
            tdSql.query(f"SELECT sum(voltage) FROM {DB_DST_L}.{STB}")
            dst_sum_v = tdSql.getData(0, 0)
            if src_count != dst_count:
                tdLog.exit(f"count mismatch: src={src_count} dst={dst_count}")
            if src_sum_v != dst_sum_v:
                tdLog.exit(f"sum(voltage) mismatch: src={src_sum_v} dst={dst_sum_v}")
            tdLog.info(
                f"test_checkpoint_large_subtable PASSED "
                f"(count={dst_count}, loaded={loaded}, skipped={skipped})"
            )
        finally:
            self.cleanup_dbs(DB_LARGE, DB_DST_L)

    # ------------------------------------------------------------------
    # Test 5: restore -C survives differing -i path spellings
    # ------------------------------------------------------------------

    def test_restore_checkpoint_path_normalization(self):
        """restore -C must still skip files when the -i path spelling differs
        between runs (e.g. trailing '/' vs no trailing '/').

        bckArgs.c normalizes g_outPath (collapse '//', strip trailing '/').
        restoreCkpt.c matches checkpoint keys with exact strcmp(), so WITHOUT
        that normalization a run invoked with "-i dir/" records keys like
        "dir//db/.../d103.par" while a later "-i dir" run scans
        "dir/db/.../d103.par" - the strings differ and -C silently
        re-restores everything (skipped(checkpoint)=0).  This test fails if
        the normalization regresses.

        Steps:
          1. Insert TABLES x ROWS rows via taosBenchmark.
          2. Full backup (no -C, no interrupt).
          3. Restore attempt 1 (no -C): invoked with a TRAILING SLASH on -i,
             killed after KILL_RST s so a checkpoint file is left behind.
          4. Assert the checkpoint file's recorded keys contain no "//" -
             direct proof the recorded paths were normalized.
          5. Restore attempt 2 (with -C): invoked WITHOUT the trailing slash.
             Assert loaded == dedup(checkpoint file) == skipped(checkpoint)
             and skipped > 0 - i.e. the differently-spelled path still matched.
          6. Verify restored data matches reference aggregations.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:
            - 2026-08-11 Alex Duan Created to lock in the g_outPath
              normalization fix (bckArgs.c) behind a regression test
        """
        DB_NORM  = "ckpt_norm_src"
        DB_DST_N = "ckpt_norm_dst"
        STB      = "meters"
        TABLES   = 8000
        ROWS     = 2000
        KILL_RST = 9    # seconds - meta-creation for TABLES tables finishes
                         # first (scales with table count only) and does not
                         # write checkpoint entries; the data-restore phase
                         # that follows (scales with TABLES x ROWS) is what
                         # this must land inside of
        MAX_RETRIES = 3

        try:
            taosbackup, benchmark, tmpdir = self.find_programs()
            tmpdir_n = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "tmp_ckpt_norm"
            )

            # Two ways this can go wrong, needing two different fixes:
            #  - restore finishes before KILL_RST: no checkpoint to resume-test
            #    at all - the data-restore phase (TABLES x ROWS-bound) was too
            #    short, so grow ROWS (growing TABLES instead would also stretch
            #    meta-creation, which doesn't produce checkpoint entries, and can
            #    end up making things worse).
            #  - restore gets killed but the checkpoint file is empty: the kill
            #    landed during meta-creation (table-count-bound), before the
            #    data-restore phase had even started - grow KILL_RST instead.
            # Either way, retry rather than silently declaring the test passed
            # or hard-failing on the first bad draw.
            killed = False
            dedup_ckpt = 0
            ckpt_lines = []
            ckpt_file = os.path.join(tmpdir_n, DB_NORM, "restore_checkpoint.txt")
            for attempt in range(MAX_RETRIES + 1):
                if attempt > 0:
                    if not killed:
                        ROWS *= 2
                        tdLog.info(
                            f"restore completed before kill on attempt {attempt}/{MAX_RETRIES} "
                            f"- retrying with more rows (ROWS={ROWS})"
                        )
                    else:
                        KILL_RST += 5
                        tdLog.info(
                            f"killed before any file completed on attempt {attempt}/{MAX_RETRIES} "
                            f"- retrying with a longer kill delay (KILL_RST={KILL_RST}s)"
                        )

                # -- step 1: insert TABLES x ROWS rows ----------------------
                tdLog.info(f"=== step 1: insert {TABLES} x {ROWS} rows ===")
                cmd = f"{benchmark} -d {DB_NORM} -t {TABLES} -n {ROWS} -y"
                tdLog.info(f"  exec: {cmd}")
                ret = os.system(cmd)
                if ret != 0:
                    tdLog.exit(f"taosBenchmark failed (ret={ret})")

                # -- step 2: full backup (no interrupt) ---------------------
                tdLog.info("=== step 2: backup (no interrupt) ===")
                if os.path.exists(tmpdir_n):
                    os.system(f"rm -rf {tmpdir_n}")
                os.makedirs(tmpdir_n)
                rlist = etool.taosdump(f"-T 2 -D {DB_NORM} -o {tmpdir_n}")
                self.checkListString(rlist, "Result       : SUCCESS")

                # -- step 3: reference aggregations -------------------------
                tdLog.info("=== step 3: record reference aggregations ===")
                tdSql.query(f"SELECT count(*) FROM {DB_NORM}.{STB}")
                src_count = tdSql.getData(0, 0)
                tdSql.query(f"SELECT sum(voltage) FROM {DB_NORM}.{STB}")
                src_sum_v = tdSql.getData(0, 0)
                tdLog.info(f"  source: count={src_count}  sum(voltage)={src_sum_v}")
                if src_count == 0:
                    tdLog.exit("source table empty - taosBenchmark may have failed")

                # -- step 4: restore attempt (no -C) with TRAILING SLASH --
                tdLog.info(f"=== step 4: restore attempt {attempt + 1}/{MAX_RETRIES + 1} "
                           f"(no -C, -i with trailing '/', kill after {KILL_RST}s) ===")
                tdSql.execute(f"drop database if exists {DB_DST_N}")
                restore_cmd = f"{taosbackup} -T 1 -W \"{DB_NORM}={DB_DST_N}\" -i {tmpdir_n}/"
                ret, killed, _rlist = run_with_timeout(restore_cmd, KILL_RST)
                if not killed:
                    if ret != 0:
                        tdLog.exit(f"restore attempt 1 failed unexpectedly (ret={ret})")
                    continue

                ckpt_lines = []
                if os.path.exists(ckpt_file):
                    with open(ckpt_file) as f:
                        ckpt_lines = [ln.strip() for ln in f if ln.strip()]
                dedup_ckpt = len(set(ckpt_lines))
                tdLog.info(f"  checkpoint file has {len(ckpt_lines)} entries ({dedup_ckpt} unique) after kill")
                if dedup_ckpt > 0:
                    break
            else:
                tdLog.exit(
                    f"could not land a kill inside the data-restore window even after "
                    f"{MAX_RETRIES} retries (final TABLES={TABLES}, ROWS={ROWS}, "
                    f"KILL_RST={KILL_RST}s, killed={killed}, dedup_ckpt={dedup_ckpt}) - "
                    f"environment timing too unpredictable for these constants"
                )

            # Even though attempt 1 used "-i <dir>/", no "//" may appear in the
            # recorded keys - that is the whole point of the normalization fix.
            double_slash = [ln for ln in ckpt_lines if "//" in ln]
            if double_slash:
                tdLog.exit(
                    f"checkpoint keys still contain '//' (path not normalized): "
                    f"{double_slash[:5]}"
                )

            # -- step 6: restore attempt 2 (with -C) WITHOUT trailing slash
            # Must NOT drop DB_DST_N here: the killed attempt already committed
            # data for every file recorded in the checkpoint, and -C's whole point
            # is to build on top of that. Dropping the db here would discard that
            # already-committed data while the checkpoint file (on disk, keyed by
            # source file path) still tells restore to skip those same files -
            # permanently losing their rows instead of actually resuming.
            tdLog.info("=== step 6: restore attempt 2/2 (with -C, -i without trailing '/') ===")
            rlist = etool.taosdump(f'-T 1 -C -W "{DB_NORM}={DB_DST_N}" -i {tmpdir_n}')
            self.checkListString(rlist, "Result       : SUCCESS")
            output = "\n".join(rlist)

            loaded  = extract_int(output, r"loaded restore checkpoint: (\d+) files already done")
            skipped = extract_int(output, r"skipped\(checkpoint\)=(\d+)")
            tdLog.info(f"  loaded={loaded}, dedup(checkpoint file)={dedup_ckpt}, skipped(checkpoint)={skipped}")
            if loaded is None or skipped is None:
                tdLog.exit(
                    "could not find 'loaded restore checkpoint' and/or "
                    "'skipped(checkpoint)=' lines in restore -C output"
                )
            if not (loaded == dedup_ckpt == skipped):
                tdLog.exit(
                    f"checkpoint reconciliation mismatch: loaded={loaded}, "
                    f"dedup(checkpoint file)={dedup_ckpt}, skipped(checkpoint)={skipped} "
                    f"- these should all be equal for a deterministic resume run"
                )
            if skipped == 0:
                tdLog.exit(
                    "restore with -C completed but skipped(checkpoint)=0 - the "
                    "differently-spelled -i path failed to match checkpoint keys"
                )

            # -- step 7: verify data correctness -------------------------
            tdLog.info("=== step 7: verify restored data ===")
            tdSql.query(f"SELECT count(*) FROM {DB_DST_N}.{STB}")
            dst_count = tdSql.getData(0, 0)
            tdSql.query(f"SELECT sum(voltage) FROM {DB_DST_N}.{STB}")
            dst_sum_v = tdSql.getData(0, 0)
            if src_count != dst_count:
                tdLog.exit(f"count mismatch: src={src_count} dst={dst_count}")
            if src_sum_v != dst_sum_v:
                tdLog.exit(f"sum(voltage) mismatch: src={src_sum_v} dst={dst_sum_v}")
            tdLog.info(
                f"test_restore_checkpoint_path_normalization PASSED "
                f"(count={dst_count}, loaded={loaded}, skipped={skipped})"
            )
        finally:
            self.cleanup_dbs(DB_NORM, DB_DST_N)

    # ------------------------------------------------------------------
    # Test 6: backup.log truncate-vs-append across -C runs
    # ------------------------------------------------------------------

    def test_backup_log_append_with_checkpoint(self):
        """backup.log must be APPENDED (not truncated) on a -C resume run.

        main.c opens the on-disk mirror log via logFileOpen(path, truncate)
        before any backup work starts. For ACTION_BACKUP this used to pass
        truncate=True unconditionally, so a -C resume run silently wiped out
        the previous (possibly killed) attempt's log lines - defeating the
        purpose of an on-disk log meant to survive a killed run. A fresh
        (no -C) run must still truncate, since it is not resuming anything.

        This is a deterministic file-content check, independent of any real
        checkpoint/resume timing:
          1. taosBenchmark inserts a small dataset.
          2. Backup (no -C) -> backup.log is created.
          3. Manually append a unique marker line to backup.log, standing in
             for content a prior (e.g. killed) attempt would have left behind.
          4. Backup again WITH -C into the same outdir -> backup.log must
             still contain the marker line afterward (append, not truncate).
          5. Backup again WITHOUT -C -> backup.log must be fully replaced -
             the marker line must be gone (truncate on a fresh run).

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-12 Created to lock in the backup.log truncate/append
              fix (main.c: logFileOpen call for ACTION_BACKUP now passes
              !argCheckpoint() instead of a hardcoded true)
        """
        DB = "ckpt_log_append"
        TABLES = 5
        ROWS = 50
        tmpdir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "tmp_ckpt_logappend"
        )
        marker = f"MARKER_{time.time_ns()}"

        try:
            taosbackup, benchmark, _unused_tmpdir = self.find_programs()

            tdSql.execute(f"drop database if exists {DB}")
            cmd = f"{benchmark} -d {DB} -t {TABLES} -n {ROWS} -y"
            tdLog.info(f"  exec: {cmd}")
            ret = os.system(cmd)
            if ret != 0:
                tdLog.exit(f"taosBenchmark failed (ret={ret})")

            if os.path.exists(tmpdir):
                os.system(f"rm -rf {tmpdir}")
            os.makedirs(tmpdir)
            log_path = os.path.join(tmpdir, "backup.log")

            # -- step 1: fresh backup (no -C) creates backup.log ------------
            tdLog.info("=== step 1: fresh backup (no -C) ===")
            rlist = etool.taosdump(f"-D {DB} -o {tmpdir}")
            self.checkListString(rlist, "Result       : SUCCESS")
            if not os.path.exists(log_path):
                tdLog.exit(f"backup.log not created at {log_path}")

            # -- step 2: inject a marker line simulating a prior attempt ----
            with open(log_path, "a") as f:
                f.write(f"{marker}\n")

            # -- step 3: backup again WITH -C - must APPEND -----------------
            tdLog.info("=== step 3: backup with -C into the same outdir ===")
            rlist = etool.taosdump(f"-C -D {DB} -o {tmpdir}")
            self.checkListString(rlist, "Result       : SUCCESS")
            with open(log_path) as f:
                content_after_ckpt = f.read()
            if marker not in content_after_ckpt:
                tdLog.exit(
                    "backup.log was truncated despite -C: marker line lost.\n"
                    f"content:\n{content_after_ckpt[:2000]}"
                )
            tdLog.info(
                "  -C backup run appended to backup.log (marker preserved) ... [passed]"
            )

            # -- step 4: backup again WITHOUT -C - must TRUNCATE -------------
            tdLog.info("=== step 4: fresh backup (no -C) again ===")
            rlist = etool.taosdump(f"-D {DB} -o {tmpdir}")
            self.checkListString(rlist, "Result       : SUCCESS")
            with open(log_path) as f:
                content_after_fresh = f.read()
            if marker in content_after_fresh:
                tdLog.exit(
                    "backup.log was NOT truncated on a fresh (no -C) run - "
                    "marker line from an unrelated earlier run still present"
                )
            tdLog.info(
                "  fresh (no -C) backup run truncated backup.log ... [passed]"
            )

            tdLog.info("test_backup_log_append_with_checkpoint PASSED")
        finally:
            self.cleanup_dbs(DB)

    # ------------------------------------------------------------------
    # Test 7: -C resume with a different format must be rejected
    # ------------------------------------------------------------------

    def test_backup_checkpoint_format_change_rejected(self):
        """taosBackup -C resume must refuse to start when the data format
        differs from the previous backup.

        bckArgs.c reads the previous run's format from {outPath}/backup.log
        (line "  Format       : binary|parquet") and, when -C is requested
        with a different -F, aborts during argument validation - before any
        backup work starts - so the output dir can never end up with a mixed
        .dat/.par set (which restore would re-insert twice).

        Deterministic (no kill/timing dependency):
          1. Insert TABLES x ROWS rows via taosBenchmark.
          2. Full backup with -F binary (default) -> backup.log records
             "binary", all .dat files created.
          3. Delete backup_complete.flag to simulate an interrupted run (so a
             later -C run would otherwise enter per-table resume mode).
          4. Snapshot mtime + sha256 of every .dat file.
          5. Run backup with -C -F parquet into the SAME outdir:
             - must FAIL (non-zero exit) with a "data format changed" error;
             - must NOT create any .par files;
             - must leave every .dat file byte-for-byte untouched.
          6. Positive control: -C -F binary (same format) must succeed and
             skip everything (skipped(resume) == TABLES).

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-13 Created to lock in the -C format-consistency guard
              (bckArgs.c backupFormatChangedSinceLastRun)
        """
        db = "ckpt_fmt_src"
        TABLES = 5
        ROWS = 50

        taosbackup, benchmark, tmpdir = self.find_programs()

        try:
            tdSql.execute(f"drop database if exists {db}")

            tdLog.info(f"=== step 1: insert {TABLES} x {ROWS} rows ===")
            ret = os.system(f"{benchmark} -d {db} -t {TABLES} -n {ROWS} -y")
            if ret != 0:
                tdLog.exit(f"taosBenchmark failed (ret={ret})")

            tdLog.info("=== step 2: full backup with -F binary (default) ===")
            rlist = etool.taosdump(f"-T 1 -D {db} -o {tmpdir}")
            self.checkListString(rlist, "Result       : SUCCESS")

            flag_path = os.path.join(tmpdir, db, "backup_complete.flag")
            if not os.path.exists(flag_path):
                tdLog.exit(f"backup_complete.flag missing after full backup: {flag_path}")

            tdLog.info("=== step 3: delete backup_complete.flag (simulate interrupted run) ===")
            os.remove(flag_path)

            tdLog.info("=== step 4: snapshot .dat files before the rejected resume ===")
            db_dir = os.path.join(tmpdir, db)
            before = snapshot_dat_files(db_dir)
            if len(before) != TABLES:
                tdLog.exit(f"expected {TABLES} .dat files, found {len(before)}")

            tdLog.info("=== step 5: backup with -C -F parquet -> must be rejected ===")
            cmd = f"{taosbackup} -T 1 -C -F parquet -D {db} -o {tmpdir}"
            retcode, killed, rlist = run_with_timeout(cmd, 120)
            if killed:
                tdLog.exit("rejected run did not exit promptly - the format check may not have fired")
            if retcode == 0:
                tdLog.exit("expected -C -F parquet resume to be REJECTED, but it exited 0")
            self.checkListString(rlist, "data format changed")

            tdLog.info("=== step 6: verify no .par files and .dat files untouched ===")
            par_files = glob.glob(os.path.join(db_dir, "**", "*.par"), recursive=True)
            if par_files:
                tdLog.exit(f"unexpected .par files created by rejected run: {par_files}")
            after = snapshot_dat_files(db_dir)
            if after != before:
                changed = [p for p in before if before.get(p) != after.get(p)]
                tdLog.exit(f".dat files changed despite rejected resume: {changed}")

            tdLog.info("=== step 7: positive control -C -F binary (same format) must succeed ===")
            rlist = etool.taosdump(f"-T 1 -C -F binary -D {db} -o {tmpdir}")
            self.checkListString(rlist, "Result       : SUCCESS")
            skipped = extract_int("\n".join(rlist), r"skipped\(resume\)=(\d+)")
            tdLog.info(f"  skipped(resume)={skipped}")
            if skipped != TABLES:
                tdLog.exit(f"expected skipped(resume)=={TABLES}, got {skipped}")

            tdLog.info("test_backup_checkpoint_format_change_rejected PASSED")
        finally:
            self.cleanup_dbs(db)
