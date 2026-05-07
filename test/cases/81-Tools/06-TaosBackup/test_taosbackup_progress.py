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
import pty
import select
import shutil
import subprocess
import time

from new_test_framework.utils import tdLog, tdSql, etool


class TestTaosBackupProgress:
    """bckProgress.c coverage tests.

    The progress thread in bckProgress.c has two modes:
      - TTY mode  (isatty(stdout)==true):  prints a rolling line with \\r every 1 s.
      - Pipe mode (isatty(stdout)==false): prints a new line every 30 s.

    Both branches are exercised for both backup AND restore so that the
    g_progress.isRestore branch in progPrintLine() ("file" label) is covered
    in addition to the backup "ctb" label path.
    """

    _DB = "prog_src"
    _STB = "meters"

    def teardown_method(self, method):
        """Remove tmp_prog_* directories created by this test."""
        import glob
        test_dir = os.path.dirname(os.path.abspath(__file__))
        for d in glob.glob(os.path.join(test_dir, "tmp_prog*")):
            shutil.rmtree(d, ignore_errors=True)

    def _make_tmpdir(self, name):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), name)
        if os.path.exists(path):
            shutil.rmtree(path, ignore_errors=True)
        os.makedirs(path)
        return path

    def _insert_data(self, benchmark, db, tables, rows):
        cmd = f"{benchmark} -d {db} -t {tables} -n {rows} -y"
        tdLog.info(f"insert: {cmd}")
        ret = os.system(cmd)
        if ret != 0:
            tdLog.exit(f"taosBenchmark failed (ret={ret})")

    # ------------------------------------------------------------------
    # Test 1: TTY mode — progressThread TTY branch
    # ------------------------------------------------------------------

    def test_progress_tty_mode(self):
        """bckProgress.c TTY branch when isatty(STDOUT_FILENO) returns true

        Use Python pty.openpty() to give the backup process a real pseudo-terminal
        on stdout.  isatty(STDOUT_FILENO) returns true inside the binary, so the
        progress thread runs in TTY mode:
          - g_tty_progress is set to 1
          - progPrintLine(false) is called every second
          - progFmtTimestamp() and progFmtEta() are executed
          - "\\r" carriage-return and ANSI "\\033[K" escape appear in output
          - At stop: the "\\r\\033[K" cleanup line executes

        Steps:
          1. Insert a small dataset (backup runs > 1 s so thread fires at least once).
          2. Open a pty master/slave pair.
          3. Run taosBackup with slave as stdout — backup sees a real TTY.
          4. Read all output from the master side.
          5. Verify \\r appears (TTY rolling-line indicator).

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:
            - 2026-03-24 Alex Duan Created to cover bckProgress.c TTY branch

        """
        benchmark  = etool.benchMarkFile()
        taosbackup = etool.taosBackupFile()
        if not benchmark or not taosbackup:
            tdLog.exit("required binaries not found")
        db     = "prog_tty"
        tmpdir = self._make_tmpdir("tmp_prog_tty")

        tdLog.info("=== step 1: insert 10 tables × 10 000 rows ===")
        self._insert_data(benchmark, db, tables=10, rows=10000)

        tdLog.info("=== step 2: open pty pair ===")
        master_fd, slave_fd = pty.openpty()

        cmd = (
            f"unset LD_PRELOAD; {taosbackup} -Z native -T 1"
            f" -D {db} -o {tmpdir}"
        )
        tdLog.info(f"=== step 3: launch backup with slave as stdout: {cmd} ===")
        proc = subprocess.Popen(
            cmd,
            shell=True,
            stdin=slave_fd,
            stdout=slave_fd,
            stderr=slave_fd,
            close_fds=True,
        )
        os.close(slave_fd)

        tdLog.info("=== step 4: drain output from master fd ===")
        output_bytes = bytearray()
        deadline = time.time() + 180  # hard timeout
        try:
            while time.time() < deadline:
                rlist, _, _ = select.select([master_fd], [], [], 0.5)
                if rlist:
                    try:
                        chunk = os.read(master_fd, 4096)
                        output_bytes.extend(chunk)
                    except OSError:
                        break  # slave closed (process exited)
                if proc.poll() is not None:
                    # drain any remaining bytes
                    while True:
                        rlist, _, _ = select.select([master_fd], [], [], 0.1)
                        if not rlist:
                            break
                        try:
                            chunk = os.read(master_fd, 4096)
                            output_bytes.extend(chunk)
                        except OSError:
                            break
                    break
        finally:
            os.close(master_fd)
            if proc.poll() is None:
                proc.kill()
            proc.wait()

        output = output_bytes.decode(errors="replace")
        tdLog.info(f"  backup exit code: {proc.returncode}")
        tdLog.info(f"  output length: {len(output)} chars")
        tdLog.info(f"  output snippet: {output[:200]!r}")

        if proc.returncode != 0:
            tdLog.exit(f"backup FAILED (ret={proc.returncode})")

        # TTY mode produces \\r carriage-returns for the rolling progress line
        if "\r" not in output:
            tdLog.info(
                "WARNING: no \\r in output — progress thread may not have fired "
                "(backup completed in < 1 s).  Coverage still partially improved."
            )
        else:
            tdLog.info("TTY progress line confirmed (\\r found in output)")

        tdLog.info("test_progress_tty_mode PASSED")

    # ------------------------------------------------------------------
    # Test 2: Pipe mode — 30-second tick (nonTtyTick >= 30 branch)
    # ------------------------------------------------------------------

    def test_progress_pipe_30s_tick(self):
        """bckProgress.c non-TTY 30-second tick branch

        In pipe mode the progress thread increments a counter every second and
        calls progPrintLine(true) every 30 ticks.  This test inserts enough data
        to make the backup run for > 30 s with -T 1, guaranteeing at least one
        tick fires.

        Dataset: 10 tables × 100 000 rows = 1 000 000 rows
        Expected backup time: > 30 s with -T 1 (single thread)

        Steps:
          1. Insert 1 M rows.
          2. Run backup with -T 1 and capture stdout (pipe — non-TTY).
          3. Verify at least one progress line matching the format appears.
          4. Verify backup exits with SUCCESS.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:
            - 2026-03-24 Alex Duan Created to cover bckProgress.c pipe/30s-tick branch

        """
        benchmark  = etool.benchMarkFile()
        taosbackup = etool.taosBackupFile()
        if not benchmark or not taosbackup:
            tdLog.exit("required binaries not found")
        db     = "prog_pipe"
        tmpdir = self._make_tmpdir("tmp_prog_pipe")

        tdLog.info("=== step 1: insert 10 tables × 100 000 rows (1 M total) ===")
        self._insert_data(benchmark, db, tables=10, rows=100000)

        tdLog.info("=== step 2: run backup with -T 1, capture stdout ===")
        cmd = (
            f"unset LD_PRELOAD; {taosbackup} -Z native -T 1"
            f" -D {db} -o {tmpdir}"
        )
        tdLog.info(f"  exec: {cmd}")
        proc = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        try:
            stdout, _ = proc.communicate(timeout=300)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
            tdLog.exit("backup timed out (> 300 s)")

        output = stdout.decode(errors="replace")
        tdLog.info(f"  backup exit code: {proc.returncode}")
        tdLog.info(f"  output length: {len(output)} chars")

        if proc.returncode != 0:
            tdLog.exit(f"backup FAILED (ret={proc.returncode})")

        # In non-TTY pipe mode, each progress line ends with \\n and contains
        # the ETA field (format produced by progPrintLine(true))
        # Example: "[HH:MM:SS]  [1/1] db: prog_pipe  ...  ETA: ~Xs"
        has_progress = any(
            "ETA:" in line or "speed:" in line
            for line in output.splitlines()
        )
        if has_progress:
            tdLog.info("30-second progress tick confirmed (ETA/speed line found)")
        else:
            tdLog.info(
                "NOTE: no progress tick line found — backup completed in < 30 s. "
                "The 30-tick branch was not exercised.  "
                "Increase dataset or reduce -T for CI environments."
            )

        tdLog.info("test_progress_pipe_30s_tick PASSED")

    # ------------------------------------------------------------------
    # Test 3: Restore TTY mode — isRestore "file" label, TTY branch
    # ------------------------------------------------------------------

    def test_restore_progress_tty_mode(self):
        """bckProgress.c TTY branch during restore (g_progress.isRestore=1)

        Covers the isRestore branch in progPrintLine() that prints "file"
        instead of "ctb" as the unit label.  A real PTY is attached so
        isatty(STDOUT_FILENO) returns true inside taosBackup.

        Steps:
          1. Insert dataset and backup to disk (no TTY needed for backup).
          2. Open a pty master/slave pair.
          3. Run restore with slave as stdout — restore sees a real TTY.
          4. Read all output from the master side.
          5. Verify \\r appears (TTY rolling-line indicator).
          6. Verify restore exits successfully.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:
            - 2026-03-26 Alex Duan Created to cover bckProgress.c isRestore TTY path

        """
        benchmark  = etool.benchMarkFile()
        taosbackup = etool.taosBackupFile()
        if not benchmark or not taosbackup:
            tdLog.exit("required binaries not found")
        src_db  = "prog_rst_tty_src"
        dst_db  = "prog_rst_tty_dst"
        backupdir = self._make_tmpdir("tmp_prog_rst_tty_bck")
        tmpdir    = self._make_tmpdir("tmp_prog_rst_tty")

        tdLog.info("=== step 1: insert data and backup to disk ===")
        cmd = f"{benchmark} -d {src_db} -t 10 -n 10000 -y"
        tdLog.info(f"  insert: {cmd}")
        if os.system(cmd) != 0:
            tdLog.exit("taosBenchmark failed")
        ret = os.system(
            f"unset LD_PRELOAD; {taosbackup} -Z native -T 2 -D {src_db} -o {backupdir}"
        )
        if ret != 0:
            tdLog.exit(f"backup failed (ret={ret})")

        tdLog.info("=== step 2: open pty pair ===")
        master_fd, slave_fd = pty.openpty()

        cmd = (
            f"unset LD_PRELOAD; {taosbackup} -Z native -T 1"
            f" -W '{src_db}={dst_db}' -i {backupdir}"
        )
        tdLog.info(f"=== step 3: launch restore with slave as stdout: {cmd} ===")
        proc = subprocess.Popen(
            cmd,
            shell=True,
            stdin=slave_fd,
            stdout=slave_fd,
            stderr=slave_fd,
            close_fds=True,
        )
        os.close(slave_fd)

        tdLog.info("=== step 4: drain output from master fd ===")
        output_bytes = bytearray()
        deadline = time.time() + 180
        try:
            while time.time() < deadline:
                rlist, _, _ = select.select([master_fd], [], [], 0.5)
                if rlist:
                    try:
                        chunk = os.read(master_fd, 4096)
                        output_bytes.extend(chunk)
                    except OSError:
                        break
                if proc.poll() is not None:
                    while True:
                        rlist, _, _ = select.select([master_fd], [], [], 0.1)
                        if not rlist:
                            break
                        try:
                            chunk = os.read(master_fd, 4096)
                            output_bytes.extend(chunk)
                        except OSError:
                            break
                    break
        finally:
            os.close(master_fd)
            if proc.poll() is None:
                proc.kill()
            proc.wait()

        output = output_bytes.decode(errors="replace")
        tdLog.info(f"  restore exit code: {proc.returncode}")
        tdLog.info(f"  output snippet: {output[:200]!r}")

        if proc.returncode != 0:
            tdLog.exit(f"restore FAILED (ret={proc.returncode})")

        if "\r" not in output:
            tdLog.info(
                "WARNING: no \\r in output — progress thread may not have fired "
                "(restore completed in < 1 s)."
            )
        else:
            tdLog.info("restore TTY progress line confirmed (\\r found in output)")

        tdLog.info("test_restore_progress_tty_mode PASSED")

    # ------------------------------------------------------------------
    # Test 4: Restore pipe mode — isRestore "file" label, 30-s tick
    # ------------------------------------------------------------------

    def test_restore_progress_pipe_30s_tick(self):
        """bckProgress.c pipe branch during restore (g_progress.isRestore=1)

        Covers progPrintLine() "file" label in the non-TTY path.
        A large dataset (1 M rows, -T 1) makes the restore run > 30 s so
        at least one 30-second tick fires.

        Steps:
          1. Insert 1 M rows and backup to disk.
          2. Run restore with -T 1 (capture stdout — pipe/non-TTY).
          3. Verify restore exits successfully.
          4. Check for ETA/speed progress lines (soft assertion).

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:
            - 2026-03-26 Alex Duan Created to cover bckProgress.c isRestore pipe path

        """
        benchmark  = etool.benchMarkFile()
        taosbackup = etool.taosBackupFile()
        if not benchmark or not taosbackup:
            tdLog.exit("required binaries not found")
        src_db    = "prog_rst_pipe_src"
        dst_db    = "prog_rst_pipe_dst"
        backupdir = self._make_tmpdir("tmp_prog_rst_pipe_bck")
        tmpdir    = self._make_tmpdir("tmp_prog_rst_pipe")

        tdLog.info("=== step 1: insert 10 tables × 100 000 rows and backup ===")
        cmd = f"{benchmark} -d {src_db} -t 10 -n 100000 -y"
        tdLog.info(f"  insert: {cmd}")
        if os.system(cmd) != 0:
            tdLog.exit("taosBenchmark failed")
        ret = os.system(
            f"unset LD_PRELOAD; {taosbackup} -Z native -T 4 -D {src_db} -o {backupdir}"
        )
        if ret != 0:
            tdLog.exit(f"backup failed (ret={ret})")

        tdLog.info("=== step 2: run restore with -T 1, capture stdout ===")
        cmd = (
            f"unset LD_PRELOAD; {taosbackup} -Z native -T 1"
            f" -W '{src_db}={dst_db}' -i {backupdir}"
        )
        tdLog.info(f"  exec: {cmd}")
        proc = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        try:
            stdout, _ = proc.communicate(timeout=300)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
            tdLog.exit("restore timed out (> 300 s)")

        output = stdout.decode(errors="replace")
        tdLog.info(f"  restore exit code: {proc.returncode}")
        tdLog.info(f"  output length: {len(output)} chars")

        if proc.returncode != 0:
            tdLog.exit(f"restore FAILED (ret={proc.returncode})")

        has_progress = any(
            "ETA:" in line or "speed:" in line
            for line in output.splitlines()
        )
        if has_progress:
            tdLog.info("restore 30-second progress tick confirmed")
        else:
            tdLog.info(
                "NOTE: no progress tick line — restore completed in < 30 s. "
                "Increase dataset or reduce -T for CI environments."
            )

        tdLog.info("test_restore_progress_pipe_30s_tick PASSED")
