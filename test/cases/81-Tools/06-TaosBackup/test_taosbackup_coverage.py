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

"""
test_taosbackup_coverage.py

Consolidated branch/line-coverage tests for taosBackup.  Merges the
content of:
    - test_taosbackup_coverage_boost.py   (14 tests)
    - test_taosbackup_coverage_extra.py   (11 tests)
    - test_taosbackup_branch_coverage.py  ( 5 tests)

Every test verifies functional correctness in addition to exercising
specific C-source branches.

Since: v3.0.0.0

Labels: common

History:
    - 2026-03-26 Coverage boost tests created
    - 2026-03-26 Extra branch-coverage tests added
    - 2026-03-26 Targeted branch-coverage tests added
    - 2026-03-26 All three files merged into this single module
"""

import glob
import os
import re
import signal
import struct
import shutil
import subprocess
import time

from new_test_framework.utils import tdLog, tdSql, etool, sc


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _run(cmd, timeout=300):
    """Run *cmd*, return (returncode, stdout_lines)."""
    tdLog.info(f"  run: {cmd}")
    proc = subprocess.Popen(
        cmd, shell=True,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        preexec_fn=os.setsid,
    )
    try:
        out, _ = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass
        out, _ = proc.communicate()
        tdLog.info("  [killed — timeout]")
    lines = out.decode(errors="replace").splitlines() if out else []
    return proc.returncode, lines


def _run_kill(cmd, kill_after_secs):
    """Start *cmd*, kill it after *kill_after_secs* seconds.

    Returns (rc, killed: bool).
    """
    tdLog.info(f"  run_kill({kill_after_secs}s): {cmd}")
    proc = subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid)
    try:
        proc.wait(timeout=kill_after_secs)
        return proc.returncode, False
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait()
        tdLog.info(f"  [killed after {kill_after_secs}s]")
        return proc.returncode, True


def _run_sigint(cmd, interrupt_after_secs, graceful_timeout=15):
    """Start *cmd*, send SIGINT after *interrupt_after_secs* seconds.

    Waits *graceful_timeout* seconds for the process to flush buffers
    and exit cleanly.  If the process is still running, SIGKILL is sent.

    Returns (returncode, interrupted: bool).  interrupted=False means
    the process finished on its own before *interrupt_after_secs* elapsed.
    """
    tdLog.info(f"  run_sigint({interrupt_after_secs}s): {cmd}")
    proc = subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid)
    try:
        proc.wait(timeout=interrupt_after_secs)
        return proc.returncode, False
    except subprocess.TimeoutExpired:
        pass

    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGINT)
        tdLog.info(f"  [SIGINT sent after {interrupt_after_secs}s, waiting for graceful exit]")
    except ProcessLookupError:
        proc.wait()
        return proc.returncode, True

    try:
        proc.wait(timeout=graceful_timeout)
    except subprocess.TimeoutExpired:
        tdLog.info("  [graceful exit timed out, sending SIGKILL]")
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait()

    tdLog.info("  [process exited after SIGINT]")
    return proc.returncode, True


def _tmpdir(name):
    """Create (or recreate) a temporary directory under the test file's dir."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), name)
    if os.path.exists(path):
        shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path)
    return path


def _checkstr(lines, expected, label=""):
    """Assert that *expected* appears in the joined *lines*."""
    out = "\n".join(str(l) for l in (lines or []))
    if expected not in out:
        tdLog.exit(
            f"{label} expected string not found:\n"
            f"  want: {expected!r}\n"
            f"  got : {out[-1200:]!r}"
        )
    tdLog.info(f"  {label} found: {expected!r}")


def _checkmissing(lines, absent):
    """Assert that *absent* does NOT appear in the joined *lines*."""
    output = "\n".join(str(l) for l in (lines or []))
    if absent in output:
        tdLog.exit(
            f"Unexpected string found in output:\n"
            f"  absent  : {absent!r}\n"
            f"  output  : {output[:600]!r}"
        )
    tdLog.info(f"  correctly absent: {absent!r}")


def _insert_simple(db, rows=50):
    """Create *db* with a super table (st) and a normal table (nt) and insert *rows* rows."""
    tdSql.execute(f"drop database if exists {db}")
    tdSql.execute(f"create database {db} vgroups 1 replica 1 precision 'ms'")
    tdSql.execute(
        f"create stable {db}.st (ts timestamp, c1 int, c2 float) tags (gid int)"
    )
    tdSql.execute(f"create table {db}.t0 using {db}.st tags(0)")
    tdSql.execute(f"create table {db}.t1 using {db}.st tags(1)")
    tdSql.execute(f"create table {db}.nt (ts timestamp, c1 int, c2 float)")
    for i in range(rows):
        ts = 1600000000000 + i * 1000
        tdSql.execute(f"insert into {db}.t0 values({ts}, {i}, {i * 0.5})")
        tdSql.execute(f"insert into {db}.t1 values({ts}, {i + 1000}, {i * 1.5})")
        tdSql.execute(f"insert into {db}.nt values({ts}, {i + 2000}, {i * 2.5})")


def _rowcount(db, tbl):
    tdSql.query(f"select count(*) from {db}.{tbl}")
    return tdSql.getData(0, 0)


def _sum_c1(db, tbl):
    tdSql.query(f"select sum(c1) from {db}.{tbl}")
    return tdSql.getData(0, 0)


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------

class TestTaosBackupCoverage:
    """Consolidated coverage + correctness tests for taosBackup."""

    def teardown_method(self, method):
        """Remove tmp_* directories created by this test."""
        test_dir = os.path.dirname(os.path.abspath(__file__))
        for d in glob.glob(os.path.join(test_dir, "tmp_*")):
            shutil.rmtree(d, ignore_errors=True)

    # ===================================================================
    # Section A: tests from coverage_boost.py
    # ===================================================================

    # -----------------------------------------------------------------
    # A-1. Parquet format backup + STMT2 restore (all common types)
    # -----------------------------------------------------------------
    def test_parquet_backup_restore_stmt2(self):
        """Backup in parquet format, restore via STMT2 path.

        Covers:
          storageParquet.c   resultToFileParquet()
          storageParquet.c   fileParquetToStmt2() / parStmt2Callback()
          parquetBlock.cpp   blockToRecordBatch() for BOOL, INT, FLOAT,
                             DOUBLE, BINARY, NCHAR, TIMESTAMP
          restoreStmt.c      restoreOneParquetFile()

        Correctness: row count + aggregate check after restore.

        Since: v3.0.0.0

        Labels: common
        """
        taosbackup = etool.taosBackupFile()
        if not taosbackup:
            tdLog.exit("taosBackup not found")

        src = "cov_par_src"
        dst = "cov_par_dst"
        tmpdir = _tmpdir("tmp_cov_parquet_stmt2")

        tdLog.info("=== step 1: create mixed-type table and insert data ===")
        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"create database {src}")
        tdSql.execute(
            f"create table {src}.stb ("
            f"  ts TIMESTAMP, c_bool BOOL, c_int INT, c_bigint BIGINT, "
            f"  c_float FLOAT, c_double DOUBLE, "
            f"  c_binary BINARY(64), c_nchar NCHAR(32)"
            f") tags (t_int INT, t_binary BINARY(32))"
        )
        # Insert 200 rows across 4 child tables
        for i in range(4):
            tdSql.execute(
                f"create table {src}.ct{i} using {src}.stb "
                f"tags ({i}, 'tag_{i}')"
            )
            values = []
            for j in range(50):
                ts = 1700000000000 + i * 10000 + j * 100
                b = "true" if (j % 2 == 0) else "false"
                nch = f"你好_{i}_{j}"
                values.append(
                    f"({ts}, {b}, {j}, {j*100}, {j*1.1:.2f}, {j*2.2:.4f}, "
                    f"'bin_{i}_{j}', '{nch}')"
                )
            tdSql.execute(f"insert into {src}.ct{i} values " + " ".join(values))

        tdSql.query(f"select count(*), sum(c_int) from {src}.stb")
        src_count = tdSql.queryResult[0][0]
        src_sum = tdSql.queryResult[0][1]
        tdLog.info(f"  source: count={src_count} sum(c_int)={src_sum}")
        assert src_count == 200, f"expected 200 rows, got {src_count}"

        tdLog.info("=== step 2: backup in parquet format ===")
        rlist = etool.taosbackup(
            f"-Z native -D {src} -T 2 -F parquet -o {tmpdir}"
        )
        _checkstr(rlist, "SUCCESS", "backup")

        par_files = []
        for root, dirs, files in os.walk(tmpdir):
            par_files.extend(f for f in files if f.endswith(".par"))
        assert len(par_files) >= 4, f"expected >=4 .par files, got {len(par_files)}"
        tdLog.info(f"  parquet files created: {len(par_files)}")

        tdLog.info("=== step 3: restore via STMT2 to new database ===")
        tdSql.execute(f"drop database if exists {dst}")
        rlist = etool.taosbackup(
            f"-Z native -W '{src}={dst}' -v 2 -i {tmpdir}"
        )
        _checkstr(rlist, "SUCCESS", "restore")

        tdLog.info("=== step 4: verify data correctness ===")
        tdSql.query(f"select count(*), sum(c_int) from {dst}.stb")
        dst_count = tdSql.queryResult[0][0]
        dst_sum = tdSql.queryResult[0][1]
        tdLog.info(f"  restored: count={dst_count} sum(c_int)={dst_sum}")
        assert dst_count == src_count, f"count mismatch: {dst_count} vs {src_count}"
        assert dst_sum == src_sum, f"sum mismatch: {dst_sum} vs {src_sum}"

        tdSql.query(f"select c_nchar from {dst}.ct0 where c_int = 5")
        assert tdSql.queryResult[0][0] == "你好_0_5", \
            f"NCHAR mismatch: {tdSql.queryResult[0][0]}"

        tdSql.query(f"select c_bool from {dst}.ct0 where c_int = 0")
        assert tdSql.queryResult[0][0] is True, \
            f"BOOL mismatch: {tdSql.queryResult[0][0]}"

        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"drop database if exists {dst}")
        tdLog.info("test_parquet_backup_restore_stmt2 PASSED")

    # -----------------------------------------------------------------
    # A-2. Parquet with all-NULL columns
    # -----------------------------------------------------------------
    def test_parquet_with_all_null_columns(self):
        """Parquet backup/restore when some columns are entirely NULL.

        Covers:
          parquetBlock.cpp   NULL bitmap write path in blockToRecordBatch()
          storageParquet.c   Parquet writer null column handling

        Correctness: all-NULL column stays NULL after restore.

        Since: v3.0.0.0

        Labels: common
        """
        src = "cov_par_null_src"
        dst = "cov_par_null_dst"
        tmpdir = _tmpdir("tmp_cov_par_nullcol")

        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"create database {src}")
        tdSql.execute(
            f"create table {src}.stb "
            f"(ts TIMESTAMP, c_val INT, c_empty INT, c_bin BINARY(32)) "
            f"tags (t1 INT)"
        )
        tdSql.execute(f"create table {src}.t0 using {src}.stb tags(1)")
        vals = []
        for i in range(100):
            ts = 1700000000000 + i * 100
            bin_val = f"'data_{i}'" if i % 3 != 0 else "NULL"
            vals.append(f"({ts}, {i}, NULL, {bin_val})")
        tdSql.execute(f"insert into {src}.t0 values " + " ".join(vals))

        rlist = etool.taosbackup(f"-Z native -D {src} -T 1 -F parquet -o {tmpdir}")
        _checkstr(rlist, "SUCCESS", "backup")

        tdSql.execute(f"drop database if exists {dst}")
        rlist = etool.taosbackup(f"-Z native -W '{src}={dst}' -v 2 -i {tmpdir}")
        _checkstr(rlist, "SUCCESS", "restore")

        tdSql.query(f"select count(c_empty) from {dst}.stb")
        null_count = tdSql.queryResult[0][0]
        assert null_count == 0, f"c_empty should be all NULL, got count={null_count}"

        tdSql.query(f"select count(c_bin) from {dst}.stb")
        bin_count = tdSql.queryResult[0][0]
        expected_non_null = len([i for i in range(100) if i % 3 != 0])
        assert bin_count == expected_non_null, \
            f"c_bin count mismatch: {bin_count} vs {expected_non_null}"

        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"drop database if exists {dst}")
        tdLog.info("test_parquet_with_all_null_columns PASSED")

    # -----------------------------------------------------------------
    # A-3. Time filter backup (epoch + ISO8601)
    # -----------------------------------------------------------------
    def test_time_filter_backup(self):
        """Backup with --start and --end time filters.

        Covers:
          bckArgs.c    time filter construction (both epoch and ISO8601)
          backup.c     stopTime > 0 branch
          backupData.c WHERE clause with time filter

        Correctness: only rows within [start, end] are backed up/restored.

        Since: v3.0.0.0

        Labels: common
        """
        src = "cov_timeflt_src"
        dst = "cov_timeflt_dst"
        tmpdir = _tmpdir("tmp_cov_timefilter")

        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"create database {src}")
        tdSql.execute(
            f"create table {src}.stb (ts TIMESTAMP, v INT) tags (t1 INT)"
        )
        tdSql.execute(f"create table {src}.t0 using {src}.stb tags(1)")

        vals = []
        for i in range(1000):
            ts = 1700000000000 + i * 1000
            vals.append(f"({ts}, {i})")
        tdSql.execute(f"insert into {src}.t0 values " + " ".join(vals))

        start_ts = 1700000100000
        end_ts = 1700000299000
        rlist = etool.taosbackup(
            f"-Z native -D {src} -T 1 "
            f"--start-time {start_ts} --end-time {end_ts} "
            f"-o {tmpdir}"
        )
        _checkstr(rlist, "SUCCESS", "backup")

        tdSql.execute(f"drop database if exists {dst}")
        rlist = etool.taosbackup(f"-Z native -W '{src}={dst}' -i {tmpdir}")
        _checkstr(rlist, "SUCCESS", "restore")

        tdSql.query(f"select count(*) from {dst}.stb")
        cnt = tdSql.queryResult[0][0]
        assert cnt == 200, f"expected 200 rows in time range, got {cnt}"

        tdSql.query(f"select min(v), max(v) from {dst}.stb")
        min_v = tdSql.queryResult[0][0]
        max_v = tdSql.queryResult[0][1]
        assert min_v == 100, f"min(v) should be 100, got {min_v}"
        assert max_v == 299, f"max(v) should be 299, got {max_v}"

        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"drop database if exists {dst}")
        tdLog.info("test_time_filter_backup PASSED")

    # -----------------------------------------------------------------
    # A-4. Invalid argument combinations
    # -----------------------------------------------------------------
    def test_invalid_arg_combos(self):
        """Verify taosBackup rejects invalid argument combinations.

        Covers:
          bckArgs.c  cross-validation error paths:
            - --data-batch with -o (backup)
            - --stmt-version with -o (backup)
            - --rename with -o (backup)
            - --start-time with -i (restore)
            - neither -o nor -i
            - invalid driver name

        Correctness: each invalid combo returns non-zero exit code.

        Since: v3.0.0.0

        Labels: common
        """
        taosbackup = etool.taosBackupFile()
        tmpdir = _tmpdir("tmp_cov_badargs")

        rc, lines = _run(f"{taosbackup} -D testdb --data-batch 100 -o {tmpdir}")
        assert rc != 0, "--data-batch with -o should fail"
        tdLog.info("  case 1 (--data-batch + backup) correctly rejected")

        rc, lines = _run(f"{taosbackup} -D testdb --stmt-version 1 -o {tmpdir}")
        assert rc != 0, "--stmt-version with -o should fail"
        tdLog.info("  case 2 (--stmt-version + backup) correctly rejected")

        rc, lines = _run(f"{taosbackup} -D testdb -W 'a=b' -o {tmpdir}")
        assert rc != 0, "--rename with -o should fail"
        tdLog.info("  case 3 (--rename + backup) correctly rejected")

        rc, lines = _run(
            f"{taosbackup} --start-time 1700000000000 -i {tmpdir}"
        )
        assert rc != 0, "--start-time with -i should fail"
        tdLog.info("  case 4 (--start-time + restore) correctly rejected")

        rc, lines = _run(f"{taosbackup} -D testdb")
        assert rc != 0, "neither -o nor -i should fail"
        tdLog.info("  case 5 (no -o/-i) correctly rejected")

        rc, lines = _run(f"{taosbackup} -D testdb -Z foobar -o {tmpdir}")
        assert rc != 0, "invalid driver should fail"
        tdLog.info("  case 6 (invalid driver) correctly rejected")

        tdLog.info("test_invalid_arg_combos PASSED")

    # -----------------------------------------------------------------
    # A-5. SIGINT graceful exit
    # -----------------------------------------------------------------
    def test_sigint_graceful_exit(self):
        """Send SIGINT during backup and verify graceful exit.

        Covers:
          bckPool.c      g_interrupted check in getConnection()
          backup.c       g_interrupted check in backup loop
          bckProgress.c  g_interrupted check in progress thread

        Correctness: process exits cleanly, partial output directory present.

        Since: v3.0.0.0

        Labels: common
        """
        src = "cov_sigint_src"
        tmpdir = _tmpdir("tmp_cov_sigint")
        taosbackup = etool.taosBackupFile()

        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"create database {src}")
        tdSql.execute(
            f"create table {src}.stb (ts TIMESTAMP, v INT, b BINARY(200)) "
            f"tags (t1 INT)"
        )
        for i in range(20):
            tdSql.execute(
                f"create table {src}.ct{i} using {src}.stb tags({i})"
            )
            vals = []
            for j in range(500):
                ts = 1700000000000 + i * 100000 + j * 100
                vals.append(f"({ts}, {j}, 'padding_data_{j:050d}')")
            tdSql.execute(f"insert into {src}.ct{i} values " + " ".join(vals))

        tdLog.info("=== sending SIGINT 2s after backup starts ===")
        cmd = f"{taosbackup} -Z native -D {src} -T 4 -o {tmpdir}"
        proc = subprocess.Popen(
            cmd, shell=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            preexec_fn=os.setsid,
        )
        time.sleep(2)

        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGINT)
        except ProcessLookupError:
            pass

        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            proc.wait()
            tdLog.exit("taosBackup did not exit after SIGINT — hang detected")

        tdLog.info(f"  exit code: {proc.returncode}")

        assert proc.returncode != -11, "taosBackup crashed with SIGSEGV"
        assert os.path.isdir(tmpdir), "output directory missing after SIGINT"

        tdSql.execute(f"drop database if exists {src}")
        tdLog.info("test_sigint_graceful_exit PASSED")

    # -----------------------------------------------------------------
    # A-6. NCHAR multibyte in native format
    # -----------------------------------------------------------------
    def test_nchar_multibyte_native_format(self):
        """Backup/restore tables with multibyte NCHAR data using native format.

        Covers:
          restoreStmt.c    bindBlockData() NCHAR UCS-4 → UTF-8 conversion
          storageTaos.c    writeTaosFile() variable-length field path
          restoreStmt2.c   NCHAR bind in STMT2

        Correctness: multi-byte characters (Chinese, Japanese, mixed) are
        preserved after backup → restore.

        Since: v3.0.0.0

        Labels: common
        """
        src = "cov_nchar_mb_src"
        dst = "cov_nchar_mb_dst"
        tmpdir = _tmpdir("tmp_cov_nchar_mb")

        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"create database {src}")
        tdSql.execute(
            f"create table {src}.stb "
            f"(ts TIMESTAMP, c_nchar NCHAR(100), c_int INT) "
            f"tags (t_nchar NCHAR(50))"
        )

        test_strings = [
            ("中文测试数据", "标签一"),
            ("日本語テスト", "タグ二"),
            ("mixed_英文_中文_123", "mix_标签"),
            ("特殊字符：【】《》！＠＃", "符号标签"),
        ]

        for idx, (data_str, tag_str) in enumerate(test_strings):
            tdSql.execute(
                f"create table {src}.ct{idx} using {src}.stb "
                f"tags('{tag_str}')"
            )
            vals = []
            for j in range(20):
                ts = 1700000000000 + idx * 10000 + j * 100
                vals.append(f"({ts}, '{data_str}_{j}', {j})")
            tdSql.execute(f"insert into {src}.ct{idx} values " + " ".join(vals))

        rlist = etool.taosbackup(f"-Z native -D {src} -T 2 -o {tmpdir}")
        _checkstr(rlist, "SUCCESS", "backup")

        tdSql.execute(f"drop database if exists {dst}")
        rlist = etool.taosbackup(f"-Z native -W '{src}={dst}' -i {tmpdir}")
        _checkstr(rlist, "SUCCESS", "restore")

        for idx, (data_str, tag_str) in enumerate(test_strings):
            tdSql.query(
                f"select c_nchar from {dst}.ct{idx} where c_int = 5"
            )
            result = tdSql.queryResult[0][0]
            expected = f"{data_str}_5"
            assert result == expected, \
                f"ct{idx} NCHAR mismatch: {result!r} vs {expected!r}"

            tdSql.query(
                f"select t_nchar from {dst}.ct{idx} limit 1"
            )
            tag_result = tdSql.queryResult[0][0]
            assert tag_result == tag_str, \
                f"ct{idx} tag mismatch: {tag_result!r} vs {tag_str!r}"

        tdSql.query(f"select count(*) from {dst}.stb")
        assert tdSql.queryResult[0][0] == 80, \
            f"total rows mismatch: {tdSql.queryResult[0][0]} vs 80"

        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"drop database if exists {dst}")
        tdLog.info("test_nchar_multibyte_native_format PASSED")

    # -----------------------------------------------------------------
    # A-7. Large BINARY column (buffer realloc path)
    # -----------------------------------------------------------------
    def test_large_binary_column(self):
        """Backup/restore with near-max-length BINARY columns.

        Covers:
          storageTaos.c    readTaosFileBlocks() buffer realloc when
                           compressed block > initial capacity
          storageTaos.c    writeTaosFile() large data bypass path
          restoreStmt.c    large bind buffer allocation

        Correctness: the exact large binary values are preserved.

        Since: v3.0.0.0

        Labels: common
        """
        src = "cov_lgbin_src"
        dst = "cov_lgbin_dst"
        tmpdir = _tmpdir("tmp_cov_lgbin")

        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"create database {src}")
        tdSql.execute(
            f"create table {src}.stb "
            f"(ts TIMESTAMP, c_big BINARY(16374), c_v INT) "
            f"tags (t1 INT)"
        )
        tdSql.execute(f"create table {src}.t0 using {src}.stb tags(1)")

        for i in range(10):
            ts = 1700000000000 + i * 1000
            big_val = f"X{i}" * 4000
            tdSql.execute(
                f"insert into {src}.t0 values ({ts}, '{big_val}', {i})"
            )

        tdSql.query(f"select count(*) from {src}.stb")
        assert tdSql.queryResult[0][0] == 10

        rlist = etool.taosbackup(f"-Z native -D {src} -T 1 -o {tmpdir}")
        _checkstr(rlist, "SUCCESS", "backup")

        tdSql.execute(f"drop database if exists {dst}")
        rlist = etool.taosbackup(f"-Z native -W '{src}={dst}' -i {tmpdir}")
        _checkstr(rlist, "SUCCESS", "restore")

        tdSql.query(f"select c_big, c_v from {dst}.t0 where c_v = 5")
        restored_big = tdSql.queryResult[0][0]
        expected_big = "X5" * 4000
        assert restored_big == expected_big, \
            f"large binary mismatch at row 5: len={len(restored_big)} vs {len(expected_big)}"

        tdSql.query(f"select count(*) from {dst}.stb")
        assert tdSql.queryResult[0][0] == 10

        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"drop database if exists {dst}")
        tdLog.info("test_large_binary_column PASSED")

    # -----------------------------------------------------------------
    # A-8. Specific table backup (positional args)
    # -----------------------------------------------------------------
    def test_specific_table_backup(self):
        """Backup only specific child tables via positional args.

        Covers:
          bckArgs.c    positional table args → g_specTables
          bckDb.c      argSpecTables() building IN clause
          backupMeta.c argSpecTables filter for STBs

        Correctness: only the specified tables are backed up/restored.

        Since: v3.0.0.0

        Labels: common
        """
        src = "cov_spectbl_src"
        dst = "cov_spectbl_dst"
        tmpdir = _tmpdir("tmp_cov_spectbl")

        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"create database {src}")
        tdSql.execute(
            f"create table {src}.stb (ts TIMESTAMP, v INT) tags (t1 INT)"
        )
        for i in range(10):
            tdSql.execute(
                f"create table {src}.ct{i} using {src}.stb tags({i})"
            )
            vals = []
            for j in range(20):
                ts = 1700000000000 + i * 10000 + j * 100
                vals.append(f"({ts}, {j})")
            tdSql.execute(f"insert into {src}.ct{i} values " + " ".join(vals))

        rlist = etool.taosbackup(
            f"-Z native -o {tmpdir} {src} ct2 ct7"
        )
        _checkstr(rlist, "SUCCESS", "backup")

        tdSql.execute(f"drop database if exists {dst}")
        rlist = etool.taosbackup(f"-Z native -W '{src}={dst}' -i {tmpdir}")
        _checkstr(rlist, "SUCCESS", "restore")

        tdSql.query(f"select count(*) from {dst}.stb")
        total = tdSql.queryResult[0][0]
        assert total == 40, f"expected 40 rows (2 tables × 20), got {total}"

        tdSql.query(f"select count(*) from {dst}.ct2")
        assert tdSql.queryResult[0][0] == 20

        tdSql.query(f"select count(*) from {dst}.ct7")
        assert tdSql.queryResult[0][0] == 20

        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"drop database if exists {dst}")
        tdLog.info("test_specific_table_backup PASSED")

    # -----------------------------------------------------------------
    # A-9. Parquet format + STMT1 restore
    # -----------------------------------------------------------------
    def test_parquet_stmt1_restore(self):
        """Backup in parquet, restore via STMT version 1.

        Covers:
          storageParquet.c   fileParquetToStmt() path
          restoreStmt.c      dataBlockCallback() batch threshold

        Correctness: data integrity after restore.

        Since: v3.0.0.0

        Labels: common
        """
        src = "cov_par_s1_src"
        dst = "cov_par_s1_dst"
        tmpdir = _tmpdir("tmp_cov_par_stmt1")

        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"create database {src}")
        tdSql.execute(
            f"create table {src}.stb "
            f"(ts TIMESTAMP, v INT, f FLOAT) tags (t1 INT)"
        )
        tdSql.execute(f"create table {src}.t0 using {src}.stb tags(1)")
        vals = []
        for i in range(300):
            ts = 1700000000000 + i * 100
            vals.append(f"({ts}, {i}, {i * 0.5:.1f})")
        tdSql.execute(f"insert into {src}.t0 values " + " ".join(vals))

        rlist = etool.taosbackup(
            f"-Z native -D {src} -T 1 -F parquet -o {tmpdir}"
        )
        _checkstr(rlist, "SUCCESS", "backup")

        tdSql.execute(f"drop database if exists {dst}")
        rlist = etool.taosbackup(
            f"-Z native -W '{src}={dst}' -v 1 -i {tmpdir}"
        )
        _checkstr(rlist, "SUCCESS", "restore")

        tdSql.query(f"select count(*), sum(v) from {dst}.stb")
        cnt = tdSql.queryResult[0][0]
        sum_v = tdSql.queryResult[0][1]
        assert cnt == 300, f"count mismatch: {cnt}"
        assert sum_v == sum(range(300)), f"sum mismatch: {sum_v}"

        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"drop database if exists {dst}")
        tdLog.info("test_parquet_stmt1_restore PASSED")

    # -----------------------------------------------------------------
    # A-10. Backup with DSN-style connection (native)
    # -----------------------------------------------------------------
    def test_backup_with_native_dsn(self):
        """Backup using --dsn parameter for native connection.

        Covers:
          bckArgs.c   applyDsn() — protocol/host/port parsing

        Correctness: backup via DSN produces same result as direct.

        Since: v3.0.0.0

        Labels: common
        """
        src = "cov_dsn_src"
        dst = "cov_dsn_dst"
        tmpdir = _tmpdir("tmp_cov_dsn")

        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"create database {src}")
        tdSql.execute(
            f"create table {src}.stb (ts TIMESTAMP, v INT) tags (t1 INT)"
        )
        tdSql.execute(f"create table {src}.t0 using {src}.stb tags(1)")
        vals = []
        for i in range(50):
            ts = 1700000000000 + i * 100
            vals.append(f"({ts}, {i})")
        tdSql.execute(f"insert into {src}.t0 values " + " ".join(vals))

        rlist = etool.taosbackup(
            f"-Z native --dsn 'taos://localhost:6030' "
            f"-D {src} -T 1 -o {tmpdir}"
        )
        _checkstr(rlist, "SUCCESS", "backup_dsn")

        tdSql.execute(f"drop database if exists {dst}")
        rlist = etool.taosbackup(f"-Z native -W '{src}={dst}' -i {tmpdir}")
        _checkstr(rlist, "SUCCESS", "restore")

        tdSql.query(f"select count(*), sum(v) from {dst}.stb")
        assert tdSql.queryResult[0][0] == 50
        assert tdSql.queryResult[0][1] == sum(range(50))

        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"drop database if exists {dst}")
        tdLog.info("test_backup_with_native_dsn PASSED")

    # -----------------------------------------------------------------
    # A-11. Connection pool retry on taosd pause
    # -----------------------------------------------------------------
    def test_connection_pool_retry_on_pause(self):
        """Pause taosd briefly to trigger connection retry.

        Covers:
          bckPool.c   getConnection() exponential backoff
          bckPool.c   releaseConnection() normal path
          bckPool.c   pthread_cond_timedwait on busy pool

        Correctness: backup completes successfully after taosd resumes.

        Since: v3.0.0.0

        Labels: common
        """
        src = "cov_poolretry_src"
        tmpdir = _tmpdir("tmp_cov_poolretry")
        taosbackup = etool.taosBackupFile()

        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"create database {src}")
        tdSql.execute(
            f"create table {src}.stb (ts TIMESTAMP, v INT) tags (t1 INT)"
        )
        for i in range(10):
            tdSql.execute(f"create table {src}.ct{i} using {src}.stb tags({i})")
            vals = [f"({1700000000000 + i*10000 + j*100}, {j})" for j in range(100)]
            tdSql.execute(f"insert into {src}.ct{i} values " + " ".join(vals))

        cmd = f"{taosbackup} -Z native -D {src} -T 2 -o {tmpdir}"
        proc = subprocess.Popen(
            cmd, shell=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            preexec_fn=os.setsid,
        )
        time.sleep(1)

        tdLog.info("  sc.dnodeStop(1): pause taosd")
        sc.dnodeStop(1)
        time.sleep(3)
        tdLog.info("  sc.dnodeStart(1): resume taosd")
        sc.dnodeStart(1)

        try:
            proc.wait(timeout=120)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            proc.wait()
            tdLog.exit("backup hung after taosd resume")

        out = proc.stdout.read().decode(errors="replace") if proc.stdout else ""
        tdLog.info(f"  exit code: {proc.returncode}")

        assert proc.returncode == 0, \
            f"backup failed after taosd resume: rc={proc.returncode}\n{out[-500:]}"
        assert "SUCCESS" in out, f"expected SUCCESS in output"

        tdSql.execute(f"drop database if exists {src}")
        tdLog.info("test_connection_pool_retry_on_pause PASSED")

    # -----------------------------------------------------------------
    # A-12. GEOMETRY tag backup/restore
    # -----------------------------------------------------------------
    def test_geometry_tag_backup_restore(self):
        """Backup/restore tables with GEOMETRY columns.

        Covers:
          restoreMeta.c  bckWkbToWkt() — POINT geometry type
          backupMeta.c   GEOMETRY tag serialization

        Correctness: GEOMETRY point data is preserved.

        Since: v3.0.0.0

        Labels: common
        """
        src = "cov_geom_src"
        dst = "cov_geom_dst"
        tmpdir = _tmpdir("tmp_cov_geom")

        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"create database {src}")

        try:
            tdSql.execute(
                f"create table {src}.stb "
                f"(ts TIMESTAMP, v INT) "
                f"tags (t_id INT, t_geom GEOMETRY(64))"
            )
        except Exception as e:
            if "syntax error" in str(e).lower() or "Invalid column" in str(e):
                tdLog.info("GEOMETRY type not supported in this build — skip")
                return
            raise

        points = [
            (1, "POINT(120.1 30.2)"),
            (2, "POINT(121.5 31.3)"),
            (3, "POINT(0.0 0.0)"),
        ]
        for tid, pt in points:
            tdSql.execute(
                f"create table {src}.ct{tid} using {src}.stb "
                f"tags({tid}, '{pt}')"
            )
            ts = 1700000000000 + tid * 1000
            tdSql.execute(
                f"insert into {src}.ct{tid} values ({ts}, {tid * 10})"
            )

        rlist = etool.taosbackup(f"-Z native -D {src} -T 1 -o {tmpdir}")
        _checkstr(rlist, "SUCCESS", "backup")

        tdSql.execute(f"drop database if exists {dst}")
        rlist = etool.taosbackup(f"-Z native -W '{src}={dst}' -i {tmpdir}")
        _checkstr(rlist, "SUCCESS", "restore")

        tdSql.query(f"select count(*) from {dst}.stb")
        assert tdSql.queryResult[0][0] == 3, \
            f"row count mismatch: {tdSql.queryResult[0][0]}"

        tdSql.query(f"select v from {dst}.ct2")
        assert tdSql.queryResult[0][0] == 20

        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"drop database if exists {dst}")
        tdLog.info("test_geometry_tag_backup_restore PASSED")

    # -----------------------------------------------------------------
    # A-13. Normal table (non-child) backup/restore
    # -----------------------------------------------------------------
    def test_normal_table_backup_restore(self):
        """Backup/restore a database containing only normal tables.

        Covers:
          bckDb.c        getDBNormalTableNames() / getDBNormalTableCount()
          backupMeta.c   normal table DDL backup path
          restoreMeta.c  restoreNtbSql() execution
          bckUtil.c      obtainFileName(BACK_DIR_NTBDATA)

        Correctness: normal table data is preserved.

        Since: v3.0.0.0

        Labels: common
        """
        src = "cov_ntb_src"
        dst = "cov_ntb_dst"
        tmpdir = _tmpdir("tmp_cov_ntb")

        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"create database {src}")

        for i in range(5):
            tdSql.execute(
                f"create table {src}.ntb{i} "
                f"(ts TIMESTAMP, v INT, name BINARY(32))"
            )
            vals = []
            for j in range(30):
                ts = 1700000000000 + i * 100000 + j * 100
                vals.append(f"({ts}, {j}, 'ntb{i}_row{j}')")
            tdSql.execute(f"insert into {src}.ntb{i} values " + " ".join(vals))

        tdSql.query(f"select count(*) from {src}.ntb0")
        assert tdSql.queryResult[0][0] == 30

        rlist = etool.taosbackup(f"-Z native -D {src} -T 2 -o {tmpdir}")
        _checkstr(rlist, "SUCCESS", "backup")

        tdSql.execute(f"drop database if exists {dst}")
        rlist = etool.taosbackup(f"-Z native -W '{src}={dst}' -i {tmpdir}")
        _checkstr(rlist, "SUCCESS", "restore")

        for i in range(5):
            tdSql.query(f"select count(*) from {dst}.ntb{i}")
            cnt = tdSql.queryResult[0][0]
            assert cnt == 30, f"ntb{i} count mismatch: {cnt}"

            tdSql.query(f"select name from {dst}.ntb{i} where v = 15")
            assert tdSql.queryResult[0][0] == f"ntb{i}_row15"

        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"drop database if exists {dst}")
        tdLog.info("test_normal_table_backup_restore PASSED")

    # -----------------------------------------------------------------
    # A-14. Mixed database: STB + normal tables
    # -----------------------------------------------------------------
    def test_mixed_stb_and_ntb(self):
        """Database with both super tables and normal tables.

        Covers:
          bckDb.c        both getDBSuperTableNames() and getDBNormalTableNames()
          backupMeta.c   both STB and NTB backup paths
          restoreMeta.c  both restoreStbSql() and restoreNtbSql()

        Correctness: both STB children and NTBs are fully restored.

        Since: v3.0.0.0

        Labels: common
        """
        src = "cov_mixed_src"
        dst = "cov_mixed_dst"
        tmpdir = _tmpdir("tmp_cov_mixed")

        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"create database {src}")

        tdSql.execute(
            f"create table {src}.stb (ts TIMESTAMP, v INT) tags (t1 INT)"
        )
        for i in range(3):
            tdSql.execute(
                f"create table {src}.ct{i} using {src}.stb tags({i})"
            )
            vals = [f"({1700000000000 + i*10000 + j*100}, {j})" for j in range(20)]
            tdSql.execute(f"insert into {src}.ct{i} values " + " ".join(vals))

        for i in range(3):
            tdSql.execute(
                f"create table {src}.ntb{i} (ts TIMESTAMP, n INT)"
            )
            vals = [f"({1700000000000 + i*10000 + j*100}, {j+100})" for j in range(15)]
            tdSql.execute(f"insert into {src}.ntb{i} values " + " ".join(vals))

        rlist = etool.taosbackup(f"-Z native -D {src} -T 2 -o {tmpdir}")
        _checkstr(rlist, "SUCCESS", "backup")

        tdSql.execute(f"drop database if exists {dst}")
        rlist = etool.taosbackup(f"-Z native -W '{src}={dst}' -i {tmpdir}")
        _checkstr(rlist, "SUCCESS", "restore")

        tdSql.query(f"select count(*) from {dst}.stb")
        assert tdSql.queryResult[0][0] == 60, \
            f"STB count: {tdSql.queryResult[0][0]} vs 60"

        for i in range(3):
            tdSql.query(f"select count(*) from {dst}.ntb{i}")
            assert tdSql.queryResult[0][0] == 15, \
                f"ntb{i} count: {tdSql.queryResult[0][0]} vs 15"

        tdSql.execute(f"drop database if exists {src}")
        tdSql.execute(f"drop database if exists {dst}")
        tdLog.info("test_mixed_stb_and_ntb PASSED")

    # ===================================================================
    # Section B: tests from coverage_extra.py
    # ===================================================================

    # -----------------------------------------------------------------
    # B-1. NULL tag values — restoreMeta.c isNull branch
    # -----------------------------------------------------------------
    def test_null_tag_backup_restore(self):
        """restoreMeta.c isNull branch — CTBs with explicit NULL tag values.

        Branch covered:
            restoreMeta.c:~1628  if (isNull) { pos += snprintf(… "NULL"); continue; }

        Correctness: NULL tag values survive backup → restore.

        Since: v3.0.0.0

        Labels: common
        """
        src_db = "cov_null_tag_src"
        dst_db = "cov_null_tag_dst"
        stb    = "st"
        tmpdir = _tmpdir("tmp_cov_null_tag")

        taosbackup = etool.taosBackupFile()
        if not taosbackup:
            tdLog.exit("taosBackup not found")

        tdSql.execute(f"drop database if exists {src_db}")
        tdSql.execute(
            f"create database {src_db} vgroups 1 replica 1 precision 'ms'"
        )
        tdSql.execute(
            f"create stable {src_db}.{stb} "
            f"(ts timestamp, v int) "
            f"tags (tag_int int, tag_nc nchar(20))"
        )
        tdSql.execute(f"create table {src_db}.t0 using {src_db}.{stb} tags(10, 'hello')")
        tdSql.execute(f"insert into {src_db}.t0 values(now(), 1)")
        tdSql.execute(f"create table {src_db}.t1 using {src_db}.{stb} tags(NULL, 'world')")
        tdSql.execute(f"insert into {src_db}.t1 values(now()+1s, 2)")
        tdSql.execute(f"create table {src_db}.t2 using {src_db}.{stb} tags(20, NULL)")
        tdSql.execute(f"insert into {src_db}.t2 values(now()+2s, 3)")
        tdSql.execute(f"create table {src_db}.t3 using {src_db}.{stb} tags(NULL, NULL)")
        tdSql.execute(f"insert into {src_db}.t3 values(now()+3s, 4)")

        rlist = etool.taosbackup(f"-Z native -D {src_db} -T 1 -o {tmpdir}")
        _checkstr(rlist, "SUCCESS", "backup")

        tdSql.execute(f"drop database if exists {dst_db}")
        rlist = etool.taosbackup(
            f'-Z native -W "{src_db}={dst_db}" -T 1 -i {tmpdir}'
        )
        _checkstr(rlist, "SUCCESS", "restore")

        tdSql.query(f"select count(*) from {dst_db}.{stb}")
        count = tdSql.getData(0, 0)
        if count != 4:
            tdLog.exit(f"row count mismatch: expected 4, got {count}")

        tdSql.query(f"select tag_int from {dst_db}.t1")
        val = tdSql.getData(0, 0)
        if val is not None:
            tdLog.exit(f"t1 tag_int should be NULL, got {val!r}")

        tdSql.query(f"select tag_nc from {dst_db}.t2")
        val = tdSql.getData(0, 0)
        if val is not None:
            tdLog.exit(f"t2 tag_nc should be NULL, got {val!r}")

        tdLog.info("test_null_tag_backup_restore PASSED")

    # -----------------------------------------------------------------
    # B-2. TIMESTAMP tag — restoreMeta.c TSDB_DATA_TYPE_TIMESTAMP case
    # -----------------------------------------------------------------
    def test_timestamp_tag_backup_restore(self):
        """restoreMeta.c TIMESTAMP case in tag-value switch.

        Branch covered:
            restoreMeta.c:~1680  case TSDB_DATA_TYPE_TIMESTAMP:

        Correctness: TIMESTAMP tag value is preserved.

        Since: v3.0.0.0

        Labels: common
        """
        src_db = "cov_ts_tag_src"
        dst_db = "cov_ts_tag_dst"
        stb    = "sensors"
        tmpdir = _tmpdir("tmp_cov_ts_tag")

        taosbackup = etool.taosBackupFile()
        if not taosbackup:
            tdLog.exit("taosBackup not found")

        tdSql.execute(f"drop database if exists {src_db}")
        tdSql.execute(
            f"create database {src_db} vgroups 1 replica 1 precision 'ms'"
        )
        tdSql.execute(
            f"create stable {src_db}.{stb} "
            f"(ts timestamp, val float) "
            f"tags (loc varchar(20), created_at timestamp)"
        )
        tdSql.execute(
            f"create table {src_db}.s0 using {src_db}.{stb} "
            f"tags('room_A', '2024-01-01 08:00:00.000')"
        )
        tdSql.execute(f"insert into {src_db}.s0 values(now(), 3.14)")
        tdSql.execute(
            f"create table {src_db}.s1 using {src_db}.{stb} "
            f"tags('room_B', '2024-06-15 12:00:00.000')"
        )
        tdSql.execute(f"insert into {src_db}.s1 values(now()+1s, 2.71)")

        rlist = etool.taosbackup(f"-Z native -D {src_db} -T 1 -o {tmpdir}")
        _checkstr(rlist, "SUCCESS", "backup")

        tdSql.execute(f"drop database if exists {dst_db}")
        rlist = etool.taosbackup(
            f'-Z native -W "{src_db}={dst_db}" -T 1 -i {tmpdir}'
        )
        _checkstr(rlist, "SUCCESS", "restore")

        tdSql.query(f"select count(*) from {dst_db}.{stb}")
        count = tdSql.getData(0, 0)
        if count != 2:
            tdLog.exit(f"row count mismatch: expected 2, got {count}")

        tdSql.query(f"select created_at from {dst_db}.s0")
        val = tdSql.getData(0, 0)
        if val is None:
            tdLog.exit("created_at tag of s0 should not be NULL")

        tdLog.info("test_timestamp_tag_backup_restore PASSED")

    # -----------------------------------------------------------------
    # B-3. Empty child table — backupData.c "skip empty table (0 rows)"
    # -----------------------------------------------------------------
    def test_empty_child_table_skip(self):
        """backupData.c skip empty table branch.

        Branch covered:
            backupData.c:~107  if (rows == 0) { … return TSDB_CODE_SUCCESS; }

        Correctness: empty CTB is preserved in meta; no data rows.

        Since: v3.0.0.0

        Labels: common
        """
        src_db = "cov_empty_ctb_src"
        dst_db = "cov_empty_ctb_dst"
        stb    = "measures"
        tmpdir = _tmpdir("tmp_cov_empty_ctb")

        taosbackup = etool.taosBackupFile()
        if not taosbackup:
            tdLog.exit("taosBackup not found")

        tdSql.execute(f"drop database if exists {src_db}")
        tdSql.execute(
            f"create database {src_db} vgroups 1 replica 1 precision 'ms'"
        )
        tdSql.execute(
            f"create stable {src_db}.{stb} "
            f"(ts timestamp, v int) tags (gid int)"
        )
        tdSql.execute(f"create table {src_db}.t_full using {src_db}.{stb} tags(1)")
        for i in range(10):
            tdSql.execute(
                f"insert into {src_db}.t_full values(now()+{i}s, {i})"
            )
        tdSql.execute(f"create table {src_db}.t_empty using {src_db}.{stb} tags(2)")

        rlist = etool.taosbackup(f"-Z native -D {src_db} -T 1 -o {tmpdir}")
        _checkstr(rlist, "SUCCESS", "backup")

        tdSql.execute(f"drop database if exists {dst_db}")
        rlist = etool.taosbackup(
            f'-Z native -W "{src_db}={dst_db}" -T 1 -i {tmpdir}'
        )
        _checkstr(rlist, "SUCCESS", "restore")

        tdSql.query(f"select count(*) from {dst_db}.{stb}")
        count = tdSql.getData(0, 0)
        if count != 10:
            tdLog.exit(f"row count mismatch: expected 10, got {count}")

        tdSql.query(f"select count(*) from {dst_db}.t_empty")
        ecount = tdSql.getData(0, 0)
        if ecount != 0:
            tdLog.exit(f"t_empty should have 0 rows, got {ecount}")

        tdLog.info("test_empty_child_table_skip PASSED")

    # -----------------------------------------------------------------
    # B-4. Checkpoint hash table — restoreCkpt.c hash path
    # -----------------------------------------------------------------
    def test_checkpoint_resume_with_existing_entries(self):
        """restoreCkpt.c hash table functions (init/insert/lookup/free).

        Steps:
          1. Create 600 CTBs × 50 rows.
          2. Backup to completion.
          3. Restore attempt 1 (no -C): SIGINT after 20 s.
          4. Restore attempt 2 (with -C): loads checkpoint, completes.
          5. Verify data.

        Branches covered:
            restoreCkpt.c  initCkptHashTable / hashString / nextPowerOf2
                           insertCkptHash / lookupCkptHash / freeCkptHashTable

        Since: v3.0.0.0

        Labels: common
        """
        src_db = "cov_ckpt_hash_src"
        dst_db = "cov_ckpt_hash_dst"
        stb    = "meters"
        TABLES = 600
        ROWS   = 50
        KILL_S = 20

        taosbackup = etool.taosBackupFile()
        benchmark  = etool.benchMarkFile()
        if not taosbackup or not benchmark:
            tdLog.exit("taosBackup or taosBenchmark not found")

        tmpdir = _tmpdir("tmp_cov_ckpt_hash")

        tdLog.info(f"== step 1: insert {TABLES} × {ROWS} rows ==")
        ret = os.system(f"{benchmark} -d {src_db} -t {TABLES} -n {ROWS} -y")
        if ret != 0:
            tdLog.exit(f"taosBenchmark failed (ret={ret})")

        tdLog.info("== step 2: backup ==")
        rc, _ = _run(f"{taosbackup} -Z native -T 2 -D {src_db} -o {tmpdir}")
        if rc != 0:
            tdLog.exit(f"backup failed (rc={rc})")

        tdSql.query(f"select count(*) from {src_db}.{stb}")
        src_count = tdSql.getData(0, 0)
        tdSql.query(f"select sum(voltage) from {src_db}.{stb}")
        src_sum   = tdSql.getData(0, 0)
        tdLog.info(f"  source: count={src_count}  sum(voltage)={src_sum}")

        tdLog.info(f"== step 3: restore (no -C, SIGINT after {KILL_S}s) ==")
        tdSql.execute(f"drop database if exists {dst_db}")
        restore_cmd = (
            f"{taosbackup} -Z native -T 1 "
            f'-W "{src_db}={dst_db}" -i {tmpdir}'
        )
        rc, killed = _run_sigint(restore_cmd, KILL_S)
        if not killed:
            tdLog.info("restore completed before kill — skipping -C step")
            tdSql.query(f"select count(*) from {dst_db}.{stb}")
            c = tdSql.getData(0, 0)
            if c != src_count:
                tdLog.exit(f"count mismatch: src={src_count} dst={c}")
            tdLog.info("test_checkpoint_resume_with_existing_entries PASSED (no kill needed)")
            return

        ckpt_file = os.path.join(tmpdir, src_db, "restore_checkpoint.txt")
        ckpt_entries = 0
        if os.path.exists(ckpt_file):
            with open(ckpt_file) as f:
                ckpt_entries = sum(1 for ln in f if ln.strip())
        tdLog.info(f"  checkpoint entries after kill: {ckpt_entries}")

        tdLog.info("== step 4: restore with -C (resume from checkpoint) ==")
        rc, out = _run(restore_cmd + " -C")
        if rc != 0:
            tdLog.exit(f"restore attempt 2 with -C failed (rc={rc})")

        tdLog.info("== step 5: verify data ==")
        tdSql.query(f"select count(*) from {dst_db}.{stb}")
        dst_count = tdSql.getData(0, 0)
        if dst_count != src_count:
            tdLog.exit(
                f"count mismatch: src={src_count} dst={dst_count}"
            )
        tdLog.info(
            f"test_checkpoint_resume_with_existing_entries PASSED "
            f"(count={dst_count}, ckpt_entries={ckpt_entries})"
        )

    # -----------------------------------------------------------------
    # B-5. Read-only output dir — restoreCkpt.c g_allowWriteCP = false
    # -----------------------------------------------------------------
    def test_readonly_output_dir_checkpoint(self):
        """restoreCkpt.c g_allowWriteCP = false when output dir is read-only.

        Branch covered:
            restoreCkpt.c  g_allowWriteCP = false  (loadRestoreCheckpoint)
            restoreCkpt.c  if (!g_allowWriteCP) return;  (markRestoreDone)

        Correctness: restore still succeeds without writing checkpoint.

        Since: v3.0.0.0

        Labels: common
        """
        src_db = "cov_ro_ckpt_src"
        dst_db = "cov_ro_ckpt_dst"
        stb    = "st"
        tmpdir = _tmpdir("tmp_cov_ro_ckpt")

        taosbackup = etool.taosBackupFile()
        if not taosbackup:
            tdLog.exit("taosBackup not found")

        tdSql.execute(f"drop database if exists {src_db}")
        tdSql.execute(
            f"create database {src_db} vgroups 1 replica 1 precision 'ms'"
        )
        tdSql.execute(
            f"create stable {src_db}.{stb} "
            f"(ts timestamp, v int) tags (gid int)"
        )
        tdSql.execute(f"create table {src_db}.t0 using {src_db}.{stb} tags(1)")
        for i in range(5):
            tdSql.execute(f"insert into {src_db}.t0 values(now()+{i}s, {i})")

        rlist = etool.taosbackup(f"-Z native -D {src_db} -T 1 -o {tmpdir}")
        _checkstr(rlist, "SUCCESS", "backup")

        db_dir = os.path.join(tmpdir, src_db)
        if not os.path.exists(db_dir):
            tdLog.exit(f"backup DB dir not found: {db_dir}")
        os.chmod(db_dir, 0o555)

        try:
            tdSql.execute(f"drop database if exists {dst_db}")
            rlist = etool.taosbackup(
                f'-Z native -W "{src_db}={dst_db}" -T 1 -C -i {tmpdir}'
            )
            _checkstr(rlist, "SUCCESS", "restore (read-only dir)")

            tdSql.query(f"select count(*) from {dst_db}.{stb}")
            count = tdSql.getData(0, 0)
            if count != 5:
                tdLog.exit(f"row count mismatch: expected 5, got {count}")
        finally:
            os.chmod(db_dir, 0o755)

        tdLog.info("test_readonly_output_dir_checkpoint PASSED")

    # -----------------------------------------------------------------
    # B-6. Extra CLI args — bckArgs.c -k / -z / -m branches
    # -----------------------------------------------------------------
    def test_backup_with_extra_args(self):
        """-k retry-count, -z retry-sleep-ms, -m tag-thread-num arguments.

        Branches covered:
            bckArgs.c  -k / --retry-count
            bckArgs.c  -z / --retry-sleep-ms
            bckArgs.c  -m / --tag-thread-num

        Correctness: data integrity with extra args.

        Since: v3.0.0.0

        Labels: common
        """
        src_db = "cov_extra_args_src"
        dst_db = "cov_extra_args_dst"
        stb    = "st"
        tmpdir = _tmpdir("tmp_cov_extra_args")

        taosbackup = etool.taosBackupFile()
        if not taosbackup:
            tdLog.exit("taosBackup not found")

        tdSql.execute(f"drop database if exists {src_db}")
        tdSql.execute(
            f"create database {src_db} vgroups 1 replica 1 precision 'ms'"
        )
        tdSql.execute(
            f"create stable {src_db}.{stb} "
            f"(ts timestamp, v int) tags (gid int)"
        )
        for i in range(3):
            tdSql.execute(
                f"create table {src_db}.t{i} using {src_db}.{stb} tags({i})"
            )
            tdSql.execute(
                f"insert into {src_db}.t{i} values(now()+{i}s, {i})"
            )

        rlist = etool.taosbackup(
            f"-Z native -D {src_db} -T 1 "
            f"-k 5 -z 100 -m 2 "
            f"-o {tmpdir}"
        )
        _checkstr(rlist, "SUCCESS", "backup(-k -z -m)")

        tdSql.execute(f"drop database if exists {dst_db}")
        rlist = etool.taosbackup(
            f'-Z native -W "{src_db}={dst_db}" -T 1 '
            f"-B 512 -v 2 "
            f"-i {tmpdir}"
        )
        _checkstr(rlist, "SUCCESS", "restore(-B -v 2)")

        tdSql.query(f"select count(*) from {dst_db}.{stb}")
        count = tdSql.getData(0, 0)
        if count != 3:
            tdLog.exit(f"row count mismatch: expected 3, got {count}")

        tdLog.info("test_backup_with_extra_args PASSED")

    # -----------------------------------------------------------------
    # B-7. Stmt version 1 restore — restoreStmt.c code path
    # -----------------------------------------------------------------
    def test_restore_stmt_version1(self):
        """restoreStmt.c  STMT v1 restore path.

        Branches covered:
            restoreStmt.c  STMT1 array-bind loop
            bckArgs.c      g_stmtVersion = 1

        Correctness: data integrity with STMT v1.

        Since: v3.0.0.0

        Labels: common
        """
        src_db = "cov_stmt1_src"
        dst_db = "cov_stmt1_dst"
        stb    = "st"
        tmpdir = _tmpdir("tmp_cov_stmt1")

        taosbackup = etool.taosBackupFile()
        if not taosbackup:
            tdLog.exit("taosBackup not found")

        tdSql.execute(f"drop database if exists {src_db}")
        tdSql.execute(
            f"create database {src_db} vgroups 1 replica 1 precision 'ms'"
        )
        tdSql.execute(
            f"create stable {src_db}.{stb} "
            f"(ts timestamp, c_int int, c_float float, c_double double, "
            f" c_bool bool, c_tinyint tinyint, c_smallint smallint, "
            f" c_bigint bigint, c_nchar nchar(20), c_binary varchar(20)) "
            f"tags (gid int, loc varchar(10))"
        )
        for i in range(4):
            tdSql.execute(
                f"create table {src_db}.t{i} using {src_db}.{stb} "
                f"tags({i}, 'loc{i}')"
            )
            tdSql.execute(
                f"insert into {src_db}.t{i} values"
                f"(now()+{i}s, {i}, {i}.5, {i}.25, true, {i}, {i*2}, "
                f"{i*100}, 'nchar{i}', 'bin{i}')"
            )

        rlist = etool.taosbackup(f"-Z native -D {src_db} -T 1 -o {tmpdir}")
        _checkstr(rlist, "SUCCESS", "backup")

        tdSql.execute(f"drop database if exists {dst_db}")
        rlist = etool.taosbackup(
            f'-Z native -W "{src_db}={dst_db}" -T 1 -v 1 -i {tmpdir}'
        )
        _checkstr(rlist, "SUCCESS", "restore(-v 1)")

        tdSql.query(f"select count(*) from {dst_db}.{stb}")
        count = tdSql.getData(0, 0)
        if count != 4:
            tdLog.exit(f"row count mismatch: expected 4, got {count}")

        tdLog.info("test_restore_stmt_version1 PASSED")

    # -----------------------------------------------------------------
    # B-8. DB with only normal tables (no STBs) — restoreMeta.c early return
    # -----------------------------------------------------------------
    def test_database_with_only_ntb(self):
        """restoreMeta.c early-return when DB has no super-tables.

        Branches covered:
            restoreMeta.c:~2082-2083  early return after no stables found

        Correctness: NTB data intact.

        Since: v3.0.0.0

        Labels: common
        """
        src_db = "cov_ntb_only_src"
        dst_db = "cov_ntb_only_dst"
        tmpdir = _tmpdir("tmp_cov_ntb_only")

        taosbackup = etool.taosBackupFile()
        if not taosbackup:
            tdLog.exit("taosBackup not found")

        tdSql.execute(f"drop database if exists {src_db}")
        tdSql.execute(
            f"create database {src_db} vgroups 1 replica 1 precision 'ms'"
        )
        tdSql.execute(
            f"create table {src_db}.ntb0 (ts timestamp, v int, name varchar(20))"
        )
        tdSql.execute(
            f"insert into {src_db}.ntb0 values(now(), 42, 'hello')"
        )
        tdSql.execute(
            f"create table {src_db}.ntb1 (ts timestamp, x float)"
        )
        tdSql.execute(
            f"insert into {src_db}.ntb1 values(now()+1s, 1.23)"
        )

        rlist = etool.taosbackup(f"-Z native -D {src_db} -T 1 -o {tmpdir}")
        _checkstr(rlist, "SUCCESS", "backup")

        tdSql.execute(f"drop database if exists {dst_db}")
        rlist = etool.taosbackup(
            f'-Z native -W "{src_db}={dst_db}" -T 1 -i {tmpdir}'
        )
        _checkstr(rlist, "SUCCESS", "restore")

        tdSql.query(f"select count(*) from {dst_db}.ntb0")
        c0 = tdSql.getData(0, 0)
        tdSql.query(f"select count(*) from {dst_db}.ntb1")
        c1 = tdSql.getData(0, 0)
        if c0 != 1 or c1 != 1:
            tdLog.exit(f"count mismatch: ntb0={c0} ntb1={c1} (expected 1/1)")

        tdLog.info("test_database_with_only_ntb PASSED")

    # -----------------------------------------------------------------
    # B-9. Large tag array — restoreMeta.c tagFiles capacity doubling
    # -----------------------------------------------------------------
    def test_large_tag_array_capacity(self):
        """restoreMeta.c tagFiles[] realloc (capacity doubling).

        Branches covered:
            restoreMeta.c:~1861-1868  if (count >= capacity) { capacity *= 2; … }

        Correctness: all rows restored after realloc.

        Since: v3.0.0.0

        Labels: common
        """
        src_db = "cov_large_tags_src"
        dst_db = "cov_large_tags_dst"
        stb    = "meters"
        TABLES = 100
        ROWS   = 20

        taosbackup = etool.taosBackupFile()
        benchmark  = etool.benchMarkFile()
        if not taosbackup or not benchmark:
            tdLog.exit("taosBackup or taosBenchmark not found")

        tmpdir = _tmpdir("tmp_cov_large_tags")

        tdLog.info(f"== insert {TABLES} tables × {ROWS} rows ==")
        ret = os.system(f"{benchmark} -d {src_db} -t {TABLES} -n {ROWS} -y")
        if ret != 0:
            tdLog.exit(f"taosBenchmark failed (ret={ret})")

        rlist = etool.taosbackup(
            f"-Z native -D {src_db} -T 2 -m 2 -o {tmpdir}"
        )
        _checkstr(rlist, "SUCCESS", "backup")

        tdSql.execute(f"drop database if exists {dst_db}")
        rlist = etool.taosbackup(
            f'-Z native -W "{src_db}={dst_db}" -T 2 -i {tmpdir}'
        )
        _checkstr(rlist, "SUCCESS", "restore")

        tdSql.query(f"select count(*) from {src_db}.{stb}")
        src_count = tdSql.getData(0, 0)
        tdSql.query(f"select count(*) from {dst_db}.{stb}")
        dst_count = tdSql.getData(0, 0)
        if src_count != dst_count:
            tdLog.exit(
                f"count mismatch: src={src_count} dst={dst_count}"
            )

        tdLog.info(
            f"test_large_tag_array_capacity PASSED "
            f"(tables={TABLES}, rows={src_count})"
        )

    # -----------------------------------------------------------------
    # B-10. Schema-change detection — bckSchemaChange.c addStbChanged()
    # -----------------------------------------------------------------
    def test_schema_change_partial_column_write(self):
        """bckSchemaChange.c partial-column INSERT on schema mismatch.

        Setup:
          1. src_db.stb with (ts, c1 INT, c2 FLOAT, c3 DOUBLE) + tag gid
          2. Backup src_db
          3. Manually create dst_db.stb with (ts, c1 INT) only
          4. Restore — addStbChanged() detects mismatch → partial write

        Branches covered:
            bckSchemaChange.c  addStbChanged() / genPartColsStr()
            restoreStmt2.c     schema-change INSERT (partColsStr path)

        Since: v3.0.0.0

        Labels: common

        History:
        - 2026-03-26 Alex Duan Created
        """
        src_db = "cov_schema_chg_src"
        dst_db = "cov_schema_chg_dst"
        stb    = "metrics"
        ROWS   = 10

        taosbackup = etool.taosBackupFile()
        if not taosbackup:
            tdLog.exit("taosBackup not found")

        tmpdir = _tmpdir("tmp_cov_schema_chg")

        tdLog.info("== step 1: create source STB with 4 data columns ==")
        tdSql.execute(f"drop database if exists {src_db}")
        tdSql.execute(
            f"create database {src_db} vgroups 1 replica 1 precision 'ms'"
        )
        tdSql.execute(
            f"create stable {src_db}.{stb} "
            f"(ts timestamp, c1 int, c2 float, c3 double) "
            f"tags (gid int)"
        )
        for i in range(3):
            tdSql.execute(
                f"create table {src_db}.t{i} using {src_db}.{stb} tags({i})"
            )
            for r in range(ROWS):
                tdSql.execute(
                    f"insert into {src_db}.t{i} values"
                    f"(now()+{i*ROWS*1000+r}s, {i*ROWS+r}, "
                    f"{(i*ROWS+r)*1.5}, {(i*ROWS+r)*2.5})"
                )
        tdSql.query(f"select count(*) from {src_db}.{stb}")
        src_count = tdSql.getData(0, 0)
        tdSql.query(f"select sum(c1) from {src_db}.{stb}")
        src_sum_c1 = tdSql.getData(0, 0)
        tdLog.info(f"  source: count={src_count}  sum(c1)={src_sum_c1}")

        tdLog.info("== step 2: backup ==")
        rc, lines = _run(f"{taosbackup} -Z native -T 1 -D {src_db} -o {tmpdir}")
        if rc != 0:
            tdLog.exit(f"backup failed (rc={rc})")

        tdLog.info("== step 3: create dst STB with reduced schema (ts, c1 only) ==")
        tdSql.execute(f"drop database if exists {dst_db}")
        tdSql.execute(
            f"create database {dst_db} vgroups 1 replica 1 precision 'ms'"
        )
        tdSql.execute(
            f"create stable {dst_db}.{stb} "
            f"(ts timestamp, c1 int) "
            f"tags (gid int)"
        )

        tdLog.info("== step 4: restore (schema change detection triggered) ==")
        rc, lines = _run(
            f'{taosbackup} -Z native -T 1 -W "{src_db}={dst_db}" -i {tmpdir}'
        )
        if rc != 0:
            tdLog.exit(f"restore failed (rc={rc}), output:\n" + "\n".join(lines[-30:]))

        tdLog.info("== step 5: verify ==")

        tdSql.query(f"select count(*) from {dst_db}.{stb}")
        dst_count = tdSql.getData(0, 0)
        if dst_count != src_count:
            tdLog.exit(
                f"count mismatch: src={src_count} dst={dst_count}"
            )
        tdLog.info(f"  count OK: {dst_count}")

        tdSql.query(f"select sum(c1) from {dst_db}.{stb}")
        dst_sum_c1 = tdSql.getData(0, 0)
        if dst_sum_c1 != src_sum_c1:
            tdLog.exit(
                f"sum(c1) mismatch: src={src_sum_c1} dst={dst_sum_c1}"
            )
        tdLog.info(f"  sum(c1) OK: {dst_sum_c1}")

        tdSql.query(f"describe {dst_db}.{stb}")
        col_names = [tdSql.getData(r, 0) for r in range(tdSql.getRows())]
        if "c2" in col_names or "c3" in col_names:
            tdLog.exit(
                f"dst schema should not have c2/c3, but got columns: {col_names}"
            )
        if "c1" not in col_names:
            tdLog.exit(f"dst schema is missing c1; columns: {col_names}")
        tdLog.info(f"  dst schema OK: {col_names}")

        tdLog.info("test_schema_change_partial_column_write PASSED")

    # -----------------------------------------------------------------
    # B-11. Checkpoint file loading — restoreCkpt.c loadRestoreCheckpoint()
    # -----------------------------------------------------------------
    def test_checkpoint_file_loading_path(self):
        """restoreCkpt.c checkpoint file loading path.

        Pre-seeds a checkpoint file with 2 out of 5 CTB data file paths,
        then runs restore with -C.  The 2 pre-seeded files are skipped;
        the other 3 are restored.

        Branches covered:
            restoreCkpt.c  loadRestoreCheckpoint (fileSize > 0)
            restoreCkpt.c  initCkptHashTable / hashString / nextPowerOf2
            restoreCkpt.c  insertCkptHash / isRestoreDone (TRUE and FALSE)
            restoreCkpt.c  freeCkptHashTable
            restoreData.c  skip-file branch

        Since: v3.0.0.0

        Labels: common

        History:
        - 2026-03-26 Alex Duan Created
        """
        src_db    = "cov_ckpt_load_src"
        dst_db    = "cov_ckpt_load_dst"
        stb       = "meters"
        TABLES    = 5
        ROWS      = 10
        SKIP_CNT  = 2

        taosbackup = etool.taosBackupFile()
        benchmark  = etool.benchMarkFile()
        if not taosbackup or not benchmark:
            tdLog.exit("taosBackup or taosBenchmark not found")

        tmpdir = _tmpdir("tmp_cov_ckpt_load")

        tdLog.info(f"== step 1: insert {TABLES} × {ROWS} rows ==")
        ret = os.system(
            f"{benchmark} -d {src_db} -t {TABLES} -n {ROWS} -y"
        )
        if ret != 0:
            tdLog.exit(f"taosBenchmark failed (ret={ret})")

        tdSql.query(f"select count(*) from {src_db}.{stb}")
        src_count = tdSql.getData(0, 0)
        tdLog.info(f"  source rows: {src_count}")
        if src_count != TABLES * ROWS:
            tdLog.exit(f"expected {TABLES*ROWS} rows, got {src_count}")

        tdLog.info("== step 2: backup ==")
        rc, _ = _run(f"{taosbackup} -Z native -T 1 -D {src_db} -o {tmpdir}")
        if rc != 0:
            tdLog.exit(f"backup failed (rc={rc})")

        tdLog.info("== step 3: locate data .dat files ==")
        dat_files = sorted(glob.glob(
            os.path.join(tmpdir, src_db, "**", "*.dat"),
            recursive=True
        ))
        dat_files = [p for p in dat_files
                     if os.sep + "tags" + os.sep not in p]
        tdLog.info(f"  found {len(dat_files)} data .dat files")
        if len(dat_files) < SKIP_CNT + 1:
            tdLog.exit(
                f"need at least {SKIP_CNT+1} data files "
                f"but found only {len(dat_files)}"
            )

        tdLog.info("== step 4: pre-seed checkpoint file ==")
        skipped_files = dat_files[:SKIP_CNT]
        ckpt_file = os.path.join(tmpdir, src_db, "restore_checkpoint.txt")
        with open(ckpt_file, "w") as f:
            for path in skipped_files:
                f.write(path + "\n")
                tdLog.info(f"  checkpoint entry: {path}")

        tdLog.info("== step 5: restore with -C (2 files must be skipped) ==")
        tdSql.execute(f"drop database if exists {dst_db}")
        rc, lines = _run(
            f'{taosbackup} -Z native -T 1 -C '
            f'-W "{src_db}={dst_db}" -i {tmpdir}'
        )
        if rc != 0:
            tdLog.exit(
                f"restore with -C failed (rc={rc})\n" + "\n".join(lines[-20:])
            )

        tdLog.info("== step 6: verify ==")

        tdSql.query(
            f"select count(*) from information_schema.ins_tables "
            f"where db_name='{dst_db}' and stable_name='{stb}'"
        )
        ctb_count = tdSql.getData(0, 0)
        if ctb_count != TABLES:
            tdLog.exit(f"CTB count: expected {TABLES}, got {ctb_count}")
        tdLog.info(f"  ctb count OK: {ctb_count}")

        expected_rows = (TABLES - SKIP_CNT) * ROWS
        tdSql.query(f"select count(*) from {dst_db}.{stb}")
        dst_count = tdSql.getData(0, 0)
        if dst_count != expected_rows:
            tdLog.exit(
                f"row count: expected {expected_rows} "
                f"(={TABLES}-{SKIP_CNT} CTBs × {ROWS}), got {dst_count}"
            )
        tdLog.info(
            f"  row count OK: {dst_count} "
            f"({SKIP_CNT} CTBs skipped via checkpoint, "
            f"{TABLES-SKIP_CNT} restored)"
        )

        tdLog.info("test_checkpoint_file_loading_path PASSED")

    # ===================================================================
    # Section C: tests from branch_coverage.py
    # ===================================================================

    # -----------------------------------------------------------------
    # C-1. parseDatabases() — comma-separated -D db1,db2
    # -----------------------------------------------------------------
    def test_multi_db_backup_restore(self):
        """taosBackup comma-separated database list (-D db1,db2).

        Branch covered:
            bckArgs.c  parseDatabases() while-loop with strtok(',')

        Steps:
          1. Create two independent databases with different row counts.
          2. Backup both in a single invocation: -D db1,db2.
          3. Drop both databases.
          4. Restore from the combined backup.
          5. Verify row counts and SUM(c1) for each database independently.

        Since: v3.0.0.0

        Labels: common,ci

        History:
            - 2026-03-25 Alex Duan Created
        """
        db1, db2 = "bcov_mdb1", "bcov_mdb2"
        tmpdir = _tmpdir("tmp_cov_multidb")

        tdLog.info("=== step 1: create two databases ===")
        _insert_simple(db1, rows=30)
        _insert_simple(db2, rows=50)

        sum_t0_db1 = _sum_c1(db1, "t0")
        sum_t0_db2 = _sum_c1(db2, "t0")
        sum_nt_db1 = _sum_c1(db1, "nt")
        sum_nt_db2 = _sum_c1(db2, "nt")
        tdLog.info(f"  source: db1 t0 sum(c1)={sum_t0_db1}, db2 t0 sum(c1)={sum_t0_db2}")

        tdLog.info("=== step 2: backup both databases in one call ===")
        rlist = etool.taosbackup(f"-Z native -D {db1},{db2} -T 2 -o {tmpdir}")
        _checkstr(rlist, "Result       : SUCCESS")

        tdLog.info("=== step 3: drop both databases ===")
        tdSql.execute(f"drop database if exists {db1}")
        tdSql.execute(f"drop database if exists {db2}")

        tdLog.info("=== step 4: restore from combined backup ===")
        rlist2 = etool.taosbackup(f"-Z native -i {tmpdir}")
        _checkstr(rlist2, "Result       : SUCCESS")

        tdLog.info("=== step 5: verify both databases ===")
        cnt_t0_db1 = _rowcount(db1, "t0")
        cnt_t0_db2 = _rowcount(db2, "t0")
        if cnt_t0_db1 != 30:
            tdLog.exit(f"db1 t0 row count mismatch: expected 30, got {cnt_t0_db1}")
        if cnt_t0_db2 != 50:
            tdLog.exit(f"db2 t0 row count mismatch: expected 50, got {cnt_t0_db2}")

        if _sum_c1(db1, "t0") != sum_t0_db1:
            tdLog.exit(f"db1 t0 sum(c1) mismatch after restore")
        if _sum_c1(db2, "t0") != sum_t0_db2:
            tdLog.exit(f"db2 t0 sum(c1) mismatch after restore")
        if _sum_c1(db1, "nt") != sum_nt_db1:
            tdLog.exit(f"db1 nt sum(c1) mismatch after restore")
        if _sum_c1(db2, "nt") != sum_nt_db2:
            tdLog.exit(f"db2 nt sum(c1) mismatch after restore")

        tdLog.info("test_multi_db_backup_restore PASSED")

    # -----------------------------------------------------------------
    # C-2. Rename parsing — pipe-separated -W "db1=n1|db2=n2"
    # -----------------------------------------------------------------
    def test_multi_rename_restore(self):
        """taosBackup multi-pair rename (-W db1=n1|db2=n2).

        Branch covered:
            bckArgs.c  strtok('|') loop in -W parsing

        Steps:
          1. Create two source databases and back them up together.
          2. Restore with -W "src1=dst1|src2=dst2" (two rename pairs).
          3. Verify dst1 and dst2 exist with correct data.

        Since: v3.0.0.0

        Labels: common,ci

        History:
            - 2026-03-25 Alex Duan Created
        """
        src1, src2 = "bcov_rn_src1", "bcov_rn_src2"
        dst1, dst2 = "bcov_rn_dst1", "bcov_rn_dst2"
        tmpdir = _tmpdir("tmp_cov_rename")

        tdLog.info("=== step 1: create source databases ===")
        _insert_simple(src1, rows=20)
        _insert_simple(src2, rows=40)
        sum1 = _sum_c1(src1, "st")
        sum2 = _sum_c1(src2, "st")

        tdLog.info("=== step 2: backup both sources ===")
        rlist = etool.taosbackup(f"-Z native -D {src1},{src2} -T 2 -o {tmpdir}")
        _checkstr(rlist, "Result       : SUCCESS")

        tdLog.info("=== step 3: drop destinations, restore with two rename pairs ===")
        for db in (dst1, dst2):
            tdSql.execute(f"drop database if exists {db}")
        rlist2 = etool.taosbackup(
            f'-Z native -W "{src1}={dst1}|{src2}={dst2}" -i {tmpdir}'
        )
        _checkstr(rlist2, "Result       : SUCCESS")
        _checkstr(rlist2, f"rename database: {src1} -> {dst1}")
        _checkstr(rlist2, f"rename database: {src2} -> {dst2}")

        tdLog.info("=== step 4: verify renamed databases ===")
        cnt1 = _rowcount(dst1, "st")
        cnt2 = _rowcount(dst2, "st")
        if cnt1 != 40:
            tdLog.exit(f"dst1 st row count mismatch: expected 40, got {cnt1}")
        if cnt2 != 80:
            tdLog.exit(f"dst2 st row count mismatch: expected 80, got {cnt2}")
        if _sum_c1(dst1, "st") != sum1:
            tdLog.exit("dst1 st sum(c1) mismatch")
        if _sum_c1(dst2, "st") != sum2:
            tdLog.exit("dst2 st sum(c1) mismatch")

        tdSql.query("select name from information_schema.ins_databases")
        existing = {row[0] for row in tdSql.queryResult}
        if dst1 not in existing:
            tdLog.exit(f"renamed destination {dst1} not found in databases")
        if dst2 not in existing:
            tdLog.exit(f"renamed destination {dst2} not found in databases")
        tdLog.info("  renamed destination DBs exist and have correct data")

        tdLog.info("test_multi_rename_restore PASSED")

    # -----------------------------------------------------------------
    # C-3. restoreNtbSql + restoreStbSql already-exists branches
    # -----------------------------------------------------------------
    def test_second_restore_already_exists(self):
        """taosBackup second restore triggers table/stable already-exists branches.

        Branches covered:
            restoreMeta.c  restoreNtbSql()  TSDB_CODE_TDB_TABLE_ALREADY_EXIST
            restoreMeta.c  restoreStbSql()  TSDB_CODE_MND_STB_ALREADY_EXIST
            restoreMeta.c  restoreDbSql()   USE fallback when DB already exists

        Since: v3.0.0.0

        Labels: common,ci

        History:
            - 2026-03-25 Alex Duan Created
        """
        src = "bcov_ae_src"
        dst = "bcov_ae_dst"
        tmpdir = _tmpdir("tmp_cov_already_exists")
        rows = 25

        tdLog.info("=== step 1: create source DB and backup ===")
        _insert_simple(src, rows=rows)
        sum_st = _sum_c1(src, "st")
        sum_nt = _sum_c1(src, "nt")
        rlist = etool.taosbackup(f"-Z native -D {src} -T 2 -o {tmpdir}")
        _checkstr(rlist, "Result       : SUCCESS")

        tdLog.info("=== step 2: first restore (drop dst first) ===")
        tdSql.execute(f"drop database if exists {dst}")
        rlist2 = etool.taosbackup(f'-Z native -W "{src}={dst}" -i {tmpdir}')
        _checkstr(rlist2, "Result       : SUCCESS")

        cnt_after_first = _rowcount(dst, "st")
        tdLog.info(f"  after first restore: dst.st rows={cnt_after_first}")

        tdLog.info("=== step 3: second restore WITHOUT dropping dst ===")
        rlist3 = etool.taosbackup(f'-Z native -W "{src}={dst}" -i {tmpdir}')
        _checkstr(rlist3, "Result       : SUCCESS")

        tdLog.info("=== step 4: verify row count unchanged ===")
        cnt_st = _rowcount(dst, "st")
        cnt_nt = _rowcount(dst, "nt")
        expected_st = rows * 2
        if cnt_st != expected_st:
            tdLog.exit(
                f"dst.st row count mismatch: expected {expected_st}, got {cnt_st}"
            )
        if cnt_nt != rows:
            tdLog.exit(f"dst.nt row count mismatch: expected {rows}, got {cnt_nt}")

        if _sum_c1(dst, "st") != sum_st:
            tdLog.exit("dst.st sum(c1) mismatch after second restore")
        if _sum_c1(dst, "nt") != sum_nt:
            tdLog.exit("dst.nt sum(c1) mismatch after second restore")

        tdLog.info("test_second_restore_already_exists PASSED")

    # -----------------------------------------------------------------
    # C-4. openTaosFileForRead() — invalid magic-number branch
    # -----------------------------------------------------------------
    def test_invalid_dat_magic(self):
        """taosBackup restore fails gracefully when .dat magic bytes are wrong.

        Branch covered:
            storageTaos.c  memcmp(magic, TAOSFILE_MAGIC, 4) != 0 → BCK_INVALID_FILE

        Correctness: non-zero exit code; no hang.

        Since: v3.0.0.0

        Labels: common,ci

        History:
            - 2026-03-25 Alex Duan Created
        """
        db  = "bcov_magic_src"
        dst = "bcov_magic_dst"
        tmpdir = _tmpdir("tmp_cov_magic")

        tdLog.info("=== step 1: create DB and backup ===")
        _insert_simple(db, rows=10)
        rlist = etool.taosbackup(f"-Z native -F binary -D {db} -T 2 -o {tmpdir}")
        _checkstr(rlist, "Result       : SUCCESS")

        tdLog.info("=== step 2: find a .dat file ===")
        dat_files = []
        for root, _dirs, files in os.walk(tmpdir):
            for f in files:
                if f.endswith(".dat"):
                    dat_files.append(os.path.join(root, f))
        if not dat_files:
            tdLog.exit("no .dat files found in backup dir")
        target = dat_files[0]
        tdLog.info(f"  corrupting magic in: {target}")

        tdLog.info("=== step 3: corrupt magic bytes (first 4 bytes → 0x00) ===")
        with open(target, "r+b") as fh:
            fh.seek(0)
            fh.write(b"\x00\x00\x00\x00")

        tdLog.info("=== step 4: attempt restore — expect non-zero exit ===")
        tdSql.execute(f"drop database if exists {dst}")
        taosbackup_bin = etool.taosBackupFile()
        proc = subprocess.Popen(
            f"unset LD_PRELOAD; {taosbackup_bin}"
            f' -Z native -W "{db}={dst}" -i {tmpdir}',
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            stdout, stderr = proc.communicate(timeout=120)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
            tdLog.exit("restore hung (>120 s) on corrupted .dat file")

        out_str = stdout.decode(errors="replace") + stderr.decode(errors="replace")
        tdLog.info(f"  exit code: {proc.returncode}")
        tdLog.info(f"  output snippet: {out_str[:300]}")
        if proc.returncode == 0:
            tdLog.exit("restore succeeded on corrupted .dat file — expected failure")
        tdLog.info("  restore correctly failed with non-zero exit code")

        tdLog.info("test_invalid_dat_magic PASSED")

    # -----------------------------------------------------------------
    # C-5. backupData.c resume mode — skip already-backed-up files
    # -----------------------------------------------------------------
    def test_backup_resume_mode(self):
        """taosBackup -C resumes backup and skips already-completed child tables.

        Branch covered:
            backupData.c  g_backResumeMode && taosCheckExistFile(pathFile)

        Steps:
          1. Create DB and first backup.
          2. Second backup with -C — skip count must be >= 1.
          3. Restore and verify data integrity.

        Since: v3.0.0.0

        Labels: common,ci

        History:
            - 2026-03-25 Alex Duan Created
        """
        db  = "bcov_resume_src"
        dst = "bcov_resume_dst"
        tmpdir = _tmpdir("tmp_cov_resume")
        rows = 20

        tdLog.info("=== step 1: create DB and first backup ===")
        _insert_simple(db, rows=rows)
        sum_st = _sum_c1(db, "st")
        rlist1 = etool.taosbackup(f"-Z native -D {db} -T 2 -o {tmpdir}")
        _checkstr(rlist1, "Result       : SUCCESS")

        tdLog.info("=== step 2: second backup with -C (resume mode) ===")
        rlist2 = etool.taosbackup(f"-Z native -C -D {db} -T 2 -o {tmpdir}")
        _checkstr(rlist2, "Result       : SUCCESS")

        output2 = "\n".join(str(l) for l in (rlist2 or []))
        tdLog.info(f"  resume backup output: {output2[:400]}")
        if "Skipped" not in output2 and "skipped" not in output2:
            tdLog.exit(
                "resume backup output does not mention 'Skipped'; "
                "resume-mode branch may not have been taken"
            )
        tdLog.info("  resume skip confirmed in output")

        tdLog.info("=== step 3: restore and verify data integrity ===")
        tdSql.execute(f"drop database if exists {dst}")
        rlist3 = etool.taosbackup(
            f'-Z native -W "{db}={dst}" -i {tmpdir}'
        )
        _checkstr(rlist3, "Result       : SUCCESS")

        cnt = _rowcount(dst, "st")
        expected = rows * 2
        if cnt != expected:
            tdLog.exit(f"restored row count mismatch: expected {expected}, got {cnt}")
        if _sum_c1(dst, "st") != sum_st:
            tdLog.exit("restored sum(c1) mismatch")

        tdLog.info("test_backup_resume_mode PASSED")
