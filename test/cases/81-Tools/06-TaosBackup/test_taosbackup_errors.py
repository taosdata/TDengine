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
import shutil
import signal
import subprocess
import time

from new_test_framework.utils import tdLog, tdSql, etool, sc


class TestTaosBackupErrors:
    """Error-path coverage tests.

    Covers code branches that normal happy-path tests never reach:
      1. bckPool.c getConnection() exponential back-off when pool is empty
         (server paused before the very first connection is made)
      2. backup.c getAllDatabases() - backup with no -D discovers all databases
      3. storageParquet.c - restore from a deliberately corrupted .par file
      4. storageTaos.c readTaosFileBlocks() - large BINARY realloc path
    """

    _STB = "meters"

    def teardown_method(self, method):
        """Remove tmp_* directories created by this test."""
        import glob
        test_dir = os.path.dirname(os.path.abspath(__file__))
        for d in glob.glob(os.path.join(test_dir, "tmp_*")):
            shutil.rmtree(d, ignore_errors=True)

    def _make_tmpdir(self, name):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), name)
        if os.path.exists(path):
            os.system(f"rm -rf {path}")
        os.makedirs(path)
        return path

    def _insert_small(self, benchmark, db, tables=4, rows=1000):
        cmd = f"{benchmark} -d {db} -t {tables} -n {rows} -y"
        tdLog.info(f"insert: {cmd}")
        ret = os.system(cmd)
        if ret != 0:
            tdLog.exit(f"taosBenchmark failed (ret={ret})")

    # ------------------------------------------------------------------
    # Test 1: bckPool getConnection() exponential back-off (count==0 path)
    # ------------------------------------------------------------------

    def test_backup_server_pause_before_connect(self):
        """bckPool connection back-off

        stop taosd before backup starts so the first taos_connect() fails
        (pool count==0).  After 7 s the server is restarted and the back-off
        loop must succeed transparently.

        Steps:
          1. Insert small dataset.
          2. sc.dnodeStop(1): stop taosd so no connection can be established.
          3. Launch backup with -Z native (no taosadapter dependency).
          4. Wait 7 s - covers 1-s, 2-s, 4-s back-off sleep intervals.
          5. sc.dnodeStart(1): restart taosd.
          6. Verify backup completes successfully.
          7. Restore and verify row count.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:
            - 2026-03-24 Alex Duan Created to cover bckPool.c back-off path

        """
        taosbackup = etool.taosBackupFile()
        benchmark  = etool.benchMarkFile()
        if not taosbackup or not benchmark:
            tdLog.exit("required binaries not found")
        db     = "err_pause_src"
        dst_db = "err_pause_dst"
        tmpdir = self._make_tmpdir("tmp_err_pause")

        tdLog.info("=== step 1: insert data ===")
        self._insert_small(benchmark, db)

        tdLog.info("=== step 2: sc.dnodeStop(1): stop taosd before backup starts ===")
        sc.dnodeStop(1)

        cmd = f"{taosbackup} -Z native -T 1 -D {db} -o {tmpdir}"
        tdLog.info(f"=== step 3: start backup in background: {cmd} ===")
        proc = subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid)

        try:
            tdLog.info("=== step 4: wait 7 s for back-off intervals to execute ===")
            time.sleep(7)

            tdLog.info("=== step 5: sc.dnodeStart(1): restart taosd ===")
            sc.dnodeStart(1)

            tdLog.info("=== waiting for backup to finish (timeout 120 s) ===")
            try:
                proc.wait(timeout=120)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                proc.wait()
                tdLog.exit("backup timed out (>120 s) after sc.dnodeStart")
        finally:
            # Always ensure taosd is running even if test errors out
            sc.dnodeStart(1)

        if proc.returncode != 0:
            tdLog.exit(
                f"backup FAILED (ret={proc.returncode}) - back-off did not recover"
            )

        tdLog.info("=== step 6: restore and verify ===")
        tdSql.execute(f"drop database if exists {dst_db}")
        rlist = etool.taosbackup(f'-Z native -W "{db}={dst_db}" -i {tmpdir}')
        out = "\n".join(rlist) if rlist else ""
        if "SUCCESS" not in out:
            tdLog.exit(f"restore failed:\n{out[:400]}")
        tdSql.query(f"SELECT count(*) FROM {dst_db}.{self._STB}")
        count = tdSql.getData(0, 0)
        if count == 0:
            tdLog.exit("restored table is empty")
        tdLog.info(f"test_backup_server_pause_before_connect PASSED (rows={count})")

    # ------------------------------------------------------------------
    # Test 2: backup.c getAllDatabases - no -D flag
    # ------------------------------------------------------------------

    def test_backup_all_databases(self):
        """Backup all databases

        Running backup without -D triggers the SHOW DATABASES path, exercising
        getAllDatabases() including the names array and capacity-doubling realloc.

        Steps:
          1. Insert data into err_alldb.
          2. Backup with no -D - getAllDatabases() runs SHOW DATABASES.
          3. Verify "discovered N database(s)" appears (confirms path was taken).
          4. Backup err_alldb with -D for restore verification.
          5. Restore err_alldb into err_alldb_r and verify row count.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:
            - 2026-03-24 Alex Duan Created to cover backup.c getAllDatabases()

        """
        benchmark = etool.benchMarkFile()
        if not benchmark:
            tdLog.exit("taosBenchmark not found")
        db     = "err_alldb"
        dst_db = "err_alldb_r"
        tmpdir = self._make_tmpdir("tmp_err_alldb")

        tdLog.info("=== step 1: insert data ===")
        self._insert_small(benchmark, db, tables=4, rows=500)

        tdLog.info("=== step 2: backup ALL databases (no -D) ===")
        # checkRun=False: other databases on this server may be in a broken state.
        # We only verify getAllDatabases() was invoked (no -D argument triggers it).
        # SHOW DATABASES returns by creation time, so err_alldb (just created) may
        # come after a broken DB and not be reached.  Step 3 uses a separate -D backup.
        rlist = etool.taosbackup(f"-Z native -T 2 -o {tmpdir}", checkRun=False)
        out = "\n".join(rlist) if rlist else ""
        if "discovered" not in out:
            tdLog.exit(f"getAllDatabases() was not invoked:\n{out[:400]}")
        tdLog.info("getAllDatabases() confirmed invoked")

        tdLog.info("=== step 3: backup err_alldb only for restore verification ===")
        tmpdir2 = self._make_tmpdir("tmp_err_alldb2")
        rlist = etool.taosbackup(f"-Z native -D {db} -T 2 -o {tmpdir2}")

        tdLog.info("=== step 4: restore err_alldb ===")
        tdSql.execute(f"drop database if exists {dst_db}")
        rlist = etool.taosbackup(f'-Z native -W "{db}={dst_db}" -i {tmpdir2}')
        out = "\n".join(rlist) if rlist else ""
        if "SUCCESS" not in out:
            tdLog.exit(f"restore failed:\n{out[:400]}")

        tdLog.info("=== step 5: verify row count ===")
        tdSql.query(f"SELECT count(*) FROM {dst_db}.{self._STB}")
        count = tdSql.getData(0, 0)
        if count == 0:
            tdLog.exit("restored table is empty")
        tdLog.info(f"test_backup_all_databases PASSED (rows={count})")

    # ------------------------------------------------------------------
    # Test 3: storageParquet.c - restore from a corrupted .par file
    # ------------------------------------------------------------------

    def test_restore_corrupted_parquet(self):
        """Corrupted parquet restore

        Overwrite the first 128 bytes of a .par file with zeros, then attempt
        restore.  The restore must return non-zero and must not hang or crash.

        Steps:
          1. Insert small dataset.
          2. Backup with -F parquet.
          3. Zero-out first 128 bytes of one .par file.
          4. Attempt restore - expect non-zero exit, no hang.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:
            - 2026-03-24 Alex Duan Created to cover storageParquet.c error path

        """
        benchmark = etool.benchMarkFile()
        if not benchmark:
            tdLog.exit("taosBenchmark not found")
        db     = "err_parquet"
        dst_db = "err_parquet_r"
        tmpdir = self._make_tmpdir("tmp_err_par")

        tdLog.info("=== step 1: insert data ===")
        self._insert_small(benchmark, db, tables=4, rows=500)

        tdLog.info("=== step 2: backup with parquet format ===")
        rlist = etool.taosbackup(f"-Z native -F parquet -D {db} -T 2 -o {tmpdir}")
        out = "\n".join(rlist) if rlist else ""
        if "SUCCESS" not in out:
            tdLog.exit(f"parquet backup failed:\n{out[:400]}")

        tdLog.info("=== step 3: find and corrupt a .par file ===")
        par_files = []
        for root, _dirs, files in os.walk(tmpdir):
            for f in files:
                if f.endswith(".par"):
                    par_files.append(os.path.join(root, f))
        if not par_files:
            tdLog.exit("no .par files found in backup dir")
        target = par_files[0]
        tdLog.info(f"  corrupting: {target}")
        with open(target, "r+b") as fh:
            corrupt_len = min(128, os.path.getsize(target))
            fh.seek(0)
            fh.write(b"\x00" * corrupt_len)

        tdLog.info("=== step 4: attempt restore - expect failure ===")
        tdSql.execute(f"drop database if exists {dst_db}")
        proc = subprocess.Popen(
            f"{etool.taosBackupFile()}"
            f' -Z native -W "{db}={dst_db}" -i {tmpdir}',
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            stdout, stderr = proc.communicate(timeout=120)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
            tdLog.exit("restore hung (>120 s) on corrupted parquet")
        out_str = stdout.decode(errors="replace") + stderr.decode(errors="replace")
        tdLog.info(f"  exit code: {proc.returncode}")
        tdLog.info(f"  output snippet: {out_str[:300]}")
        if proc.returncode == 0:
            tdLog.exit("restore succeeded on corrupted parquet - expected failure")
        tdLog.info("test_restore_corrupted_parquet PASSED (failed as expected)")

    # ------------------------------------------------------------------
    # Test 4: storageTaos.c readTaosFileBlocks - large BINARY realloc path
    # ------------------------------------------------------------------

    def test_restore_large_binary(self):
        """Large binary realloc

        A BINARY(16000) column with ~15000-byte values produces a compressed block
        that can exceed the initial read buffer, triggering taosMemoryRealloc().

        Steps:
          1. Create table with BINARY(16000) column.
          2. Insert 50 rows with ~15000-byte payload.
          3. Backup with -Z native -F binary (Taos native format).
          4. Restore and verify COUNT(*) == 50.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:
            - 2026-03-24 Alex Duan Created to cover storageTaos.c realloc path

        """
        db     = "err_lbin"
        dst_db = "err_lbin_r"
        tmpdir = self._make_tmpdir("tmp_err_lbin")

        tdLog.info("=== step 1: create db and table with BINARY(16000) col ===")
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(
            f"create database {db} vgroups 1 replica 1 precision 'ms'"
        )
        tdSql.execute(
            f"create stable {db}.stb "
            f"(ts timestamp, payload binary(16000)) tags (gid int)"
        )
        tdSql.execute(f"create table {db}.t0 using {db}.stb tags(1)")

        tdLog.info("=== step 2: insert 50 rows with ~15000-byte payload ===")
        big_str = "X" * 15000
        for i in range(50):
            ts = 1600000000000 + i * 1000
            tdSql.execute(f"insert into {db}.t0 values({ts}, '{big_str}')")

        tdLog.info("=== step 3: backup with native binary format ===")
        rlist = etool.taosbackup(f"-Z native -F binary -D {db} -T 2 -o {tmpdir}")
        out = "\n".join(rlist) if rlist else ""
        if "SUCCESS" not in out:
            tdLog.exit(f"backup failed:\n{out[:400]}")

        tdLog.info("=== step 4: restore and verify ===")
        tdSql.execute(f"drop database if exists {dst_db}")
        rlist = etool.taosbackup(f'-Z native -W "{db}={dst_db}" -i {tmpdir}')
        out = "\n".join(rlist) if rlist else ""
        if "SUCCESS" not in out:
            tdLog.exit(f"restore failed:\n{out[:400]}")
        tdSql.query(f"SELECT count(*) FROM {dst_db}.stb")
        count = tdSql.getData(0, 0)
        if count != 50:
            tdLog.exit(f"row count mismatch: expected 50, got {count}")
        tdLog.info(f"test_restore_large_binary PASSED (rows={count})")

    # ------------------------------------------------------------------
    # Test 5: User-cancel during backup  (E: g_interrupted paths)
    # ------------------------------------------------------------------

    def test_sigint_during_backup(self):
        """SIGINT during backup

        Sends SIGINT to taosBackup while it is actively writing data to
        exercise the volatile g_interrupted checks in:
          - backup/backupData.c:241,592,608   (per-CTB / per-NTB loop)
          - backup/backupMeta.c:415,510,596   (meta loop)
          - storage/storageTaos.c:330         (resultToFileTaos write loop)
          - main.c:251                        (final g_interrupted override)

        Correctness verification:
          After the interrupt test, a fresh complete backup + restore is
          performed to confirm the source data on the server is intact and
          a full backup/restore cycle still produces correct row counts.

        Steps:
          1. Insert ~100 K rows (20 CTBs × 5 000 rows); record source row count.
          2. Start backup with -T 1 (single thread) so it takes several seconds.
          3. After 2 s send SIGINT to the taosBackup process group.
          4. Verify the output contains "CANCELLED BY USER" (or process already
             finished cleanly - accepted when timing window is missed).
          5. Do a fresh complete backup to a separate directory.
          6. Restore into sigint_bck_r and verify COUNT(*) == source row count.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:
            - 2026-03-27 Alex Duan Created to cover g_interrupted backup code paths

        """
        taosbackup = etool.taosBackupFile()
        benchmark  = etool.benchMarkFile()
        if not taosbackup or not benchmark:
            tdLog.exit("required binaries not found")
        db     = "sigint_bck"
        dst_db = "sigint_bck_r"
        tmpdir      = self._make_tmpdir("tmp_sigint_bck")
        tmpdir_full = self._make_tmpdir("tmp_sigint_bck_full")

        tdLog.info("=== step 1: insert ~100 K rows ===")
        if os.system(f"{benchmark} -d {db} -t 20 -n 5000 -y") != 0:
            tdLog.exit("taosBenchmark failed")
        tdSql.query(f"SELECT count(*) FROM {db}.meters")
        src_count = tdSql.getData(0, 0)
        tdLog.info(f"source row count: {src_count}")
        if not src_count or src_count == 0:
            tdLog.exit("source table is empty after benchmark insert")

        tdLog.info("=== step 2: start single-thread backup ===")
        proc = subprocess.Popen(
            f"{taosbackup} -Z native -T 1 -D {db} -o {tmpdir}",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid,
        )

        time.sleep(2)
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGINT)
            tdLog.info("SIGINT sent to backup process group")
        except ProcessLookupError:
            tdLog.info("backup already finished before SIGINT (timing window)")

        try:
            stdout, stderr = proc.communicate(timeout=120)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            proc.communicate()
            tdLog.exit("backup did not finish within 120 s after SIGINT")

        out_str = stdout.decode(errors="replace") + stderr.decode(errors="replace")
        tdLog.info(f"backup exit code: {proc.returncode}")
        tdLog.info(f"backup output snippet: {out_str[:400]}")

        if "CANCELLED BY USER" in out_str:
            tdLog.info("SIGINT handled correctly: 'CANCELLED BY USER' confirmed")
        elif proc.returncode == 0:
            tdLog.info("backup finished before SIGINT - timing window, cancel check skipped")
        elif proc.returncode == -signal.SIGINT:
            tdLog.info("SIGINT handled: process terminated by SIGINT signal (no cleanup message)")
        else:
            tdLog.exit(
                f"unexpected backup failure (ret={proc.returncode}):\n{out_str[:400]}"
            )

        # Verify source data is still intact and a full backup+restore works.
        tdLog.info("=== step 5: fresh complete backup to verify data integrity ===")
        rlist = etool.taosbackup(f"-Z native -T 2 -D {db} -o {tmpdir_full}")
        full_out = "\n".join(rlist) if rlist else ""
        if "SUCCESS" not in full_out:
            tdLog.exit(f"full backup after SIGINT test failed:\n{full_out[:400]}")
        tdLog.info("full backup SUCCESS")

        tdLog.info("=== step 6: restore and verify COUNT(*) == source ===")
        tdSql.execute(f"drop database if exists {dst_db}")
        rlist2 = etool.taosbackup(f'-Z native -W "{db}={dst_db}" -i {tmpdir_full}')
        rst_out = "\n".join(rlist2) if rlist2 else ""
        if "SUCCESS" not in rst_out:
            tdLog.exit(f"restore after SIGINT test failed:\n{rst_out[:400]}")
        tdSql.query(f"SELECT count(*) FROM {dst_db}.meters")
        rst_count = tdSql.getData(0, 0)
        if rst_count != src_count:
            tdLog.exit(
                f"row count mismatch after full restore: "
                f"expected {src_count}, got {rst_count}"
            )
        tdLog.info(f"row count verified: {rst_count} == {src_count}")
        tdLog.info("test_sigint_during_backup PASSED")

    # ------------------------------------------------------------------
    # Test 6: User-cancel during restore  (E: g_interrupted paths)
    # ------------------------------------------------------------------

    def test_sigint_during_restore(self):
        """SIGINT during restore

        Sends SIGINT to taosBackup while it is actively restoring data to
        exercise the volatile g_interrupted checks in:
          - restore/restore.c:119              (main restore loop)
          - restore/restoreData.c:119,723      (restore thread loop)
          - restore/restoreMeta.c:210,311,2106 (meta restore loops)
          - restore/restoreStmt.c:424          (STMT1 insert loop)
          - core/bckPool.c:126,145             (pool wait interrupt)

        Correctness verification:
          After the interrupt test, the partially-restored database is dropped
          and a fresh complete restore is performed.  The final row count must
          exactly equal the source row count, confirming that an interrupted
          restore does not corrupt the backup files or the server state.

        Steps:
          1. Insert ~100 K rows; record source COUNT(*).
          2. Create a clean backup.
          3. Start restore with -T 1 -v 1 (single thread, STMT1).
          4. After 2 s send SIGINT.
          5. Verify "CANCELLED BY USER" (or finished cleanly - timing window).
          6. Drop the partial dst_db, re-run full restore.
          7. Verify COUNT(*) in restored DB == source COUNT(*).

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:
            - 2026-03-27 Alex Duan Created to cover g_interrupted restore code paths

        """
        taosbackup = etool.taosBackupFile()
        benchmark  = etool.benchMarkFile()
        if not taosbackup or not benchmark:
            tdLog.exit("required binaries not found")
        db     = "sigint_rst"
        dst_db = "sigint_rst_r"
        bckdir = self._make_tmpdir("tmp_sigint_rst_bck")

        tdLog.info("=== step 1: insert data ===")
        if os.system(f"{benchmark} -d {db} -t 20 -n 5000 -y") != 0:
            tdLog.exit("taosBenchmark failed")
        tdSql.query(f"SELECT count(*) FROM {db}.meters")
        src_count = tdSql.getData(0, 0)
        tdLog.info(f"source row count: {src_count}")
        if not src_count or src_count == 0:
            tdLog.exit("source table is empty after benchmark insert")
        src_sum = tdSql.getFirstValue(f"SELECT sum(voltage) FROM {db}.meters")
        tdLog.info(f"source voltage sum: {src_sum}")

        tdLog.info("=== step 2: create clean backup ===")
        rlist = etool.taosbackup(f"-Z native -T 2 -D {db} -o {bckdir}")
        out = "\n".join(rlist) if rlist else ""
        if "SUCCESS" not in out:
            tdLog.exit(f"clean backup failed:\n{out[:400]}")

        tdLog.info("=== step 3: start single-thread STMT1 restore ===")
        tdSql.execute(f"drop database if exists {dst_db}")
        proc = subprocess.Popen(
            f"{taosbackup} -Z native -T 1 -v 1"
            f' -W "{db}={dst_db}" -i {bckdir}',
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid,
        )

        time.sleep(2)
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGINT)
            tdLog.info("SIGINT sent to restore process group")
        except ProcessLookupError:
            tdLog.info("restore already finished before SIGINT (timing window)")

        try:
            stdout, stderr = proc.communicate(timeout=120)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            proc.communicate()
            tdLog.exit("restore did not finish within 120 s after SIGINT")

        out_str = stdout.decode(errors="replace") + stderr.decode(errors="replace")
        tdLog.info(f"restore exit code: {proc.returncode}")
        tdLog.info(f"restore output snippet: {out_str[:400]}")

        if "CANCELLED BY USER" in out_str:
            tdLog.info("SIGINT handled correctly: 'CANCELLED BY USER' confirmed")
        elif proc.returncode == 0:
            tdLog.info("restore finished before SIGINT - timing window, cancel check skipped")
        elif proc.returncode == -signal.SIGINT:
            tdLog.info("SIGINT handled: process terminated by SIGINT signal (no cleanup message)")
        else:
            tdLog.exit(
                f"unexpected restore failure (ret={proc.returncode}):\n{out_str[:400]}"
            )

        # Verify backup files are intact by resuming the partial restore with checkpoint.
        tdLog.info("=== step 6: resume restore via checkpoint (dst_db kept as-is) ===")
        rlist2 = etool.taosbackup(f'-Z native -T 2 -C -W "{db}={dst_db}" -i {bckdir}')
        rst_out = "\n".join(rlist2) if rlist2 else ""
        if "SUCCESS" not in rst_out:
            tdLog.exit(f"checkpoint restore after SIGINT test failed:\n{rst_out[:400]}")

        tdLog.info("=== step 7: verify COUNT(*) and SUM(voltage) match source ===")
        tdSql.query(f"SELECT count(*) FROM {dst_db}.meters")
        rst_count = tdSql.getData(0, 0)
        if rst_count != src_count:
            tdLog.exit(
                f"row count mismatch: expected {src_count}, got {rst_count}"
            )
        tdSql.query(f"SELECT sum(voltage) FROM {dst_db}.meters")
        rst_sum = tdSql.getData(0, 0)
        if rst_sum != src_sum:
            tdLog.exit(
                f"voltage sum mismatch: expected {src_sum}, got {rst_sum}"
            )
        tdLog.info(f"row count verified: {rst_count} == {src_count}")
        tdLog.info(f"voltage sum verified: {rst_sum} == {src_sum}")
        tdLog.info("test_sigint_during_restore PASSED")

    # ------------------------------------------------------------------
    # Test 7: Backup output path cannot be created  (B: IO failure)
    # ------------------------------------------------------------------

    def test_backup_path_create_failure(self):
        """Backup path create failure

        Points -o at a path whose parent component on-disk is a regular file,
        forcing taosMulMkDir to fail with ENOTDIR.  Works even for root because
        POSIX mkdir(2) always returns ENOTDIR when an intermediate component is
        not a directory.

        Covers:
          - backup/backup.c:42-43  (create db main path failed)

        Correctness verification:
          1. Bad-path backup must exit non-zero AND print a failure message.
          2. The source database must be unaffected - a subsequent backup to a
             valid directory and restore must produce the correct row count.

        Steps:
          1. Insert 200 rows; record COUNT(*).
          2. Backup to {tempfile}/subdir - must fail (ENOTDIR).
          3. Verify nonzero exit AND output contains "FAILED" or error keyword.
          4. Backup to a valid tmpdir (normal run).
          5. Restore into io_pathfail_r; verify COUNT(*) == 200.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:
            - 2026-03-27 Alex Duan Created to cover backup.c path-creation failure path

        """
        import tempfile as _tempfile

        taosbackup = etool.taosBackupFile()
        benchmark  = etool.benchMarkFile()
        if not taosbackup or not benchmark:
            tdLog.exit("required binaries not found")
        db     = "io_pathfail"
        dst_db = "io_pathfail_r"
        EXPECTED_ROWS = 200

        tdLog.info("=== step 1: create database with known row count ===")
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"create stable {db}.st (ts timestamp, c1 int) tags(gid int)")
        for t in range(2):
            tdSql.execute(f"create table {db}.t{t} using {db}.st tags({t})")
            for i in range(100):
                tdSql.execute(f"insert into {db}.t{t} values({1640000000000 + i*1000}, {i})")
        tdSql.query(f"SELECT count(*) FROM {db}.st")
        src_count = tdSql.getData(0, 0)
        if src_count != EXPECTED_ROWS:
            tdLog.exit(f"expected {EXPECTED_ROWS} rows, got {src_count}")
        tdLog.info(f"source row count: {src_count}")

        tdLog.info("=== step 2: backup to path inside a regular file (must fail) ===")
        tf = _tempfile.NamedTemporaryFile(delete=False, suffix="_notadir")
        tf.close()
        fake_outdir = tf.name + "/subdir"
        proc = subprocess.Popen(
            f"{taosbackup} -Z native -D {db} -o {fake_outdir}",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            stdout, stderr = proc.communicate(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
            tdLog.exit("backup hung unexpectedly on bad output path")
        finally:
            try:
                os.unlink(tf.name)
            except OSError:
                pass

        out_str = stdout.decode(errors="replace") + stderr.decode(errors="replace")
        tdLog.info(f"backup exit code: {proc.returncode}")
        tdLog.info(f"backup output snippet: {out_str[:300]}")

        # Step 3: verify failure is reported correctly
        if proc.returncode == 0:
            tdLog.exit("backup should fail with a file-as-directory output path, but succeeded")
        if "FAILED" not in out_str and "failed" not in out_str and "Error" not in out_str:
            tdLog.exit(
                f"backup failed (ret={proc.returncode}) but produced no error message:\n"
                f"{out_str[:300]}"
            )
        tdLog.info("bad-path backup correctly rejected with error message")

        # Steps 4-5: verify source data intact via normal backup+restore
        tdLog.info("=== step 4: normal backup to valid directory ===")
        tmpdir_ok = self._make_tmpdir("tmp_io_pathfail_ok")
        rlist = etool.taosbackup(f"-Z native -D {db} -o {tmpdir_ok}")
        ok_out = "\n".join(rlist) if rlist else ""
        if "SUCCESS" not in ok_out:
            tdLog.exit(f"normal backup failed:\n{ok_out[:400]}")

        tdLog.info("=== step 5: restore and verify row count ===")
        tdSql.execute(f"drop database if exists {dst_db}")
        rlist2 = etool.taosbackup(f'-Z native -W "{db}={dst_db}" -i {tmpdir_ok}')
        rst_out = "\n".join(rlist2) if rlist2 else ""
        if "SUCCESS" not in rst_out:
            tdLog.exit(f"restore failed:\n{rst_out[:400]}")
        tdSql.query(f"SELECT count(*) FROM {dst_db}.st")
        rst_count = tdSql.getData(0, 0)
        if rst_count != EXPECTED_ROWS:
            tdLog.exit(
                f"row count mismatch after restore: expected {EXPECTED_ROWS}, got {rst_count}"
            )
        tdLog.info(f"row count verified: {rst_count} == {EXPECTED_ROWS}")
        tdLog.info("test_backup_path_create_failure PASSED")

    # ------------------------------------------------------------------
    # Test 8: stb.sql pre-exists as directory  (B: IO failure)
    # ------------------------------------------------------------------

    def test_backup_stbsql_dir_collision(self):
        """stb.sql write collision

        Pre-creates {outdir}/{db}/stb.sql as a subdirectory so that
        taosOpenFile({outdir}/{db}/stb.sql, O_WRONLY|O_CREAT|O_APPEND) fails
        with EISDIR.  This failure occurs even for root because POSIX open(2)
        always returns EISDIR when the path names a directory and O_WRONLY is set.

        The prior directory-creation steps in backup.c succeed because we
        pre-populate both {outdir}/{db}/ and {outdir}/{db}/tags/, so
        taosMulMkDir / taosMkDir return 0 immediately (path already exists).

        Covers:
          - backup/backupMeta.c:80-83  (open stb.sql for append failed)

        Correctness verification:
          1. Bad-directory backup must exit non-zero AND output an error message.
          2. A subsequent backup to a clean directory must succeed.
          3. Restoring that clean backup must reproduce the exact source row count.

        Steps:
          1. Insert 200 rows; record COUNT(*).
          2. Pre-create {tmpdir}/{db}/stb.sql as a directory.
          3. Run backup → must fail; verify nonzero exit + error message.
          4. Backup to a fresh clean directory (normal run).
          5. Restore into io_stbsql_r; verify COUNT(*) == 200.

        Since: v3.0.0.0

        Labels: common

        Jira: None

        History:
            - 2026-03-27 Alex Duan Created to cover backupMeta.c stb.sql open-failure path

        """
        taosbackup = etool.taosBackupFile()
        benchmark  = etool.benchMarkFile()
        if not taosbackup or not benchmark:
            tdLog.exit("required binaries not found")
        db     = "io_stbsql"
        dst_db = "io_stbsql_r"
        tmpdir = self._make_tmpdir("tmp_io_stbsql")
        EXPECTED_ROWS = 200

        tdLog.info("=== step 1: insert small dataset with known row count ===")
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"create stable {db}.st (ts timestamp, c1 int) tags(gid int)")
        for t in range(2):
            tdSql.execute(f"create table {db}.t{t} using {db}.st tags({t})")
            for i in range(100):
                tdSql.execute(f"insert into {db}.t{t} values({1640000000000 + i*1000}, {i})")
        tdSql.query(f"SELECT count(*) FROM {db}.st")
        src_count = tdSql.getData(0, 0)
        if src_count != EXPECTED_ROWS:
            tdLog.exit(f"expected {EXPECTED_ROWS} rows, got {src_count}")
        tdLog.info(f"source row count: {src_count}")

        tdLog.info("=== step 2: pre-create stb.sql as a directory ===")
        # taosMulMkDir / taosMkDir skip creation for pre-existing dirs → OK.
        # Only the file-open of {tmpdir}/{db}/stb.sql fails (EISDIR, even root).
        os.makedirs(os.path.join(tmpdir, db))
        os.makedirs(os.path.join(tmpdir, db, "tags"))
        os.makedirs(os.path.join(tmpdir, db, "stb.sql"))   # directory, not file

        tdLog.info("=== step 3: run backup - must fail at stb.sql ===")
        proc = subprocess.Popen(
            f"{taosbackup} -Z native -D {db} -o {tmpdir}",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            stdout, stderr = proc.communicate(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
            tdLog.exit("backup hung on stb.sql directory collision")

        out_str = stdout.decode(errors="replace") + stderr.decode(errors="replace")
        tdLog.info(f"backup exit code: {proc.returncode}")
        tdLog.info(f"backup output snippet: {out_str[:300]}")
        if proc.returncode == 0:
            tdLog.exit("backup should fail when stb.sql is a directory, but succeeded")
        if "FAILED" not in out_str and "failed" not in out_str and "Error" not in out_str:
            tdLog.exit(
                f"backup failed (ret={proc.returncode}) but produced no error message:\n"
                f"{out_str[:300]}"
            )
        tdLog.info("stb.sql collision correctly rejected with error message")

        # Steps 4-5: confirm source data is intact via normal backup+restore
        tdLog.info("=== step 4: normal backup to fresh clean directory ===")
        tmpdir_ok = self._make_tmpdir("tmp_io_stbsql_ok")
        rlist = etool.taosbackup(f"-Z native -D {db} -o {tmpdir_ok}")
        ok_out = "\n".join(rlist) if rlist else ""
        if "SUCCESS" not in ok_out:
            tdLog.exit(f"normal backup after collision test failed:\n{ok_out[:400]}")

        tdLog.info("=== step 5: restore and verify row count ===")
        tdSql.execute(f"drop database if exists {dst_db}")
        rlist2 = etool.taosbackup(f'-Z native -W "{db}={dst_db}" -i {tmpdir_ok}')
        rst_out = "\n".join(rlist2) if rlist2 else ""
        if "SUCCESS" not in rst_out:
            tdLog.exit(f"restore failed:\n{rst_out[:400]}")
        tdSql.query(f"SELECT count(*) FROM {dst_db}.st")
        rst_count = tdSql.getData(0, 0)
        if rst_count != EXPECTED_ROWS:
            tdLog.exit(
                f"row count mismatch after restore: expected {EXPECTED_ROWS}, got {rst_count}"
            )
        tdLog.info(f"row count verified: {rst_count} == {EXPECTED_ROWS}")
        tdLog.info("test_backup_stbsql_dir_collision PASSED")
