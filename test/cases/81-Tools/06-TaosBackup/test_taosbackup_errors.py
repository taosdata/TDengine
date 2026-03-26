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


def _taosd_pids():
    """Return list of taosd PIDs."""
    try:
        out = subprocess.check_output(["pidof", "taosd"], stderr=subprocess.DEVNULL)
        return [int(p) for p in out.decode().split() if p.strip()]
    except (subprocess.CalledProcessError, ValueError, FileNotFoundError):
        return []


class TestTaosBackupErrors:
    """Error-path coverage tests.

    Covers code branches that normal happy-path tests never reach:
      1. bckPool.c getConnection() exponential back-off when pool is empty
         (server paused before the very first connection is made)
      2. backup.c getAllDatabases() — backup with no -D discovers all databases
      3. storageParquet.c — restore from a deliberately corrupted .par file
      4. storageTaos.c readTaosFileBlocks() — large BINARY realloc path
    """

    _STB = "meters"

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
        """bckPool.c getConnection() exponential back-off when pool is empty

        SIGSTOP taosd before backup starts so the first taos_connect() fails
        (pool count==0).  After 7 s the server is resumed and the back-off
        loop must succeed transparently.

        Steps:
          1. Insert small dataset.
          2. SIGSTOP taosd so no connection can be established.
          3. Launch backup with -Z native (no taosadapter dependency).
          4. Wait 7 s — covers 1-s, 2-s, 4-s back-off sleep intervals.
          5. SIGCONT taosd.
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

        tdLog.info("=== step 2: SIGSTOP taosd before backup starts ===")
        pids = _taosd_pids()
        for pid in pids:
            try:
                os.kill(pid, signal.SIGSTOP)
            except OSError:
                pass

        cmd = f"unset LD_PRELOAD; {taosbackup} -Z native -T 1 -D {db} -o {tmpdir}"
        tdLog.info(f"=== step 3: start backup in background: {cmd} ===")
        proc = subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid)

        try:
            tdLog.info("=== step 4: wait 7 s for back-off intervals to execute ===")
            time.sleep(7)

            tdLog.info("=== step 5: SIGCONT taosd ===")
            pids2 = _taosd_pids() or pids
            for pid in pids2:
                try:
                    os.kill(pid, signal.SIGCONT)
                except OSError:
                    pass

            tdLog.info("=== waiting for backup to finish (timeout 120 s) ===")
            try:
                proc.wait(timeout=120)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                proc.wait()
                tdLog.exit("backup timed out (>120 s) after SIGCONT")
        finally:
            # Always resume taosd even if test errors out
            for pid in (_taosd_pids() or pids):
                try:
                    os.kill(pid, signal.SIGCONT)
                except OSError:
                    pass

        if proc.returncode != 0:
            tdLog.exit(
                f"backup FAILED (ret={proc.returncode}) — back-off did not recover"
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
    # Test 2: backup.c getAllDatabases — no -D flag
    # ------------------------------------------------------------------

    def test_backup_all_databases(self):
        """backup.c getAllDatabases() — discover all databases when -D is omitted

        Running backup without -D triggers the SHOW DATABASES path, exercising
        getAllDatabases() including the names array and capacity-doubling realloc.

        Steps:
          1. Insert data into err_alldb.
          2. Backup with no -D — getAllDatabases() runs SHOW DATABASES.
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
    # Test 3: storageParquet.c — restore from a corrupted .par file
    # ------------------------------------------------------------------

    def test_restore_corrupted_parquet(self):
        """storageParquet.c graceful error on corrupted .par file

        Overwrite the first 128 bytes of a .par file with zeros, then attempt
        restore.  The restore must return non-zero and must not hang or crash.

        Steps:
          1. Insert small dataset.
          2. Backup with -F parquet.
          3. Zero-out first 128 bytes of one .par file.
          4. Attempt restore — expect non-zero exit, no hang.

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

        tdLog.info("=== step 4: attempt restore — expect failure ===")
        tdSql.execute(f"drop database if exists {dst_db}")
        proc = subprocess.Popen(
            f"unset LD_PRELOAD; {etool.taosBackupFile()}"
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
            tdLog.exit("restore succeeded on corrupted parquet — expected failure")
        tdLog.info("test_restore_corrupted_parquet PASSED (failed as expected)")

    # ------------------------------------------------------------------
    # Test 4: storageTaos.c readTaosFileBlocks — large BINARY realloc path
    # ------------------------------------------------------------------

    def test_restore_large_binary(self):
        """storageTaos.c readTaosFileBlocks() realloc when block exceeds initial buffer

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
