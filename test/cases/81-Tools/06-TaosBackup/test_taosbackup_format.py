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

from new_test_framework.utils import tdLog, tdSql, etool
import hashlib
import os
import shutil
import socket
import time
import zlib


# Source DB created by insertFormat.json
SRC_DB = "test"
STB    = "meters"


class TestTaosBackupFormat:
    """
    Verify taosBackup -F binary / -F parquet combined with -v 1 (STMT1) and
    -v 2 (STMT2) restore options.

    Test matrix (2 formats × 2 stmt versions = 4 restores):
        binary  + STMT1  →  fmt_bin_v1
        binary  + STMT2  →  fmt_bin_v2
        parquet + STMT1  →  fmt_par_v1
        parquet + STMT2  →  fmt_par_v2

    After each restore the imported DB is compared against the source DB.
    Comparison strategy:
        · COUNT(*)       – total row count must match
        · SUM(col)       – all numeric / decimal columns
        · SUM(CASE WHEN bc …) – boolean columns
        · COUNT(col)     – string / varbinary / geometry columns (non-NULL count)
        · SELECT DISTINCT tbname, <int-tags> ORDER BY tbname
                         – 20-row tag snapshot per child table
    """

    # -----------------------------------------------------------------------
    # Schema knowledge  (from insertFormat.json)
    # -----------------------------------------------------------------------

    # Numeric data columns: integers → exact SUM; float/double → approximate SUM
    _INT_COLS  = ["ti", "si", "ic", "bi", "uti", "usi", "ui", "ubi"]
    _FP_COLS   = ["fc", "dc"]
    # Keep the combined list so existing references still work
    _NUM_COLS  = _INT_COLS + _FP_COLS
    # Decimal data columns → compare SUM
    _DEC_COLS  = ["dec64", "dec128"]
    # Bool data columns → compare SUM(CASE WHEN … THEN 1 ELSE 0 END)
    _BOOL_COLS = ["bc"]
    # String / binary / var data columns → compare COUNT (non-NULL count)
    _VAR_COLS  = ["bin", "nch", "vab", "vac"]

    # Numeric tag columns: integers → exact SUM; float/double → approximate
    _INT_TAGS  = ["tti", "tsi", "tic", "tbi", "tuti", "tusi", "tui", "tubi"]
    _FP_TAGS   = ["tfc", "tdc"]
    _NUM_TAGS  = _INT_TAGS + _FP_TAGS
    # Bool tag → SUM CASE
    _BOOL_TAGS = ["tbc"]
    # String / var / geometry tags → COUNT (non-NULL)
    _VAR_TAGS  = ["tbin", "tnch", "tvab", "tvac", "tgeo"]
    # Integer-type tag subset kept for distinct-per-CTB snapshot
    _SNAP_TAGS = ["tti", "tsi", "tic", "tbi"]

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    def makeDir(self, path):
        if os.path.exists(path):
            shutil.rmtree(path)
        os.makedirs(path)

    def _wait_adapter_ready(self, host="127.0.0.1", port=6041, timeout=60):
        """Poll until taosadapter accepts TCP connections or timeout expires."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                s = socket.create_connection((host, port), timeout=2)
                s.close()
                tdLog.info(f"taosadapter ready on {host}:{port}")
                return
            except OSError:
                time.sleep(1)
        tdLog.info(f"WARNING: taosadapter not ready after {timeout}s, proceeding anyway")

    def insertData(self):
        """Insert test data via taosBenchmark using insertFormat.json."""
        json_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "json", "insertFormat.json"
        )
        self.benchmark(f"-f {json_file}")

    # -----------------------------------------------------------------------
    # Correctness comparison
    # -----------------------------------------------------------------------

    def _assertSame(self, sql1, sql2, label):
        """Assert that two SQL queries return identical results."""
        r1 = tdSql.getResult(sql1)
        r2 = tdSql.getResult(sql2)
        if r1 != r2:
            tdLog.exit(
                f"MISMATCH [{label}]\n"
                f"  sql1: {sql1}\n  -> {r1}\n"
                f"  sql2: {sql2}\n  -> {r2}"
            )
        tdLog.info(f"  [{label}] OK  ({r1[0][0] if r1 else 'empty'})")

    def _assertApprox(self, sql1, sql2, label, rel_tol=1e-4):
        """Assert two aggregate SQL queries agree within a relative tolerance.

        Used for FLOAT / DOUBLE SUM() comparisons where tiny rounding
        differences are acceptable (rel_tol=1e-4 ≈ 0.01 %).
        """
        r1 = tdSql.getResult(sql1)
        r2 = tdSql.getResult(sql2)
        v1 = r1[0][0] if r1 else None
        v2 = r2[0][0] if r2 else None
        if v1 is None and v2 is None:
            tdLog.info(f"  [{label}] ~OK  (both NULL)")
            return
        if v1 is None or v2 is None:
            tdLog.exit(f"MISMATCH [{label}]: one side is NULL: {v1!r} vs {v2!r}")
        diff = abs(float(v1) - float(v2))
        base = max(abs(float(v1)), abs(float(v2)), 1.0)
        if diff / base > rel_tol:
            tdLog.exit(
                f"MISMATCH [{label}]: {v1} vs {v2} "
                f"(rel_diff={diff/base:.2e} > tol={rel_tol})"
            )
        tdLog.info(f"  [{label}] ~OK  ({v1}, rel_diff={diff/base:.2e})")

    def _crcOf(self, sql, col_idx=0):
        """Fetch one column from *sql* and return CRC32 of its packed content.

        Each value is serialised as:
            NULL  → b'\\x00'
            bytes → raw bytes
            other → UTF-8 encoded string
        followed by the row separator b'\\xff\\xff' to prevent adjacent
        values from aliasing (e.g. "ab"+"c" ≠ "a"+"bc").

        The ORDER BY clause in *sql* must be identical for both source and
        destination so that row ordering is deterministic.
        """
        rows = tdSql.getResult(sql)
        buf = bytearray()
        for row in (rows or []):
            val = row[col_idx]
            if val is None:
                buf += b'\x00'
            elif isinstance(val, (bytes, bytearray)):
                buf += bytes(val)
            else:
                buf += str(val).encode('utf-8')
            buf += b'\xff\xff'   # row separator
        return zlib.crc32(bytes(buf)) & 0xFFFFFFFF

    def checkDbEqual(self, src_db, dst_db, label):
        """Full correctness verification: src_db vs dst_db for the meters STB."""
        tdLog.info(f"--- checkDbEqual: {src_db} vs {dst_db}  ({label}) ---")
        stb = STB

        # 1. Row count
        self._assertSame(
            f"SELECT COUNT(*) FROM {src_db}.{stb}",
            f"SELECT COUNT(*) FROM {dst_db}.{stb}",
            f"{label} COUNT(*)"
        )

        # 2. Child table count
        self._assertSame(
            f"SELECT COUNT(*) FROM information_schema.ins_tables "
            f"WHERE db_name='{src_db}' AND stable_name='{stb}'",
            f"SELECT COUNT(*) FROM information_schema.ins_tables "
            f"WHERE db_name='{dst_db}' AND stable_name='{stb}'",
            f"{label} CTB count"
        )

        # 3. SUM of integer columns (exact)
        for col in self._INT_COLS:
            self._assertSame(
                f"SELECT SUM({col}) FROM {src_db}.{stb}",
                f"SELECT SUM({col}) FROM {dst_db}.{stb}",
                f"{label} SUM({col})"
            )

        # 3b. SUM of float/double columns (approximate, rel_tol=1e-4)
        for col in self._FP_COLS:
            self._assertApprox(
                f"SELECT SUM({col}) FROM {src_db}.{stb}",
                f"SELECT SUM({col}) FROM {dst_db}.{stb}",
                f"{label} SUM~({col})"
            )

        # 4. SUM of decimal columns
        for col in self._DEC_COLS:
            self._assertSame(
                f"SELECT SUM({col}) FROM {src_db}.{stb}",
                f"SELECT SUM({col}) FROM {dst_db}.{stb}",
                f"{label} SUM({col})"
            )

        # 5. Bool column: count of TRUE values
        for col in self._BOOL_COLS:
            self._assertSame(
                f"SELECT SUM(CASE WHEN {col} THEN 1 ELSE 0 END) FROM {src_db}.{stb}",
                f"SELECT SUM(CASE WHEN {col} THEN 1 ELSE 0 END) FROM {dst_db}.{stb}",
                f"{label} SUM_BOOL({col})"
            )

        # 6. String / varbinary / varchar columns: full-content CRC32.
        #    Data is fetched in deterministic ORDER BY tbname, ts order and
        #    packed into a byte buffer; CRC32 checksums must match exactly.
        for col in self._VAR_COLS:
            crc1 = self._crcOf(
                f"SELECT {col} FROM {src_db}.{stb} ORDER BY tbname, ts"
            )
            crc2 = self._crcOf(
                f"SELECT {col} FROM {dst_db}.{stb} ORDER BY tbname, ts"
            )
            if crc1 != crc2:
                tdLog.exit(
                    f"MISMATCH [{label}] CRC data col '{col}': "
                    f"{crc1:#010x} vs {crc2:#010x}"
                )
            tdLog.info(f"  [{label}] CRC({col}) OK  (crc={crc1:#010x})")

        # 7. SUM of integer tags (exact) + float/double tags (approximate)
        for tag in self._INT_TAGS:
            self._assertSame(
                f"SELECT SUM({tag}) FROM {src_db}.{stb}",
                f"SELECT SUM({tag}) FROM {dst_db}.{stb}",
                f"{label} SUM(tag:{tag})"
            )
        for tag in self._FP_TAGS:
            self._assertApprox(
                f"SELECT SUM({tag}) FROM {src_db}.{stb}",
                f"SELECT SUM({tag}) FROM {dst_db}.{stb}",
                f"{label} SUM~(tag:{tag})"
            )

        # 8. Bool tag
        for tag in self._BOOL_TAGS:
            self._assertSame(
                f"SELECT SUM(CASE WHEN {tag} THEN 1 ELSE 0 END) FROM {src_db}.{stb}",
                f"SELECT SUM(CASE WHEN {tag} THEN 1 ELSE 0 END) FROM {dst_db}.{stb}",
                f"{label} SUM_BOOL(tag:{tag})"
            )

        # 9. Var / geometry tags: full-content CRC32 per child table.
        #    DISTINCT tbname + tag, ordered by tbname, so each CTB contributes
        #    exactly one entry regardless of row count.
        for tag in self._VAR_TAGS:
            crc1 = self._crcOf(
                f"SELECT DISTINCT tbname, {tag} FROM {src_db}.{stb} ORDER BY tbname",
                col_idx=1
            )
            crc2 = self._crcOf(
                f"SELECT DISTINCT tbname, {tag} FROM {dst_db}.{stb} ORDER BY tbname",
                col_idx=1
            )
            if crc1 != crc2:
                tdLog.exit(
                    f"MISMATCH [{label}] CRC tag '{tag}': "
                    f"{crc1:#010x} vs {crc2:#010x}"
                )
            tdLog.info(f"  [{label}] CRC(tag:{tag}) OK  (crc={crc1:#010x})")

        # 10. Per-CTB tag snapshot (20 rows × 4 int tags); ORDER BY guarantees
        #     deterministic comparison.
        snap_cols = ", ".join(self._SNAP_TAGS)
        self._assertSame(
            f"SELECT DISTINCT tbname, {snap_cols} FROM {src_db}.{stb} ORDER BY tbname",
            f"SELECT DISTINCT tbname, {snap_cols} FROM {dst_db}.{stb} ORDER BY tbname",
            f"{label} tag_snapshot"
        )

        tdLog.info(f"--- checkDbEqual PASSED: {label} ---")

    # -----------------------------------------------------------------------
    # Single-format test: backup once, restore with STMT1 and STMT2
    # -----------------------------------------------------------------------

    def doFormatTest(self, fmt, tmpdir):
        """Backup SRC_DB in `fmt` format, then restore twice (-v 1 and -v 2).

        Args:
            fmt:    "binary" or "parquet"
            tmpdir: directory used for the backup files
        """
        self.makeDir(tmpdir)

        short = fmt[:3]  # "bin" or "par"

        # -- Backup ----------------------------------------------------------
        rlist = etool.taosbackup(f"-F {fmt} -D {SRC_DB} -o {tmpdir}")
        output = "\n".join(rlist) if rlist else ""
        if "SUCCESS" not in output:
            tdLog.exit(f"Backup ({fmt}) failed:\n{output[:600]}")
        tdLog.info(f"backup ({fmt}) SUCCESS")

        # -- Restore STMT1 ---------------------------------------------------
        dst_v1 = f"fmt_{short}_v1"
        tdSql.execute(f"drop database if exists {dst_v1}")
        rlist1 = etool.taosbackup(
            f'-v 1 -W "{SRC_DB}={dst_v1}" -i {tmpdir}'
        )
        out1 = "\n".join(rlist1) if rlist1 else ""
        if "SUCCESS" not in out1:
            tdLog.exit(f"Restore ({fmt}/STMT1 -> {dst_v1}) failed:\n{out1[:600]}")
        tdLog.info(f"restore ({fmt}/STMT1) SUCCESS")
        self.checkDbEqual(SRC_DB, dst_v1, f"{fmt}/STMT1")

        # -- Restore STMT2 ---------------------------------------------------
        dst_v2 = f"fmt_{short}_v2"
        tdSql.execute(f"drop database if exists {dst_v2}")
        rlist2 = etool.taosbackup(
            f'-v 2 -W "{SRC_DB}={dst_v2}" -i {tmpdir}'
        )
        out2 = "\n".join(rlist2) if rlist2 else ""
        if "SUCCESS" not in out2:
            tdLog.exit(f"Restore ({fmt}/STMT2 -> {dst_v2}) failed:\n{out2[:600]}")
        tdLog.info(f"restore ({fmt}/STMT2) SUCCESS")
        self.checkDbEqual(SRC_DB, dst_v2, f"{fmt}/STMT2")

        tdLog.info(f"doFormatTest({fmt}) .......................... [passed]")

    # -----------------------------------------------------------------------
    # Write-buffer overflow – covers storageTaos.c lines 52-66
    # -----------------------------------------------------------------------

    def do_binary_buffer_overflow_test(self):
        """Trigger writeTaosFile large-write and buffer-overflow paths.

        TAOS_FILE_WRITE_BUF_SIZE = 4 MB.  For each CTB file the write buffer
        starts with the small header + schema (a few hundred bytes).  When the
        first compressed data-block arrives it is either:

          • larger than the cap  → 'len >= writeBufCap' branch (lines 52-60):
              flushWriteBuffer() is called first (writeBufPos > 0 → the header
              bytes are flushed, exercising lines 25/29/31), then the block is
              written directly to disk.
          • such that accumulated writes exceed the cap → 'writeBufPos + len >
              writeBufCap' branch (lines 64-66).

        We create a single CTB with binary(2000) filled with SHA-256-based hex
        strings (32 independent segments per row).  SHA-256 hex output is
        pseudo-random and incompressible by LZ4, so the stored block size is
        essentially equal to the raw size:

            4096 rows × 2000 bytes ≈ 8 MB  >>  TAOS_FILE_WRITE_BUF_SIZE (4 MB)

        That makes each fetched batch trigger the 'len >= cap' branch.
        Before the first large write, writeBufPos holds the header/schema bytes
        (> 0), so flushWriteBuffer() performs an actual disk write and the
        'taosFile->writeBufPos = 0' reset on line 29 is hit.
        """
        db = "fmt_buf_ovflow"
        dst = "fmt_buf_ovflow_r"
        tmpdir = "./taosbackuptest/tmpdir_buf_overflow"
        self.makeDir(tmpdir)

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"create table {db}.t1 (ts timestamp, data binary(2000))")

        # Build 5 000 rows each with 2 000 bytes of pseudo-random hex data.
        # Using 32 _different_ SHA-256 digests per row prevents LZ4 from
        # finding any repeating patterns across the column.
        ROWS = 5000
        BASE_TS = 1640000000000
        BATCH = 200
        for batch_start in range(0, ROWS, BATCH):
            vals = []
            for i in range(batch_start, min(batch_start + BATCH, ROWS)):
                segments = [
                    hashlib.sha256(f"r{i}s{j}".encode()).hexdigest()
                    for j in range(32)
                ]
                data = "".join(segments)[:1999]
                vals.append(f"({BASE_TS + i * 1000}, '{data}')")
            tdSql.execute(f"insert into {db}.t1 values{','.join(vals)}")

        # Binary backup: the first 4096-row block is ~8 MB compressed
        # (incompressible SHA-256 data), which is > the 4 MB write buffer cap.
        rlist = etool.taosbackup(f"-F binary -D {db} -o {tmpdir}")
        out = "\n".join(rlist) if rlist else ""
        if "SUCCESS" not in out:
            tdLog.exit(f"binary backup (buffer overflow test) failed:\n{out[:400]}")
        tdLog.info("buffer-overflow binary backup SUCCESS")

        # Restore and verify row count is preserved exactly.
        tdSql.execute(f"drop database if exists {dst}")
        rlist2 = etool.taosbackup(f'-F binary -v 2 -W "{db}={dst}" -i {tmpdir}')
        out2 = "\n".join(rlist2) if rlist2 else ""
        if "SUCCESS" not in out2:
            tdLog.exit(f"restore (buffer overflow test) failed:\n{out2[:400]}")
        tdSql.query(f"select count(*) from {dst}.t1")
        tdSql.checkData(0, 0, ROWS)

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"drop database if exists {dst}")
        tdLog.info("do_binary_buffer_overflow_test .............. [passed]")

    # -----------------------------------------------------------------------
    # Main test entry point
    # -----------------------------------------------------------------------

    def test_taosbackup_format(self):
        """taosBackup -F binary / -F parquet × -v 1 / -v 2 correctness

        1.  Insert a rich multi-type dataset (bool, float, double, tinyint,
            smallint, int, bigint, unsigned variants, binary, nchar, varbinary,
            varchar, decimal(10,6), decimal(24,10)) plus geometry tags via
            taosBenchmark / insertFormat.json.
        2.  Backup SRC_DB in binary format.
        3.  Restore with STMT1 (-v 1) → fmt_bin_v1; verify correctness.
        4.  Restore with STMT2 (-v 2) → fmt_bin_v2; verify correctness.
        5.  Backup SRC_DB in parquet format.
        6.  Restore with STMT1 (-v 1) → fmt_par_v1; verify correctness.
        7.  Restore with STMT2 (-v 2) → fmt_par_v2; verify correctness.
        8.  Binary backup of a wide-binary CTB (binary(2000) × 5000 rows of
            pseudo-random SHA-256 data) to exercise the 4 MB write-buffer
            overflow paths in storageTaos.c (lines 52-60, 64-66).

        Verification per restore:
            · COUNT(*)  matches source
            · SUM of every numeric / decimal column matches source
            · SUM(CASE WHEN bool_col …) matches source
            · COUNT of every string / binary / var column matches source
            · DISTINCT per-CTB tag snapshot matches source

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-07 Created; validates both binary and parquet formats
              against STMT1 and STMT2 restore paths.
        """
        tdLog.info("=== test_taosbackup_format START ===")

        # Ensure taosadapter is accepting connections before any backup attempt.
        # The prior except test class kills/restarts adapter; without this guard
        # the backup hangs indefinitely on a dead adapter (connection pool spin).
        self._wait_adapter_ready()

        # Step 1 – generate data
        tdLog.info("Step 1: insert data via taosBenchmark")
        self.insertData()

        # Verify source data is present
        tdSql.query(f"SELECT COUNT(*) FROM {SRC_DB}.{STB}")
        src_rows = tdSql.getData(0, 0)
        if src_rows == 0:
            tdLog.exit("No data in source DB after benchmark insert")
        tdLog.info(f"Source rows: {src_rows}")

        # Step 2-4 – binary format
        tdLog.info("Step 2-4: binary format")
        self.doFormatTest("binary",  "./taosbackuptest/tmpdir_fmt_binary")

        # Step 5-7 – parquet format
        tdLog.info("Step 5-7: parquet format")
        self.doFormatTest("parquet", "./taosbackuptest/tmpdir_fmt_parquet")

        # Step 8 – large binary column triggers writeTaosFile buffer-overflow path
        tdLog.info("Step 8: binary buffer overflow path in storageTaos.c")
        self.do_binary_buffer_overflow_test()

        tdLog.info("test_taosbackup_format ...................... [passed]")

    # -----------------------------------------------------------------------
    # Parquet-specific: NULL tag values and TIMESTAMP tag type
    # -----------------------------------------------------------------------

    def test_parquet_null_and_timestamp_tags(self):
        """taosBackup parquet NULL and TIMESTAMP tag restore

        The parquet tag-restore path (restoreMeta.c) has dedicated branches for:
          - isNull  → emit literal NULL in the CREATE TABLE … TAGS() SQL
          - TSDB_DATA_TYPE_TIMESTAMP → emit raw int64 epoch value

        These branches are distinct from the binary-format path covered by
        test_taosbackup_coverage_extra.py, so a parquet-specific test is needed.

        Test matrix: one parquet backup → restore with STMT1 (-v 1) and STMT2 (-v 2).

        Branches covered (restoreMeta.c – parquet tag restore thread):
            L~1628  if (isNull) { … snprintf(…, "NULL"); continue; }
            L~1680  case TSDB_DATA_TYPE_TIMESTAMP: snprintf(…, PRId64, …);

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-26 Alex Duan Created; parquet-format NULL/TIMESTAMP tag tests

        """
        tdLog.info("=== test_parquet_null_and_timestamp_tags START ===")

        src_db = "fmt_parq_tags_src"
        dst_v1 = "fmt_parq_tags_v1"
        dst_v2 = "fmt_parq_tags_v2"
        stb    = "sensors"
        tmpdir = "./taosbackuptest/tmpdir_parq_tags"
        self.makeDir(tmpdir)

        # ---- setup ---------------------------------------------------------
        tdLog.info("Step 1: create STB with TIMESTAMP tag and nullable INT tag")
        tdSql.execute(f"drop database if exists {src_db}")
        tdSql.execute(
            f"create database {src_db} vgroups 1 replica 1 precision 'ms'"
        )
        tdSql.execute(
            f"create stable {src_db}.{stb} "
            f"(ts timestamp, v float) "
            f"tags (tid int, created_at timestamp, name varchar(20))"
        )
        # s0: all tags non-NULL including TIMESTAMP tag
        tdSql.execute(
            f"create table {src_db}.s0 using {src_db}.{stb} "
            f"tags(1, '2024-01-01 00:00:00.000', 'sensor_A')"
        )
        tdSql.execute(f"insert into {src_db}.s0 values(now(), 1.1)")
        # s1: NULL int tag  → exercises isNull branch
        tdSql.execute(
            f"create table {src_db}.s1 using {src_db}.{stb} "
            f"tags(NULL, '2024-06-01 12:00:00.000', 'sensor_B')"
        )
        tdSql.execute(f"insert into {src_db}.s1 values(now()+1s, 2.2)")
        # s2: NULL timestamp tag  → exercises isNull for TIMESTAMP type
        tdSql.execute(
            f"create table {src_db}.s2 using {src_db}.{stb} "
            f"tags(3, NULL, 'sensor_C')"
        )
        tdSql.execute(f"insert into {src_db}.s2 values(now()+2s, 3.3)")

        # ---- backup (parquet) ----------------------------------------------
        tdLog.info("Step 2: backup with -F parquet")
        rlist = etool.taosbackup(f"-F parquet -D {src_db} -o {tmpdir}")
        output = "\n".join(rlist) if rlist else ""
        if "SUCCESS" not in output:
            tdLog.exit(f"Backup (parquet) failed:\n{output[:600]}")
        tdLog.info("backup (parquet) SUCCESS")

        # ---- restore STMT1 -------------------------------------------------
        tdLog.info("Step 3: restore with STMT1 (-v 1)")
        tdSql.execute(f"drop database if exists {dst_v1}")
        rlist = etool.taosbackup(f'-v 1 -W "{src_db}={dst_v1}" -i {tmpdir}')
        out1 = "\n".join(rlist) if rlist else ""
        if "SUCCESS" not in out1:
            tdLog.exit(f"Restore (parquet/STMT1 → {dst_v1}) failed:\n{out1[:600]}")
        tdLog.info(f"restore (parquet/STMT1) SUCCESS")

        # verify row count
        tdSql.query(f"select count(*) from {dst_v1}.{stb}")
        if tdSql.getData(0, 0) != 3:
            tdLog.exit(f"[STMT1] row count mismatch, expected 3")

        # verify NULL int tag preserved (s1.tid must be NULL)
        tdSql.query(f"select tid from {dst_v1}.s1")
        val = tdSql.getData(0, 0)
        if val is not None:
            tdLog.exit(f"[STMT1] s1.tid should be NULL after parquet restore, got {val!r}")

        # verify TIMESTAMP tag preserved for s0 (non-NULL)
        tdSql.query(f"select created_at from {dst_v1}.s0")
        val = tdSql.getData(0, 0)
        if val is None:
            tdLog.exit(f"[STMT1] s0.created_at should not be NULL after parquet restore")

        # verify NULL TIMESTAMP tag preserved for s2
        tdSql.query(f"select created_at from {dst_v1}.s2")
        val = tdSql.getData(0, 0)
        if val is not None:
            tdLog.exit(f"[STMT1] s2.created_at should be NULL after parquet restore, got {val!r}")

        tdLog.info("parquet NULL/TIMESTAMP tags STMT1 PASSED")

        # ---- restore STMT2 -------------------------------------------------
        tdLog.info("Step 4: restore with STMT2 (-v 2)")
        tdSql.execute(f"drop database if exists {dst_v2}")
        rlist = etool.taosbackup(f'-v 2 -W "{src_db}={dst_v2}" -i {tmpdir}')
        out2 = "\n".join(rlist) if rlist else ""
        if "SUCCESS" not in out2:
            tdLog.exit(f"Restore (parquet/STMT2 → {dst_v2}) failed:\n{out2[:600]}")
        tdLog.info(f"restore (parquet/STMT2) SUCCESS")

        # verify row count
        tdSql.query(f"select count(*) from {dst_v2}.{stb}")
        if tdSql.getData(0, 0) != 3:
            tdLog.exit(f"[STMT2] row count mismatch, expected 3")

        # verify NULL int tag preserved
        tdSql.query(f"select tid from {dst_v2}.s1")
        val = tdSql.getData(0, 0)
        if val is not None:
            tdLog.exit(f"[STMT2] s1.tid should be NULL after parquet restore, got {val!r}")

        # verify TIMESTAMP tag preserved
        tdSql.query(f"select created_at from {dst_v2}.s0")
        val = tdSql.getData(0, 0)
        if val is None:
            tdLog.exit(f"[STMT2] s0.created_at should not be NULL after parquet restore")

        # verify NULL TIMESTAMP tag preserved
        tdSql.query(f"select created_at from {dst_v2}.s2")
        val = tdSql.getData(0, 0)
        if val is not None:
            tdLog.exit(f"[STMT2] s2.created_at should be NULL after parquet restore, got {val!r}")

        tdLog.info("parquet NULL/TIMESTAMP tags STMT2 PASSED")

        tdLog.info("test_parquet_null_and_timestamp_tags ........ [passed]")
