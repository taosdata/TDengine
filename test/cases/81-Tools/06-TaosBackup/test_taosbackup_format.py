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

        tdLog.info("test_taosbackup_format ...................... [passed]")
