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
test_taosbackup_allnull.py

Tests the block-version-2 all-NULL column skip optimisation (COL_LEN_ALL_NULL).

Schema design
─────────────
Super table  : stb_alltype
Data columns : one of every fixed type and one of every variable type that
               taosBackup supports:

  Dense  columns (value in EVERY row  → no all-NULL blocks):
    c_bool      BOOL
    c_int       INT
    c_float     FLOAT
    c_usmallint SMALLINT UNSIGNED
    c_ubigint   BIGINT UNSIGNED
    c_varchar   VARCHAR(64)

  Sparse columns (value only in rows 0 .. BLOCK_SIZE-1 per child table;
                  all rows [BLOCK_SIZE, NROWS) are NULL):
    c_tinyint   TINYINT
    c_smallint  SMALLINT
    c_bigint    BIGINT
    c_double    DOUBLE
    c_utinyint  TINYINT UNSIGNED
    c_uint      INT UNSIGNED
    c_nchar     NCHAR(32)

Tag: gid INT

Why "sparse" triggers the optimisation
───────────────────────────────────────
taosBackup fetches raw blocks of BLOCK_SIZE (4096) rows per child table.
For a sparse column, every batch from index-1 onward (50000/4096 - 1 ≈ 11
batches) contains 4096 consecutive NULL rows.  Those batches trigger the
COL_LEN_ALL_NULL sentinel path in compressBlock() for every non-VAR sparse
column, saving the compressed null-bitmap + zero-payload.

Verification
────────────
For every data column we compare COUNT(col), MIN(col), MAX(col) between
the source DB and the restored DB.  COUNT(*) total is also verified.
"""

import os
import random
import subprocess

from new_test_framework.utils import tdLog, tdSql, etool

# ─── tunables ─────────────────────────────────────────────────────────────────
NTABLES    = 2        # child tables
NROWS      = 50_000   # rows per child table
BLOCK_SIZE = 4_096    # taosBackup raw-block size (rows per fetch)
BATCH      = 1_000    # SQL INSERT batch size
_SEED      = 20260308

# columns that have a value in every row (no all-NULL blocks)
DENSE_COLS = [
    # (col_name, SQL_type)
    ("c_bool",      "BOOL"),
    ("c_int",       "INT"),
    ("c_float",     "FLOAT"),
    ("c_usmallint", "SMALLINT UNSIGNED"),
    ("c_ubigint",   "BIGINT UNSIGNED"),
    ("c_varchar",   "VARCHAR(64)"),
]

# columns NULL for rows >= BLOCK_SIZE  → blocks 1..11 are all-NULL per table
SPARSE_COLS = [
    ("c_tinyint",   "TINYINT"),
    ("c_smallint",  "SMALLINT"),
    ("c_bigint",    "BIGINT"),
    ("c_double",    "DOUBLE"),
    ("c_utinyint",  "TINYINT UNSIGNED"),
    ("c_uint",      "INT UNSIGNED"),
    ("c_nchar",     "NCHAR(32)"),
]

ALL_DATA_COLS = DENSE_COLS + SPARSE_COLS   # 13 columns, excluding ts

# TDengine does not support MIN/MAX on these types
_NO_MINMAX = {"BOOL", "VARCHAR", "NCHAR"}


def no_minmax(col_type: str) -> bool:
    """Return True if MIN/MAX is not applicable for col_type."""
    return col_type.split("(")[0].upper().strip() in _NO_MINMAX


STB = "stb_alltype"
SRC = "tc_allnull_src"
DST = "tc_allnull_dst"


class TestTaosBackupAllNull:

    # ──────────────────────────────────────────────────────────────────────────
    # Low-level helpers
    # ──────────────────────────────────────────────────────────────────────────

    def exec(self, command: str):
        """Run a shell command; fail the test on non-zero exit."""
        tdLog.info(command)
        result = subprocess.run(
            command, shell=True, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        )
        if result.stdout:
            for line in result.stdout.splitlines():
                tdLog.info(line)
        if result.returncode != 0:
            tdLog.exit(f"Command failed (rc={result.returncode}): {command}")

    def mkdir(self, path: str):
        if os.path.exists(path):
            os.system(f"rm -rf {path}")
        os.makedirs(path)

    def rmdir(self, path: str):
        if os.path.exists(path):
            os.system(f"rm -rf {path}")

    # ──────────────────────────────────────────────────────────────────────────
    # Value generators  (return SQL literal strings, never NULL)
    # ──────────────────────────────────────────────────────────────────────────

    def gen_value(self, col: str, col_type: str, row: int, tid: int,
                   rng: random.Random) -> str:
        ct = col_type.split("(")[0].upper().strip()
        if ct == "BOOL":
            return str(row % 2)
        if ct == "TINYINT":
            return str(rng.randint(-127, 127))
        if ct == "SMALLINT":
            return str(rng.randint(-32767, 32767))
        if ct == "INT":
            return str(rng.randint(-2_000_000_000, 2_000_000_000))
        if ct == "BIGINT":
            return str(rng.randint(-2**62, 2**62))
        if ct == "FLOAT":
            return f"{rng.uniform(-1e6, 1e6):.4f}"
        if ct == "DOUBLE":
            return f"{rng.uniform(-1e12, 1e12):.8f}"
        if ct == "TINYINT UNSIGNED":
            return str(rng.randint(0, 254))
        if ct == "SMALLINT UNSIGNED":
            return str(rng.randint(0, 65534))
        if ct == "INT UNSIGNED":
            return str(rng.randint(0, 4_000_000_000))
        if ct == "BIGINT UNSIGNED":
            return str(rng.randint(0, 2**63))
        if ct == "VARCHAR":
            return f"'v{tid}_{row}'"
        if ct == "NCHAR":
            return f"'中文{tid}_{row}'"
        tdLog.exit(f"Unknown type '{col_type}' for column '{col}'")

    # ──────────────────────────────────────────────────────────────────────────
    # Source DB build
    # ──────────────────────────────────────────────────────────────────────────

    def create_source_db(self):
        """
        Create SRC database with stb_alltype and insert NTABLES × NROWS rows.
        For each sparse column, only rows 0 .. BLOCK_SIZE-1 carry a value;
        rows BLOCK_SIZE .. NROWS-1 are inserted as NULL.
        This guarantees ≈11 fully all-NULL raw blocks per sparse column per table.
        """
        tdSql.execute(f"DROP DATABASE IF EXISTS {SRC}")
        tdSql.execute(f"CREATE DATABASE {SRC} VGROUPS 2 PRECISION 'ms'")
        tdSql.execute(f"USE {SRC}")

        col_defs = ",\n    ".join(
            f"{name} {typ}" for name, typ in ALL_DATA_COLS
        )
        tdSql.execute(
            f"CREATE STABLE {STB} (\n"
            f"    ts TIMESTAMP,\n"
            f"    {col_defs}\n"
            f") TAGS (gid INT)"
        )

        sparse_names = {sc for sc, _ in SPARSE_COLS}
        rng = random.Random(_SEED)

        for tid in range(NTABLES):
            tbname = f"t{tid}"
            tdSql.execute(f"CREATE TABLE {tbname} USING {STB} TAGS ({tid})")
            base_ts = 1_700_000_000_000 + tid * NROWS * 1_000
            batch_rows = []

            for r in range(NROWS):
                ts = base_ts + r * 1_000
                col_vals = []
                for col, typ in ALL_DATA_COLS:
                    if col in sparse_names and r >= BLOCK_SIZE:
                        col_vals.append("NULL")
                    else:
                        col_vals.append(
                            self.gen_value(col, typ, r, tid, rng)
                        )
                batch_rows.append(f"({ts}, {', '.join(col_vals)})")

                if len(batch_rows) == BATCH or r == NROWS - 1:
                    tdSql.execute(
                        f"INSERT INTO {tbname} VALUES {', '.join(batch_rows)}"
                    )
                    batch_rows = []

        total = NTABLES * NROWS
        tdSql.query(f"SELECT COUNT(*) FROM {SRC}.{STB}")
        tdSql.checkData(0, 0, total)

        null_blocks = NTABLES * (NROWS // BLOCK_SIZE - 1)
        tdLog.info(
            f"Source DB created: {total} rows | "
            f"{len(SPARSE_COLS)} sparse cols × ≈{null_blocks} all-NULL blocks"
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Pre-backup sanity: confirm all-NULL blocks really exist
    # ──────────────────────────────────────────────────────────────────────────

    def verify_allnull_blocks_present(self):
        """
        For each child table, query the rows beyond BLOCK_SIZE for sparse
        columns and verify that COUNT(col) == 0 (all NULL).
        """
        for tid in range(NTABLES):
            base_ts  = 1_700_000_000_000 + tid * NROWS * 1_000
            start_ts = base_ts + BLOCK_SIZE * 1_000
            end_ts   = base_ts + NROWS * 1_000 - 1
            for col, _ in SPARSE_COLS:
                tdSql.query(
                    f"SELECT COUNT({col}) FROM {SRC}.t{tid} "
                    f"WHERE ts >= {start_ts} AND ts <= {end_ts}"
                )
                tdSql.checkData(0, 0, 0)

        tdLog.info(
            f"Pre-backup check: rows [{BLOCK_SIZE}, {NROWS}) are all-NULL "
            f"for every sparse column in every child table"
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Aggregate snapshot
    # ──────────────────────────────────────────────────────────────────────────

    def snapshot(self, db: str) -> dict:
        """
        Return dict:
          '__total__'  → total row count
          col_name     → (count, min, max)  for each data column
        """
        snap = {}
        tdSql.query(f"SELECT COUNT(*) FROM {db}.{STB}")
        snap["__total__"] = tdSql.queryResult[0][0]

        for col, typ in ALL_DATA_COLS:
            if no_minmax(typ):
                tdSql.query(f"SELECT COUNT({col}) FROM {db}.{STB}")
                snap[col] = (tdSql.queryResult[0][0], None, None)
            else:
                tdSql.query(
                    f"SELECT COUNT({col}), MIN({col}), MAX({col})"
                    f" FROM {db}.{STB}"
                )
                row = tdSql.queryResult[0]
                snap[col] = (row[0], row[1], row[2])

        return snap

    def compare_snapshots(self, snap_src: dict, snap_dst: dict):
        """
        Assert every aggregate in snap_src equals snap_dst.
        Calls tdLog.exit on the first mismatch found.
        """
        if snap_src["__total__"] != snap_dst["__total__"]:
            tdLog.exit(
                f"Total row-count mismatch: "
                f"src={snap_src['__total__']} dst={snap_dst['__total__']}"
            )

        for col, typ in ALL_DATA_COLS:
            cnt_s, mn_s, mx_s = snap_src[col]
            cnt_d, mn_d, mx_d = snap_dst[col]
            if cnt_s != cnt_d:
                tdLog.exit(
                    f"COUNT mismatch on {col}: src={cnt_s} dst={cnt_d}"
                )
            if not no_minmax(typ):
                if mn_s != mn_d:
                    tdLog.exit(
                        f"MIN mismatch on {col}: src={mn_s} dst={mn_d}"
                    )
                if mx_s != mx_d:
                    tdLog.exit(
                        f"MAX mismatch on {col}: src={mx_s} dst={mx_d}"
                    )

        tdLog.info(
            f"Snapshot OK: {len(ALL_DATA_COLS)} columns × "
            "(COUNT, MIN, MAX) all match"
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Core scenario
    # ──────────────────────────────────────────────────────────────────────────

    def do_allnull_alltype(self):
        """
        1. Build source DB (all taosBackup-supported types, 2 tables × 50K rows).
        2. Confirm all-NULL blocks are present in sparse columns.
        3. Snapshot source aggregates (COUNT / MIN / MAX per column).
        4. Binary backup → restore to DST DB.
        5. Snapshot DST aggregates and compare with source – must match exactly.
        """
        binPath = etool.taosBackupFile()
        if not binPath:
            tdLog.exit("taosBackup binary not found")

        outdir = "./taosbackuptest/allnull_alltype"
        self.mkdir(outdir)

        # ── 1. build source DB ───────────────────────────────────────────────
        self.create_source_db()

        # ── 2. confirm all-NULL blocks exist ─────────────────────────────────
        self.verify_allnull_blocks_present()

        # ── 3. sanity-check expected counts ──────────────────────────────────
        snap_src = self.snapshot(SRC)

        expected_dense  = NROWS * NTABLES
        expected_sparse = BLOCK_SIZE * NTABLES
        for col, _ in DENSE_COLS:
            if snap_src[col][0] != expected_dense:
                tdLog.exit(
                    f"Dense column {col}: COUNT={snap_src[col][0]}, "
                    f"expected {expected_dense}"
                )
        for col, _ in SPARSE_COLS:
            if snap_src[col][0] != expected_sparse:
                tdLog.exit(
                    f"Sparse column {col}: COUNT={snap_src[col][0]}, "
                    f"expected {expected_sparse}"
                )
        tdLog.info(
            f"Count check: dense={expected_dense}, sparse={expected_sparse} ✓"
        )

        # ── 4. backup ────────────────────────────────────────────────────────
        self.exec(f"{binPath} -D {SRC} -F binary -o {outdir} -T 4")

        # ── 5. restore ───────────────────────────────────────────────────────
        tdSql.execute(f"DROP DATABASE IF EXISTS {DST}")
        self.exec(f"{binPath} -i {outdir} -W {SRC}={DST} -T 4")

        # ── 6. compare ───────────────────────────────────────────────────────
        snap_dst = self.snapshot(DST)
        self.compare_snapshots(snap_src, snap_dst)
        
        # Cleanup
        self.rmdir(outdir)

        tdLog.info("do_allnull_alltype ......................... [passed]")

    # ──────────────────────────────────────────────────────────────────────────
    # Entry point
    # ──────────────────────────────────────────────────────────────────────────

    def test_taosbackup_allnull(self):
        """
        Test: taosBackup all-NULL column skip optimisation — all supported types

        Creates a super table with one column of every type supported by
        taosBackup.  Sparse columns carry values only in the first BLOCK_SIZE
        (4096) rows per child table; subsequent raw-block fetches return
        fully-NULL columns, triggering the COL_LEN_ALL_NULL sentinel path
        (block format version 2) for every non-VAR sparse column.

        Setup : 2 child tables × 50 000 rows = 100 000 total rows
        Verify: COUNT / MIN / MAX for every column match between the source
                and the restored database.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-08 Initial implementation for COL_LEN_ALL_NULL optimisation
        """
        self.do_allnull_alltype()

