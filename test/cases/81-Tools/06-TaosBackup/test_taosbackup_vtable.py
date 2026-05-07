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
test_taosbackup_vtable.py

Full backup/restore test for all three virtual table types supported by taosBackup.

Virtual table types covered (per 05-virtualtable.md)
-----------------------------------------------------
1. Virtual normal table
   CREATE VTABLE vtb_normal (ts TIMESTAMP,
                              vc1 FLOAT FROM SRC_DB.phy_ctb1.c1,
                              vc2 INT   FROM SRC_DB.phy_ctb1.c2)

2. Virtual super table (VSTB)
   CREATE STABLE vstb (ts TIMESTAMP, vc1 FLOAT, vc2 INT)
          TAGS (vid INT, vname VARCHAR(32)) VIRTUAL 1

   Two TAG columns intentionally exercise the columnar (binary) tag
   storage introduced in Phase 2 of the vtable feature.

3. Virtual child tables
   CREATE VTABLE vtb_child1 (vc1 FROM SRC_DB.phy_ctb1.c1,
                              vc2 FROM SRC_DB.phy_ctb1.c2)
          USING vstb TAGS(10, 'alice')
   CREATE VTABLE vtb_child2 (vc1 FROM SRC_DB.phy_ctb2.c1,
                              vc2 FROM SRC_DB.phy_ctb2.c2)
          USING vstb TAGS(20, 'bob')

Physical database layout (SRC_DB)
----------------------------------
  phy_stb   - physical super table  (ts TIMESTAMP, c1 FLOAT, c2 INT)  TAGS (gid INT)
  phy_ctb1  - physical child table  (USING phy_stb TAGS(1))
  phy_ctb2  - physical child table  (USING phy_stb TAGS(2))
  phy_ntb   - physical normal table (ts TIMESTAMP, c1 FLOAT, c2 INT)

Test procedure
--------------
1. Create SRC_DB: physical tables + all three vtable types; insert rows.
2. Snapshot source data: query each virtual table and save results.
3. Full backup of SRC_DB (schema + data, no -s flag).
4. Verify backup artifacts:
   a. vtags/ directory and vstb_data1.{dat|par} (columnar tag binary) exist.
   b. vtb.sql skeleton lines for child tables have NO inline TAGS (tags are
      stored separately in the vtags binary file).
5. Restore into DST_DB (rename: -W SRC_DB=DST_DB).
6. Verify schema in DST_DB:
   a. Virtual super table (vstb) exists.
   b. 1 virtual normal table and 2 virtual child tables exist.
   c. Total virtual table count matches source.
   d. All expected table names (vtb_normal, vtb_child1, vtb_child2) exist.
   e. Child table TAG values are correct (vid=10/vname=alice, vid=20/vname=bob).
7. Verify data in DST_DB: row-by-row comparison for every virtual table
   against the snapshot captured in step 2 -- every column, every row must
   match exactly.
"""

import os
import glob
import subprocess

from new_test_framework.utils import tdLog, tdSql, etool

# ---------------------------------------------------------------------------
# Database / table name constants
# ---------------------------------------------------------------------------
SRC_DB = "tc_vtable_src"
DST_DB = "tc_vtable_dst"

PHY_STB  = "phy_stb"
PHY_CTB1 = "phy_ctb1"
PHY_CTB2 = "phy_ctb2"
PHY_NTB  = "phy_ntb"

VSTB       = "vstb"
VTB_NORMAL = "vtb_normal"
VTB_CHILD1 = "vtb_child1"
VTB_CHILD2 = "vtb_child2"


class TestTaosBackupVtable:

    # -----------------------------------------------------------------------
    # Shell / filesystem helpers
    # -----------------------------------------------------------------------

    def exec(self, command: str) -> str:
        """Run a shell command; terminate the test on non-zero exit."""
        tdLog.info(command)
        result = subprocess.run(
            command, shell=True, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        if result.returncode != 0:
            tdLog.exit(
                f"Command failed (exit {result.returncode}): {command}\n"
                f"{result.stdout}"
            )
        return result.stdout

    def mkdir(self, path: str):
        os.makedirs(path, exist_ok=True)

    def rmdir(self, path: str):
        import shutil
        if os.path.exists(path):
            shutil.rmtree(path)

    # -----------------------------------------------------------------------
    # Database setup
    # -----------------------------------------------------------------------

    def create_source_db(self):
        """
        Create SRC_DB with physical tables, insert rows, then create all
        three virtual table types on top of the physical data.
        """
        tdSql.execute(f"DROP DATABASE IF EXISTS {SRC_DB}")
        tdSql.execute(f"CREATE DATABASE {SRC_DB} VGROUPS 1 PRECISION 'ms'")
        tdSql.execute(f"USE {SRC_DB}")

        # -- Physical super table + child tables + normal table --------------
        tdSql.execute(
            f"CREATE STABLE {PHY_STB} "
            f"(ts TIMESTAMP, c1 FLOAT, c2 INT) TAGS (gid INT)"
        )
        tdSql.execute(f"CREATE TABLE {PHY_CTB1} USING {PHY_STB} TAGS(1)")
        tdSql.execute(f"CREATE TABLE {PHY_CTB2} USING {PHY_STB} TAGS(2)")
        tdSql.execute(
            f"CREATE TABLE {PHY_NTB} (ts TIMESTAMP, c1 FLOAT, c2 INT)"
        )

        # Insert rows so virtual columns have a valid data source.
        # Timestamps are fixed so source vs restored comparisons are
        # deterministic regardless of wall-clock time.
        tdSql.execute(
            f"INSERT INTO {PHY_CTB1} VALUES "
            f"('2025-01-01 00:00:00', 1.1, 10) "
            f"('2025-01-01 00:00:01', 2.2, 20) "
            f"('2025-01-01 00:00:02', 3.3, 30)"
        )
        tdSql.execute(
            f"INSERT INTO {PHY_CTB2} VALUES "
            f"('2025-01-01 00:00:00', 4.4, 40) "
            f"('2025-01-01 00:00:01', 5.5, 50)"
        )
        tdSql.execute(
            f"INSERT INTO {PHY_NTB} VALUES "
            f"('2025-01-01 00:00:00', 9.9, 99)"
        )

        # -- 1. Virtual normal table -----------------------------------------
        tdSql.execute(
            f"CREATE VTABLE {VTB_NORMAL} ("
            f"ts TIMESTAMP, "
            f"vc1 FLOAT FROM {SRC_DB}.{PHY_CTB1}.c1, "
            f"vc2 INT   FROM {SRC_DB}.{PHY_CTB1}.c2)"
        )

        # -- 2. Virtual super table (two TAG columns) ------------------------
        tdSql.execute(
            f"CREATE STABLE {VSTB} "
            f"(ts TIMESTAMP, vc1 FLOAT, vc2 INT) "
            f"TAGS (vid INT, vname VARCHAR(32)) "
            f"VIRTUAL 1"
        )

        # -- 3. Virtual child tables -----------------------------------------
        tdSql.execute(
            f"CREATE VTABLE {VTB_CHILD1} "
            f"(vc1 FROM {SRC_DB}.{PHY_CTB1}.c1, "
            f"vc2 FROM {SRC_DB}.{PHY_CTB1}.c2) "
            f"USING {VSTB} TAGS(10, 'alice')"
        )
        tdSql.execute(
            f"CREATE VTABLE {VTB_CHILD2} "
            f"(vc1 FROM {SRC_DB}.{PHY_CTB2}.c1, "
            f"vc2 FROM {SRC_DB}.{PHY_CTB2}.c2) "
            f"USING {VSTB} TAGS(20, 'bob')"
        )

        tdLog.info(
            f"Created SRC_DB={SRC_DB} with physical tables and all three "
            f"virtual table types"
        )

    # -----------------------------------------------------------------------
    # Schema verification helpers
    # -----------------------------------------------------------------------

    def count_virtual_tables(self, db: str) -> int:
        """Return the number of virtual tables (normal + child, not VSTB) in db."""
        tdSql.query(
            f"SELECT count(*) FROM information_schema.ins_tables "
            f"WHERE db_name='{db}' AND "
            f"(type='VIRTUAL_NORMAL_TABLE' OR type='VIRTUAL_CHILD_TABLE')"
        )
        return int(tdSql.getData(0, 0))

    def has_table(self, db: str, tbl: str) -> bool:
        """Return True if tbl exists in db (any table type)."""
        tdSql.query(
            f"SELECT count(*) FROM information_schema.ins_tables "
            f"WHERE db_name='{db}' AND table_name='{tbl}'"
        )
        return int(tdSql.getData(0, 0)) > 0

    def has_stable(self, db: str, stb: str) -> bool:
        """Return True if super table stb exists in db."""
        tdSql.query(
            f"SELECT count(*) FROM information_schema.ins_stables "
            f"WHERE db_name='{db}' AND stable_name='{stb}'"
        )
        return int(tdSql.getData(0, 0)) > 0

    def get_vtable_count_by_type(self, db: str, vtype: str) -> int:
        """Return the count of virtual tables of the specified type in db."""
        tdSql.query(
            f"SELECT count(*) FROM information_schema.ins_tables "
            f"WHERE db_name='{db}' AND type='{vtype}'"
        )
        return int(tdSql.getData(0, 0))

    def get_tag_value(self, db: str, tbl: str, tag: str) -> str:
        """Return the tag value (as string) for a virtual child table tag."""
        tdSql.query(
            f"SELECT tag_value FROM information_schema.ins_tags "
            f"WHERE db_name='{db}' AND table_name='{tbl}' AND tag_name='{tag}'"
        )
        if tdSql.getRows() == 0:
            return None
        v = tdSql.getData(0, 0)
        return str(v) if v is not None else None

    # -----------------------------------------------------------------------
    # Data snapshot / comparison helpers
    # -----------------------------------------------------------------------

    def snapshot_table(self, db: str, tbl: str, cols: str) -> list:
        """
        Query SELECT {cols} FROM {db}.{tbl} ORDER BY ts and return all rows
        as a list of tuples.  Each cell is normalised to a string so that
        int / float / timestamp representations are comparable across the
        source and restored databases.
        """
        tdSql.query(f"SELECT {cols} FROM `{db}`.`{tbl}` ORDER BY ts")
        rows = []
        col_count = len(cols.split(","))
        for r in range(tdSql.getRows()):
            row = tuple(str(tdSql.getData(r, c)) for c in range(col_count))
            rows.append(row)
        return rows

    def assert_table_data_equal(self, src_rows: list, dst_rows: list,
                                tbl: str):
        """
        Compare two row lists captured by snapshot_table().
        Calls tdLog.exit() on any row-count or cell-value mismatch.
        """
        if len(src_rows) != len(dst_rows):
            tdLog.exit(
                f"Row count mismatch for {tbl}: "
                f"src={len(src_rows)}, dst={len(dst_rows)}"
            )
        for i, (sr, dr) in enumerate(zip(src_rows, dst_rows)):
            if sr != dr:
                tdLog.exit(
                    f"Data mismatch in {tbl} row {i}: "
                    f"src={sr}, dst={dr}"
                )
        tdLog.info(
            f"[OK] {tbl}: {len(src_rows)} row(s) match exactly"
        )

    # -----------------------------------------------------------------------
    # Main test flow
    # -----------------------------------------------------------------------

    def do_vtable_backup_restore(self):
        """
        Full end-to-end flow:
          create -> snapshot -> full backup -> verify artifacts
          -> restore -> verify schema -> verify tag values -> verify data
        """
        binPath = etool.taosBackupFile()
        if not binPath:
            tdLog.exit("taosBackup binary not found")

        outdir = "./taosbackuptest/vtable"
        self.mkdir(outdir)

        try:
            # ----------------------------------------------------------------
            # Step 1: Build source database
            # ----------------------------------------------------------------
            self.create_source_db()

            # Sanity-check source virtual table count.
            src_vtb_total = self.count_virtual_tables(SRC_DB)
            tdLog.info(
                f"Source virtual table count (normal+child): {src_vtb_total}"
            )
            if src_vtb_total != 3:
                tdLog.exit(
                    f"Expected 3 virtual tables in source "
                    f"(1 normal + 2 child), got {src_vtb_total}"
                )

            # ----------------------------------------------------------------
            # Step 2: Snapshot source data BEFORE backup
            # Capture results here so we can verify after restore without
            # depending on the source DB still being accessible.
            # ----------------------------------------------------------------
            tdLog.info("Snapshotting source virtual table data ...")

            snap_vtb_normal = self.snapshot_table(SRC_DB, VTB_NORMAL, "ts,vc1,vc2")
            snap_vtb_child1 = self.snapshot_table(SRC_DB, VTB_CHILD1, "ts,vc1,vc2")
            snap_vtb_child2 = self.snapshot_table(SRC_DB, VTB_CHILD2, "ts,vc1,vc2")

            tdLog.info(
                f"Snapshot row counts: "
                f"{VTB_NORMAL}={len(snap_vtb_normal)}, "
                f"{VTB_CHILD1}={len(snap_vtb_child1)}, "
                f"{VTB_CHILD2}={len(snap_vtb_child2)}"
            )

            # ----------------------------------------------------------------
            # Step 3: Full backup (schema + data; no -s flag)
            # ----------------------------------------------------------------
            tdLog.info(f"Full backup of {SRC_DB} (schema + data) to {outdir}")
            self.exec(f"{binPath} -D {SRC_DB} -o {outdir} -T 4")

            # ----------------------------------------------------------------
            # Step 4: Verify backup artifacts
            # ----------------------------------------------------------------

            # 4a. vtags columnar binary file must exist for the VSTB.
            vtag_pattern = os.path.join(
                outdir, SRC_DB, "vtags", f"{VSTB}_data1.*"
            )
            vtag_files = glob.glob(vtag_pattern)
            if not vtag_files:
                tdLog.exit(
                    f"Columnar vtag binary not found: {vtag_pattern}\n"
                    f"Expected: {{outdir}}/{SRC_DB}/vtags/"
                    f"{VSTB}_data1.{{dat|par}}"
                )
            tdLog.info(f"[OK] vtag binary exists: {vtag_files[0]}")

            # 4b. vtb.sql skeleton lines for child tables must NOT contain
            #     " TAGS (" -- tag values live in the vtags binary, not inline.
            vtb_sql_path = os.path.join(outdir, SRC_DB, "vtb.sql")
            if not os.path.exists(vtb_sql_path):
                tdLog.exit(f"vtb.sql not found: {vtb_sql_path}")
            with open(vtb_sql_path, "r") as f:
                vtb_sql_content = f.read()
            for child in (VTB_CHILD1, VTB_CHILD2):
                child_lines = [
                    ln for ln in vtb_sql_content.splitlines()
                    if child in ln and " USING " in ln
                ]
                if not child_lines:
                    tdLog.exit(
                        f"No skeleton line found for '{child}' in vtb.sql"
                    )
                for line in child_lines:
                    if " TAGS " in line or " TAGS(" in line:
                        tdLog.exit(
                            f"vtb.sql skeleton for '{child}' contains "
                            f"inline TAGS (expected stripped for columnar "
                            f"storage):\n  {line}"
                        )
                tdLog.info(
                    f"[OK] vtb.sql skeleton for '{child}' has no inline TAGS"
                )

            # ----------------------------------------------------------------
            # Step 5: Restore into DST_DB
            # ----------------------------------------------------------------
            tdSql.execute(f"DROP DATABASE IF EXISTS {DST_DB}")
            tdLog.info(f"Restoring to {DST_DB}")
            self.exec(f"{binPath} -i {outdir} -W {SRC_DB}={DST_DB} -T 4")

            # ----------------------------------------------------------------
            # Step 6: Schema verification
            # ----------------------------------------------------------------

            # 6a. Virtual super table must exist.
            if not self.has_stable(DST_DB, VSTB):
                tdLog.exit(
                    f"Virtual super table '{VSTB}' not found in {DST_DB}"
                )
            tdLog.info(
                f"[OK] Virtual super table '{VSTB}' exists in {DST_DB}"
            )

            # 6b. Count by type.
            vtb_normal_count = self.get_vtable_count_by_type(
                DST_DB, "VIRTUAL_NORMAL_TABLE"
            )
            if vtb_normal_count < 1:
                tdLog.exit(
                    f"Expected >= 1 VIRTUAL_NORMAL_TABLE in {DST_DB}, "
                    f"got {vtb_normal_count}"
                )
            tdLog.info(
                f"[OK] {vtb_normal_count} virtual normal table(s) in {DST_DB}"
            )

            vtb_child_count = self.get_vtable_count_by_type(
                DST_DB, "VIRTUAL_CHILD_TABLE"
            )
            if vtb_child_count < 2:
                tdLog.exit(
                    f"Expected >= 2 VIRTUAL_CHILD_TABLE in {DST_DB}, "
                    f"got {vtb_child_count}"
                )
            tdLog.info(
                f"[OK] {vtb_child_count} virtual child table(s) in {DST_DB}"
            )

            # 6c. Total vtable count must match source.
            dst_vtb_total = self.count_virtual_tables(DST_DB)
            if dst_vtb_total != src_vtb_total:
                tdLog.exit(
                    f"Virtual table count mismatch: "
                    f"src={src_vtb_total}, dst={dst_vtb_total}"
                )
            tdLog.info(
                f"[OK] Virtual table count matches: "
                f"src={src_vtb_total}, dst={dst_vtb_total}"
            )

            # 6d. Each expected table name must be present.
            for tbl in (VTB_NORMAL, VTB_CHILD1, VTB_CHILD2):
                if not self.has_table(DST_DB, tbl):
                    tdLog.exit(
                        f"Virtual table '{tbl}' not found in {DST_DB}"
                    )
                tdLog.info(
                    f"[OK] Virtual table '{tbl}' exists in {DST_DB}"
                )

            # ----------------------------------------------------------------
            # Step 7: TAG value verification for virtual child tables
            # ----------------------------------------------------------------
            expected_tags = {
                VTB_CHILD1: {"vid": "10",  "vname": "alice"},
                VTB_CHILD2: {"vid": "20",  "vname": "bob"},
            }
            for child_tbl, tags in expected_tags.items():
                for tag_name, expected_val in tags.items():
                    actual = self.get_tag_value(DST_DB, child_tbl, tag_name)
                    if actual is None:
                        tdLog.exit(
                            f"Tag '{tag_name}' not found for "
                            f"'{child_tbl}' in {DST_DB}"
                        )
                    if actual != expected_val:
                        tdLog.exit(
                            f"Tag value mismatch for "
                            f"{child_tbl}.{tag_name}: "
                            f"expected='{expected_val}', actual='{actual}'"
                        )
                    tdLog.info(
                        f"[OK] {child_tbl}.{tag_name} = '{actual}' (correct)"
                    )

            # ----------------------------------------------------------------
            # Step 8: Row-by-row data verification
            # Query each virtual table in DST_DB and compare every row
            # against the pre-backup snapshot from SRC_DB.
            # All columns and all rows must match exactly.
            # ----------------------------------------------------------------
            tdLog.info(
                "Starting row-by-row data verification for virtual tables ..."
            )

            # vtb_normal: maps to phy_ctb1 -- 3 rows
            dst_vtb_normal = self.snapshot_table(DST_DB, VTB_NORMAL, "ts,vc1,vc2")
            self.assert_table_data_equal(
                snap_vtb_normal, dst_vtb_normal, VTB_NORMAL
            )

            # vtb_child1: maps to phy_ctb1 -- 3 rows
            dst_vtb_child1 = self.snapshot_table(DST_DB, VTB_CHILD1, "ts,vc1,vc2")
            self.assert_table_data_equal(
                snap_vtb_child1, dst_vtb_child1, VTB_CHILD1
            )

            # vtb_child2: maps to phy_ctb2 -- 2 rows
            dst_vtb_child2 = self.snapshot_table(DST_DB, VTB_CHILD2, "ts,vc1,vc2")
            self.assert_table_data_equal(
                snap_vtb_child2, dst_vtb_child2, VTB_CHILD2
            )

            tdLog.info(
                "All virtual table backup/restore checks passed "
                "(schema, tags, and data)."
            )

        finally:
            self.rmdir(outdir)

    # -----------------------------------------------------------------------
    # Test entry point
    # -----------------------------------------------------------------------

    def test_taosbackup_vtable(self):
        """Full backup/restore for virtual tables.

        Covers all three virtual table types defined in 05-virtualtable.md:
          1. Virtual normal table  -- vtb_normal
          2. Virtual super table   -- vstb  (VIRTUAL 1, 2 TAG columns)
          3. Virtual child tables  -- vtb_child1 (TAGS 10,'alice'),
                                      vtb_child2 (TAGS 20,'bob')

        Procedure:
          1. Build a source DB with physical tables and all three vtable types;
             insert deterministic rows into physical tables.
          2. Snapshot source virtual table data (row lists).
          3. Full backup -- schema AND data (no -s flag).
          4. Verify backup artifacts: vtags/ binary and vtb.sql skeleton format.
          5. Restore into a renamed destination DB (-W SRC_DB=DST_DB).
          6. Verify schema: VSTB, virtual normal table, virtual child tables exist
             and counts match source.
          7. Verify child table TAG values (vid, vname) are correctly restored.
          8. Verify data: row-by-row comparison of every virtual table column
             between the source snapshot and the restored DB.  All rows must match
             exactly -- this is the primary data-correctness guarantee.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-03-06 AlexDuan Added

        """
        tdLog.info("=== test_taosbackup_vtable: START ===")
        self.do_vtable_backup_restore()
        tdLog.info("=== test_taosbackup_vtable: PASS ===")
