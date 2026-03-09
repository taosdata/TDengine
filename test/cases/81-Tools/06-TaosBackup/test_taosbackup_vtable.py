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

Tests backup and restore of virtual tables (vtables) by taosBackup.

Virtual table types covered (all three types mentioned in 05-virtualtable.md)
─────────────────────────────────────────────────────────────────────────────
1. 虚拟普通表 (virtual normal table)
   CREATE VTABLE vtb_normal (..., c1 FLOAT FROM phydb.phy_ntb.c1)

2. 虚拟超级表 (virtual super table / VSTB)
   CREATE STABLE vstb (...) TAGS (vid INT, vname VARCHAR(32)) VIRTUAL 1

3. 虚拟子表 (virtual child table)
   CREATE VTABLE vtb_child1 USING vstb TAGS (10, 'alice')
   CREATE VTABLE vtb_child2 USING vstb TAGS (20, 'bob')

The VSTB intentionally uses **two TAG columns** to exercise columnar
(binary-file) tag storage introduced in Phase 2 of the vtable feature.

Physical database layout
────────────────────────
Database: SRC_DB
  - phy_stb   : physical super table  (ts TIMESTAMP, c1 FLOAT, c2 INT)  TAGS (gid INT)
  - phy_ctb1  : physical child table  (USING phy_stb TAGS(1))
  - phy_ctb2  : physical child table  (USING phy_stb TAGS(2))
  - phy_ntb   : physical normal table (ts TIMESTAMP, c1 FLOAT, c2 INT)

Virtual table layout (also in SRC_DB)
──────────────────────────────────────
  - vtb_normal : virtual normal table  (ts TIMESTAMP, vc1 FLOAT FROM phy_ctb1.c1)
  - vstb       : virtual STB           (ts TIMESTAMP, vc1 FLOAT) TAGS (vid INT, vname VARCHAR(32)) VIRTUAL 1
  - vtb_child1 : virtual child table   (USING vstb TAGS(10, 'alice'))
  - vtb_child2 : virtual child table   (USING vstb TAGS(20, 'bob'))

Test procedure
──────────────
1. Create SRC_DB with all the above tables and insert a few rows into
   physical tables so that the vtable columns have a valid data source.
2. Backup SRC_DB with schema-only flag (-s).
3. Verify backup artifacts:
   a. vtags/ directory and vstb_data1.dat (columnar tag binary) exist.
   b. vtb.sql skeleton lines for child tables have NO inline TAGS.
4. Restore into DST_DB (rename: -W SRC_DB=DST_DB).
5. Verify in DST_DB:
   a. Virtual super table (vstb) exists.
   b. Virtual normal table (vtb_normal) exists.
   c. Two virtual child tables (vtb_child1, vtb_child2) exist.
   d. Total virtual table count matches source (3 vtables).
   e. Child table tag values are correct (vid=10/vname='alice', vid=20/vname='bob').
"""

import os
import glob
import subprocess

from new_test_framework.utils import tdLog, tdSql, etool

# ─── database / table names ───────────────────────────────────────────────────
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

    # ──────────────────────────────────────────────────────────────────────────
    # Low-level helpers
    # ──────────────────────────────────────────────────────────────────────────

    def exec(self, command: str):
        """Run a shell command; fail the test on non-zero exit."""
        tdLog.info(command)
        result = subprocess.run(
            command, shell=True, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        if result.returncode != 0:
            tdLog.exit(f"Command failed (exit {result.returncode}): {command}\n{result.stdout}")
        return result.stdout

    def mkdir(self, path: str):
        os.makedirs(path, exist_ok=True)

    def rmdir(self, path: str):
        import shutil
        if os.path.exists(path):
            shutil.rmtree(path)

    # ──────────────────────────────────────────────────────────────────────────
    # Setup
    # ──────────────────────────────────────────────────────────────────────────

    def create_source_db(self):
        """
        Create SRC_DB with:
          - Physical super table, two child tables, one normal table
          - Virtual normal table, virtual super table (2 TAGs), two virtual child tables
        """
        tdSql.execute(f"DROP DATABASE IF EXISTS {SRC_DB}")
        tdSql.execute(f"CREATE DATABASE {SRC_DB} VGROUPS 1 PRECISION 'ms'")
        tdSql.execute(f"USE {SRC_DB}")

        # ── physical tables ─────────────────────────────────────────────────
        tdSql.execute(
            f"CREATE STABLE {PHY_STB} "
            f"(ts TIMESTAMP, c1 FLOAT, c2 INT) TAGS (gid INT)"
        )
        tdSql.execute(f"CREATE TABLE {PHY_CTB1} USING {PHY_STB} TAGS(1)")
        tdSql.execute(f"CREATE TABLE {PHY_CTB2} USING {PHY_STB} TAGS(2)")
        tdSql.execute(
            f"CREATE TABLE {PHY_NTB} "
            f"(ts TIMESTAMP, c1 FLOAT, c2 INT)"
        )

        # insert a few rows so virtual columns have a valid data source
        tdSql.execute(
            f"INSERT INTO {PHY_CTB1} VALUES "
            f"('2025-01-01 00:00:00', 1.1, 10) "
            f"('2025-01-01 00:00:01', 2.2, 20)"
        )
        tdSql.execute(
            f"INSERT INTO {PHY_CTB2} VALUES "
            f"('2025-01-01 00:00:00', 3.3, 30)"
        )
        tdSql.execute(
            f"INSERT INTO {PHY_NTB} VALUES "
            f"('2025-01-01 00:00:00', 9.9, 99)"
        )

        # ── 1. 虚拟普通表 (virtual normal table) ────────────────────────────
        tdSql.execute(
            f"CREATE VTABLE {VTB_NORMAL} ("
            f"ts TIMESTAMP, "
            f"vc1 FLOAT FROM {SRC_DB}.{PHY_CTB1}.c1, "
            f"vc2 INT FROM {SRC_DB}.{PHY_CTB1}.c2)"
        )

        # ── 2. 虚拟超级表 (virtual super table) — two TAG columns ───────────
        # Two tags (vid INT, vname VARCHAR(32)) exercise multi-column
        # columnar tag storage during backup.
        tdSql.execute(
            f"CREATE STABLE {VSTB} "
            f"(ts TIMESTAMP, vc1 FLOAT, vc2 INT) "
            f"TAGS (vid INT, vname VARCHAR(32)) "
            f"VIRTUAL 1"
        )

        # ── 3. 虚拟子表 (virtual child tables) ──────────────────────────────
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

        tdLog.info(f"Created SRC_DB={SRC_DB} with physical + virtual tables")

    # ──────────────────────────────────────────────────────────────────────────
    # Verification helpers
    # ──────────────────────────────────────────────────────────────────────────

    def count_virtual_tables(self, db: str) -> int:
        """Return the number of virtual tables (normal + child, not VSTB) in db."""
        tdSql.query(
            f"SELECT count(*) FROM information_schema.ins_tables "
            f"WHERE db_name='{db}' AND "
            f"(type='VIRTUAL_NORMAL_TABLE' OR type='VIRTUAL_CHILD_TABLE')"
        )
        return int(tdSql.getData(0, 0))

    def has_table(self, db: str, tbl: str) -> bool:
        """Return True if table tbl exists in db (any type)."""
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
        """Return count of virtual tables of given type in db."""
        tdSql.query(
            f"SELECT count(*) FROM information_schema.ins_tables "
            f"WHERE db_name='{db}' AND type='{vtype}'"
        )
        return int(tdSql.getData(0, 0))

    def get_tag_value(self, db: str, tbl: str, tag: str) -> str:
        """Return the tag value (as string) for a virtual child table."""
        tdSql.query(
            f"SELECT tag_value FROM information_schema.ins_tags "
            f"WHERE db_name='{db}' AND table_name='{tbl}' AND tag_name='{tag}'"
        )
        if tdSql.getRows() == 0:
            return None
        v = tdSql.getData(0, 0)
        return str(v) if v is not None else None

    # ──────────────────────────────────────────────────────────────────────────
    # Main test logic
    # ──────────────────────────────────────────────────────────────────────────

    def do_vtable_backup_restore(self):
        """
        Full flow:
          create → backup (schema-only) → verify artifacts → restore → verify
        """
        binPath = etool.taosBackupFile()
        if not binPath:
            tdLog.exit("taosBackup binary not found")

        outdir = "./taosbackuptest/vtable"
        self.mkdir(outdir)

        try:
            # ── 1. build source ──────────────────────────────────────────────
            self.create_source_db()

            # ── 2. verify source virtual table counts ────────────────────────
            src_vtb_total = self.count_virtual_tables(SRC_DB)
            tdLog.info(f"Source virtual table count (normal+child): {src_vtb_total}")
            if src_vtb_total != 3:
                tdLog.exit(
                    f"Expected 3 virtual tables in source (1 normal + 2 child), "
                    f"got {src_vtb_total}"
                )

            # ── 3. schema-only backup ────────────────────────────────────────
            tdLog.info(f"Backing up {SRC_DB} (schema only) to {outdir}")
            self.exec(f"{binPath} -D {SRC_DB} -s -o {outdir} -T 4")

            # ── 4. verify backup artifacts ───────────────────────────────────
            # 4a. vtags binary file must exist for the VSTB
            vtag_pattern = os.path.join(outdir, SRC_DB, "vtags", f"{VSTB}_data1.*")
            vtag_files = glob.glob(vtag_pattern)
            if not vtag_files:
                tdLog.exit(
                    f"Columnar vtag binary file not found: {vtag_pattern}\n"
                    f"Expected: {{outdir}}/{SRC_DB}/vtags/{VSTB}_data1.{{dat|par}}"
                )
            tdLog.info(f"[OK] vtag binary exists: {vtag_files[0]}")

            # 4b. vtb.sql child table skeleton lines must NOT contain " TAGS ("
            vtb_sql_path = os.path.join(outdir, SRC_DB, "vtb.sql")
            if not os.path.exists(vtb_sql_path):
                tdLog.exit(f"vtb.sql not found: {vtb_sql_path}")
            with open(vtb_sql_path, "r") as f:
                vtb_sql_content = f.read()
            for child in (VTB_CHILD1, VTB_CHILD2):
                # Find the skeleton line for this child table
                child_lines = [l for l in vtb_sql_content.splitlines()
                               if child in l and " USING " in l]
                if not child_lines:
                    tdLog.exit(f"No skeleton line for '{child}' in vtb.sql")
                for line in child_lines:
                    if " TAGS " in line or " TAGS(" in line:
                        tdLog.exit(
                            f"vtb.sql skeleton for '{child}' contains inline TAGS "
                            f"(should be stripped for columnar storage):\n  {line}"
                        )
                tdLog.info(f"[OK] vtb.sql skeleton for '{child}' has no inline TAGS")

            # ── 5. restore to DST_DB ─────────────────────────────────────────
            tdSql.execute(f"DROP DATABASE IF EXISTS {DST_DB}")
            tdLog.info(f"Restoring to {DST_DB}")
            self.exec(f"{binPath} -i {outdir} -W {SRC_DB}={DST_DB} -T 4")

            # ── 6. verify: virtual super table (VSTB) ────────────────────────
            if not self.has_stable(DST_DB, VSTB):
                tdLog.exit(f"Virtual super table '{VSTB}' not found in {DST_DB}")
            tdLog.info(f"[OK] Virtual super table '{VSTB}' exists in {DST_DB}")

            # ── 7. verify: virtual normal table ──────────────────────────────
            vtb_normal_count = self.get_vtable_count_by_type(DST_DB, "VIRTUAL_NORMAL_TABLE")
            if vtb_normal_count < 1:
                tdLog.exit(
                    f"Expected at least 1 VIRTUAL_NORMAL_TABLE in {DST_DB}, "
                    f"got {vtb_normal_count}"
                )
            tdLog.info(f"[OK] {vtb_normal_count} virtual normal table(s) in {DST_DB}")

            # ── 8. verify: virtual child tables ──────────────────────────────
            vtb_child_count = self.get_vtable_count_by_type(DST_DB, "VIRTUAL_CHILD_TABLE")
            if vtb_child_count < 2:
                tdLog.exit(
                    f"Expected at least 2 VIRTUAL_CHILD_TABLE in {DST_DB}, "
                    f"got {vtb_child_count}"
                )
            tdLog.info(f"[OK] {vtb_child_count} virtual child table(s) in {DST_DB}")

            # ── 9. verify: total vtable count matches source ──────────────────
            dst_vtb_total = self.count_virtual_tables(DST_DB)
            if dst_vtb_total != src_vtb_total:
                tdLog.exit(
                    f"Virtual table count mismatch: "
                    f"src={src_vtb_total}, dst={dst_vtb_total}"
                )
            tdLog.info(
                f"[OK] Virtual table count matches: src={src_vtb_total}, "
                f"dst={dst_vtb_total}"
            )

            # ── 10. verify specific table names exist in DST ──────────────────
            for tbl in (VTB_NORMAL, VTB_CHILD1, VTB_CHILD2):
                if not self.has_table(DST_DB, tbl):
                    tdLog.exit(f"Virtual table '{tbl}' not found in {DST_DB}")
                tdLog.info(f"[OK] Virtual table '{tbl}' exists in {DST_DB}")

            # ── 11. verify: child table TAG VALUES are correctly restored ──────
            # vtb_child1 must have vid=10, vname='alice'
            # vtb_child2 must have vid=20, vname='bob'
            expected_tags = {
                VTB_CHILD1: {"vid": "10",    "vname": "alice"},
                VTB_CHILD2: {"vid": "20",    "vname": "bob"},
            }
            for child_tbl, tags in expected_tags.items():
                for tag_name, expected_val in tags.items():
                    actual = self.get_tag_value(DST_DB, child_tbl, tag_name)
                    if actual is None:
                        tdLog.exit(
                            f"Tag '{tag_name}' not found for '{child_tbl}' in {DST_DB}"
                        )
                    if actual != expected_val:
                        tdLog.exit(
                            f"Tag value mismatch for {child_tbl}.{tag_name}: "
                            f"expected='{expected_val}', actual='{actual}'"
                        )
                    tdLog.info(
                        f"[OK] {child_tbl}.{tag_name} = '{actual}' (correct)"
                    )

            tdLog.info("All virtual table backup/restore checks passed.")

        finally:
            self.rmdir(outdir)

    # ──────────────────────────────────────────────────────────────────────────
    # Test entry point
    # ──────────────────────────────────────────────────────────────────────────

    def test_taosbackup_vtable(self):
        """
        Test: taosBackup virtual table backup and restore

        Covers all three virtual table types defined in 05-virtualtable.md:
          1. Virtual normal table  – vtb_normal
          2. Virtual super table  – vstb  (VIRTUAL 1, 2 TAG columns)
          3. Virtual child table   – vtb_child1 (TAGS 10,'alice'), vtb_child2 (TAGS 20,'bob')

        Procedure:
          1. Build a source DB with physical tables and all three vtable types.
          2. Backup with schema-only flag (-s).
          3. Verify backup artifacts: vtags/ binary files, vtb.sql skeleton format.
          4. Restore into a renamed destination DB.
          5. Verify the VSTB, virtual normal table, virtual child tables exist
             AND the child table TAG VALUES are correctly restored.
        """
        tdLog.info("=== test_taosbackup_vtable: START ===")
        self.do_vtable_backup_restore()
        tdLog.info("=== test_taosbackup_vtable: PASS ===")


# -*- coding: utf-8 -*-

"""
test_taosbackup_vtable.py

Tests backup and restore of virtual tables (vtables) by taosBackup.

Virtual table types covered (all three types mentioned in 05-virtualtable.md)
─────────────────────────────────────────────────────────────────────────────
1. 虚拟普通表 (virtual normal table)
   CREATE VTABLE vtb_normal (..., c1 FLOAT FROM phydb.phy_ntb.c1)

2. 虚拟超级表 (virtual super table / VSTB)
   CREATE STABLE vstb (...) TAGS (...) VIRTUAL 1

3. 虚拟子表 (virtual child table)
   CREATE VTABLE vtb_child1 USING vstb TAGS (1)

Physical database layout
────────────────────────
Database: SRC_DB
  - phy_stb   : physical super table  (ts TIMESTAMP, c1 FLOAT, c2 INT)  TAGS (gid INT)
  - phy_ctb1  : physical child table  (USING phy_stb TAGS(1))
  - phy_ctb2  : physical child table  (USING phy_stb TAGS(2))
  - phy_ntb   : physical normal table (ts TIMESTAMP, c1 FLOAT, c2 INT)

Virtual table layout (also in SRC_DB)
──────────────────────────────────────
  - vtb_normal : virtual normal table  (ts TIMESTAMP, vc1 FLOAT FROM phy_ctb1.c1)
  - vstb       : virtual STB           (ts TIMESTAMP, vc1 FLOAT) TAGS (vid INT) VIRTUAL 1
  - vtb_child1 : virtual child table   (USING vstb TAGS(10))
  - vtb_child2 : virtual child table   (USING vstb TAGS(20))

Test procedure
──────────────
1. Create SRC_DB with all the above tables and insert a few rows into
   physical tables so that the vtable columns have a valid data source.
2. Backup SRC_DB with schema-only flag (-s).
3. Restore into DST_DB (rename: -W SRC_DB=DST_DB).
4. Verify in DST_DB:
   a. Virtual super table (vstb) exists and its DDL is a VSTB.
   b. Virtual normal table (vtb_normal) exists.
   c. Two virtual child tables (vtb_child1, vtb_child2) exist.
   d. Total virtual table count matches source (4 vtables).
"""

import os
import subprocess

from new_test_framework.utils import tdLog, tdSql, etool

# ─── database / table names ───────────────────────────────────────────────────
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


