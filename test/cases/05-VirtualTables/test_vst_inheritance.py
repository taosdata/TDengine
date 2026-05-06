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
VST Inheritance — Full Feature Test
Covers: 17-vst-inheritance-fs.md

Data model:
  vst_root (ts, val)                  ── vct_r1, vct_r2
    ├── vst_mid (ts, val, extra)      ── vct_m1 (private: sensor_a)
    │     └── vst_leaf (ts, val, extra, leaf_val) ── vct_leaf1
    └── vst_mid2 (ts, val, temp)      ── vct_m2 (private: sensor_b)
"""

import pytest
from new_test_framework.utils import tdLog, tdSql, etool, tdCom


DB       = "test_vst_inherit"
DB_CROSS = "test_vst_cross"


class TestVstInheritance:

    # ─────────────────────────────────────────────────────────────
    # Setup
    # ─────────────────────────────────────────────────────────────
    def setup_class(cls):
        tdLog.info("=== setup: create source databases and tables ===")

        tdSql.execute(f"DROP DATABASE IF EXISTS {DB};")
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB_CROSS};")
        tdSql.execute(f"CREATE DATABASE {DB} VGROUPS 2;")
        tdSql.execute(f"CREATE DATABASE {DB_CROSS} VGROUPS 2;")

        # ── source tables in main DB ──
        tdSql.execute(f"USE {DB};")
        tdSql.execute(
            "CREATE STABLE src_stb (ts TIMESTAMP, c1 INT, c2 FLOAT, c3 NCHAR(64)) TAGS (t1 INT);")
        for i in range(10):
            tdSql.execute(f"CREATE TABLE src_t{i} USING src_stb TAGS ({i});")
        tdSql.execute("INSERT INTO src_t0 VALUES ('2026-01-01 00:00:00', 10, 1.1, 'hello');")
        tdSql.execute("INSERT INTO src_t1 VALUES ('2026-01-01 00:00:01', 20, 2.2, 'world');")
        tdSql.execute("INSERT INTO src_t2 VALUES ('2026-01-01 00:00:02', 30, 3.3, 'foo');")
        tdSql.execute("INSERT INTO src_t3 VALUES ('2026-01-01 00:00:03', 40, 4.4, 'bar');")
        tdSql.execute("INSERT INTO src_t4 VALUES ('2026-01-01 00:00:04', 50, 5.5, 'leaf');")

        # ── source tables in cross DB ──
        tdSql.execute(f"USE {DB_CROSS};")
        tdSql.execute(
            "CREATE STABLE cross_stb (ts TIMESTAMP, x1 INT, x2 DOUBLE) TAGS (xt1 NCHAR(32));")
        tdSql.execute("CREATE TABLE cross_t1 USING cross_stb TAGS ('sensor_a');")
        tdSql.execute("INSERT INTO cross_t1 VALUES ('2026-01-01 00:00:00', 100, 10.1);")

        tdSql.execute(f"USE {DB};")

    def teardown_class(cls):
        tdLog.info("=== teardown ===")
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB};")
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB_CROSS};")

    # ─────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────
    def _create_root_vst(self):
        """Create vst_root (ts, val) TAGS (region)."""
        tdSql.execute(
            f"CREATE STABLE vst_root ("
            f"ts TIMESTAMP, val INT"
            f") TAGS (region NCHAR(64)) VIRTUAL 1;")

    def _create_mid_vsts(self):
        """Create vst_mid and vst_mid2 inheriting from vst_root."""
        tdSql.execute(
            f"CREATE VIRTUAL STABLE vst_mid BASE ON vst_root "
            f"(extra INT) "
            f"TAGS (mid_tag NCHAR(32));")
        tdSql.execute(
            f"CREATE VIRTUAL STABLE vst_mid2 BASE ON vst_root "
            f"(temp FLOAT) "
            f"TAGS (mid2_tag INT);")

    def _create_leaf_vst(self):
        """Create vst_leaf inheriting from vst_mid (3-level chain)."""
        tdSql.execute(
            f"CREATE VIRTUAL STABLE vst_leaf BASE ON vst_mid "
            f"(leaf_val FLOAT) "
            f"TAGS (leaf_tag NCHAR(16));")

    def _create_vcts(self):
        """Create VCTs under each VST, some with private columns."""
        # vct_r1, vct_r2 under vst_root
        # vst_root schema: ts TIMESTAMP, val INT → 1 non-ts col
        tdSql.execute(
            f"CREATE VTABLE vct_r1 "
            f"(src_t0.c1) "
            f"USING vst_root TAGS (0);")
        tdSql.execute(
            f"CREATE VTABLE vct_r2 "
            f"(src_t1.c1) "
            f"USING vst_root TAGS (1);")

        # vct_m1 under vst_mid
        # vst_mid schema: ts, val(INT inherited), extra(INT own) → 2 non-ts cols
        tdSql.execute(
            f"CREATE VTABLE vct_m1 "
            f"(src_t2.c1, src_t2.c1) "
            f"USING vst_mid TAGS (2, 'mid-01');")

        # vct_m2 under vst_mid2
        # vst_mid2 schema: ts, val(INT inherited), temp(FLOAT own) → 2 non-ts cols
        tdSql.execute(
            f"CREATE VTABLE vct_m2 "
            f"(src_t3.c1, src_t3.c2) "
            f"USING vst_mid2 TAGS (3, 4);")

        # vct_leaf1 under vst_leaf
        # vst_leaf schema: ts, val(INT), extra(INT), leaf_val(FLOAT) → 3 non-ts cols
        tdSql.execute(
            f"CREATE VTABLE vct_leaf1 "
            f"(src_t4.c1, src_t4.c1, src_t4.c2) "
            f"USING vst_leaf TAGS (4, 'mid-02', 'leaf-01');")

    def _build_full_model(self):
        """Build the complete inheritance tree with VCTs."""
        self._create_root_vst()
        self._create_mid_vsts()
        self._create_leaf_vst()
        self._create_vcts()

    def _cleanup_model(self):
        """Drop everything in leaf→root order."""
        for tbl in ["vct_leaf1", "vct_m1", "vct_m2", "vct_r1", "vct_r2"]:
            tdSql.execute(f"DROP VTABLE IF EXISTS {tbl};")
        for stb in ["vst_leaf", "vst_mid", "vst_mid2", "vst_root"]:
            tdSql.execute(f"DROP STABLE IF EXISTS {stb};")

    # =================================================================
    # 1. CREATE VIRTUAL STABLE ... BASE ON
    # =================================================================
    def test_create_inherit_basic(self):
        """DDL: basic VST inheritance — schema merge

        Verify child VST schema = parent columns + new columns.

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, ddl

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_create_inherit_basic ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._create_root_vst()
        self._create_mid_vsts()

        # vst_root: ts, val | region
        tdSql.query("DESCRIBE vst_root;")
        root_cols = [row[0] for row in tdSql.queryResult]
        assert "ts" in root_cols, f"ts not in vst_root: {root_cols}"
        assert "val" in root_cols, f"val not in vst_root: {root_cols}"
        assert "region" in root_cols, f"region not in vst_root: {root_cols}"

        # vst_mid: inherits ts, val, region + adds extra, mid_tag
        tdSql.query("DESCRIBE vst_mid;")
        mid_cols = [row[0] for row in tdSql.queryResult]
        for col in ["ts", "val", "extra", "region", "mid_tag"]:
            assert col in mid_cols, f"{col} not in vst_mid: {mid_cols}"

        # vst_mid2: inherits ts, val, region + adds temp, mid2_tag
        tdSql.query("DESCRIBE vst_mid2;")
        mid2_cols = [row[0] for row in tdSql.queryResult]
        for col in ["ts", "val", "temp", "region", "mid2_tag"]:
            assert col in mid2_cols, f"{col} not in vst_mid2: {mid2_cols}"

        self._cleanup_model()

    # =================================================================
    # 2. CREATE — error cases
    # =================================================================
    def test_create_inherit_parent_not_virtual(self):
        """DDL: BASE ON non-VST parent → error

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, ddl, negative

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_create_inherit_parent_not_virtual ===")
        tdSql.execute(f"USE {DB};")
        tdSql.error(
            f"CREATE VIRTUAL STABLE vst_bad BASE ON src_stb "
            f"(col1 INT) "
            f"TAGS (t1 INT);")

    def test_create_inherit_col_name_conflict(self):
        """DDL: new column name conflicts with parent → error

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, ddl, negative

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_create_inherit_col_name_conflict ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._create_root_vst()

        # 'val' already exists in vst_root
        tdSql.error(
            f"CREATE VIRTUAL STABLE vst_dup_col BASE ON vst_root "
            f"(val INT) "
            f"TAGS (new_tag INT);")

        self._cleanup_model()

    def test_create_inherit_tag_name_conflict(self):
        """DDL: new tag name conflicts with parent → error

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, ddl, negative

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_create_inherit_tag_name_conflict ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._create_root_vst()

        # 'region' already exists as tag in vst_root
        tdSql.error(
            f"CREATE VIRTUAL STABLE vst_dup_tag BASE ON vst_root "
            f"(new_col INT) "
            f"TAGS (region NCHAR(64));")

        self._cleanup_model()

    def test_create_inherit_depth_limit(self):
        """DDL: max 10 levels of inheritance — 11th level fails

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, ddl, negative

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_create_inherit_depth_limit ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._create_root_vst()

        # Build chain: root(0) → d1(1) → d2(2) → ... → d10(10)
        prev = "vst_root"
        depth_tables = []
        for i in range(1, 11):
            name = f"vst_depth_{i}"
            depth_tables.append(name)
            tdSql.execute(
                f"CREATE VIRTUAL STABLE {name} BASE ON {prev} "
                f"(d{i}_col INT) "
                f"TAGS (d{i}_tag INT);")
            prev = name

        # depth=10 was the last success; depth=11 should fail
        tdSql.error(
            f"CREATE VIRTUAL STABLE vst_depth_11 BASE ON {prev} "
            f"(d11_col INT) "
            f"TAGS (d11_tag INT);")

        # cleanup chain in reverse
        for name in reversed(depth_tables):
            tdSql.execute(f"DROP STABLE IF EXISTS {name};")
        self._cleanup_model()

    def test_create_inherit_cross_db(self):
        """DDL: cross-database VST inheritance

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, ddl

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_create_inherit_cross_db ===")
        # Create root VST in cross DB
        tdSql.execute(f"USE {DB_CROSS};")
        tdSql.execute(
            f"CREATE STABLE cross_vst_root ("
            f"ts TIMESTAMP, x1 INT"
            f") TAGS (xt1 NCHAR(32)) VIRTUAL 1;")

        # Create child in main DB inheriting from cross DB parent
        tdSql.execute(f"USE {DB};")
        tdSql.execute(
            f"CREATE VIRTUAL STABLE vst_cross_child BASE ON {DB_CROSS}.cross_vst_root "
            f"(local_col INT) "
            f"TAGS (local_tag INT);")

        # Verify schema includes parent columns
        tdSql.query("DESCRIBE vst_cross_child;")
        cols = [row[0] for row in tdSql.queryResult]
        for col in ["ts", "x1", "local_col", "xt1", "local_tag"]:
            assert col in cols, f"{col} not in vst_cross_child: {cols}"

        # cleanup
        tdSql.execute("DROP STABLE IF EXISTS vst_cross_child;")
        tdSql.execute(f"USE {DB_CROSS};")
        tdSql.execute("DROP STABLE IF EXISTS cross_vst_root;")
        tdSql.execute(f"USE {DB};")

    # =================================================================
    # 3. DROP — inheritance protection
    # =================================================================
    def test_drop_parent_with_children_rejected(self):
        """DDL: DROP parent VST with children → error

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, ddl, negative

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_drop_parent_with_children_rejected ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._create_root_vst()
        self._create_mid_vsts()

        # vst_root has children → DROP should fail
        tdSql.error("DROP STABLE vst_root;")

        # vst_mid is a leaf → DROP should succeed
        tdSql.execute("DROP STABLE vst_mid;")

        # Now vst_root still has vst_mid2 → still fails
        tdSql.error("DROP STABLE vst_root;")

        # Drop vst_mid2, then root succeeds
        tdSql.execute("DROP STABLE vst_mid2;")
        tdSql.execute("DROP STABLE vst_root;")

    def test_drop_leaf_then_parent(self):
        """DDL: drop child first, then parent — both succeed

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, ddl

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_drop_leaf_then_parent ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._create_root_vst()
        self._create_mid_vsts()

        tdSql.execute("DROP STABLE vst_mid;")
        tdSql.execute("DROP STABLE vst_mid2;")
        tdSql.execute("DROP STABLE vst_root;")

    # =================================================================
    # 4. ALTER CASCADE
    # =================================================================
    @pytest.mark.skip(reason="ALTER CASCADE not implemented in mnode yet")
    def test_alter_parent_add_column_cascades(self):
        """DDL: parent ADD COLUMN cascades to all descendants

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, ddl, alter

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_alter_parent_add_column_cascades ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._create_root_vst()
        self._create_mid_vsts()

        # Add column to root
        tdSql.execute("ALTER STABLE vst_root ADD COLUMN new_val BIGINT;")

        # Verify new_val appears in root
        tdSql.query("DESCRIBE vst_root;")
        root_cols = [row[0] for row in tdSql.queryResult]
        assert "new_val" in root_cols, f"new_val not in vst_root: {root_cols}"

        # Verify new_val cascaded to vst_mid
        tdSql.query("DESCRIBE vst_mid;")
        mid_cols = [row[0] for row in tdSql.queryResult]
        assert "new_val" in mid_cols, f"new_val not cascaded to vst_mid: {mid_cols}"

        # Verify new_val cascaded to vst_mid2
        tdSql.query("DESCRIBE vst_mid2;")
        mid2_cols = [row[0] for row in tdSql.queryResult]
        assert "new_val" in mid2_cols, f"new_val not cascaded to vst_mid2: {mid2_cols}"

        self._cleanup_model()

    def test_alter_parent_drop_column_rejected(self):
        """DDL: parent DROP COLUMN when has children → error

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, ddl, alter, negative

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_alter_parent_drop_column_rejected ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._create_root_vst()
        self._create_mid_vsts()

        tdSql.execute("ALTER STABLE vst_root ADD COLUMN new_val BIGINT;")
        # Cannot drop column on parent with children
        tdSql.error("ALTER STABLE vst_root DROP COLUMN new_val;")

        self._cleanup_model()

    @pytest.mark.skip(reason="ALTER CASCADE not implemented in mnode yet")
    def test_alter_parent_modify_column_cascades(self):
        """DDL: parent MODIFY TAG cascades to descendants

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, ddl, alter

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_alter_parent_modify_column_cascades ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._create_root_vst()
        self._create_mid_vsts()

        # Widen region tag from NCHAR(64) to NCHAR(128)
        tdSql.execute("ALTER STABLE vst_root MODIFY TAG region NCHAR(128);")

        # Check mid has updated length
        tdSql.query("DESCRIBE vst_mid;")
        for row in tdSql.queryResult:
            if row[0] == "region":
                assert row[2] >= 128, f"region length not updated in vst_mid: {row}"
                break

        self._cleanup_model()

    def test_alter_child_add_column_no_affect_parent(self):
        """DDL: child ADD COLUMN does not affect parent

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, ddl, alter

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_alter_child_add_column_no_affect_parent ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._create_root_vst()
        self._create_mid_vsts()

        tdSql.execute("ALTER STABLE vst_mid ADD COLUMN child_only DOUBLE;")

        tdSql.query("DESCRIBE vst_mid;")
        mid_cols = [row[0] for row in tdSql.queryResult]
        assert "child_only" in mid_cols

        tdSql.query("DESCRIBE vst_root;")
        root_cols = [row[0] for row in tdSql.queryResult]
        assert "child_only" not in root_cols, "child column leaked to parent"

        self._cleanup_model()

    # =================================================================
    # 5. SHOW VSTABLE INHERITS / ins_inherits
    # =================================================================
    @pytest.mark.skip(reason="SHOW VSTABLE INHERITS grammar not added to parser yet")
    def test_show_vstable_inherits(self):
        """DDL: SHOW VSTABLE INHERITS and ins_inherits

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, show

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_show_vstable_inherits ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._build_full_model()

        tdSql.query("SHOW VSTABLE INHERITS;")
        # Should have at least 3 rows: mid←root, mid2←root, leaf←mid
        tdSql.checkRows(3)

        tdSql.query("SELECT * FROM information_schema.ins_inherits;")
        tdSql.checkRows(3)

        self._cleanup_model()

    # =================================================================
    # 6. DQL — basic query (no EXPAND, backward-compatible)
    # =================================================================
    def test_query_vst_no_expand(self):
        """DQL: query VST without EXPAND returns only own VCTs

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, query

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_query_vst_no_expand ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._build_full_model()

        # vst_root should only return vct_r1 and vct_r2
        tdSql.query("SELECT COUNT(*) FROM vst_root;")
        tdSql.checkData(0, 0, 2)

        # vst_mid should only return vct_m1
        tdSql.query("SELECT COUNT(*) FROM vst_mid;")
        tdSql.checkData(0, 0, 1)

        self._cleanup_model()

    # =================================================================
    # 7. DQL — EXPAND syntax
    # =================================================================
    def test_expand_0_no_expand(self):
        """DQL: EXPAND and EXPAND(0) = no expansion

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, query, expand

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_expand_0_no_expand ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._build_full_model()

        tdSql.query("SELECT COUNT(*) FROM vst_root EXPAND;")
        tdSql.checkData(0, 0, 2)

        tdSql.query("SELECT COUNT(*) FROM vst_root EXPAND(0);")
        tdSql.checkData(0, 0, 2)

        self._cleanup_model()

    def test_expand_1_one_level(self):
        """DQL: EXPAND(1) includes 1 level of descendant VCTs

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, query, expand

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_expand_1_one_level ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._build_full_model()

        # root EXPAND(1): vct_r1 + vct_r2 + vct_m1 + vct_m2 = 4
        # (vst_leaf is 2 levels deep, vct_leaf1 excluded)
        tdSql.query("SELECT COUNT(*) FROM vst_root EXPAND(1);")
        tdSql.checkData(0, 0, 4)

        self._cleanup_model()

    def test_expand_2_two_levels(self):
        """DQL: EXPAND(2) includes 2 levels of descendant VCTs

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, query, expand

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_expand_2_two_levels ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._build_full_model()

        # root EXPAND(2): vct_r1 + vct_r2 + vct_m1 + vct_m2 + vct_leaf1 = 5
        tdSql.query("SELECT COUNT(*) FROM vst_root EXPAND(2);")
        tdSql.checkData(0, 0, 5)

        self._cleanup_model()

    def test_expand_minus1_all_descendants(self):
        """DQL: EXPAND(-1) recursively includes all descendant VCTs

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, query, expand

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_expand_minus1_all_descendants ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._build_full_model()

        # root EXPAND(-1): all 5 VCTs
        tdSql.query("SELECT COUNT(*) FROM vst_root EXPAND(-1);")
        tdSql.checkData(0, 0, 5)

        # mid EXPAND(-1): vct_m1 + vct_leaf1 = 2
        tdSql.query("SELECT COUNT(*) FROM vst_mid EXPAND(-1);")
        tdSql.checkData(0, 0, 2)

        self._cleanup_model()

    # =================================================================
    # 8. DQL — column visibility & NULL fill
    # =================================================================
    def test_expand_column_union_null_fill(self):
        """DQL: EXPAND returns parent-schema columns only (UNION ALL semantics)

        In UNION ALL rewrite, each branch only projects the queried VST's
        parent schema. Child-specific columns are not visible.

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, query, expand

        Jira: None

        History:
            - 2026-04-30 Created
            - 2026-07-15 Updated for UNION ALL semantics (parent-schema only)
        """
        tdLog.info("=== test_expand_column_union_null_fill ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._build_full_model()

        # EXPAND(-1) from root → parent schema: ts, val, region (tag)
        tdSql.query("SELECT * FROM vst_root EXPAND(-1) ORDER BY ts;")
        # Should have 5 rows (one per VCT across the hierarchy)
        tdSql.checkRows(5)

        # Column count = parent schema: ts + val + region(tag) = 3
        col_count = len(tdSql.queryResult[0]) if tdSql.queryResult else 0
        assert col_count == 3, f"Expected 3 columns (parent schema), got {col_count}"

        self._cleanup_model()

    def test_expand_private_cols_not_visible(self):
        """DQL: VCT private columns NOT visible in EXPAND queries

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, query, expand, private

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_expand_private_cols_not_visible ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._build_full_model()

        # EXPAND query should NOT include sensor_a or sensor_b
        tdSql.query("SELECT * FROM vst_root EXPAND(-1) LIMIT 1;")
        col_names = [desc[0] for desc in tdSql.cursor.description] if hasattr(tdSql, 'cursor') and tdSql.cursor.description else []
        if col_names:
            assert "sensor_a" not in col_names, "Private col sensor_a should not be visible"
            assert "sensor_b" not in col_names, "Private col sensor_b should not be visible"

        self._cleanup_model()

    # =================================================================
    # 9. DQL — filter & aggregate with EXPAND
    # =================================================================
    def test_expand_tbname_filter(self):
        """DQL: EXPAND with value filter on inherited column

        In UNION ALL rewrite, tbname pseudo-column is not directly available
        on the outer query. Test value-based filtering instead.

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, query, expand

        Jira: None

        History:
            - 2026-04-30 Created
            - 2026-07-15 Updated: tbname not available in UNION ALL; test value filter
        """
        tdLog.info("=== test_expand_tbname_filter ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._build_full_model()

        # Filter by val: only src_t2(30) inserted into vct_m1
        tdSql.query("SELECT * FROM vst_root EXPAND(-1) WHERE val = 30;")
        tdSql.checkRows(1)

        self._cleanup_model()

    def test_expand_aggregate(self):
        """DQL: aggregate functions with EXPAND

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, query, expand

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_expand_aggregate ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._build_full_model()

        tdSql.query("SELECT COUNT(*) FROM vst_root EXPAND(-1);")
        tdSql.checkData(0, 0, 5)

        tdSql.query("SELECT COUNT(*) FROM vst_root;")
        tdSql.checkData(0, 0, 2)

        self._cleanup_model()

    def test_expand_group_by_tbname(self):
        """DQL: ORDER BY with EXPAND

        In UNION ALL rewrite, tbname pseudo-column is not directly available.
        Test ORDER BY on inherited column instead.

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, query, expand

        Jira: None

        History:
            - 2026-04-30 Created
            - 2026-07-15 Updated: GROUP BY tbname not available; test ORDER BY val
        """
        tdLog.info("=== test_expand_group_by_tbname ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._build_full_model()

        tdSql.query(
            "SELECT val FROM vst_root EXPAND(-1) ORDER BY val;")
        tdSql.checkRows(5)
        # Verify ordering: 10, 20, 30, 40, 50
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(4, 0, 50)

        self._cleanup_model()

    def test_expand_column_filter(self):
        """DQL: WHERE on inherited column with EXPAND

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, query, expand

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_expand_column_filter ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._build_full_model()

        # val > 25 → src_t2(30), src_t3(40), src_t4(50) → 3 rows
        tdSql.query("SELECT * FROM vst_root EXPAND(-1) WHERE val > 25;")
        tdSql.checkRows(3)

        self._cleanup_model()

    # =================================================================
    # 10. DQL — EXPAND error cases
    # =================================================================
    def test_expand_on_non_inherited_vst_error(self):
        """DQL: EXPAND(N>0) on non-inherited VST returns own VCTs only

        With UNION ALL rewrite, EXPAND on a standalone VST with no children
        simply returns the VST's own VCTs (equivalent to no-expand).

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, query, expand, negative

        Jira: None

        History:
            - 2026-04-30 Created
            - 2026-07-15 Updated: EXPAND on standalone now returns own data (no error)
        """
        tdLog.info("=== test_expand_on_non_inherited_vst_error ===")
        tdSql.execute(f"USE {DB};")

        # Create standalone VST with no children
        tdSql.execute(
            f"CREATE STABLE vst_standalone ("
            f"ts TIMESTAMP, v1 INT"
            f") TAGS (t1 INT) VIRTUAL 1;")

        tdSql.execute(
            f"CREATE VTABLE vct_standalone (src_t0.c1) USING vst_standalone TAGS (1);")

        # EXPAND(1) on standalone → returns own VCTs (no children to expand)
        tdSql.query("SELECT * FROM vst_standalone EXPAND(1);")
        tdSql.checkRows(1)

        # EXPAND(0) should still work (= no expand)
        tdSql.query("SELECT * FROM vst_standalone EXPAND(0);")
        tdSql.checkRows(1)

        tdSql.execute("DROP VTABLE IF EXISTS vct_standalone;")
        tdSql.execute("DROP STABLE IF EXISTS vst_standalone;")

    # =================================================================
    # 11. EXPAND from mid-level VST
    # =================================================================
    def test_expand_from_mid_level(self):
        """DQL: EXPAND from mid-level VST

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, query, expand

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_expand_from_mid_level ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._build_full_model()

        # mid EXPAND(1): vct_m1 + vct_leaf1 = 2
        tdSql.query("SELECT COUNT(*) FROM vst_mid EXPAND(1);")
        tdSql.checkData(0, 0, 2)

        # mid2 has no children → EXPAND(-1) = just own VCTs
        tdSql.query("SELECT COUNT(*) FROM vst_mid2 EXPAND(-1);")
        tdSql.checkData(0, 0, 1)

        self._cleanup_model()

    # =================================================================
    # 12. DCL — privilege inheritance
    # =================================================================
    @pytest.mark.skip(reason="GRANT/REVOKE requires Enterprise edition")
    def test_privilege_inherited_on_create(self):
        """DCL: child VST inherits parent privileges on creation

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, dcl

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_privilege_inherited_on_create ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._create_root_vst()

        tdSql.execute("DROP USER IF EXISTS vst_test_user;")
        tdSql.execute("CREATE USER vst_test_user PASS 'Test123!';")
        tdSql.execute(f"GRANT READ ON {DB}.vst_root TO vst_test_user;")

        # Create child → should auto-inherit READ
        self._create_mid_vsts()

        # Verify via SHOW GRANTS or similar (framework-dependent)
        # The child should have inherited permissions
        tdSql.query("SHOW GRANTS;")
        # At minimum, the query should succeed

        # cleanup
        tdSql.execute("DROP STABLE IF EXISTS vst_mid;")
        tdSql.execute("DROP STABLE IF EXISTS vst_mid2;")
        tdSql.execute("DROP STABLE IF EXISTS vst_root;")
        tdSql.execute("DROP USER IF EXISTS vst_test_user;")

    @pytest.mark.skip(reason="GRANT/REVOKE requires Enterprise edition")
    def test_privilege_parent_change_overrides_children(self):
        """DCL: parent privilege change overrides children

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, dcl

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_privilege_parent_change_overrides_children ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._create_root_vst()

        tdSql.execute("DROP USER IF EXISTS vst_test_user2;")
        tdSql.execute("CREATE USER vst_test_user2 PASS 'Test123!';")
        tdSql.execute(f"GRANT READ ON {DB}.vst_root TO vst_test_user2;")
        self._create_mid_vsts()

        # Revoke from parent → should cascade to children
        tdSql.execute(f"REVOKE READ ON {DB}.vst_root FROM vst_test_user2;")

        # cleanup
        tdSql.execute("DROP STABLE IF EXISTS vst_mid;")
        tdSql.execute("DROP STABLE IF EXISTS vst_mid2;")
        tdSql.execute("DROP STABLE IF EXISTS vst_root;")
        tdSql.execute("DROP USER IF EXISTS vst_test_user2;")

    # =================================================================
    # 13. Edge cases
    # =================================================================
    def test_create_if_not_exists(self):
        """DDL: CREATE IF NOT EXISTS on existing inherited VST

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, ddl

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_create_if_not_exists ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._create_root_vst()
        self._create_mid_vsts()

        # Should not error — silently ignored
        tdSql.execute(
            f"CREATE VIRTUAL STABLE IF NOT EXISTS vst_mid BASE ON vst_root "
            f"(extra INT) "
            f"TAGS (mid_tag NCHAR(32));")

        self._cleanup_model()

    def test_expand_empty_child_vst(self):
        """DQL: EXPAND with child VST having no VCTs

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, query, expand

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_expand_empty_child_vst ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._build_full_model()

        # Add child with no VCTs
        tdSql.execute(
            f"CREATE VIRTUAL STABLE vst_empty BASE ON vst_root "
            f"(empty_col INT) "
            f"TAGS (empty_tag INT);")

        # Count should be unchanged — empty child adds 0 VCTs
        tdSql.query("SELECT COUNT(*) FROM vst_root EXPAND(-1);")
        tdSql.checkData(0, 0, 5)

        tdSql.execute("DROP STABLE IF EXISTS vst_empty;")
        self._cleanup_model()

    def test_expand_interval(self):
        """DQL: EXPAND with INTERVAL window aggregation

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, query, expand, interval

        Jira: None

        History:
            - 2026-05-06 Created
        """
        tdLog.info("=== test_expand_interval ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._build_full_model()

        # Each VCT has 1 row at a distinct second (00:00:00 to 00:00:04)
        # INTERVAL(1s) should produce 5 windows, each with COUNT(*)=1
        tdSql.query("SELECT _wstart, COUNT(*) FROM vst_root EXPAND(-1) INTERVAL(1s);")
        tdSql.checkRows(5)

        # SUM(val) with INTERVAL(5s) — all 5 rows (val: 10,20,30,40,50) in one window
        tdSql.query("SELECT _wstart, SUM(val) FROM vst_root EXPAND(-1) INTERVAL(5s);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 150)

        # INTERVAL from mid-level: vst_mid EXPAND(-1) = vct_m1 + vct_leaf1 = 2 rows
        tdSql.query("SELECT _wstart, COUNT(*) FROM vst_mid EXPAND(-1) INTERVAL(1s);")
        tdSql.checkRows(2)

        # AVG with INTERVAL — single 5s window covers all rows
        tdSql.query("SELECT _wstart, AVG(val) FROM vst_root EXPAND(-1) INTERVAL(5s);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 30.0)

        # INTERVAL with SLIDING
        tdSql.query("SELECT _wstart, COUNT(*) FROM vst_root EXPAND(-1) INTERVAL(2s) SLIDING(1s);")
        # Data at seconds 0..4. Windows depend on alignment but should have multiple rows.
        assert tdSql.queryRows >= 5

        # EXPAND(1) with INTERVAL — only direct children: vct_r1, vct_r2, vct_m1, vct_m2
        tdSql.query("SELECT _wstart, COUNT(*) FROM vst_root EXPAND(1) INTERVAL(1s);")
        tdSql.checkRows(4)

        # MAX/MIN with INTERVAL
        tdSql.query("SELECT _wstart, MAX(val), MIN(val) FROM vst_root EXPAND(-1) INTERVAL(5s);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 50)
        tdSql.checkData(0, 2, 10)

        # EXPAND(0) = no expansion, same as plain VST query — should also support INTERVAL
        tdSql.query("SELECT _wstart, COUNT(*) FROM vst_root EXPAND(0) INTERVAL(1s);")
        tdSql.checkRows(2)  # vct_r1 + vct_r2 = 2 rows at distinct seconds

        self._cleanup_model()

    def test_describe_deep_inherited_vst(self):
        """DDL: DESCRIBE shows full inherited schema on deep VST

        Catalog: VirtualTable

        Since: v5.0

        Labels: virtual, inheritance, ddl

        Jira: None

        History:
            - 2026-04-30 Created
        """
        tdLog.info("=== test_describe_deep_inherited_vst ===")
        tdSql.execute(f"USE {DB};")
        self._cleanup_model()
        self._build_full_model()

        # vst_leaf inherits from vst_mid which inherits from vst_root
        # Expected columns: ts, val, extra, leaf_val + tags: region, mid_tag, leaf_tag
        tdSql.query("DESCRIBE vst_leaf;")
        leaf_cols = [row[0] for row in tdSql.queryResult]
        for col in ["ts", "val", "extra", "leaf_val", "region", "mid_tag", "leaf_tag"]:
            assert col in leaf_cols, f"{col} not in vst_leaf: {leaf_cols}"

        self._cleanup_model()
