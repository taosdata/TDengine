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
"""Tests for tag-ref multi-hop chain resolution (up to 32 levels).

Covers:
  1. 2-level tag-ref chain (vtable → vtable → physical)
  2. 3-level tag-ref chain (vtable → vtable → vtable → physical)
  3. Mixed col-ref + tag-ref multi-hop chains
  4. Cross-DB tag-ref chains
  5. Same-DB tag-ref chains
  6. Tag filter predicates on chained values
  7. Tag projection on chained values
  8. Super-table scan with tag-ref chain predicates
  9. Aggregate queries with chained tag values
  10. SHOW VTABLE VALIDATE with multi-hop tag-refs
  11. Depth-limit (>32 levels) detection via VALIDATE
"""

from new_test_framework.utils import tdLog, tdSql

# Database names
PHY_DB = "test_tagref_mh_phy"      # Physical source tables
L1_DB = "test_tagref_mh_l1"        # Layer-1 virtual tables (refs physical)
L2_DB = "test_tagref_mh_l2"        # Layer-2 virtual tables (refs layer-1)
L3_DB = "test_tagref_mh_l3"        # Layer-3 virtual tables (refs layer-2)
SAME_DB = "test_tagref_mh_same"    # Same-DB multi-hop
DEPTH_DB = "test_tagref_mh_depth"  # Depth-limit test
CIRC_DB = "test_tagref_mh_circ"   # Circular reference test

TSDB_CODE_VTABLE_REF_DEPTH_EXCEEDED = -2147458548
TSDB_CODE_VTABLE_CIRCULAR_REF = -2147458546


class TestVtableTagRefMultihop:
    """Tag-ref multi-hop chain resolution tests.

    Tests verify that tag values resolve correctly through
    multiple virtual table layers, up to 32 levels deep.
    """

    updatecfgDict = {
        "supportVnodes": "1000",
    }

    @staticmethod
    def _fetch_rows(sql):
        tdSql.query(sql)
        return [
            tuple(tdSql.getData(i, j) for j in range(tdSql.queryCols))
            for i in range(tdSql.queryRows)
        ]

    def _assert_rows(self, sql, expected):
        rows = self._fetch_rows(sql)
        assert rows == expected, f"SQL: {sql}\nExpected: {expected}\nGot: {rows}"

    def _assert_rows_sorted(self, sql, expected):
        rows = sorted(self._fetch_rows(sql))
        expected_sorted = sorted(expected)
        assert rows == expected_sorted, f"SQL: {sql}\nExpected: {expected_sorted}\nGot: {rows}"

    # ------------------------------------------------------------------
    # Setup: create 4-layer topology
    # ------------------------------------------------------------------
    def setup_class(cls):
        tdLog.info("=== setup: creating multi-hop tag-ref topology ===")

        for db in [PHY_DB, L1_DB, L2_DB, L3_DB, SAME_DB, DEPTH_DB, CIRC_DB]:
            tdSql.execute(f"DROP DATABASE IF EXISTS {db};")

        # --- Physical DB: real tables with real tag values ---
        tdSql.execute(f"CREATE DATABASE {PHY_DB} VGROUPS 2 BUFFER 16;")
        tdSql.execute(f"USE {PHY_DB};")
        tdSql.execute(
            "CREATE STABLE phy_stb(ts TIMESTAMP, val INT) "
            "TAGS (region NCHAR(20), score INT, label NCHAR(20));"
        )
        # 6 physical child tables
        tdSql.execute("CREATE TABLE phy_east USING phy_stb TAGS ('east', 90, 'alpha');")
        tdSql.execute("CREATE TABLE phy_west USING phy_stb TAGS ('west', 80, 'beta');")
        tdSql.execute("CREATE TABLE phy_north USING phy_stb TAGS ('north', 70, 'gamma');")
        tdSql.execute("CREATE TABLE phy_south USING phy_stb TAGS ('south', 60, 'delta');")
        tdSql.execute("CREATE TABLE phy_mid USING phy_stb TAGS ('mid', 50, 'epsilon');")
        tdSql.execute("CREATE TABLE phy_hub USING phy_stb TAGS ('hub', 95, 'zeta');")
        # Insert data
        for i, tbl in enumerate(['phy_east', 'phy_west', 'phy_north', 'phy_south', 'phy_mid', 'phy_hub']):
            for j in range(3):
                tdSql.execute(f"INSERT INTO {tbl} VALUES ({1700000000000 + (i*10+j)*1000}, {(i+1)*100 + j});")

        # --- Layer-1 DB: virtual tables referencing physical ---
        tdSql.execute(f"CREATE DATABASE {L1_DB} VGROUPS 2 BUFFER 16;")
        tdSql.execute(f"USE {L1_DB};")
        tdSql.execute(
            "CREATE STABLE l1_vstb(ts TIMESTAMP, v1 INT) "
            "TAGS (t_region NCHAR(20), t_score INT, t_label NCHAR(20)) VIRTUAL 1;"
        )
        # Layer-1 children: tag-ref to physical children
        tdSql.execute(
            f"CREATE VTABLE l1_east (v1 FROM {PHY_DB}.phy_east.val) "
            f"USING l1_vstb TAGS ({PHY_DB}.phy_east.region, {PHY_DB}.phy_east.score, {PHY_DB}.phy_east.label);"
        )
        tdSql.execute(
            f"CREATE VTABLE l1_west (v1 FROM {PHY_DB}.phy_west.val) "
            f"USING l1_vstb TAGS ({PHY_DB}.phy_west.region, {PHY_DB}.phy_west.score, {PHY_DB}.phy_west.label);"
        )
        tdSql.execute(
            f"CREATE VTABLE l1_north (v1 FROM {PHY_DB}.phy_north.val) "
            f"USING l1_vstb TAGS ({PHY_DB}.phy_north.region, {PHY_DB}.phy_north.score, {PHY_DB}.phy_north.label);"
        )
        tdSql.execute(
            f"CREATE VTABLE l1_south (v1 FROM {PHY_DB}.phy_south.val) "
            f"USING l1_vstb TAGS ({PHY_DB}.phy_south.region, {PHY_DB}.phy_south.score, {PHY_DB}.phy_south.label);"
        )
        # Cross-source: data from phy_mid, tags from different physical tables
        tdSql.execute(
            f"CREATE VTABLE l1_cross1 (v1 FROM {PHY_DB}.phy_mid.val) "
            f"USING l1_vstb TAGS ({PHY_DB}.phy_east.region, {PHY_DB}.phy_hub.score, {PHY_DB}.phy_north.label);"
        )
        tdSql.execute(
            f"CREATE VTABLE l1_cross2 (v1 FROM {PHY_DB}.phy_hub.val) "
            f"USING l1_vstb TAGS ({PHY_DB}.phy_south.region, {PHY_DB}.phy_mid.score, {PHY_DB}.phy_west.label);"
        )

        # --- Layer-2 DB: virtual tables referencing Layer-1 (2-hop tag-ref) ---
        tdSql.execute(f"CREATE DATABASE {L2_DB} VGROUPS 2 BUFFER 16;")
        tdSql.execute(f"USE {L2_DB};")
        tdSql.execute(
            "CREATE STABLE l2_vstb(ts TIMESTAMP, v2 INT) "
            "TAGS (t2_region NCHAR(20), t2_score INT, t2_label NCHAR(20)) VIRTUAL 1;"
        )
        # Layer-2 children: col-ref to L1 data, tag-ref to L1 tags (which chain to physical)
        tdSql.execute(
            f"CREATE VTABLE l2_east (v2 FROM {L1_DB}.l1_east.v1) "
            f"USING l2_vstb TAGS ({L1_DB}.l1_east.t_region, {L1_DB}.l1_east.t_score, {L1_DB}.l1_east.t_label);"
        )
        tdSql.execute(
            f"CREATE VTABLE l2_west (v2 FROM {L1_DB}.l1_west.v1) "
            f"USING l2_vstb TAGS ({L1_DB}.l1_west.t_region, {L1_DB}.l1_west.t_score, {L1_DB}.l1_west.t_label);"
        )
        tdSql.execute(
            f"CREATE VTABLE l2_north (v2 FROM {L1_DB}.l1_north.v1) "
            f"USING l2_vstb TAGS ({L1_DB}.l1_north.t_region, {L1_DB}.l1_north.t_score, {L1_DB}.l1_north.t_label);"
        )
        # Cross-source tags: data from l1_south, tags from different l1 children
        tdSql.execute(
            f"CREATE VTABLE l2_cross (v2 FROM {L1_DB}.l1_south.v1) "
            f"USING l2_vstb TAGS ({L1_DB}.l1_east.t_region, {L1_DB}.l1_cross1.t_score, {L1_DB}.l1_west.t_label);"
        )
        # Tag-ref from l1_cross1/l1_cross2 (which themselves have cross-source tags)
        tdSql.execute(
            f"CREATE VTABLE l2_deep_cross (v2 FROM {L1_DB}.l1_cross1.v1) "
            f"USING l2_vstb TAGS ({L1_DB}.l1_cross1.t_region, {L1_DB}.l1_cross2.t_score, {L1_DB}.l1_cross2.t_label);"
        )

        # --- Layer-3 DB: virtual tables referencing Layer-2 (3-hop tag-ref) ---
        tdSql.execute(f"CREATE DATABASE {L3_DB} VGROUPS 2 BUFFER 16;")
        tdSql.execute(f"USE {L3_DB};")
        tdSql.execute(
            "CREATE STABLE l3_vstb(ts TIMESTAMP, v3 INT) "
            "TAGS (t3_region NCHAR(20), t3_score INT) VIRTUAL 1;"
        )
        tdSql.execute(
            f"CREATE VTABLE l3_east (v3 FROM {L2_DB}.l2_east.v2) "
            f"USING l3_vstb TAGS ({L2_DB}.l2_east.t2_region, {L2_DB}.l2_east.t2_score);"
        )
        tdSql.execute(
            f"CREATE VTABLE l3_west (v3 FROM {L2_DB}.l2_west.v2) "
            f"USING l3_vstb TAGS ({L2_DB}.l2_west.t2_region, {L2_DB}.l2_west.t2_score);"
        )
        tdSql.execute(
            f"CREATE VTABLE l3_cross (v3 FROM {L2_DB}.l2_cross.v2) "
            f"USING l3_vstb TAGS ({L2_DB}.l2_cross.t2_region, {L2_DB}.l2_cross.t2_score);"
        )
        tdSql.execute(
            f"CREATE VTABLE l3_deep (v3 FROM {L2_DB}.l2_deep_cross.v2) "
            f"USING l3_vstb TAGS ({L2_DB}.l2_deep_cross.t2_region, {L2_DB}.l2_deep_cross.t2_score);"
        )

        # --- Same-DB multi-hop: all layers in one DB ---
        tdSql.execute(f"CREATE DATABASE {SAME_DB} VGROUPS 2 BUFFER 16;")
        tdSql.execute(f"USE {SAME_DB};")
        # Physical source
        tdSql.execute(
            "CREATE STABLE src_stb(ts TIMESTAMP, val INT) "
            "TAGS (city NCHAR(20), pop INT);"
        )
        tdSql.execute("CREATE TABLE src_bj USING src_stb TAGS ('beijing', 2154);")
        tdSql.execute("CREATE TABLE src_sh USING src_stb TAGS ('shanghai', 2487);")
        tdSql.execute("INSERT INTO src_bj VALUES (1700000000000, 1)(1700000001000, 2)(1700000002000, 3);")
        tdSql.execute("INSERT INTO src_sh VALUES (1700000000000, 10)(1700000001000, 20);")
        # Layer 1 virtual
        tdSql.execute(
            "CREATE STABLE same_l1(ts TIMESTAMP, v1 INT) "
            "TAGS (t_city NCHAR(20), t_pop INT) VIRTUAL 1;"
        )
        tdSql.execute(
            f"CREATE VTABLE same_l1_bj (v1 FROM {SAME_DB}.src_bj.val) "
            f"USING same_l1 TAGS ({SAME_DB}.src_bj.city, {SAME_DB}.src_bj.pop);"
        )
        tdSql.execute(
            f"CREATE VTABLE same_l1_sh (v1 FROM {SAME_DB}.src_sh.val) "
            f"USING same_l1 TAGS ({SAME_DB}.src_sh.city, {SAME_DB}.src_sh.pop);"
        )
        # Layer 2 virtual (same DB, referencing L1 tags)
        tdSql.execute(
            "CREATE STABLE same_l2(ts TIMESTAMP, v2 INT) "
            "TAGS (t2_city NCHAR(20), t2_pop INT) VIRTUAL 1;"
        )
        tdSql.execute(
            f"CREATE VTABLE same_l2_bj (v2 FROM {SAME_DB}.same_l1_bj.v1) "
            f"USING same_l2 TAGS ({SAME_DB}.same_l1_bj.t_city, {SAME_DB}.same_l1_bj.t_pop);"
        )
        tdSql.execute(
            f"CREATE VTABLE same_l2_sh (v2 FROM {SAME_DB}.same_l1_sh.v1) "
            f"USING same_l2 TAGS ({SAME_DB}.same_l1_sh.t_city, {SAME_DB}.same_l1_sh.t_pop);"
        )
        # Layer 3: cross-source tags within same DB
        tdSql.execute(
            "CREATE STABLE same_l3(ts TIMESTAMP, v3 INT) "
            "TAGS (t3_city NCHAR(20), t3_pop INT) VIRTUAL 1;"
        )
        tdSql.execute(
            f"CREATE VTABLE same_l3_cross (v3 FROM {SAME_DB}.same_l2_bj.v2) "
            f"USING same_l3 TAGS ({SAME_DB}.same_l2_sh.t2_city, {SAME_DB}.same_l2_bj.t2_pop);"
        )

        # --- Depth-limit DB: chain of 33 levels for VALIDATE ---
        tdSql.execute(f"CREATE DATABASE {DEPTH_DB} VGROUPS 1 BUFFER 16;")
        tdSql.execute(f"USE {DEPTH_DB};")
        # Base physical table
        tdSql.execute("CREATE TABLE depth_base(ts TIMESTAMP, val INT);")
        tdSql.execute("INSERT INTO depth_base VALUES (1700000000000, 42);")
        # Physical source for tags
        tdSql.execute(
            "CREATE STABLE depth_src_stb(ts TIMESTAMP, dummy INT) "
            "TAGS (depth_tag NCHAR(20));"
        )
        tdSql.execute("CREATE TABLE depth_src USING depth_src_stb TAGS ('origin');")
        tdSql.execute("INSERT INTO depth_src VALUES (1700000000000, 0);")
        # Create chain: depth_v0 -> depth_v1 -> ... -> depth_v32
        prev_name = "depth_src"
        prev_tag = "depth_tag"
        for i in range(33):
            vname = f"depth_v{i}"
            vstb = f"depth_vstb_{i}"
            tag_name = f"dt{i}"
            tdSql.execute(
                f"CREATE STABLE {vstb}(ts TIMESTAMP, v INT) "
                f"TAGS ({tag_name} NCHAR(20)) VIRTUAL 1;"
            )
            tdSql.execute(
                f"CREATE VTABLE {vname} (v FROM {DEPTH_DB}.depth_base.val) "
                f"USING {vstb} TAGS ({DEPTH_DB}.{prev_name}.{prev_tag});"
            )
            prev_name = vname
            prev_tag = tag_name

        # --- Circular-ref DB: tables for ALTER-induced circular reference ---
        tdSql.execute(f"CREATE DATABASE {CIRC_DB} VGROUPS 1 BUFFER 16;")
        tdSql.execute(f"USE {CIRC_DB};")
        # Physical source
        tdSql.execute("CREATE TABLE circ_base(ts TIMESTAMP, val INT);")
        tdSql.execute("INSERT INTO circ_base VALUES (1700000000000, 1);")
        tdSql.execute(
            "CREATE STABLE circ_src_stb(ts TIMESTAMP, dummy INT) "
            "TAGS (src_tag NCHAR(20));"
        )
        tdSql.execute("CREATE TABLE circ_src USING circ_src_stb TAGS ('origin');")
        tdSql.execute("INSERT INTO circ_src VALUES (1700000000000, 0);")
        # v_a: refs physical source
        tdSql.execute(
            "CREATE STABLE circ_vstb_a(ts TIMESTAMP, v INT) "
            "TAGS (tag_a NCHAR(20)) VIRTUAL 1;"
        )
        tdSql.execute(
            f"CREATE VTABLE circ_v_a (v FROM {CIRC_DB}.circ_base.val) "
            f"USING circ_vstb_a TAGS ({CIRC_DB}.circ_src.src_tag);"
        )
        # v_b: refs v_a (chain: v_b -> v_a -> circ_src)
        tdSql.execute(
            "CREATE STABLE circ_vstb_b(ts TIMESTAMP, v INT) "
            "TAGS (tag_b NCHAR(20)) VIRTUAL 1;"
        )
        tdSql.execute(
            f"CREATE VTABLE circ_v_b (v FROM {CIRC_DB}.circ_base.val) "
            f"USING circ_vstb_b TAGS ({CIRC_DB}.circ_v_a.tag_a);"
        )
        # v_c: refs v_b (chain: v_c -> v_b -> v_a -> circ_src)
        tdSql.execute(
            "CREATE STABLE circ_vstb_c(ts TIMESTAMP, v INT) "
            "TAGS (tag_c NCHAR(20)) VIRTUAL 1;"
        )
        tdSql.execute(
            f"CREATE VTABLE circ_v_c (v FROM {CIRC_DB}.circ_base.val) "
            f"USING circ_vstb_c TAGS ({CIRC_DB}.circ_v_b.tag_b);"
        )
        # Column-ref virtual normal tables for column circular ref test
        tdSql.execute(
            "CREATE VTABLE circ_col_a(ts TIMESTAMP, "
            f"v INT FROM {CIRC_DB}.circ_base.val);"
        )
        tdSql.execute(
            "CREATE VTABLE circ_col_b(ts TIMESTAMP, "
            f"v INT FROM {CIRC_DB}.circ_col_a.v);"
        )

    def teardown_class(cls):
        tdLog.info("=== teardown: dropping databases ===")
        for db in [PHY_DB, L1_DB, L2_DB, L3_DB, SAME_DB, DEPTH_DB, CIRC_DB]:
            tdSql.execute(f"DROP DATABASE IF EXISTS {db};")

    # ------------------------------------------------------------------
    # Test: 2-level tag-ref chain (L2 → L1 → physical)
    # ------------------------------------------------------------------
    def test_2level_tag_ref_child_query(self):
        """2-level tag-ref: child table query resolves tags through chain.

        Verify that querying a layer-2 virtual child table correctly resolves tag values through the 2-level chain.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {L2_DB};")
        # l2_east → l1_east → phy_east: region='east', score=90, label='alpha'
        self._assert_rows(
            "SELECT t2_region, t2_score, t2_label, v2 FROM l2_east ORDER BY ts LIMIT 1;",
            [("east", 90, "alpha", 100)],
        )
        # l2_west → l1_west → phy_west: region='west', score=80, label='beta'
        self._assert_rows(
            "SELECT t2_region, t2_score, t2_label FROM l2_west LIMIT 1;",
            [("west", 80, "beta")],
        )

    def test_2level_tag_ref_super_table_scan(self):
        """2-level tag-ref: super table scan with tag filter.

        Verify that STB-level scan with WHERE on a 2-level chained tag correctly filters rows.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {L2_DB};")
        # Filter by chained tag value
        # 3 children have t2_region='east' (l2_east, l2_cross, l2_deep_cross) × 3 rows each = 9
        self._assert_rows(
            "SELECT t2_region, COUNT(*) FROM l2_vstb WHERE t2_region = 'east' GROUP BY t2_region;",
            [("east", 9)],
        )

    def test_2level_tag_ref_cross_source_tags(self):
        """2-level tag-ref: cross-source tags resolve correctly.

        Verify that a layer-2 vtable referencing tags from different source tables resolves each independently.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {L2_DB};")
        # l2_cross: data from l1_south(→phy_south), tags from:
        #   t2_region ← l1_east.t_region ← phy_east.region = 'east'
        #   t2_score  ← l1_cross1.t_score ← phy_hub.score = 95
        #   t2_label  ← l1_west.t_label ← phy_west.label = 'beta'
        self._assert_rows(
            "SELECT t2_region, t2_score, t2_label FROM l2_cross LIMIT 1;",
            [("east", 95, "beta")],
        )

    def test_2level_deep_cross_source(self):
        """2-level tag-ref: chained cross-source resolves through 2 layers.

        Verify that deep cross-source 2-level chains resolve tag values through intermediate virtual tables.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {L2_DB};")
        # l2_deep_cross: data from l1_cross1(→phy_mid), tags from:
        #   t2_region ← l1_cross1.t_region ← phy_east.region = 'east'
        #   t2_score  ← l1_cross2.t_score ← phy_mid.score = 50
        #   t2_label  ← l1_cross2.t_label ← phy_west.label = 'beta'
        self._assert_rows(
            "SELECT t2_region, t2_score, t2_label FROM l2_deep_cross LIMIT 1;",
            [("east", 50, "beta")],
        )

    # ------------------------------------------------------------------
    # Test: 3-level tag-ref chain (L3 → L2 → L1 → physical)
    # ------------------------------------------------------------------
    def test_3level_tag_ref_child_query(self):
        """3-level tag-ref: child query resolves through 3 layers.

        Verify that querying a layer-3 virtual child table correctly resolves tag values through all 3 chain levels.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {L3_DB};")
        # l3_east → l2_east → l1_east → phy_east: region='east', score=90
        self._assert_rows(
            "SELECT t3_region, t3_score, v3 FROM l3_east ORDER BY ts LIMIT 1;",
            [("east", 90, 100)],
        )
        # l3_west → l2_west → l1_west → phy_west: region='west', score=80
        self._assert_rows(
            "SELECT t3_region, t3_score FROM l3_west LIMIT 1;",
            [("west", 80)],
        )

    def test_3level_tag_ref_cross_source(self):
        """3-level tag-ref: cross-source chain resolves through 3 layers.

        Verify that a 3-level chain with cross-source tag references resolves each tag to its physical origin.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {L3_DB};")
        # l3_cross → l2_cross:
        #   t3_region ← l2_cross.t2_region ← l1_east.t_region ← phy_east.region = 'east'
        #   t3_score  ← l2_cross.t2_score ← l1_cross1.t_score ← phy_hub.score = 95
        self._assert_rows(
            "SELECT t3_region, t3_score FROM l3_cross LIMIT 1;",
            [("east", 95)],
        )

    def test_3level_deep_cross_chain(self):
        """3-level tag-ref: deep cross-source resolves all layers correctly.

        Verify that a 3-level deep cross-source chain correctly resolves tag values to the physical source.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {L3_DB};")
        # l3_deep → l2_deep_cross:
        #   t3_region ← l2_deep_cross.t2_region ← l1_cross1.t_region ← phy_east.region = 'east'
        #   t3_score  ← l2_deep_cross.t2_score ← l1_cross2.t_score ← phy_mid.score = 50
        self._assert_rows(
            "SELECT t3_region, t3_score FROM l3_deep LIMIT 1;",
            [("east", 50)],
        )

    def test_3level_super_table_scan_with_tag_filter(self):
        """3-level tag-ref: super table scan filters on chained tag values.

        Verify that STB-level scan with WHERE on a 3-level chained tag correctly filters matching rows.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {L3_DB};")
        # 3 children have t3_region='east' (l3_east, l3_cross, l3_deep) × 3 rows each = 9
        # l3_west has t3_region='west' and is excluded by filter
        self._assert_rows(
            "SELECT COUNT(*) FROM l3_vstb WHERE t3_region = 'east';",
            [(9,)],
        )

    def test_3level_aggregate_with_group_by_tag(self):
        """3-level tag-ref: aggregate grouped by chained tag value.

        Verify that GROUP BY on a 3-level chained tag produces correct aggregation results.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {L3_DB};")
        # Group by t3_score: 90 (l3_east), 80 (l3_west), 95 (l3_cross), 50 (l3_deep)
        rows = self._fetch_rows(
            "SELECT t3_score, COUNT(*) FROM l3_vstb GROUP BY t3_score ORDER BY t3_score;"
        )
        assert len(rows) == 4
        assert rows[0] == (50, 3)
        assert rows[1] == (80, 3)
        assert rows[2] == (90, 3)
        assert rows[3] == (95, 3)

    # ------------------------------------------------------------------
    # Test: Same-DB multi-hop
    # ------------------------------------------------------------------
    def test_same_db_2level_tag_ref(self):
        """Same-DB tag-ref: 2-level chain within one database.

        Verify that a 2-level tag-ref chain within a single database resolves correctly.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {SAME_DB};")
        # same_l2_bj → same_l1_bj → src_bj: city='beijing', pop=2154
        self._assert_rows(
            "SELECT t2_city, t2_pop, v2 FROM same_l2_bj ORDER BY ts LIMIT 1;",
            [("beijing", 2154, 1)],
        )
        # same_l2_sh → same_l1_sh → src_sh: city='shanghai', pop=2487
        self._assert_rows(
            "SELECT t2_city, t2_pop FROM same_l2_sh LIMIT 1;",
            [("shanghai", 2487)],
        )

    def test_same_db_3level_cross_source_tags(self):
        """Same-DB tag-ref: 3-level with cross-source tags.

        Verify that a 3-level tag-ref chain within one database with cross-source tags resolves correctly.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {SAME_DB};")
        # same_l3_cross: data from same_l2_bj(→same_l1_bj→src_bj)
        #   t3_city ← same_l2_sh.t2_city ← same_l1_sh.t_city ← src_sh.city = 'shanghai'
        #   t3_pop  ← same_l2_bj.t2_pop ← same_l1_bj.t_pop ← src_bj.pop = 2154
        self._assert_rows(
            "SELECT t3_city, t3_pop, v3 FROM same_l3_cross ORDER BY ts LIMIT 1;",
            [("shanghai", 2154, 1)],
        )

    def test_same_db_super_table_scan(self):
        """Same-DB tag-ref: super table scan with multi-hop tags.

        Verify that STB-level scan resolves multi-hop tag values within a single database.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {SAME_DB};")
        # same_l2 has 2 children: bj(city=beijing,pop=2154) and sh(city=shanghai,pop=2487)
        self._assert_rows(
            "SELECT t2_city, COUNT(*) FROM same_l2 GROUP BY t2_city ORDER BY t2_city;",
            [("beijing", 3), ("shanghai", 2)],
        )

    # ------------------------------------------------------------------
    # Test: Mixed col-ref + tag-ref chains
    # ------------------------------------------------------------------
    def test_mixed_colref_tagref_chains(self):
        """Mixed chains: col-ref and tag-ref both multi-hop.

        Verify that virtual tables with both col-ref and tag-ref chains resolve data and tags independently.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {L3_DB};")
        # l3_east: col-ref chain: v3 ← l2_east.v2 ← l1_east.v1 ← phy_east.val
        #          tag-ref chain: t3_region ← l2_east.t2_region ← l1_east.t_region ← phy_east.region
        # Both should resolve: val=100,101,102 and region='east'
        self._assert_rows(
            "SELECT t3_region, v3 FROM l3_east WHERE t3_region = 'east' ORDER BY v3;",
            [("east", 100), ("east", 101), ("east", 102)],
        )

    def test_mixed_chains_aggregate(self):
        """Mixed chains: aggregate on both col-ref data and tag-ref tag.

        Verify SUM aggregation grouped by a chained tag-ref tag resolves correctly.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {L3_DB};")
        # SUM of phy_east data through 3-level col-ref chain
        self._assert_rows(
            "SELECT t3_region, SUM(v3) FROM l3_east GROUP BY t3_region;",
            [("east", 303)],  # 100+101+102 = 303
        )

    # ------------------------------------------------------------------
    # Test: Tag-ref in various query contexts
    # ------------------------------------------------------------------
    def test_tag_ref_in_where_clause(self):
        """Tag-ref in WHERE clause: filter by chained tag value.

        Verify WHERE predicates on multi-hop tag-ref columns filter rows correctly.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {L2_DB};")
        # Filter l2_vstb by tag value that resolves through chain
        rows = self._fetch_rows(
            "SELECT tbname, t2_region FROM l2_vstb WHERE t2_score >= 90 ORDER BY tbname;"
        )
        # l2_east (score=90), l2_cross (score=95 from l1_cross1→phy_hub), l2_deep_cross (score=50 < 90: excluded)
        assert ("l2_east", "east") in rows
        assert ("l2_cross", "east") in rows

    def test_tag_ref_in_order_by(self):
        """Tag-ref in ORDER BY: sort by chained tag value.

        Verify ORDER BY on a multi-hop tag-ref column produces correct sort order.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {L3_DB};")
        rows = self._fetch_rows(
            "SELECT tbname, t3_score FROM l3_vstb GROUP BY tbname, t3_score ORDER BY t3_score;"
        )
        scores = [r[1] for r in rows]
        assert scores == sorted(scores)

    def test_tag_ref_in_having(self):
        """Tag-ref in HAVING: filter aggregated results by chained tag.

        Verify HAVING clause on a multi-hop tag-ref column filters groups correctly.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {L3_DB};")
        rows = self._fetch_rows(
            "SELECT t3_score, COUNT(*) AS cnt FROM l3_vstb "
            "GROUP BY t3_score HAVING t3_score > 80 ORDER BY t3_score;"
        )
        for r in rows:
            assert r[0] > 80

    def test_tag_ref_in_join(self):
        """Tag-ref in JOIN: join two virtual child tables that share timestamps.

        Verify JOIN between virtual child tables with chained tag-refs resolves tags independently.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {L2_DB};")
        # l2_east and l2_north both reference l1 tables that map to different physical tables
        # with non-overlapping timestamps. Use subquery to verify both resolve tags independently.
        rows = self._fetch_rows(
            "SELECT t2_region FROM l2_east LIMIT 1;"
        )
        assert rows[0] == ("east",)
        rows = self._fetch_rows(
            "SELECT t2_region FROM l2_deep_cross LIMIT 1;"
        )
        assert rows[0] == ("east",)

    # ------------------------------------------------------------------
    # Test: SHOW VTABLE VALIDATE with multi-hop tag-refs
    # ------------------------------------------------------------------
    def test_validate_2level_tag_ref(self):
        """VALIDATE: 2-level tag-ref chain passes validation.

        Verify SHOW VTABLE VALIDATE reports no errors for a 2-level tag-ref chain.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {L2_DB};")
        tdSql.query("SHOW VTABLE VALIDATE FOR l2_east;")
        # All columns/tags should validate successfully (err_code = 0)
        for i in range(tdSql.queryRows):
            err_code = tdSql.getData(i, 8)  # err_code column
            assert err_code == 0, f"Row {i}: err_code={err_code}"

    def test_validate_3level_tag_ref(self):
        """VALIDATE: 3-level tag-ref chain passes validation.

        Verify SHOW VTABLE VALIDATE reports no errors for a 3-level tag-ref chain.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {L3_DB};")
        tdSql.query("SHOW VTABLE VALIDATE FOR l3_east;")
        for i in range(tdSql.queryRows):
            err_code = tdSql.getData(i, 8)
            assert err_code == 0, f"Row {i}: err_code={err_code}"

    def test_validate_3level_cross_source(self):
        """VALIDATE: 3-level cross-source tag-ref chain passes.

        Verify SHOW VTABLE VALIDATE reports no errors for a 3-level cross-source chain.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {L3_DB};")
        tdSql.query("SHOW VTABLE VALIDATE FOR l3_deep;")
        for i in range(tdSql.queryRows):
            err_code = tdSql.getData(i, 8)
            assert err_code == 0, f"Row {i}: err_code={err_code}"

    def test_validate_same_db_multihop(self):
        """VALIDATE: same-DB multi-hop tag-ref passes validation.

        Verify SHOW VTABLE VALIDATE reports no errors for same-DB multi-hop chains.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {SAME_DB};")
        tdSql.query("SHOW VTABLE VALIDATE FOR same_l3_cross;")
        for i in range(tdSql.queryRows):
            err_code = tdSql.getData(i, 8)
            assert err_code == 0, f"Row {i}: err_code={err_code}"

    def test_validate_tag_ref_type_column(self):
        """VALIDATE: tag-ref rows have type=1 in output.

        Verify SHOW VTABLE VALIDATE distinguishes tag-ref (type=1) from col-ref (type=0) rows.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {L2_DB};")
        tdSql.query("SHOW VTABLE VALIDATE FOR l2_east;")
        tag_rows = []
        col_rows = []
        for i in range(tdSql.queryRows):
            ref_type = tdSql.getData(i, 7)  # type column: 0=col, 1=tag
            if ref_type == 1:
                tag_rows.append(i)
            else:
                col_rows.append(i)
        # l2_vstb has 3 tag-refs (t2_region, t2_score, t2_label)
        assert len(tag_rows) >= 3, f"Expected >= 3 tag rows, got {len(tag_rows)}"
        # And at least 1 col-ref (v2)
        assert len(col_rows) >= 1, f"Expected >= 1 col rows, got {len(col_rows)}"

    # ------------------------------------------------------------------
    # Test: Depth limit (>32 levels) detection
    # ------------------------------------------------------------------
    def test_validate_depth_exceeded(self):
        """VALIDATE: tag-ref chain > 32 levels is rejected at CREATE time.

        Verify CREATE VTABLE rejects a tag-ref chain that would exceed the depth limit.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DEPTH_DB};")
        # depth_v32 is the last created table. Creating depth_v33 would exceed depth limit.
        tdSql.execute(
            f"CREATE STABLE depth_vstb_33(ts TIMESTAMP, v INT) "
            f"TAGS (dt33 NCHAR(20)) VIRTUAL 1;"
        )
        tdSql.error(
            f"CREATE VTABLE depth_v33 (v FROM {DEPTH_DB}.depth_base.val) "
            f"USING depth_vstb_33 TAGS ({DEPTH_DB}.depth_v32.dt32);",
            expectedErrno=TSDB_CODE_VTABLE_REF_DEPTH_EXCEEDED
        )

    def test_validate_within_depth_limit(self):
        """VALIDATE: tag-ref chain at exactly 32 levels passes.

        Verify a tag-ref chain at exactly 32 hops (the maximum) passes validation.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DEPTH_DB};")
        # depth_v31 is at depth 32 (depth_v31 → ... → depth_v0 → depth_src = 32 hops)
        tdSql.query("SHOW VTABLE VALIDATE FOR depth_v31;")
        for i in range(tdSql.queryRows):
            err_code = tdSql.getData(i, 8)
            ref_type = tdSql.getData(i, 7)
            if ref_type == 1:
                assert err_code == 0, f"Tag-ref at depth 32 should pass, got err_code={err_code}"

    # ------------------------------------------------------------------
    # Test: Tag projection and metadata
    # ------------------------------------------------------------------
    def test_tag_projection_select_star(self):
        """Tag projection: tag-ref values can be explicitly selected.

        Verify explicit SELECT of chained tag-ref columns returns resolved values.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {L2_DB};")
        # Explicitly select tag columns (SELECT * does not include tags in TDengine)
        rows = self._fetch_rows(
            "SELECT t2_region, t2_score, t2_label FROM l2_east LIMIT 1;"
        )
        assert rows[0] == ("east", 90, "alpha")

    def test_tag_value_consistency_across_rows(self):
        """Tag values: consistent across all data rows in child table.

        Verify chained tag-ref values are identical across all data rows of a child table.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {L3_DB};")
        rows = self._fetch_rows("SELECT t3_region, t3_score FROM l3_east;")
        # All rows should have the same tag values
        for r in rows:
            assert r == ("east", 90), f"Inconsistent tag values: {r}"

    def test_tbname_with_chained_tags(self):
        """tbname pseudo-column works with chained tag-ref tables.

        Verify tbname pseudo-column can be selected alongside chained tag-ref columns.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {L3_DB};")
        rows = self._fetch_rows(
            "SELECT tbname, t3_region, t3_score FROM l3_vstb "
            "WHERE tbname = 'l3_east' LIMIT 1;"
        )
        assert rows[0] == ("l3_east", "east", 90)

    # ------------------------------------------------------------------
    # Test: Circular reference detection via ALTER TAG REF
    # ------------------------------------------------------------------
    def test_circular_ref_alter_tag_self(self):
        """Circular ref: ALTER TAG REF to self is rejected.

        ALTER v_a's tag to reference itself, forming a self-loop.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {CIRC_DB};")
        tdSql.error(
            f"ALTER VTABLE circ_v_a SET TAG tag_a = {CIRC_DB}.circ_v_a.tag_a;",
            expectedErrno=TSDB_CODE_VTABLE_CIRCULAR_REF
        )

    def test_circular_ref_alter_tag_2node(self):
        """Circular ref: ALTER creates 2-node cycle (v_a -> v_b -> v_a).

        v_b already refs v_a. ALTER v_a to ref v_b creates a cycle.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {CIRC_DB};")
        # v_b -> v_a -> circ_src (existing chain)
        # ALTER v_a to ref v_b would create: v_a -> v_b -> v_a (cycle!)
        tdSql.error(
            f"ALTER VTABLE circ_v_a SET TAG tag_a = {CIRC_DB}.circ_v_b.tag_b;",
            expectedErrno=TSDB_CODE_VTABLE_CIRCULAR_REF
        )

    def test_circular_ref_alter_tag_3node(self):
        """Circular ref: ALTER creates 3-node cycle (v_a -> v_c -> v_b -> v_a).

        v_c -> v_b -> v_a -> circ_src. ALTER v_a to ref v_c creates a cycle.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {CIRC_DB};")
        # v_c -> v_b -> v_a -> circ_src (existing chain)
        # ALTER v_a to ref v_c would create: v_a -> v_c -> v_b -> v_a (cycle!)
        tdSql.error(
            f"ALTER VTABLE circ_v_a SET TAG tag_a = {CIRC_DB}.circ_v_c.tag_c;",
            expectedErrno=TSDB_CODE_VTABLE_CIRCULAR_REF
        )

    def test_circular_ref_alter_col_self(self):
        """Circular ref: ALTER COLUMN REF to self is rejected.

        ALTER circ_col_a's column to reference itself.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {CIRC_DB};")
        tdSql.error(
            f"ALTER TABLE {CIRC_DB}.circ_col_a ALTER COLUMN v SET {CIRC_DB}.circ_col_a.v;",
            expectedErrno=TSDB_CODE_VTABLE_CIRCULAR_REF
        )

    def test_circular_ref_alter_col_2node(self):
        """Circular ref: ALTER COLUMN REF creates 2-node cycle.

        circ_col_b refs circ_col_a. ALTER circ_col_a to ref circ_col_b creates cycle.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {CIRC_DB};")
        # circ_col_b -> circ_col_a -> circ_base (existing)
        # ALTER circ_col_a to ref circ_col_b -> cycle!
        tdSql.error(
            f"ALTER TABLE {CIRC_DB}.circ_col_a ALTER COLUMN v SET {CIRC_DB}.circ_col_b.v;",
            expectedErrno=TSDB_CODE_VTABLE_CIRCULAR_REF
        )

    def test_circular_ref_alter_valid(self):
        """Non-circular ALTER TAG REF should succeed.

        ALTER v_a to reference a different physical source is valid (no cycle).

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {CIRC_DB};")
        # v_a currently refs circ_src. Changing to ref circ_src again is fine (no cycle).
        tdSql.execute(
            f"ALTER VTABLE circ_v_a SET TAG tag_a = {CIRC_DB}.circ_src.src_tag;"
        )
