###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved
#
#  This file is proprietary and confidential to TAOS Technologies, Inc.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
"""ins_tags / SHOW TAGS for virtual child tables with tag references.

Tests sysTableUserTagsFillOneTableTags + sysTagsResolveRefTagVal paths:
  - Full scan:  SELECT * FROM information_schema.ins_tags
  - Single-table optimization: WHERE table_name = 'xxx'
  - SHOW TAGS rewrite: SHOW TAGS FROM vtable

Topology (same-DB + cross-DB):

  DB (same-database)
  ┌──────────────────────────────────────────────────────────────┐
  │ src_stb                                                       │
  │  ├ s_bj (BJ, code=100, level=1, score=9.5, active=true)      │
  │  ├ s_sh (SH, code=200, level=2, score=8.8, active=false)     │
  │  └ s_sz (SZ, code=300, level=3, score=7.2, active=true)      │
  │                                                               │
  │ vstb_a (all-ref)                                              │
  │  ├ va_bj ──tag-ref──► s_bj  (positional)                     │
  │  ├ va_sh ──tag-ref──► s_sh  (specific)                       │
  │  └ va_sz ──tag-ref──► s_sz  (FROM)                           │
  │ vstb_b (mixed)                                                │
  │  ├ vb_mix1  local=1 + ref from s_bj                           │
  │  └ vb_mix2  local=2 + ref from s_sh                           │
  │ vstb_c (literal)                                              │
  │  └ vc_lit  all literal, no refs                               │
  │ vstb_chain (multi-layer)                                      │
  │  ├ chain_01 ──tag-ref──► s_bj  (layer-1)                     │
  │  ├ chain_02 ──col-ref──► chain_01 (layer-2, literal tags)    │
  │  └ chain_03 ──col-ref──► chain_02 (layer-3, literal tags)    │
  └──────────────────────────────────────────────────────────────┘

  DB_ALPHA                 DB_BETA                  DB_GAMMA
  ┌──────────────┐        ┌────────────────┐       ┌──────────────────┐
  │ alpha_stb     │        │ vstb_b1        │       │ vstb_g1          │
  │  ├ ac0 (BJ)   │◄───────│  ├ vb_a0       │       │  ├ vg_direct     │◄── ALPHA
  │  ├ ac1 (SH)   │ tag    │  ├ vb_a1       │       │ vstb_g2          │
  │  └ ac2 (GZ)   │ ref    │  └ vb_mixed    │       │  ├ vg_chain_01   │──► ALPHA(tag)
  │               │        └────────────────┘       │  └ vg_chain_02   │──► GAMMA(col)
  │ vstb_a2       │ tag           ▲                  └──────────────────┘
  │  ├ va_b0 ─────│──────► beta_stb │
  │  └ va_b1      │ ref    │  ├ bc0 (UK)   │
  └──────────────┘        │  └ bc1 (US)   │
       ▲                  └────────────────┘
       │                        │
       └──── bidir tag-ref ─────┘
"""

from new_test_framework.utils import tdLog, tdSql

DB = "td_tagref_ins_tags"
DB_ALPHA = "td_crossdb_alpha"
DB_BETA = "td_crossdb_beta"
DB_GAMMA = "td_crossdb_gamma"
ALL_CROSS_DBS = [DB_ALPHA, DB_BETA, DB_GAMMA]


class TestVtableTagRefInsTags:

    @staticmethod
    def _tag_dict(table, db=DB):
        """Query ins_tags for a specific table, return {tag_name: tag_value}."""
        tdSql.query(
            f"SELECT tag_name, tag_value FROM information_schema.ins_tags "
            f"WHERE table_name = '{table}' AND db_name = '{db}'"
        )
        return {
            str(tdSql.getData(i, 0)).strip(): str(tdSql.getData(i, 1)).strip()
            for i in range(tdSql.queryRows)
        }

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

        # ================================================================
        # Part A: Same-database setup
        # ================================================================
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB}")
        tdSql.execute(f"CREATE DATABASE {DB}")
        tdSql.execute(f"USE {DB}")

        # Source tables — 3 child tables with diverse tag types
        tdSql.execute(
            "CREATE STABLE src_stb (ts TIMESTAMP, val INT) TAGS ("
            "city NCHAR(20), code INT, level TINYINT, score DOUBLE, active BOOL)"
        )
        tdSql.execute(
            "CREATE TABLE s_bj USING src_stb TAGS "
            "('beijing', 100, 1, 9.5, true)"
        )
        tdSql.execute(
            "CREATE TABLE s_sh USING src_stb TAGS "
            "('shanghai', 200, 2, 8.8, false)"
        )
        tdSql.execute(
            "CREATE TABLE s_sz USING src_stb TAGS "
            "('shenzhen', 300, 3, 7.2, true)"
        )

        for i, t in enumerate(["s_bj", "s_sh", "s_sz"]):
            for j in range(3):
                tdSql.execute(
                    f"INSERT INTO {t} VALUES "
                    f"({1700000000000 + (i * 3 + j) * 1000}, {i * 10 + j + 1})"
                )

        # Virtual super tables
        tdSql.execute(
            "CREATE STABLE vstb_a (ts TIMESTAMP, val INT) TAGS ("
            "ref_city NCHAR(20), ref_code INT, ref_level TINYINT, "
            "ref_score DOUBLE, ref_active BOOL) VIRTUAL 1"
        )
        tdSql.execute(
            "CREATE STABLE vstb_b (ts TIMESTAMP, val INT) TAGS ("
            "local_group INT, ref_city NCHAR(20), ref_code INT) VIRTUAL 1"
        )
        tdSql.execute(
            "CREATE STABLE vstb_c (ts TIMESTAMP, val INT) TAGS ("
            "lit_city NCHAR(20), lit_code INT) VIRTUAL 1"
        )
        tdSql.execute(
            "CREATE STABLE vstb_chain (ts TIMESTAMP, val INT) TAGS ("
            "gid INT, region NCHAR(16)) VIRTUAL 1"
        )

        # Virtual children — single-hop tag-ref (3 syntax variants)
        tdSql.execute(
            "CREATE VTABLE va_bj (s_bj.val) USING vstb_a TAGS ("
            "s_bj.city, s_bj.code, s_bj.level, s_bj.score, s_bj.active)"
        )
        tdSql.execute(
            "CREATE VTABLE va_sh (s_sh.val) USING vstb_a TAGS ("
            "ref_city FROM s_sh.city, ref_code FROM s_sh.code, "
            "ref_level FROM s_sh.level, ref_score FROM s_sh.score, "
            "ref_active FROM s_sh.active)"
        )
        tdSql.execute(
            "CREATE VTABLE va_sz (s_sz.val) USING vstb_a TAGS ("
            "FROM s_sz.city, FROM s_sz.code, FROM s_sz.level, "
            "FROM s_sz.score, FROM s_sz.active)"
        )

        # Virtual children — mixed local + ref
        tdSql.execute(
            "CREATE VTABLE vb_mix1 (s_bj.val) USING vstb_b TAGS ("
            "1, s_bj.city, s_bj.code)"
        )
        tdSql.execute(
            "CREATE VTABLE vb_mix2 (s_sh.val) USING vstb_b TAGS ("
            "2, ref_city FROM s_sh.city, ref_code FROM s_sh.code)"
        )

        # Virtual children — all literal
        tdSql.execute(
            "CREATE VTABLE vc_lit (s_bj.val) USING vstb_c TAGS ("
            "'literal_city', 999)"
        )

        # Multi-layer vchild chain + tag-ref
        # chain_01: col-ref from s_bj, tag-ref from s_bj
        # chain_02: col-ref from chain_01, literal tags
        # chain_03: col-ref from chain_02, literal tags
        tdSql.execute(
            "CREATE VTABLE chain_01 (s_bj.val) USING vstb_chain TAGS ("
            "s_bj.code, s_bj.city)"
        )
        tdSql.execute(
            "CREATE VTABLE chain_02 (chain_01.val) USING vstb_chain TAGS ("
            "50, 'chain2_region')"
        )
        tdSql.execute(
            "CREATE VTABLE chain_03 (chain_02.val) USING vstb_chain TAGS ("
            "60, 'chain3_region')"
        )

        # ================================================================
        # Part B: Cross-database setup
        # ================================================================
        for db in ALL_CROSS_DBS:
            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")

        # --- DB_ALPHA: source + vtables referencing BETA ---
        tdSql.execute(f"CREATE DATABASE {DB_ALPHA}")
        tdSql.execute(f"USE {DB_ALPHA}")

        tdSql.execute(
            "CREATE STABLE alpha_stb (ts TIMESTAMP, val INT) TAGS ("
            "city NCHAR(20), code INT, level TINYINT)"
        )
        tdSql.execute("CREATE TABLE ac0 USING alpha_stb TAGS ('beijing', 100, 1)")
        tdSql.execute("CREATE TABLE ac1 USING alpha_stb TAGS ('shanghai', 200, 2)")
        tdSql.execute("CREATE TABLE ac2 USING alpha_stb TAGS ('guangzhou', 300, 3)")

        for i, t in enumerate(["ac0", "ac1", "ac2"]):
            tdSql.execute(
                f"INSERT INTO {t} VALUES ({1700000000000 + i * 1000}, {i + 1})"
            )

        # Virtual STB in ALPHA (vtables created after Beta sources exist)
        tdSql.execute(
            "CREATE STABLE vstb_a2 (ts TIMESTAMP, val INT) TAGS ("
            "ref_city NCHAR(20), ref_code INT) VIRTUAL 1"
        )

        # --- DB_BETA: source tables (must exist before Alpha vtables reference them) ---
        tdSql.execute(f"CREATE DATABASE {DB_BETA}")
        tdSql.execute(f"USE {DB_BETA}")

        tdSql.execute(
            "CREATE STABLE beta_stb (ts TIMESTAMP, val INT) TAGS ("
            "country NCHAR(20), code INT)"
        )
        tdSql.execute("CREATE TABLE bc0 USING beta_stb TAGS ('uk', 10)")
        tdSql.execute("CREATE TABLE bc1 USING beta_stb TAGS ('us', 20)")

        for i, t in enumerate(["bc0", "bc1"]):
            tdSql.execute(
                f"INSERT INTO {t} VALUES ({1700001000000 + i * 1000}, {i + 10})"
            )

        # --- DB_BETA: virtual tables referencing ALPHA ---
        tdSql.execute(
            "CREATE STABLE vstb_b1 (ts TIMESTAMP, val INT) TAGS ("
            "ref_city NCHAR(20), ref_code INT, ref_level TINYINT) VIRTUAL 1"
        )
        tdSql.execute(
            f"CREATE VTABLE vb_a0 (val FROM {DB_ALPHA}.ac0.val) USING vstb_b1 TAGS ("
            f"{DB_ALPHA}.ac0.city, {DB_ALPHA}.ac0.code, {DB_ALPHA}.ac0.level)"
        )
        tdSql.execute(
            f"CREATE VTABLE vb_a1 (val FROM {DB_ALPHA}.ac1.val) USING vstb_b1 TAGS ("
            f"ref_city FROM {DB_ALPHA}.ac1.city, "
            f"ref_code FROM {DB_ALPHA}.ac1.code, "
            f"ref_level FROM {DB_ALPHA}.ac1.level)"
        )
        tdSql.execute(
            "CREATE STABLE vstb_b2 (ts TIMESTAMP, val INT) TAGS ("
            "local_flag INT, ref_city NCHAR(20)) VIRTUAL 1"
        )
        tdSql.execute(
            f"CREATE VTABLE vb_mixed (val FROM {DB_ALPHA}.ac2.val) USING vstb_b2 TAGS ("
            f"42, ref_city FROM {DB_ALPHA}.ac2.city)"
        )

        # --- Back to ALPHA: now create vtables referencing BETA ---
        # Beta bc0/bc1 tags are: country (NCHAR), code (INT)
        tdSql.execute(f"USE {DB_ALPHA}")
        tdSql.execute(
            f"CREATE VTABLE va_b0 (val FROM {DB_BETA}.bc0.val) USING vstb_a2 TAGS ("
            f"{DB_BETA}.bc0.country, {DB_BETA}.bc0.code)"
        )
        tdSql.execute(
            f"CREATE VTABLE va_b1 (val FROM {DB_BETA}.bc1.val) USING vstb_a2 TAGS ("
            f"ref_city FROM {DB_BETA}.bc1.country, "
            f"ref_code FROM {DB_BETA}.bc1.code)"
        )

        # --- DB_GAMMA: vtables referencing ALPHA + BETA ---
        tdSql.execute(f"CREATE DATABASE {DB_GAMMA}")
        tdSql.execute(f"USE {DB_GAMMA}")

        tdSql.execute(
            "CREATE STABLE vstb_g1 (ts TIMESTAMP, val INT) TAGS ("
            "city NCHAR(20), code INT) VIRTUAL 1"
        )
        tdSql.execute(
            f"CREATE VTABLE vg_direct (val FROM {DB_ALPHA}.ac0.val) USING vstb_g1 TAGS ("
            f"{DB_ALPHA}.ac0.city, {DB_ALPHA}.ac0.code)"
        )

        tdSql.execute(
            "CREATE STABLE vstb_g2 (ts TIMESTAMP, val INT) TAGS ("
            "tag_group INT, tag_region NCHAR(20)) VIRTUAL 1"
        )
        tdSql.execute(
            f"CREATE VTABLE vg_chain_01 (val FROM {DB_BETA}.vb_a0.val) "
            f"USING vstb_g2 TAGS ({DB_ALPHA}.ac0.code, {DB_ALPHA}.ac0.city)"
        )
        tdSql.execute(
            f"CREATE VTABLE vg_chain_02 (val FROM {DB_GAMMA}.vg_chain_01.val) "
            f"USING vstb_g2 TAGS (99, 'gamma_chain2')"
        )

        tdSql.execute(f"USE {DB}")

    def teardown_class(cls):
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB}")
        for db in ALL_CROSS_DBS:
            tdSql.execute(f"DROP DATABASE IF EXISTS {db}")

    # ================================================================
    # A-1. Same-DB: Single-table optimization (WHERE table_name='xxx')
    #     sysTableUserTagsFillOneTableTags L1534
    # ================================================================

    def test_ins_tags_single_table_all_ref(self):
        """All 5 tags are references to s_bj's tags — positional syntax.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, single_table
        """
        tdSql.query(
            f"SELECT tag_name, tag_type, tag_value "
            f"FROM information_schema.ins_tags "
            f"WHERE table_name = 'va_bj' AND db_name = '{DB}' "
            f"ORDER BY tag_name"
        )
        tdSql.checkRows(5)
        tags = {str(tdSql.getData(i, 0)).strip() for i in range(5)}
        assert tags == {"ref_active", "ref_city", "ref_code", "ref_level", "ref_score"}

    def test_ins_tags_single_table_specific_syntax(self):
        """Tags created with 'tag_name FROM table.tag' syntax.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, single_table
        """
        tdSql.query(
            f"SELECT tag_name FROM information_schema.ins_tags "
            f"WHERE table_name = 'va_sh' AND db_name = '{DB}' ORDER BY tag_name"
        )
        tdSql.checkRows(5)

    def test_ins_tags_single_table_from_syntax(self):
        """Tags created with 'FROM table.tag' syntax.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, single_table
        """
        tdSql.query(
            f"SELECT tag_name FROM information_schema.ins_tags "
            f"WHERE table_name = 'va_sz' AND db_name = '{DB}' ORDER BY tag_name"
        )
        tdSql.checkRows(5)

    def test_ins_tags_single_table_mixed_tags(self):
        """Local literal tag + referenced tags.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, single_table, mixed
        """
        tags = self._tag_dict("vb_mix1")
        assert len(tags) == 3
        assert tags.get("local_group") == "1"
        assert "beijing" in tags.get("ref_city", "").lower()
        assert tags.get("ref_code") == "100"

    def test_ins_tags_single_table_literal_tags(self):
        """All literal — skip sysTagsResolveRefTagVal entirely.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, single_table, literal
        """
        tags = self._tag_dict("vc_lit")
        assert len(tags) == 2
        assert tags.get("lit_code") == "999"

    # ================================================================
    # A-2. Same-DB: Tag value verification (local Step 1)
    #     sysTagsResolveRefTagVal local vnode path
    # ================================================================

    def test_ins_tags_tag_value_int_ref_local(self):
        """INT tag-ref: va_bj.ref_code → s_bj.code = 100.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, value_verify, local_resolve
        """
        tags = self._tag_dict("va_bj")
        assert tags.get("ref_code") == "100"

    def test_ins_tags_tag_value_nchar_ref_local(self):
        """NCHAR tag-ref: va_bj.ref_city → s_bj.city = 'beijing'.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, value_verify, local_resolve
        """
        tags = self._tag_dict("va_bj")
        assert "beijing" in tags.get("ref_city", "").lower()

    def test_ins_tags_tag_value_tinyint_ref(self):
        """TINYINT tag-ref: va_sh.ref_level → s_sh.level = 2.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, value_verify, local_resolve
        """
        tags = self._tag_dict("va_sh")
        assert tags.get("ref_level") == "2"

    def test_ins_tags_tag_value_double_ref(self):
        """DOUBLE tag-ref: va_sz.ref_score → s_sz.score = 7.2.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, value_verify, local_resolve
        """
        tags = self._tag_dict("va_sz")
        assert "7.2" in tags.get("ref_score", "")

    def test_ins_tags_tag_value_bool_ref(self):
        """BOOL tag-ref: va_bj.ref_active → s_bj.active = true.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, value_verify, local_resolve
        """
        tags = self._tag_dict("va_bj")
        assert "true" in tags.get("ref_active", "").lower()

    def test_ins_tags_tag_values_different_sources(self):
        """Verify tag values from different source children don't mix.

        va_bj→s_bj(code=100), va_sh→s_sh(code=200), va_sz→s_sz(code=300).

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, value_verify
        """
        for table, exp in [("va_bj", "100"), ("va_sh", "200"), ("va_sz", "300")]:
            tags = self._tag_dict(table)
            assert tags.get("ref_code") == exp, (
                f"{table}.ref_code: expected {exp}, got {tags.get('ref_code')}"
            )

    # ================================================================
    # A-3. Same-DB: Full scan (no WHERE table_name)
    #     sysTableUserTagsFillOneTableTags L1596
    # ================================================================

    def test_ins_tags_full_scan_includes_all_virtual(self):
        """Full scan: all virtual + physical children appear.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, full_scan
        """
        tdSql.query(
            f"SELECT table_name, tag_name FROM information_schema.ins_tags "
            f"WHERE db_name = '{DB}' ORDER BY table_name, tag_name"
        )
        assert tdSql.queryRows > 0

        vtable_tags = {}
        for i in range(tdSql.queryRows):
            tname = str(tdSql.getData(i, 0)).strip()
            vtable_tags.setdefault(tname, []).append(
                str(tdSql.getData(i, 1)).strip()
            )

        for vt, n in [("va_bj", 5), ("va_sh", 5), ("va_sz", 5),
                      ("vb_mix1", 3), ("vb_mix2", 3), ("vc_lit", 2),
                      ("chain_01", 2), ("chain_02", 2), ("chain_03", 2)]:
            assert vt in vtable_tags, f"Missing {vt}"
            assert len(vtable_tags[vt]) == n, f"{vt}: expected {n} tags, got {len(vtable_tags[vt])}"

    def test_ins_tags_full_scan_filter_on_tag_value(self):
        """Full scan filter: find ref_code = 200.

        va_sh and vb_mix2 both have ref_code=200 (from s_sh).

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, full_scan, filter
        """
        tdSql.query(
            f"SELECT table_name FROM information_schema.ins_tags "
            f"WHERE db_name = '{DB}' AND tag_name = 'ref_code' "
            f"AND tag_value = '200' ORDER BY table_name"
        )
        tdSql.checkRows(2)
        names = {str(tdSql.getData(i, 0)).strip() for i in range(tdSql.queryRows)}
        assert names == {"va_sh", "vb_mix2"}

    # ================================================================
    # A-4. Same-DB: Multi-layer chain
    # ================================================================

    def test_ins_tags_chain_layer1_tag_ref(self):
        """Chain layer-1: tag-ref from source. chain_01 → s_bj.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, vchild_chain
        """
        tags = self._tag_dict("chain_01")
        assert tags.get("gid") == "100"
        assert "beijing" in tags.get("region", "").lower()

    def test_ins_tags_chain_layer2_literal(self):
        """Chain layer-2: literal tags.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, vchild_chain
        """
        tags = self._tag_dict("chain_02")
        assert tags.get("gid") == "50"
        assert "chain2" in tags.get("region", "").lower()

    def test_ins_tags_chain_layer3_literal(self):
        """Chain layer-3: literal tags.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, vchild_chain
        """
        tags = self._tag_dict("chain_03")
        assert tags.get("gid") == "60"
        assert "chain3" in tags.get("region", "").lower()

    # ================================================================
    # A-5. Same-DB: SHOW TAGS (rewrite to ins_tags)
    # ================================================================

    def test_show_tags_positional(self):
        """SHOW TAGS FROM va_bj — positional syntax."""
        tdSql.query(f"SHOW TAGS FROM {DB}.va_bj")
        tdSql.checkRows(5)

    def test_show_tags_specific(self):
        """SHOW TAGS FROM va_sh — specific syntax."""
        tdSql.query(f"SHOW TAGS FROM {DB}.va_sh")
        tdSql.checkRows(5)

    def test_show_tags_from(self):
        """SHOW TAGS FROM va_sz — FROM syntax."""
        tdSql.query(f"SHOW TAGS FROM {DB}.va_sz")
        tdSql.checkRows(5)

    def test_show_tags_mixed(self):
        """SHOW TAGS FROM vb_mix1 — mixed."""
        tdSql.query(f"SHOW TAGS FROM {DB}.vb_mix1")
        tdSql.checkRows(3)

    def test_show_tags_literal(self):
        """SHOW TAGS FROM vc_lit — all literal."""
        tdSql.query(f"SHOW TAGS FROM {DB}.vc_lit")
        tdSql.checkRows(2)

    def test_show_tags_chain_tag_ref(self):
        """SHOW TAGS FROM chain_01 — tag-ref."""
        tdSql.query(f"SHOW TAGS FROM {DB}.chain_01")
        tdSql.checkRows(2)

    def test_show_tags_chain_literal(self):
        """SHOW TAGS FROM chain_02 — literal."""
        tdSql.query(f"SHOW TAGS FROM {DB}.chain_02")
        tdSql.checkRows(2)

    # ================================================================
    # A-6. Same-DB: stable_name, db_name, physical tables, tag type
    # ================================================================

    def test_ins_tags_stable_name(self):
        """Verify stable_name for all same-DB vtables.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, stable_name
        """
        for vt, stb in [("va_bj", "vstb_a"), ("vb_mix1", "vstb_b"),
                        ("vc_lit", "vstb_c"), ("chain_01", "vstb_chain")]:
            tdSql.query(
                f"SELECT DISTINCT stable_name FROM information_schema.ins_tags "
                f"WHERE table_name = '{vt}' AND db_name = '{DB}'"
            )
            tdSql.checkRows(1)
            assert str(tdSql.getData(0, 0)).strip() == stb

    def test_ins_tags_db_name_filter(self):
        """db_name isolates to correct database.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, db_name
        """
        tdSql.query(
            f"SELECT DISTINCT table_name FROM information_schema.ins_tags "
            f"WHERE db_name = '{DB}' AND stable_name = 'vstb_a' ORDER BY table_name"
        )
        names = [str(tdSql.getData(i, 0)).strip() for i in range(tdSql.queryRows)]
        assert names == ["va_bj", "va_sh", "va_sz"]

    def test_ins_tags_physical_children(self):
        """Physical child tables appear in ins_tags with direct tag values.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, physical_table
        """
        tags = self._tag_dict("s_bj")
        assert len(tags) == 5
        assert tags.get("city", "").lower() == "beijing"
        assert tags.get("code") == "100"
        assert tags.get("level") == "1"

    def test_ins_tags_tag_type_consistency(self):
        """Tag type preserved: s_bj.code (INT) == va_bj.ref_code (INT).

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, tag_type
        """
        tdSql.query(
            f"SELECT table_name, tag_type FROM information_schema.ins_tags "
            f"WHERE db_name = '{DB}' "
            f"AND ((table_name = 's_bj' AND tag_name = 'code') "
            f"  OR (table_name = 'va_bj' AND tag_name = 'ref_code'))"
        )
        tdSql.checkRows(2)
        types = {str(tdSql.getData(i, 1)).strip().upper() for i in range(2)}
        assert len(types) == 1 and "INT" in types.pop()

    # ================================================================
    # B-1. Cross-DB: Single-table path (Alpha ↔ Beta)
    #     sysTagsResolveRefTagVal Step 2: RPC via sysTagsFetchRemoteCfg
    # ================================================================

    def test_cross_alpha_refs_beta_positional(self):
        """Alpha va_b0 refs Beta bc0 — positional cross-DB.

        va_b0.ref_city → beta.bc0.city = 'uk'
        va_b0.ref_code → beta.bc0.code = 10

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, cross_db
        """
        tags = self._tag_dict("va_b0", DB_ALPHA)
        assert len(tags) == 2
        assert "uk" in tags.get("ref_city", "").lower()
        assert tags.get("ref_code") == "10"

    def test_cross_alpha_refs_beta_specific(self):
        """Alpha va_b1 refs Beta bc1 — specific syntax.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, cross_db
        """
        tags = self._tag_dict("va_b1", DB_ALPHA)
        assert len(tags) == 2
        assert "us" in tags.get("ref_city", "").lower()
        assert tags.get("ref_code") == "20"

    def test_cross_beta_refs_alpha_positional(self):
        """Beta vb_a0 refs Alpha ac0 — cross-DB, 3 tags.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, cross_db
        """
        tags = self._tag_dict("vb_a0", DB_BETA)
        assert len(tags) == 3
        assert "beijing" in tags.get("ref_city", "").lower()
        assert tags.get("ref_code") == "100"
        assert tags.get("ref_level") == "1"

    def test_cross_beta_refs_alpha_specific(self):
        """Beta vb_a1 refs Alpha ac1 — specific syntax.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, cross_db
        """
        tags = self._tag_dict("vb_a1", DB_BETA)
        assert len(tags) == 3
        assert "shanghai" in tags.get("ref_city", "").lower()
        assert tags.get("ref_code") == "200"
        assert tags.get("ref_level") == "2"

    def test_cross_gamma_refs_alpha_direct(self):
        """Gamma vg_direct refs Alpha ac0 — 3-way cross-DB.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, cross_db, three_way
        """
        tags = self._tag_dict("vg_direct", DB_GAMMA)
        assert len(tags) == 2
        assert "beijing" in tags.get("city", "").lower()
        assert tags.get("code") == "100"

    # ================================================================
    # B-2. Cross-DB: Mixed local + cross-DB ref
    # ================================================================

    def test_cross_mixed_local_and_ref(self):
        """vb_mixed: local_flag=42 (literal) + ref_city from Alpha ac2.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, cross_db, mixed
        """
        tags = self._tag_dict("vb_mixed", DB_BETA)
        assert len(tags) == 2
        assert tags.get("local_flag") == "42"
        assert "guangzhou" in tags.get("ref_city", "").lower()

    # ================================================================
    # B-3. Cross-DB: Multi-layer chain
    # ================================================================

    def test_cross_chain_layer1_tag_ref(self):
        """vg_chain_01: col-ref from beta.vb_a0, tag-ref from alpha.ac0.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, cross_db, vchild_chain
        """
        tags = self._tag_dict("vg_chain_01", DB_GAMMA)
        assert len(tags) == 2
        assert tags.get("tag_group") == "100"
        assert "beijing" in tags.get("tag_region", "").lower()

    def test_cross_chain_layer2_literal(self):
        """vg_chain_02: col-ref from gamma, literal tags.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, cross_db, vchild_chain
        """
        tags = self._tag_dict("vg_chain_02", DB_GAMMA)
        assert len(tags) == 2
        assert tags.get("tag_group") == "99"
        assert "gamma_chain2" in tags.get("tag_region", "").lower()

    # ================================================================
    # B-4. Cross-DB: Full scan across databases
    # ================================================================

    def test_cross_full_scan_alpha(self):
        """Full scan in ALPHA: physical + cross-DB virtual children.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, cross_db, full_scan
        """
        tdSql.query(
            f"SELECT DISTINCT table_name FROM information_schema.ins_tags "
            f"WHERE db_name = '{DB_ALPHA}' ORDER BY table_name"
        )
        names = {str(tdSql.getData(i, 0)).strip() for i in range(tdSql.queryRows)}
        assert {"ac0", "ac1", "ac2", "va_b0", "va_b1"} <= names

    def test_cross_full_scan_beta(self):
        """Full scan in BETA: physical + cross-DB virtual children.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, cross_db, full_scan
        """
        tdSql.query(
            f"SELECT DISTINCT table_name FROM information_schema.ins_tags "
            f"WHERE db_name = '{DB_BETA}' ORDER BY table_name"
        )
        names = {str(tdSql.getData(i, 0)).strip() for i in range(tdSql.queryRows)}
        assert {"bc0", "bc1", "vb_a0", "vb_a1", "vb_mixed"} <= names

    def test_cross_full_scan_gamma(self):
        """Full scan in GAMMA: cross-DB virtual children only.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, cross_db, full_scan
        """
        tdSql.query(
            f"SELECT DISTINCT table_name FROM information_schema.ins_tags "
            f"WHERE db_name = '{DB_GAMMA}' ORDER BY table_name"
        )
        names = {str(tdSql.getData(i, 0)).strip() for i in range(tdSql.queryRows)}
        assert {"vg_direct", "vg_chain_01", "vg_chain_02"} <= names

    def test_cross_full_scan_filter_tag_value(self):
        """Full scan filter: beta ref_city = 'shanghai'.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, cross_db, full_scan, filter
        """
        tdSql.query(
            f"SELECT table_name FROM information_schema.ins_tags "
            f"WHERE db_name = '{DB_BETA}' AND tag_name = 'ref_city' "
            f"AND tag_value = 'shanghai'"
        )
        tdSql.checkRows(1)
        assert str(tdSql.getData(0, 0)).strip() == "vb_a1"

    # ================================================================
    # B-5. Cross-DB: SHOW TAGS
    # ================================================================

    def test_cross_show_tags_alpha_refs_beta(self):
        """SHOW TAGS FROM alpha.va_b0 — cross-DB."""
        tdSql.query(f"SHOW TAGS FROM {DB_ALPHA}.va_b0")
        tdSql.checkRows(2)

    def test_cross_show_tags_beta_refs_alpha(self):
        """SHOW TAGS FROM beta.vb_a0 — cross-DB."""
        tdSql.query(f"SHOW TAGS FROM {DB_BETA}.vb_a0")
        tdSql.checkRows(3)

    def test_cross_show_tags_gamma_direct(self):
        """SHOW TAGS FROM gamma.vg_direct — 3-way."""
        tdSql.query(f"SHOW TAGS FROM {DB_GAMMA}.vg_direct")
        tdSql.checkRows(2)

    def test_cross_show_tags_gamma_chain(self):
        """SHOW TAGS FROM gamma.vg_chain_01 — chain."""
        tdSql.query(f"SHOW TAGS FROM {DB_GAMMA}.vg_chain_01")
        tdSql.checkRows(2)

    def test_cross_show_tags_mixed(self):
        """SHOW TAGS FROM beta.vb_mixed — mixed."""
        tdSql.query(f"SHOW TAGS FROM {DB_BETA}.vb_mixed")
        tdSql.checkRows(2)

    # ================================================================
    # B-6. Cross-DB: Bidirectional + three-way
    # ================================================================

    def test_cross_bidirectional(self):
        """Alpha→Beta and Beta→Alpha resolve independently.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, cross_db, bidirectional
        """
        alpha_tags = self._tag_dict("va_b0", DB_ALPHA)
        beta_tags = self._tag_dict("vb_a0", DB_BETA)
        assert "uk" in alpha_tags.get("ref_city", "").lower()
        assert "beijing" in beta_tags.get("ref_city", "").lower()

    def test_cross_three_way(self):
        """Gamma refs Alpha directly + via chain.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, cross_db, three_way
        """
        direct = self._tag_dict("vg_direct", DB_GAMMA)
        chain = self._tag_dict("vg_chain_01", DB_GAMMA)
        assert "beijing" in direct.get("city", "").lower()
        assert "beijing" in chain.get("tag_region", "").lower()

    # ================================================================
    # B-7. Cross-DB: stable_name + tag_type preserved
    # ================================================================

    def test_cross_stable_name(self):
        """stable_name for cross-DB virtual children.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, cross_db, stable_name
        """
        expected = {
            ("va_b0", DB_ALPHA): "vstb_a2",
            ("vb_a0", DB_BETA): "vstb_b1",
            ("vb_mixed", DB_BETA): "vstb_b2",
            ("vg_direct", DB_GAMMA): "vstb_g1",
            ("vg_chain_01", DB_GAMMA): "vstb_g2",
        }
        for (vt, db), stb in expected.items():
            tdSql.query(
                f"SELECT DISTINCT stable_name FROM information_schema.ins_tags "
                f"WHERE table_name = '{vt}' AND db_name = '{db}'"
            )
            tdSql.checkRows(1)
            assert str(tdSql.getData(0, 0)).strip() == stb, (
                f"{db}.{vt}: expected '{stb}'"
            )

    def test_cross_tag_type_preserved(self):
        """Tag types preserved across cross-DB refs.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, cross_db, tag_type
        """
        tdSql.query(
            f"SELECT tag_type FROM information_schema.ins_tags "
            f"WHERE table_name = 'vb_a0' AND tag_name = 'ref_code' "
            f"AND db_name = '{DB_BETA}'"
        )
        tdSql.checkRows(1)
        assert "INT" in str(tdSql.getData(0, 0)).strip().upper()

        tdSql.query(
            f"SELECT tag_type FROM information_schema.ins_tags "
            f"WHERE table_name = 'va_b0' AND tag_name = 'ref_city' "
            f"AND db_name = '{DB_ALPHA}'"
        )
        tdSql.checkRows(1)
        assert "NCHAR" in str(tdSql.getData(0, 0)).strip().upper()

    # ================================================================
    # B-8. Cross-DB: db_name isolation
    # ================================================================

    def test_cross_db_isolation(self):
        """ins_tags with db_name filter doesn't leak across databases.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, cross_db, isolation
        """
        tdSql.query(
            f"SELECT table_name FROM information_schema.ins_tags "
            f"WHERE db_name = '{DB_ALPHA}' AND table_name = 'bc0'"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"SELECT table_name FROM information_schema.ins_tags "
            f"WHERE db_name = '{DB_BETA}' AND table_name = 'ac0'"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"SELECT table_name FROM information_schema.ins_tags "
            f"WHERE db_name = '{DB_GAMMA}' AND table_name IN ('va_b0', 'vb_a0')"
        )
        tdSql.checkRows(0)

    # ================================================================
    # B-9. Cross-DB: Multiple refs to same source
    # ================================================================

    def test_cross_multiple_refs_same_source(self):
        """beta.vb_a0, gamma.vg_direct, gamma.vg_chain_01 all → alpha.ac0.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, cross_db, shared_source
        """
        beta = self._tag_dict("vb_a0", DB_BETA)
        gamma_d = self._tag_dict("vg_direct", DB_GAMMA)
        gamma_c = self._tag_dict("vg_chain_01", DB_GAMMA)
        assert beta.get("ref_code") == "100"
        assert gamma_d.get("code") == "100"
        assert gamma_c.get("tag_group") == "100"

    # ================================================================
    # B-10. Cross-DB: Physical children in remote databases
    # ================================================================

    def test_cross_physical_alpha(self):
        """Physical child ac0 in Alpha — direct tag values.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, cross_db, physical_table
        """
        tags = self._tag_dict("ac0", DB_ALPHA)
        assert len(tags) == 3
        assert "beijing" in tags.get("city", "").lower()
        assert tags.get("code") == "100"

    def test_cross_physical_beta(self):
        """Physical child bc0 in Beta — direct tag values.

        Catalog: VirtualTable  Since: v3.3.6.0
        Labels: virtual, tag_ref, ins_tags, cross_db, physical_table
        """
        tags = self._tag_dict("bc0", DB_BETA)
        assert len(tags) == 2
        assert "uk" in tags.get("country", "").lower()
        assert tags.get("code") == "10"
