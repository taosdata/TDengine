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
"""Large-scale SET TAG batch tests: bulk updates + query verification for virtual child tables."""

import time
from new_test_framework.utils import tdLog, tdSql

DB_MAIN = "td_set_tag_batch"
DB_CROSS = "td_set_tag_cross"
NUM_SRC = 20
NUM_VCHILD = 50
TS_BASE = 1700000000000


class TestVtableSetTagBatch:

    @staticmethod
    def _create_sources(db, prefix, num, tags_def, tag_gen, data_gen):
        """Create source stable + child tables with data."""
        tdSql.execute(f"USE {db}")
        tdSql.execute(
            f"CREATE STABLE {prefix}_stb (ts TIMESTAMP, val INT) TAGS ({tags_def})"
        )
        for i in range(num):
            tags = tag_gen(i)
            tdSql.execute(f"CREATE TABLE {prefix}_{i} USING {prefix}_stb TAGS ({tags})")
            for j, v in enumerate(data_gen(i)):
                tdSql.execute(
                    f"INSERT INTO {prefix}_{i} VALUES ({TS_BASE + j * 1000}, {v})"
                )

    @staticmethod
    def _prepare():
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB_MAIN}")
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB_CROSS}")
        tdSql.execute("RESET QUERY CACHE")
        import time; time.sleep(1)
        tdSql.execute(f"CREATE DATABASE {DB_MAIN}")
        tdSql.execute(f"CREATE DATABASE {DB_CROSS}")

        # Main DB sources: 20 child tables with city(NCHAR) and code(INT) tags
        TestVtableSetTagBatch._create_sources(
            DB_MAIN, "src", NUM_SRC,
            "city NCHAR(32), code INT",
            lambda i: f"'city_{i}', {1000 + i}",
            lambda i: list(range(i * 10, i * 10 + 3)),  # 3 rows each
        )

        # Cross DB sources: 20 child tables with region(NCHAR) and score(INT) tags
        TestVtableSetTagBatch._create_sources(
            DB_CROSS, "xsrc", NUM_SRC,
            "region NCHAR(32), score INT",
            lambda i: f"'region_{i}', {2000 + i}",
            lambda i: list(range(i * 100, i * 100 + 2)),  # 2 rows each
        )

        # Virtual super table: tags = local_name(NCHAR), ref_city(NCHAR), ref_code(INT)
        tdSql.execute(f"USE {DB_MAIN}")
        tdSql.execute(
            "CREATE STABLE vstb (ts TIMESTAMP, val INT) "
            "TAGS (local_name NCHAR(32), ref_city NCHAR(32), ref_code INT) VIRTUAL 1"
        )

        # Create 50 virtual children: each refs a different source
        for i in range(NUM_VCHILD):
            src_idx = i % NUM_SRC
            tdSql.execute(
                f"CREATE VTABLE vc_{i} (val FROM src_{src_idx}.val) USING vstb TAGS ("
                f"'name_{i}', "
                f"ref_city FROM src_{src_idx}.city, "
                f"ref_code FROM src_{src_idx}.code)"
            )

    def setup_method(self, method):
        tdLog.debug(f"start to execute {__file__}::{method.__name__}")
        self._prepare()

    def teardown_method(self, method):
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB_MAIN}")
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB_CROSS}")

    # -------------------------------------------------------
    # helpers
    # -------------------------------------------------------
    @staticmethod
    def _query_tag(vtable, tag):
        tdSql.query(f"SELECT DISTINCT {tag} FROM {DB_MAIN}.{vtable}")
        return str(tdSql.getData(0, 0))

    @staticmethod
    def _query_count(sql):
        tdSql.query(sql)
        return int(tdSql.getData(0, 0))

    # -------------------------------------------------------
    # 1. Batch: all 50 vtables literal → ref (same DB)
    # -------------------------------------------------------
    def test_batch_local_to_ref_same_db(self):
        """Batch: convert local_name from static to tag-ref for all 50 vtables.

        Verify batch local_name conversion from static to tag-ref for all 50 vtables in same DB.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, batch, set_tag

        """
        tdSql.execute(f"USE {DB_MAIN}")

        # Verify initial state: local_name is static
        for i in range(NUM_VCHILD):
            v = self._query_tag(f"vc_{i}", "local_name")
            assert v == f"name_{i}", f"vc_{i} initial local_name={v}"

        # Batch ALTER: local_name → ref to src's city
        for i in range(NUM_VCHILD):
            src_idx = i % NUM_SRC
            tdSql.execute(f"ALTER VTABLE vc_{i} SET TAG local_name = src_{src_idx}.city")

        # Verify: local_name should now resolve to city_X
        for i in range(NUM_VCHILD):
            src_idx = i % NUM_SRC
            v = self._query_tag(f"vc_{i}", "local_name")
            assert v == f"city_{src_idx}", f"vc_{i} expected city_{src_idx}, got {v}"

        # STB query: count should match all data
        total = NUM_VCHILD * 3
        cnt = self._query_count(f"SELECT COUNT(*) FROM {DB_MAIN}.vstb")
        assert cnt == total, f"expected {total}, got {cnt}"

        tdLog.info(f"PASS: batch local->ref for {NUM_VCHILD} vtables")

    # -------------------------------------------------------
    # 2. Batch: all 50 vtables ref → literal (child-level verify)
    # -------------------------------------------------------
    def test_batch_ref_to_literal(self):
        """Batch: convert ref_city from tag-ref to static literal for all 50 vtables.

        Verify batch ref_city conversion from tag-ref to static literal for all 50 vtables.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, batch, set_tag

        """
        tdSql.execute(f"USE {DB_MAIN}")

        # Verify initial state: ref_city is dynamic
        for i in range(NUM_VCHILD):
            src_idx = i % NUM_SRC
            v = self._query_tag(f"vc_{i}", "ref_city")
            assert v == f"city_{src_idx}", f"vc_{i} initial ref_city={v}"

        # Batch ALTER: ref_city → static literal
        for i in range(NUM_VCHILD):
            tdSql.execute(f"ALTER VTABLE vc_{i} SET TAG ref_city='static_{i}'")

        # Verify via child-level query: ref_city is now static
        for i in range(NUM_VCHILD):
            v = self._query_tag(f"vc_{i}", "ref_city")
            assert v == f"static_{i}", f"vc_{i} expected static_{i}, got {v}"

        # Modify source tag — should NOT affect vtable anymore
        tdSql.execute("ALTER TABLE src_0 SET TAG city='changed_city_0'")
        for i in range(NUM_VCHILD):
            v = self._query_tag(f"vc_{i}", "ref_city")
            assert v == f"static_{i}", f"vc_{i} should still be static_{i}, got {v}"

        tdLog.info(f"PASS: batch ref->literal for {NUM_VCHILD} vtables")

    # -------------------------------------------------------
    # 3. Batch: ref → different ref (re-point)
    # -------------------------------------------------------
    def test_batch_ref_to_different_ref(self):
        """Batch: re-point ref_city to a different source for all 50 vtables.

        Verify batch ref_city re-pointing to a different source child table for all 50 vtables.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, batch, set_tag

        """
        tdSql.execute(f"USE {DB_MAIN}")

        # Re-point ref_city: vc_i now refs src_((i+1) % NUM_SRC) instead of src_(i % NUM_SRC)
        for i in range(NUM_VCHILD):
            new_src = (i + 1) % NUM_SRC
            tdSql.execute(f"ALTER VTABLE vc_{i} SET TAG ref_city = src_{new_src}.city")

        # Verify
        for i in range(NUM_VCHILD):
            new_src = (i + 1) % NUM_SRC
            v = self._query_tag(f"vc_{i}", "ref_city")
            assert v == f"city_{new_src}", f"vc_{i} expected city_{new_src}, got {v}"

        tdLog.info(f"PASS: batch ref->different ref for {NUM_VCHILD} vtables")

    # -------------------------------------------------------
    # 4. Batch: cross-DB ref changes
    # -------------------------------------------------------
    def test_batch_cross_db_ref(self):
        """Batch: set ref_city to cross-DB source for all 50 vtables.

        Verify batch ref_city re-pointing to cross-DB source tables for all 50 vtables.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, batch, set_tag, cross_db

        """
        tdSql.execute(f"USE {DB_MAIN}")

        # Re-point ref_city to cross-DB source
        for i in range(NUM_VCHILD):
            xsrc_idx = i % NUM_SRC
            tdSql.execute(
                f"ALTER VTABLE vc_{i} SET TAG ref_city = {DB_CROSS}.xsrc_{xsrc_idx}.region"
            )

        # Verify
        for i in range(NUM_VCHILD):
            xsrc_idx = i % NUM_SRC
            v = self._query_tag(f"vc_{i}", "ref_city")
            assert v == f"region_{xsrc_idx}", f"vc_{i} expected region_{xsrc_idx}, got {v}"

        # Modify cross-DB source, verify dynamic
        tdSql.execute(f"ALTER TABLE {DB_CROSS}.xsrc_0 SET TAG region='new_region_0'")
        for i in range(0, NUM_VCHILD, NUM_SRC):  # vc_0, vc_20, vc_40
            v = self._query_tag(f"vc_{i}", "ref_city")
            assert v == "new_region_0", f"vc_{i} expected new_region_0, got {v}"

        tdLog.info(f"PASS: batch cross-DB ref for {NUM_VCHILD} vtables")

    # -------------------------------------------------------
    # 5. Round-trip: ref → literal → ref → literal for all
    # -------------------------------------------------------
    def test_batch_round_trip(self):
        """Batch round-trip: ref to literal to ref to literal for all 50 vtables.

        Verify complete round-trip tag-ref conversion cycles for all 50 vtables.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, batch, set_tag

        """
        tdSql.execute(f"USE {DB_MAIN}")

        # Phase 1: ref → literal
        for i in range(NUM_VCHILD):
            tdSql.execute(f"ALTER VTABLE vc_{i} SET TAG ref_city='lit_phase1_{i}'")

        for i in range(NUM_VCHILD):
            v = self._query_tag(f"vc_{i}", "ref_city")
            assert v == f"lit_phase1_{i}", f"phase1 vc_{i}: {v}"

        # Phase 2: literal → ref (back to original source)
        for i in range(NUM_VCHILD):
            src_idx = i % NUM_SRC
            tdSql.execute(f"ALTER VTABLE vc_{i} SET TAG ref_city = src_{src_idx}.city")

        for i in range(NUM_VCHILD):
            src_idx = i % NUM_SRC
            v = self._query_tag(f"vc_{i}", "ref_city")
            assert v == f"city_{src_idx}", f"phase2 vc_{i}: {v}"

        # Phase 3: ref → literal again
        for i in range(NUM_VCHILD):
            tdSql.execute(f"ALTER VTABLE vc_{i} SET TAG ref_city='lit_phase3_{i}'")

        for i in range(NUM_VCHILD):
            v = self._query_tag(f"vc_{i}", "ref_city")
            assert v == f"lit_phase3_{i}", f"phase3 vc_{i}: {v}"

        tdLog.info(f"PASS: round-trip for {NUM_VCHILD} vtables, 3 phases")

    # -------------------------------------------------------
    # 6. STB query after ALL refs cleared (uniform literal)
    # -------------------------------------------------------
    def test_stb_query_all_literal(self):
        """STB-level query after converting ALL vtables to literal tags.

        Verify STB-level queries work correctly when all children are converted to static tags.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, batch, set_tag, query

        """
        tdSql.execute(f"USE {DB_MAIN}")

        # Convert ALL to literal
        for i in range(NUM_VCHILD):
            tdSql.execute(f"ALTER VTABLE vc_{i} SET TAG ref_city='city_all'")

        tdSql.execute("RESET QUERY CACHE")
        time.sleep(1)

        # STB COUNT
        total = NUM_VCHILD * 3
        cnt = self._query_count("SELECT COUNT(*) FROM vstb")
        assert cnt == total, f"total count: expected {total}, got {cnt}"

        # Filter should find all
        cnt = self._query_count("SELECT COUNT(*) FROM vstb WHERE ref_city='city_all'")
        assert cnt == total, f"filter count: expected {total}, got {cnt}"

        tdLog.info(f"PASS: STB query after all-literal conversion")

    # -------------------------------------------------------
    # 7. STB query with ref-only (re-point all to different sources)
    # -------------------------------------------------------
    def test_stb_query_after_batch_repoint(self):
        """STB-level query after batch re-pointing all refs to different sources.

        Verify STB-level tag filter works when all children still have tag-refs (just different sources).

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, batch, set_tag, query

        """
        tdSql.execute(f"USE {DB_MAIN}")

        # Re-point all to src_0
        for i in range(NUM_VCHILD):
            tdSql.execute(f"ALTER VTABLE vc_{i} SET TAG ref_city = src_0.city")

        tdSql.execute("RESET QUERY CACHE")
        time.sleep(1)

        # All should have city_0 now
        total = NUM_VCHILD * 3
        cnt = self._query_count("SELECT COUNT(*) FROM vstb WHERE ref_city='city_0'")
        assert cnt == total, f"expected {total}, got {cnt}"

        # Modify source
        tdSql.execute("ALTER TABLE src_0 SET TAG city='repointed'")
        cnt = self._query_count("SELECT COUNT(*) FROM vstb WHERE ref_city='repointed'")
        assert cnt == total, f"after source update: expected {total}, got {cnt}"

        tdLog.info("PASS: STB query after batch repoint")

    # -------------------------------------------------------
    # 8. Mixed: SET TAG on same vtable multiple times
    # -------------------------------------------------------
    def test_rapid_toggle_single_vtable(self):
        """Rapidly toggle a single vtable's tag between ref and literal 20 times.

        Verify correct behavior after rapidly toggling a single vtable tag between ref and literal 20 times.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, set_tag, stress

        """
        tdSql.execute(f"USE {DB_MAIN}")

        for round_num in range(20):
            if round_num % 2 == 0:
                tdSql.execute(
                    f"ALTER VTABLE vc_0 SET TAG ref_city='toggle_{round_num}'"
                )
                v = self._query_tag("vc_0", "ref_city")
                assert v == f"toggle_{round_num}", f"round {round_num}: {v}"
            else:
                src_idx = round_num % NUM_SRC
                tdSql.execute(
                    f"ALTER VTABLE vc_0 SET TAG ref_city = src_{src_idx}.city"
                )
                v = self._query_tag("vc_0", "ref_city")
                assert v == f"city_{src_idx}", f"round {round_num}: {v}"

        tdLog.info("PASS: rapid toggle 20 rounds on vc_0")

    # -------------------------------------------------------
    # 9. Batch: UPDATE_CHILD_TABLE_TAG_VAL (USING syntax) clears ref
    # -------------------------------------------------------
    def test_batch_using_syntax_clears_ref(self):
        """Batch SET TAG via USING syntax clears tag-ref for matching children.

        Verify ALTER VTABLE USING vstb SET TAG clears refs and sets literal values correctly.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, batch, set_tag

        """
        tdSql.execute(f"USE {DB_MAIN}")

        # Use USING syntax to batch-update first 10 vtables
        for i in range(10):
            tdSql.execute(
                f"ALTER VTABLE USING vstb SET TAG ref_city='batch_lit_{i}' "
                f"WHERE tbname='vc_{i}'"
            )

        # Refresh catalog cache after batch ref-clearing
        tdSql.execute("RESET QUERY CACHE")
        time.sleep(1)

        # Verify first 10 are literal (child-level query)
        for i in range(10):
            v = self._query_tag(f"vc_{i}", "ref_city")
            assert v == f"batch_lit_{i}", f"vc_{i}: expected batch_lit_{i}, got {v}"

        # Verify rest (10..49) still have ref
        for i in range(10, NUM_VCHILD):
            src_idx = i % NUM_SRC
            v = self._query_tag(f"vc_{i}", "ref_city")
            assert v == f"city_{src_idx}", f"vc_{i}: expected city_{src_idx}, got {v}"

        # Modify source — vc_0 is now literal, should NOT change
        tdSql.execute("ALTER TABLE src_0 SET TAG city='modified_city_0'")
        v = self._query_tag("vc_0", "ref_city")
        assert v == "batch_lit_0", f"vc_0 should be batch_lit_0, got {v}"
        # vc_20 still refs src_0, should change
        v = self._query_tag("vc_20", "ref_city")
        assert v == "modified_city_0", f"vc_20 should be modified_city_0, got {v}"

        tdLog.info("PASS: batch USING syntax clears ref")

    # -------------------------------------------------------
    # 10. Query: all 3 tags after mixed batch modifications
    # -------------------------------------------------------
    def test_projection_all_tags_after_batch(self):
        """Project all tags after batch SET TAG to verify consistency.

        Verify projection of all three tags after batch modifications at child-table level.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, batch, set_tag, query

        """
        tdSql.execute(f"USE {DB_MAIN}")

        # Modify local_name to ref for first 10
        for i in range(10):
            src_idx = i % NUM_SRC
            tdSql.execute(f"ALTER VTABLE vc_{i} SET TAG local_name = src_{src_idx}.city")

        # Set ref_code to literal for first 10
        for i in range(10):
            tdSql.execute(f"ALTER VTABLE vc_{i} SET TAG ref_code=9999")

        # Query child: all 3 tags
        for i in range(10):
            src_idx = i % NUM_SRC
            tdSql.query(
                f"SELECT DISTINCT local_name, ref_city, ref_code FROM vc_{i}"
            )
            local = str(tdSql.getData(0, 0))
            city = str(tdSql.getData(0, 1))
            code = str(tdSql.getData(0, 2))
            assert local == f"city_{src_idx}", f"vc_{i} local_name={local}"
            assert city == f"city_{src_idx}", f"vc_{i} ref_city={city}"
            assert code == "9999", f"vc_{i} ref_code={code}"

        # Query remaining: should be unchanged
        for i in range(10, 20):
            src_idx = i % NUM_SRC
            tdSql.query(
                f"SELECT DISTINCT local_name, ref_city, ref_code FROM vc_{i}"
            )
            local = str(tdSql.getData(0, 0))
            city = str(tdSql.getData(0, 1))
            code = str(tdSql.getData(0, 2))
            assert local == f"name_{i}", f"vc_{i} local_name={local}"
            assert city == f"city_{src_idx}", f"vc_{i} ref_city={city}"
            assert code == str(1000 + src_idx), f"vc_{i} ref_code={code}"

        tdLog.info("PASS: projection all tags after batch modifications")

    # -------------------------------------------------------
    # 11. Source tag update propagation (child-level verify)
    # -------------------------------------------------------
    def test_source_update_propagation(self):
        """Source tag updates propagate correctly to ref vtables but not literal vtables.

        Verify source tag updates propagate only to vtables still holding tag-refs at child-table level.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, batch, set_tag, propagation

        """
        tdSql.execute(f"USE {DB_MAIN}")

        # Override ref_city for vc_0..vc_4 with literals
        for i in range(5):
            tdSql.execute(f"ALTER VTABLE vc_{i} SET TAG ref_city='frozen_{i}'")

        # Now update all source tags
        for i in range(NUM_SRC):
            tdSql.execute(f"ALTER TABLE src_{i} SET TAG city='updated_city_{i}'")

        # Frozen vtables: should NOT change
        for i in range(5):
            v = self._query_tag(f"vc_{i}", "ref_city")
            assert v == f"frozen_{i}", f"vc_{i} expected frozen_{i}, got {v}"

        # Dynamic vtables: should reflect update
        for i in range(5, NUM_VCHILD):
            src_idx = i % NUM_SRC
            v = self._query_tag(f"vc_{i}", "ref_city")
            assert v == f"updated_city_{src_idx}", (
                f"vc_{i} expected updated_city_{src_idx}, got {v}"
            )

        tdLog.info("PASS: source update propagation verified")

    # -------------------------------------------------------
    # 12. GROUP BY after converting ALL to same literal
    # -------------------------------------------------------
    def test_group_by_after_all_literal(self):
        """GROUP BY tag after converting all vtables to various literals.

        Verify GROUP BY queries work after batch SET TAG converts all children to static tags.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, batch, set_tag, query

        """
        tdSql.execute(f"USE {DB_MAIN}")

        # Set each vtable to a group: group_0, group_1, ..., group_4
        for i in range(NUM_VCHILD):
            grp = i % 5
            tdSql.execute(f"ALTER VTABLE vc_{i} SET TAG ref_city='group_{grp}'")

        tdSql.execute("RESET QUERY CACHE")
        time.sleep(1)

        # GROUP BY: each group has 10 vtables * 3 rows = 30
        tdSql.query(
            "SELECT ref_city, COUNT(*) FROM vstb GROUP BY ref_city ORDER BY ref_city"
        )
        rows = {}
        for i in range(tdSql.queryRows):
            city = str(tdSql.getData(i, 0))
            cnt = int(tdSql.getData(i, 1))
            rows[city] = cnt

        for grp in range(5):
            key = f"group_{grp}"
            assert rows.get(key) == 30, f"{key} count: expected 30, got {rows.get(key)}"

        tdLog.info(f"PASS: GROUP BY after all-literal, {len(rows)} groups")
