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
"""ALTER interaction coverage for virtual tables that use tag references."""

from new_test_framework.utils import tdLog, tdSql

DB = "td_tagref_alter"


class TestVtableTagRefAlter:

    @staticmethod
    def _prepare():
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB}")
        tdSql.execute(f"CREATE DATABASE {DB} BUFFER 16")
        tdSql.execute(f"USE {DB}")

        tdSql.execute(
            "CREATE STABLE src_stb (ts TIMESTAMP, val INT) "
            "TAGS (city NCHAR(20), code INT)"
        )
        tdSql.execute("CREATE TABLE src0 USING src_stb TAGS ('beijing', 100)")
        tdSql.execute("CREATE TABLE src1 USING src_stb TAGS ('shanghai', 200)")

        for offset, value in enumerate([1, 2, 3]):
            tdSql.execute(
                f"INSERT INTO src0 VALUES ({1700000000000 + offset * 1000}, {value})"
            )
        for offset, value in enumerate([10, 11]):
            tdSql.execute(
                f"INSERT INTO src1 VALUES ({1700000005000 + offset * 1000}, {value})"
            )

        tdSql.execute(
            "CREATE STABLE vstb (ts TIMESTAMP, val INT) "
            "TAGS (local_tag NCHAR(20), ref_city NCHAR(20), ref_code INT) VIRTUAL 1"
        )
        tdSql.execute(
            "CREATE VTABLE v0 (val FROM src0.val) USING vstb TAGS ("
            "'local0', "
            "ref_city FROM src0.city, "
            "ref_code FROM src1.code)"
        )
        tdSql.execute(
            "CREATE VTABLE v1 (val FROM src1.val) USING vstb TAGS ("
            "'local1', "
            "ref_city FROM src1.city, "
            "ref_code FROM src0.code)"
        )

        # Layer-2 virtual stable whose children reference layer-1 (vstb) child tags.
        # This builds a 2-hop tag-ref chain used by the multi-hop ALTER tests:
        #   v2_0.l2_ref_city -> v0.ref_city -> src0.city
        #   v2_0.l2_ref_code -> v0.ref_code -> src1.code
        tdSql.execute(
            "CREATE STABLE vstb2 (ts TIMESTAMP, val INT) "
            "TAGS (l2_local NCHAR(20), l2_ref_city NCHAR(20), l2_ref_code INT) VIRTUAL 1"
        )
        tdSql.execute(
            "CREATE VTABLE v2_0 (val FROM v0.val) USING vstb2 TAGS ("
            "'l2_local0', "
            "l2_ref_city FROM v0.ref_city, "
            "l2_ref_code FROM v0.ref_code)"
        )

    @staticmethod
    def _distinct_values(sql):
        tdSql.query(sql)
        return sorted(
            tuple(str(tdSql.getData(i, j)) for j in range(tdSql.queryCols))
            for i in range(tdSql.queryRows)
        )

    def setup_method(self, method):
        tdLog.debug(f"start to execute {__file__}::{method.__name__}")
        self._prepare()

    def teardown_method(self, method):
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB}")

    def test_alter_source_child_set_tag_updates_referenced_values(self):
        """ALTER TABLE child SET TAG propagates to referenced tag values.

        Verify that the system correctly handles the case: alter table child set tag propagates to referenced tag values.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")
        before = self._distinct_values("SELECT DISTINCT ref_city, ref_code FROM v0")
        assert before == [("beijing", "200")]

        tdSql.execute("ALTER TABLE src0 SET TAG city='beijing_new'")
        tdSql.execute("ALTER TABLE src1 SET TAG code=250")

        after = self._distinct_values("SELECT DISTINCT ref_city, ref_code FROM v0")
        assert after == [("beijing_new", "250")]

    def test_alter_source_stable_add_tag_preserves_existing_refs(self):
        """ALTER STABLE source ADD TAG does not break existing tag refs.

        Verify that the system correctly handles the case: alter stable source add tag does not break existing tag refs.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER STABLE src_stb ADD TAG region NCHAR(20)")
        tdSql.execute("ALTER TABLE src0 SET TAG region='north'")
        tdSql.execute("ALTER TABLE src1 SET TAG region='east'")

        values = self._distinct_values(
            "SELECT DISTINCT tbname, ref_city, ref_code FROM vstb"
        )
        assert values == [
            ("v0", "beijing", "200"),
            ("v1", "shanghai", "100"),
        ]

    def test_alter_virtual_stable_add_and_drop_local_tag(self):
        """ALTER STABLE on the virtual stable updates child tag metadata.

        Verify that the system correctly handles the case: alter stable on the virtual stable updates child tag metadata.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")
        tdSql.query(
            f"SELECT tag_name FROM information_schema.ins_tags "
            f"WHERE db_name='{DB}' AND table_name='v0' ORDER BY tag_name"
        )
        tdSql.checkRows(3)

        tdSql.execute("ALTER STABLE vstb ADD TAG extra_tag INT")
        tdSql.query(
            f"SELECT tag_name FROM information_schema.ins_tags "
            f"WHERE db_name='{DB}' AND table_name='v0' ORDER BY tag_name"
        )
        tdSql.checkRows(4)

        tdSql.execute("ALTER STABLE vstb DROP TAG extra_tag")
        tdSql.query(
            f"SELECT tag_name FROM information_schema.ins_tags "
            f"WHERE db_name='{DB}' AND table_name='v0' ORDER BY tag_name"
        )
        tdSql.checkRows(3)

    def test_alter_virtual_stable_rename_and_modify_local_tag(self):
        """ALTER STABLE rename/modify tag is reflected in child metadata.

        Verify that the system correctly handles the case: alter stable rename/modify tag is reflected in child metadata.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER STABLE vstb RENAME TAG local_tag local_label")
        tdSql.execute("ALTER STABLE vstb MODIFY TAG local_label NCHAR(64)")

        tdSql.query(
            f"SELECT tag_type FROM information_schema.ins_tags "
            f"WHERE db_name='{DB}' AND table_name='v0' AND tag_name='local_label'"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "NCHAR(64)")

        tdSql.query(
            f"SELECT tag_name FROM information_schema.ins_tags "
            f"WHERE db_name='{DB}' AND table_name='v0' AND tag_name='local_tag'"
        )
        tdSql.checkRows(0)

    def test_alter_tag_ref_vtable_set_local_tag(self):
        """Direct SET TAG updates a literal tag on a tag-ref vtable.

        Verify that the system correctly handles the case: direct set tag updates a literal tag on a tag-ref vtable.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER VTABLE v0 SET TAG local_tag='local0_updated'")
        tag_ref_values = self._distinct_values(
            "SELECT DISTINCT local_tag, ref_city, ref_code FROM v0"
        )
        assert tag_ref_values == [("local0_updated", "beijing", "200")]

    def test_alter_tag_ref_vtable_set_ref_tag_to_literal_clears_ref(self):
        """Direct SET TAG on a tag-ref field clears the ref and sets static value.

        Verify that the system correctly handles the case: direct set tag clears tag-ref and sets a static literal value.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER VTABLE v0 SET TAG ref_city='literal_city'")
        tag_ref_values = self._distinct_values(
            "SELECT DISTINCT local_tag, ref_city, ref_code FROM v0"
        )
        assert tag_ref_values == [("local0", "literal_city", "200")]

    def test_alter_tag_ref_vtable_set_ref_tag_to_new_reference(self):
        """Direct SET TAG can redirect a tag-ref to another source tag (2-part ref).

        ALTER VTABLE <vtb> SET TAG <tag>=<src_table>.<src_tag> repoints the
        tag-ref so the resolved value comes from the new source.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")
        # v0.ref_city initially references src0.city ('beijing')
        before = self._distinct_values("SELECT DISTINCT ref_city FROM v0")
        assert before == [("beijing",)]

        # Repoint ref_city to src1.city ('shanghai') using 2-part reference
        tdSql.execute("ALTER VTABLE v0 SET TAG ref_city=src1.city")
        after = self._distinct_values("SELECT DISTINCT ref_city FROM v0")
        assert after == [("shanghai",)]

    def test_alter_tag_ref_vtable_set_ref_tag_with_db_qualified_reference(self):
        """Direct SET TAG accepts a fully-qualified 3-part reference (db.table.tag).

        ALTER VTABLE <vtb> SET TAG <tag>=<db>.<src_table>.<src_tag> repoints the
        tag-ref using a database-qualified source.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")
        # Repoint ref_city to src1.city via db-qualified 3-part reference
        tdSql.execute(f"ALTER VTABLE v0 SET TAG ref_city={DB}.src1.city")
        after = self._distinct_values("SELECT DISTINCT ref_city FROM v0")
        assert after == [("shanghai",)]

    def test_alter_tag_ref_vtable_new_reference_follows_source(self):
        """A repointed tag-ref dynamically follows the new source's tag value.

        After redirecting ref_city to src1.city, changing src1.city must be
        reflected in the virtual table's resolved tag value.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER VTABLE v0 SET TAG ref_city=src1.city")
        assert self._distinct_values("SELECT DISTINCT ref_city FROM v0") == [("shanghai",)]

        # Updating the new source tag propagates to the referenced value
        tdSql.execute("ALTER TABLE src1 SET TAG city='shanghai_new'")
        assert self._distinct_values("SELECT DISTINCT ref_city FROM v0") == [("shanghai_new",)]

        # The old source (src0) no longer affects ref_city
        tdSql.execute("ALTER TABLE src0 SET TAG city='beijing_new'")
        assert self._distinct_values("SELECT DISTINCT ref_city FROM v0") == [("shanghai_new",)]

    # ------------------------------------------------------------------
    # Multi-hop tag-ref ALTER:  v2_0 -> v0 -> src
    # ------------------------------------------------------------------
    def test_multihop_baseline_resolves_through_two_layers(self):
        """Baseline: a 2-hop tag-ref resolves through both virtual layers.

        v2_0.l2_ref_city -> v0.ref_city -> src0.city ('beijing')
        v2_0.l2_ref_code -> v0.ref_code -> src1.code (200)

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")
        assert self._distinct_values(
            "SELECT DISTINCT l2_ref_city, l2_ref_code FROM v2_0"
        ) == [("beijing", "200")]

    def test_multihop_alter_leaf_source_propagates_to_top(self):
        """ALTER the physical leaf tag propagates through the whole 2-hop chain.

        Changing src0.city must surface at v2_0.l2_ref_city through v0.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER TABLE src0 SET TAG city='beijing_leaf'")
        assert self._distinct_values(
            "SELECT DISTINCT l2_ref_city FROM v2_0"
        ) == [("beijing_leaf",)]

    def test_multihop_alter_middle_ref_redirects_top(self):
        """Repointing the middle layer's tag-ref changes the top layer's value.

        v0.ref_city initially -> src0.city ('beijing'). Repoint it to
        src1.city ('shanghai'); the 2-hop value at v2_0 must follow.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")
        # Repoint the middle hop (v0.ref_city) to a different physical source.
        tdSql.execute("ALTER VTABLE v0 SET TAG ref_city=src1.city")
        assert self._distinct_values(
            "SELECT DISTINCT l2_ref_city FROM v2_0"
        ) == [("shanghai",)]

    def test_multihop_alter_top_ref_redirects_within_layer(self):
        """Repointing the top layer's tag-ref to another middle-layer tag.

        Repoint v2_0.l2_ref_city from v0.ref_city to v1.ref_city.
        v1.ref_city -> src1.city ('shanghai').

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")
        # Verify preconditions: v0 -> src0.city ('beijing'), v1 -> src1.city ('shanghai')
        assert self._distinct_values("SELECT DISTINCT ref_city FROM v0") == [("beijing",)], \
            "Precondition: v0.ref_city must resolve to 'beijing' (via src0)"
        assert self._distinct_values("SELECT DISTINCT ref_city FROM v1") == [("shanghai",)], \
            "Precondition: v1.ref_city must resolve to 'shanghai' (via src1)"
        # v2_0 initially chains through v0 -> 'beijing'
        assert self._distinct_values(
            "SELECT DISTINCT l2_ref_city FROM v2_0"
        ) == [("beijing",)]

        tdSql.execute(f"ALTER VTABLE v2_0 SET TAG l2_ref_city={DB}.v1.ref_city")
        assert self._distinct_values(
            "SELECT DISTINCT l2_ref_city FROM v2_0"
        ) == [("shanghai",)]

    def test_multihop_alter_top_ref_to_physical_collapses_chain(self):
        """Repointing a top-layer tag-ref directly to a physical tag.

        Bypass the middle layer entirely: v2_0.l2_ref_city -> src1.city.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")
        tdSql.execute(f"ALTER VTABLE v2_0 SET TAG l2_ref_city={DB}.src1.city")
        assert self._distinct_values(
            "SELECT DISTINCT l2_ref_city FROM v2_0"
        ) == [("shanghai",)]

        # And it dynamically follows the new physical source.
        tdSql.execute("ALTER TABLE src1 SET TAG city='shanghai_direct'")
        assert self._distinct_values(
            "SELECT DISTINCT l2_ref_city FROM v2_0"
        ) == [("shanghai_direct",)]

    def test_multihop_alter_middle_to_literal_breaks_chain(self):
        """Setting the middle hop to a literal severs the chain for the top layer.

        Overwriting v0.ref_city with a literal clears its tag-ref; the top
        layer (v2_0) then resolves to that literal value.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER VTABLE v0 SET TAG ref_city='frozen_city'")
        assert self._distinct_values(
            "SELECT DISTINCT l2_ref_city FROM v2_0"
        ) == [("frozen_city",)]

        # Changing the original physical source no longer affects the top layer.
        tdSql.execute("ALTER TABLE src0 SET TAG city='beijing_ignored'")
        assert self._distinct_values(
            "SELECT DISTINCT l2_ref_city FROM v2_0"
        ) == [("frozen_city",)]

    def test_alter_vtable_using_stable_can_update_local_tag(self):
        """Batch SET TAG through the virtual stable can update non-ref local tags.

        Verify that the system correctly handles the case: batch set tag through the virtual stable can update non-ref local tags.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER VTABLE USING vstb SET TAG local_tag='local0_batch' WHERE tbname='v0'")
        tag_ref_values = self._distinct_values(
            "SELECT DISTINCT local_tag, ref_city, ref_code FROM v0"
        )
        assert tag_ref_values == [("local0_batch", "beijing", "200")]

    def test_alter_vtable_using_stable_can_override_tag_ref_with_literal(self):
        """Batch SET TAG on a tag-ref field clears the ref and sets static value.

        Tag-ref values are cleared when overwritten with a literal via ALTER VTABLE USING ... SET TAG.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER VTABLE USING vstb SET TAG ref_city='literal_city' WHERE tbname='v0'")
        tag_ref_values = self._distinct_values(
            "SELECT DISTINCT local_tag, ref_city, ref_code FROM v0"
        )
        assert tag_ref_values == [("local0", "literal_city", "200")]

    def test_alter_vtable_using_stable_cannot_set_tag_ref_to_another_reference(self):
        """ALTER ... SET TAG only accepts literal values, not tag-reference syntax.

        Verify that the system correctly handles the case: alter ... set tag only accepts literal values, not tag-reference syntax.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdSql.execute(f"USE {DB}")
        tdSql.error("ALTER VTABLE USING vstb SET TAG ref_city=src1.city WHERE tbname='v0'")

    # ============================================================
    # SHOW CREATE / DESCRIBE after ALTER ADD TAG + SET TAG ref
    # These tests cover the bug where pTagRefs was accessed by array
    # position instead of by colId, causing wrong output when new tags
    # were added to the vstb after the vtable was created.
    #
    # Also covers the companion bug in setDescResultIntoDataBlock where
    # tag rows were never matched against tagRef (only colRef was
    # checked), so DESCRIBE always showed an empty ref column for tags.
    # ============================================================

    def _get_show_create_sql(self, table):
        tdSql.query(f"SHOW CREATE VTABLE {DB}.{table}")
        tdSql.checkRows(1)
        return str(tdSql.getData(0, 1))

    def _get_desc_ref_col(self, table):
        """Return {tag_name: ref_string} for TAG rows that have a non-empty ref column.

        Only TAG rows are included (note column contains 'TAG'); data-column rows
        are skipped because they carry colRef values unrelated to tag-ref assertions.
        """
        tdSql.query(f"DESCRIBE {DB}.{table}")
        result = {}
        for i in range(tdSql.queryRows):
            note = str(tdSql.getData(i, 3)).strip().upper() if tdSql.queryCols > 3 else ""
            if "TAG" not in note:
                continue  # skip ts / data columns
            name = str(tdSql.getData(i, 0)).strip()
            ref  = str(tdSql.getData(i, 4)).strip() if tdSql.queryCols > 4 else ""
            if ref:
                result[name] = ref
        return result

    def test_show_create_after_add_tag_and_set_ref_single(self):
        """SHOW CREATE VTABLE is correct after one ADD TAG + SET TAG ref.

        Regression for bug: pTagRefs accessed by position instead of colId,
        causing FROM reference to appear on the wrong tag when a new tag was
        added to the vstb after vtable creation.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, show_create, alter, tag_ref

        """
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER STABLE vstb ADD TAG extra1 NCHAR(20)")
        tdSql.execute("ALTER VTABLE v0 SET TAG extra1 = src0.city")

        sql = self._get_show_create_sql("v0")

        # The new tag extra1 must appear with FROM reference to src0.city
        assert f"`{DB}`.`src0`.`city`" in sql, (
            f"Expected FROM ref to src0.city for extra1, got:\n{sql}"
        )
        # The original literal tag local_tag must NOT appear as a FROM ref
        assert "local0" in sql or "local_tag" in sql, (
            f"Original literal tag missing from SHOW CREATE:\n{sql}"
        )
        # The original ref tags must still be correct
        assert f"`{DB}`.`src0`.`city`" in sql or f"`{DB}`.`src1`.`code`" in sql, (
            f"Original tag refs missing from SHOW CREATE:\n{sql}"
        )

    def test_show_create_after_add_two_tags_and_set_refs(self):
        """SHOW CREATE VTABLE is correct after two ADD TAG + SET TAG ref operations.

        Regression: with two sequentially added tag refs the position-based
        lookup placed the second ref on the wrong schema slot.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, show_create, alter, tag_ref

        """
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER STABLE vstb ADD TAG extra1 NCHAR(20)")
        tdSql.execute("ALTER VTABLE v0 SET TAG extra1 = src0.city")

        tdSql.execute("ALTER STABLE vstb ADD TAG extra2 INT")
        tdSql.execute("ALTER VTABLE v0 SET TAG extra2 = src1.code")

        sql = self._get_show_create_sql("v0")

        # Both new tags must carry their own FROM references
        assert f"`{DB}`.`src0`.`city`" in sql, (
            f"Expected FROM ref to src0.city for extra1, got:\n{sql}"
        )
        assert f"`{DB}`.`src1`.`code`" in sql, (
            f"Expected FROM ref to src1.code for extra2, got:\n{sql}"
        )

    def test_show_create_after_add_tag_with_literal_then_ref(self):
        """SHOW CREATE VTABLE mixes literal and ref tags correctly after ALTER.

        ADD TAG + literal SET TAG followed by a second ADD TAG + ref SET TAG.
        The ref tag must show FROM, the literal tag must show its value.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, show_create, alter, tag_ref

        """
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER STABLE vstb ADD TAG extra_lit NCHAR(20)")
        tdSql.execute("ALTER VTABLE v0 SET TAG extra_lit = 'literal_val'")

        tdSql.execute("ALTER STABLE vstb ADD TAG extra_ref INT")
        tdSql.execute("ALTER VTABLE v0 SET TAG extra_ref = src0.code")

        sql = self._get_show_create_sql("v0")

        assert "literal_val" in sql, (
            f"Literal tag value missing from SHOW CREATE:\n{sql}"
        )
        assert f"`{DB}`.`src0`.`code`" in sql, (
            f"Expected FROM ref to src0.code for extra_ref, got:\n{sql}"
        )

    def test_show_create_original_refs_unchanged_after_add_tag(self):
        """Original tag refs remain correct in SHOW CREATE after unrelated ADD TAG.

        Adding a new tag to vstb must not corrupt the FROM references of
        the tags that were already set at vtable creation time.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, show_create, alter, tag_ref

        """
        tdSql.execute(f"USE {DB}")
        sql_before = self._get_show_create_sql("v0")

        tdSql.execute("ALTER STABLE vstb ADD TAG extra_new INT")
        # Do NOT set a ref for the new tag — leave it NULL

        sql_after = self._get_show_create_sql("v0")

        # The pre-existing refs must still be present
        for ref in [f"`{DB}`.`src0`.`city`", f"`{DB}`.`src1`.`code`"]:
            assert ref in sql_after, (
                f"Pre-existing ref {ref} missing after ADD TAG:\n{sql_after}"
            )

    def test_describe_ref_col_shows_tag_ref_after_add_and_set(self):
        """DESCRIBE ref column shows source for tag refs added via ALTER.

        Regression for bug: DESCRIBE always showed empty ref column for TAG
        rows because the code was reading from colRef (column refs) instead
        of tagRef, and was using position indexing instead of colId lookup.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, describe, alter, tag_ref

        """
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER STABLE vstb ADD TAG extra1 NCHAR(20)")
        tdSql.execute("ALTER VTABLE v0 SET TAG extra1 = src0.city")

        refs = self._get_desc_ref_col("v0")

        # extra1 must appear in the ref column with the source path
        assert "extra1" in refs, (
            f"Tag 'extra1' has no entry in ref column; all refs: {refs}"
        )
        ref_val = refs["extra1"]
        assert "src0" in ref_val and "city" in ref_val, (
            f"Tag 'extra1' ref col expected 'src0.city', got '{ref_val}'"
        )

    def test_describe_ref_col_shows_two_tag_refs_after_two_alters(self):
        """DESCRIBE ref column shows correct source for two sequentially added tag refs.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, describe, alter, tag_ref

        """
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER STABLE vstb ADD TAG extra1 NCHAR(20)")
        tdSql.execute("ALTER VTABLE v0 SET TAG extra1 = src0.city")

        tdSql.execute("ALTER STABLE vstb ADD TAG extra2 INT")
        tdSql.execute("ALTER VTABLE v0 SET TAG extra2 = src1.code")

        refs = self._get_desc_ref_col("v0")

        assert "extra1" in refs, f"extra1 missing from desc ref col: {refs}"
        assert "src0" in refs["extra1"] and "city" in refs["extra1"], (
            f"extra1 ref col expected src0.city, got '{refs['extra1']}'"
        )

        assert "extra2" in refs, f"extra2 missing from desc ref col: {refs}"
        assert "src1" in refs["extra2"] and "code" in refs["extra2"], (
            f"extra2 ref col expected src1.code, got '{refs['extra2']}'"
        )

    def test_describe_ref_col_literal_tag_has_empty_ref(self):
        """DESCRIBE ref column is empty for literal (non-ref) tags.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, describe, alter, tag_ref

        """
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER STABLE vstb ADD TAG extra_lit NCHAR(20)")
        tdSql.execute("ALTER VTABLE v0 SET TAG extra_lit = 'literal_val'")

        refs = self._get_desc_ref_col("v0")

        assert "extra_lit" not in refs, (
            f"Literal tag 'extra_lit' should have empty ref col, but got: {refs.get('extra_lit')}"
        )

    def test_describe_ref_col_original_literal_tag_unchanged(self):
        """DESCRIBE ref column for original literal tag stays empty after ADD TAG ref.

        Adding a ref tag to vstb must not cause the literal tag (local_tag)
        to suddenly show a ref entry in DESCRIBE output.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, describe, alter, tag_ref

        """
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER STABLE vstb ADD TAG extra_ref INT")
        tdSql.execute("ALTER VTABLE v0 SET TAG extra_ref = src0.code")

        refs = self._get_desc_ref_col("v0")

        # local_tag was set to literal 'local0' at creation; must still be empty
        assert "local_tag" not in refs, (
            f"Literal tag 'local_tag' should have empty ref col after ADD TAG, "
            f"but got: {refs.get('local_tag')}"
        )

    def test_describe_ref_col_creation_time_tag_refs(self):
        """DESCRIBE ref col shows source for tag refs set at vtable creation time.

        Regression for bug: setDescResultIntoDataBlock only checked colRef
        (column refs) and never tagRef, so DESCRIBE always showed an empty
        ref column for TAG rows even when the ref was set during CREATE VTABLE.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, describe, tag_ref

        """
        tdSql.execute(f"USE {DB}")
        # v0 was created with: ref_city FROM src0.city, ref_code FROM src1.code
        refs = self._get_desc_ref_col("v0")

        assert "ref_city" in refs, (
            f"Creation-time tag 'ref_city' has empty ref col; all refs: {refs}"
        )
        assert "src0" in refs["ref_city"] and "city" in refs["ref_city"], (
            f"Tag 'ref_city' ref col expected 'src0.city', got '{refs['ref_city']}'"
        )

        assert "ref_code" in refs, (
            f"Creation-time tag 'ref_code' has empty ref col; all refs: {refs}"
        )
        assert "src1" in refs["ref_code"] and "code" in refs["ref_code"], (
            f"Tag 'ref_code' ref col expected 'src1.code', got '{refs['ref_code']}'"
        )

        # local_tag is a literal — must have empty ref col
        assert "local_tag" not in refs, (
            f"Literal tag 'local_tag' should have empty ref col, got '{refs.get('local_tag')}'"
        )

    # ============================================================
    # Edge cases: SET order vs schema order, partial SET,
    # ref→literal overwrite, multi-vtable isolation, DROP TAG,
    # many rounds, same src col from multiple tags.
    # ============================================================

    def test_show_create_set_ref_in_reverse_schema_order(self):
        """SHOW CREATE is correct when SET TAG is done in reverse addition order.

        ADD extra1 then extra2 (schema order: extra1 before extra2).
        SET extra2's ref FIRST, then extra1's ref.
        pTagRefs[0] = extra2's entry, pTagRefs[1] = extra1's entry.
        The old position-based code would assign extra2's ref to extra1
        and vice-versa. The colId fix must assign each ref to the correct tag.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, show_create, alter, tag_ref

        """
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER STABLE vstb ADD TAG extra1 NCHAR(20)")
        tdSql.execute("ALTER STABLE vstb ADD TAG extra2 INT")

        # SET in reverse order: extra2 first, extra1 second
        tdSql.execute("ALTER VTABLE v0 SET TAG extra2 = src1.code")
        tdSql.execute("ALTER VTABLE v0 SET TAG extra1 = src0.city")

        sql = self._get_show_create_sql("v0")

        # extra1 must reference src0.city, extra2 must reference src1.code
        assert f"`{DB}`.`src0`.`city`" in sql, (
            f"extra1 FROM src0.city missing (possible swap bug):\n{sql}"
        )
        assert f"`{DB}`.`src1`.`code`" in sql, (
            f"extra2 FROM src1.code missing (possible swap bug):\n{sql}"
        )

        # DESCRIBE must also be correct
        refs = self._get_desc_ref_col("v0")
        assert "extra1" in refs and "src0" in refs["extra1"] and "city" in refs["extra1"], (
            f"extra1 desc ref wrong after reverse-order SET: {refs.get('extra1')}"
        )
        assert "extra2" in refs and "src1" in refs["extra2"] and "code" in refs["extra2"], (
            f"extra2 desc ref wrong after reverse-order SET: {refs.get('extra2')}"
        )

    def test_show_create_add_several_tags_set_only_some(self):
        """SHOW CREATE is correct when only some of the added tags get a SET ref.

        ADD 4 new tags; SET refs for positions 0 and 2 (gaps at 1 and 3).
        The unset tags must have no FROM clause; the set ones must be correct.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, show_create, alter, tag_ref

        """
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER STABLE vstb ADD TAG e0 NCHAR(20)")
        tdSql.execute("ALTER STABLE vstb ADD TAG e1 NCHAR(20)")
        tdSql.execute("ALTER STABLE vstb ADD TAG e2 INT")
        tdSql.execute("ALTER STABLE vstb ADD TAG e3 INT")

        # Only set e0 (NCHAR → src0.city) and e2 (INT → src1.code); e1 and e3 left unset
        tdSql.execute("ALTER VTABLE v0 SET TAG e0 = src0.city")
        tdSql.execute("ALTER VTABLE v0 SET TAG e2 = src1.code")

        sql = self._get_show_create_sql("v0")
        refs = self._get_desc_ref_col("v0")

        # e0 and e2 must show FROM refs
        assert f"`{DB}`.`src0`.`city`" in sql, f"e0 FROM missing:\n{sql}"
        assert f"`{DB}`.`src1`.`code`" in sql, f"e2 FROM missing:\n{sql}"
        assert "e0" in refs and "src0" in refs["e0"], f"e0 desc ref wrong: {refs.get('e0')}"
        assert "e2" in refs and "src1" in refs["e2"], f"e2 desc ref wrong: {refs.get('e2')}"

        # e1 and e3 must NOT have FROM refs (they were not set)
        assert "e1" not in refs, f"e1 should have empty ref col, got: {refs.get('e1')}"
        assert "e3" not in refs, f"e3 should have empty ref col, got: {refs.get('e3')}"

    def test_show_create_five_rounds_of_add_and_set(self):
        """SHOW CREATE and DESCRIBE are correct after 5 sequential ADD + SET rounds.

        Stress-tests the colId lookup by building up a large pTagRefs array
        where every entry was appended at a different ALTER step.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, show_create, describe, alter, tag_ref

        """
        tdSql.execute(f"USE {DB}")

        rounds = [
            ("r0", "NCHAR(20)", "src0.city"),
            ("r1", "INT",       "src1.code"),
            ("r2", "NCHAR(20)", "src0.city"),
            ("r3", "INT",       "src1.code"),
            ("r4", "NCHAR(20)", "src0.city"),
        ]
        for name, dtype, ref_src in rounds:
            tdSql.execute(f"ALTER STABLE vstb ADD TAG {name} {dtype}")
            tdSql.execute(f"ALTER VTABLE v0 SET TAG {name} = {ref_src}")

        sql  = self._get_show_create_sql("v0")
        refs = self._get_desc_ref_col("v0")

        for name, _, ref_src in rounds:
            table_col = ref_src.split(".")        # e.g. ["src0", "city"]
            assert f"`{DB}`.`{table_col[0]}`.`{table_col[1]}`" in sql, (
                f"Round tag '{name}' FROM clause missing:\n{sql}"
            )
            assert name in refs, f"Round tag '{name}' missing from desc refs: {refs}"
            assert table_col[0] in refs[name] and table_col[1] in refs[name], (
                f"Round tag '{name}' desc ref wrong: {refs.get(name)}"
            )

    def test_show_create_ref_overwritten_with_literal(self):
        """SHOW CREATE and DESCRIBE reflect a ref→literal overwrite correctly.

        SET TAG extra1 = src0.city  (ref)
        then SET TAG extra1 = 'static'  (literal override)
        SHOW CREATE must show the literal; FROM must not appear for extra1.
        DESCRIBE ref col must be empty.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, show_create, describe, alter, tag_ref

        """
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER STABLE vstb ADD TAG extra1 NCHAR(20)")
        tdSql.execute("ALTER VTABLE v0 SET TAG extra1 = src0.city")

        # Overwrite with a literal
        tdSql.execute("ALTER VTABLE v0 SET TAG extra1 = 'static_val'")

        sql  = self._get_show_create_sql("v0")
        refs = self._get_desc_ref_col("v0")

        assert "static_val" in sql, f"Literal value 'static_val' missing from SHOW CREATE:\n{sql}"
        assert "extra1" not in refs, (
            f"extra1 ref col should be empty after literal override, got: {refs.get('extra1')}"
        )

    def test_show_create_and_describe_v0_v1_independent(self):
        """ALTER on v0 does not affect v1's SHOW CREATE or DESCRIBE output.

        Both vtables share the same vstb. Adding a tag to vstb and setting a
        ref only on v0 must leave v1's metadata unchanged.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, show_create, describe, alter, tag_ref

        """
        tdSql.execute(f"USE {DB}")

        sql_v1_before = self._get_show_create_sql("v1")
        refs_v1_before = self._get_desc_ref_col("v1")

        tdSql.execute("ALTER STABLE vstb ADD TAG extra1 NCHAR(20)")
        tdSql.execute("ALTER VTABLE v0 SET TAG extra1 = src0.city")

        sql_v1_after  = self._get_show_create_sql("v1")
        refs_v1_after = self._get_desc_ref_col("v1")

        # v1's existing refs must still be present
        assert f"`{DB}`.`src1`.`city`" in sql_v1_after, (
            f"v1 ref_city FROM src1.city missing after altering v0:\n{sql_v1_after}"
        )
        assert f"`{DB}`.`src0`.`code`" in sql_v1_after, (
            f"v1 ref_code FROM src0.code missing after altering v0:\n{sql_v1_after}"
        )

        # extra1 must NOT have a FROM ref on v1 (only v0 had SET TAG applied)
        assert "extra1" not in refs_v1_after, (
            f"v1 extra1 should have empty ref col (only v0 was SET), got: {refs_v1_after.get('extra1')}"
        )

        # v1 original refs must match the before snapshot
        for tag in ("ref_city", "ref_code"):
            before_val = refs_v1_before.get(tag, "")
            after_val  = refs_v1_after.get(tag, "")
            assert before_val == after_val, (
                f"v1 tag '{tag}' ref changed after altering v0: '{before_val}' → '{after_val}'"
            )

    def test_show_create_after_drop_tag_remaining_refs_correct(self):
        """Remaining tag refs are correct in SHOW CREATE after DROP TAG.

        ADD extra1 (ref) + extra2 (ref), then DROP extra1.
        extra2's ref must survive and still be correct.
        The dropped tag must not appear in SHOW CREATE or DESCRIBE.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, show_create, describe, alter, tag_ref

        """
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER STABLE vstb ADD TAG extra1 NCHAR(20)")
        tdSql.execute("ALTER VTABLE v0 SET TAG extra1 = src0.city")

        tdSql.execute("ALTER STABLE vstb ADD TAG extra2 INT")
        tdSql.execute("ALTER VTABLE v0 SET TAG extra2 = src1.code")

        # Drop the first extra tag
        tdSql.execute("ALTER STABLE vstb DROP TAG extra1")

        sql  = self._get_show_create_sql("v0")
        refs = self._get_desc_ref_col("v0")

        assert "extra1" not in sql, f"Dropped tag extra1 still in SHOW CREATE:\n{sql}"
        assert "extra1" not in refs, f"Dropped tag extra1 still in DESCRIBE refs: {refs}"

        assert f"`{DB}`.`src1`.`code`" in sql, (
            f"Surviving tag extra2 FROM ref missing after DROP:\n{sql}"
        )
        assert "extra2" in refs and "src1" in refs["extra2"] and "code" in refs["extra2"], (
            f"extra2 desc ref wrong after DROP extra1: {refs.get('extra2')}"
        )

    def test_show_create_multiple_tags_referencing_same_src_column(self):
        """SHOW CREATE is correct when multiple tags reference the same src column.

        extra1 and extra2 both FROM src0.city.
        Both must appear with the correct FROM clause; no cross-contamination.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, show_create, describe, alter, tag_ref

        """
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER STABLE vstb ADD TAG extra1 NCHAR(20)")
        tdSql.execute("ALTER STABLE vstb ADD TAG extra2 NCHAR(20)")

        tdSql.execute("ALTER VTABLE v0 SET TAG extra1 = src0.city")
        tdSql.execute("ALTER VTABLE v0 SET TAG extra2 = src0.city")

        sql  = self._get_show_create_sql("v0")
        refs = self._get_desc_ref_col("v0")

        # Both refs point to the same source; FROM must appear for each tag
        city_ref = f"`{DB}`.`src0`.`city`"
        assert sql.count(city_ref) >= 3, (
            f"Expected >=3 occurrences of src0.city (ref_city + extra1 + extra2), "
            f"found {sql.count(city_ref)}:\n{sql}"
        )

        for tag in ("extra1", "extra2"):
            assert tag in refs, f"Tag '{tag}' missing from desc refs: {refs}"
            assert "src0" in refs[tag] and "city" in refs[tag], (
                f"Tag '{tag}' desc ref expected src0.city, got: {refs.get(tag)}"
            )

    def test_show_create_add_ref_then_add_literal_interleaved(self):
        """SHOW CREATE is correct for interleaved ref and literal additions.

        Round 1: ADD e_ref1 + SET ref → src0.city
        Round 2: ADD e_lit  + SET literal
        Round 3: ADD e_ref2 + SET ref → src1.code
        Each tag must appear exactly as expected; no bleed between rounds.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, show_create, describe, alter, tag_ref

        """
        tdSql.execute(f"USE {DB}")

        tdSql.execute("ALTER STABLE vstb ADD TAG e_ref1 NCHAR(20)")
        tdSql.execute("ALTER VTABLE v0 SET TAG e_ref1 = src0.city")

        tdSql.execute("ALTER STABLE vstb ADD TAG e_lit NCHAR(20)")
        tdSql.execute("ALTER VTABLE v0 SET TAG e_lit = 'fixed_val'")

        tdSql.execute("ALTER STABLE vstb ADD TAG e_ref2 INT")
        tdSql.execute("ALTER VTABLE v0 SET TAG e_ref2 = src1.code")

        sql  = self._get_show_create_sql("v0")
        refs = self._get_desc_ref_col("v0")

        assert f"`{DB}`.`src0`.`city`" in sql, f"e_ref1 FROM missing:\n{sql}"
        assert f"`{DB}`.`src1`.`code`" in sql, f"e_ref2 FROM missing:\n{sql}"
        assert "fixed_val" in sql, f"e_lit literal value missing:\n{sql}"

        assert "e_ref1" in refs and "src0" in refs["e_ref1"], (
            f"e_ref1 desc ref wrong: {refs.get('e_ref1')}"
        )
        assert "e_ref2" in refs and "src1" in refs["e_ref2"], (
            f"e_ref2 desc ref wrong: {refs.get('e_ref2')}"
        )
        assert "e_lit" not in refs, (
            f"e_lit is a literal; desc ref col should be empty, got: {refs.get('e_lit')}"
        )

    def test_show_create_add_tag_without_any_set(self):
        """SHOW CREATE has no FROM clause for an ADD TAG that was never SET.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, show_create, describe, alter, tag_ref

        """
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER STABLE vstb ADD TAG orphan INT")
        # Intentionally no SET TAG

        sql  = self._get_show_create_sql("v0")
        refs = self._get_desc_ref_col("v0")

        # The new tag must appear in the schema but without a FROM clause
        assert "orphan" in sql, f"New tag 'orphan' missing from SHOW CREATE:\n{sql}"
        assert "orphan" not in refs, (
            f"Unset tag 'orphan' should have empty desc ref col, got: {refs.get('orphan')}"
        )

    def test_show_create_both_vtables_get_independent_refs(self):
        """SHOW CREATE is correct for v0 and v1 when each gets a different ref.

        ADD extra1 to vstb; SET extra1 = src0.city on v0 and
        SET extra1 = src1.city on v1. Each vtable must show its own source.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, show_create, describe, alter, tag_ref

        """
        tdSql.execute(f"USE {DB}")
        tdSql.execute("ALTER STABLE vstb ADD TAG extra1 NCHAR(20)")
        tdSql.execute("ALTER VTABLE v0 SET TAG extra1 = src0.city")
        tdSql.execute("ALTER VTABLE v1 SET TAG extra1 = src1.city")

        sql_v0 = self._get_show_create_sql("v0")
        sql_v1 = self._get_show_create_sql("v1")
        refs_v0 = self._get_desc_ref_col("v0")
        refs_v1 = self._get_desc_ref_col("v1")

        assert f"`{DB}`.`src0`.`city`" in sql_v0, (
            f"v0 extra1 should reference src0.city:\n{sql_v0}"
        )
        assert f"`{DB}`.`src1`.`city`" in sql_v1, (
            f"v1 extra1 should reference src1.city:\n{sql_v1}"
        )
        assert "extra1" in refs_v0 and "src0" in refs_v0["extra1"], (
            f"v0 extra1 desc ref wrong: {refs_v0.get('extra1')}"
        )
        assert "extra1" in refs_v1 and "src1" in refs_v1["extra1"], (
            f"v1 extra1 desc ref wrong: {refs_v1.get('extra1')}"
        )
