###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

# Test cases for the hasChildren cache in VST inheritance.
# The cache tracks whether a parent VST has child VSTs, enabling:
#   - Refusing DROP of a parent that still has children
#   - Setting hasInheritors in table meta responses
#
# Covered scenarios:
#   1. DROP parent with children → refused
#   2. DROP leaf (no children) → succeeds
#   3. DROP parent after all children removed → succeeds
#   4. hasInheritors correct after CREATE BASE ON
#   5. hasInheritors correct after ALTER ADD BASE ON
#   6. hasInheritors correct after ALTER DROP BASE ON
#   7. hasInheritors correct after DROP child
#   8. Multi-level: drop middle refuses while leaf exists
#   9. Multiple children: drop one, parent still protected
#  10. Repeated add/drop cycles maintain correct state

from new_test_framework.utils import tdLog, tdSql, etool, tdCom

DB = "test_has_children"


class TestVstHasChildrenCache:

    def setup_class(cls):
        tdLog.info("setup database and source tables for hasChildren cache tests")
        tdSql.execute(f"drop database if exists {DB}")
        tdSql.execute(f"create database {DB}")
        tdSql.execute(f"use {DB}")

        # Source stable for VCT column references
        tdSql.execute(
            f"create stable src (ts timestamp, c1 int, c2 float) tags (loc int)"
        )
        tdSql.execute(f"create table src_t1 using src tags (1)")
        tdSql.execute(f"insert into src_t1 values (now, 10, 1.5)")
        tdSql.execute(f"insert into src_t1 values (now+1s, 20, 2.5)")

    # -- helpers -------------------------------------------------

    @staticmethod
    def _create_vst(name, cols="own_col int", tags="own_tag int", parents=None):
        """Create a virtual stable, optionally with BASE ON parents."""
        parent_clause = f" base on {parents}" if parents else ""
        tdSql.execute(
            f"create stable {name} (ts timestamp, {cols}) "
            f"tags ({tags}){parent_clause} virtual 1"
        )

    @staticmethod
    def _create_vct(name, using, col_refs, tags):
        """Create a vtable referencing src_t1 columns."""
        ref_parts = ", ".join(
            f"{col} FROM {DB}.src_t1.{src_col}" for col, src_col in col_refs
        )
        tdSql.execute(
            f"create vtable {name} ({ref_parts}) using {DB}.{using} tags ({tags})"
        )

    @staticmethod
    def _assert_has_inheritors(stb_name, expected):
        """Check hasChildren via ins_vstable_inherits system table."""
        tdSql.query(
            f"select count(*) from information_schema.ins_vstable_inherits "
            f"where parent_stable_name = '{stb_name}'"
        )
        count = tdSql.queryResult[0][0]
        if expected:
            assert count > 0, f"Expected {stb_name} to have children, but count={count}"
        else:
            assert count == 0, f"Expected {stb_name} to have NO children, but count={count}"

    @staticmethod
    def _drop_stable_expect_fail(full_name):
        """Attempt to DROP a stable that has children; expect failure."""
        tdSql.error(f"drop stable {full_name}")

    # ============================================================
    # Test 1: DROP parent with children is refused
    # ============================================================
    def test_drop_parent_with_children_refused(self):
        """hasChildren: DROP parent with children refused

        Verify that a parent VST with at least one child VST cannot be
        dropped.  This exercises the hasChildren cache hit path.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, hasChildren

        Jira: None

        History:
            - 2025-06-02 Created
        """
        tdSql.execute(f"use {DB}")

        self._create_vst("drop_parent", "dp_col int", "dp_tag int")
        self._create_vst(
            "drop_child", "dc_col int", "dc_tag int",
            parents=f"{DB}.drop_parent"
        )

        # Parent should have children
        self._assert_has_inheritors("drop_parent", True)

        # DROP parent must fail
        self._drop_stable_expect_fail(f"{DB}.drop_parent")

        # Parent still exists
        tdSql.query(f"show stables like 'drop_parent'")
        tdSql.checkRows(1)

    # ============================================================
    # Test 2: DROP leaf (no children) succeeds
    # ============================================================
    def test_drop_leaf_succeeds(self):
        """hasChildren: DROP leaf VST succeeds

        A leaf VST (no children) should be droppable.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, hasChildren

        Jira: None

        History:
            - 2025-06-02 Created
        """
        tdSql.execute(f"use {DB}")

        self._create_vst("leaf_only", "lo_col int", "lo_tag int")
        self._assert_has_inheritors("leaf_only", False)

        tdSql.execute(f"drop stable {DB}.leaf_only")

        tdSql.query(f"show stables like 'leaf_only'")
        tdSql.checkRows(0)

    # ============================================================
    # Test 3: DROP parent after all children removed
    # ============================================================
    def test_drop_parent_after_children_removed(self):
        """hasChildren: DROP parent succeeds after children dropped

        After all child VSTs are dropped, the parent's hasChildren
        cache should be invalidated and the parent becomes droppable.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, hasChildren

        Jira: None

        History:
            - 2025-06-02 Created
        """
        tdSql.execute(f"use {DB}")

        self._create_vst("rem_parent", "rp_col int", "rp_tag int")
        self._create_vst(
            "rem_child1", "rc1_col int", "rc1_tag int",
            parents=f"{DB}.rem_parent"
        )
        self._create_vst(
            "rem_child2", "rc2_col int", "rc2_tag int",
            parents=f"{DB}.rem_parent"
        )

        self._assert_has_inheritors("rem_parent", True)
        self._drop_stable_expect_fail(f"{DB}.rem_parent")

        # Drop first child — parent still has one child
        tdSql.execute(f"drop stable {DB}.rem_child1")
        self._drop_stable_expect_fail(f"{DB}.rem_parent")

        # Drop last child — parent becomes droppable
        tdSql.execute(f"drop stable {DB}.rem_child2")
        tdSql.execute(f"drop stable {DB}.rem_parent")

        tdSql.query(f"show stables like 'rem_%'")
        tdSql.checkRows(0)

    # ============================================================
    # Test 4: hasInheritors correct after CREATE BASE ON
    # ============================================================
    def test_has_inheritors_after_create(self):
        """hasChildren: cache correct after CREATE with BASE ON

        After creating a child VST with BASE ON, the parent should
        report hasInheritors = true.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, hasChildren

        Jira: None

        History:
            - 2025-06-02 Created
        """
        tdSql.execute(f"use {DB}")

        self._create_vst("hi_parent", "hi_col int", "hi_tag int")
        self._assert_has_inheritors("hi_parent", False)

        self._create_vst(
            "hi_child", "hc_col int", "hc_tag int",
            parents=f"{DB}.hi_parent"
        )
        self._assert_has_inheritors("hi_parent", True)

    # ============================================================
    # Test 5: hasInheritors correct after ALTER ADD BASE ON
    # ============================================================
    def test_has_inheritors_after_alter_add(self):
        """hasChildren: cache correct after ALTER ADD BASE ON

        After ALTER ADD BASE ON, the new parent should show
        hasInheritors = true.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, hasChildren

        Jira: None

        History:
            - 2025-06-02 Created
        """
        tdSql.execute(f"use {DB}")

        self._create_vst("aa_parent", "aa_col int", "aa_tag int")
        self._create_vst("aa_child", "ac_col int", "ac_tag int")

        self._assert_has_inheritors("aa_parent", False)

        tdSql.execute(f"alter stable {DB}.aa_child add base on {DB}.aa_parent")
        self._assert_has_inheritors("aa_parent", True)

    # ============================================================
    # Test 6: hasInheritors correct after ALTER DROP BASE ON
    # ============================================================
    def test_has_inheritors_after_alter_drop(self):
        """hasChildren: cache correct after ALTER DROP BASE ON

        After ALTER DROP BASE ON, if the parent has no remaining
        children, hasInheritors should become false.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, hasChildren

        Jira: None

        History:
            - 2025-06-02 Created
        """
        tdSql.execute(f"use {DB}")

        self._create_vst("ad_parent", "ad_col int", "ad_tag int")
        self._create_vst(
            "ad_child", "adc_col int", "adc_tag int",
            parents=f"{DB}.ad_parent"
        )
        self._assert_has_inheritors("ad_parent", True)

        tdSql.execute(f"alter stable {DB}.ad_child drop base on {DB}.ad_parent")
        self._assert_has_inheritors("ad_parent", False)

        # Parent is now droppable
        tdSql.execute(f"drop stable {DB}.ad_parent")

    # ============================================================
    # Test 7: hasInheritors correct after DROP child VST
    # ============================================================
    def test_has_inheritors_after_drop_child(self):
        """hasChildren: cache correct after DROP child VST

        After dropping a child VST, the parent's hasInheritors
        should be recomputed correctly.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, hasChildren

        Jira: None

        History:
            - 2025-06-02 Created
        """
        tdSql.execute(f"use {DB}")

        self._create_vst("dc_parent", "dc_col int", "dc_tag int")
        self._create_vst(
            "dc_child", "dcc int", "dct int",
            parents=f"{DB}.dc_parent"
        )
        self._assert_has_inheritors("dc_parent", True)

        tdSql.execute(f"drop stable {DB}.dc_child")
        self._assert_has_inheritors("dc_parent", False)

    # ============================================================
    # Test 8: Multi-level — drop middle refuses while leaf exists
    # ============================================================
    def test_multi_level_drop_middle_refused(self):
        """hasChildren: multi-level, drop middle refused

        In a chain grandparent → middle → leaf, the middle VST
        cannot be dropped because it has the leaf as a child.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, hasChildren

        Jira: None

        History:
            - 2025-06-02 Created
        """
        tdSql.execute(f"use {DB}")

        self._create_vst("ml_gp", "gp_col int", "gp_tag int")
        self._create_vst(
            "ml_mid", "mid_col int", "mid_tag int",
            parents=f"{DB}.ml_gp"
        )
        self._create_vst(
            "ml_leaf", "leaf_col int", "leaf_tag int",
            parents=f"{DB}.ml_mid"
        )

        # Middle has leaf → cannot drop
        self._drop_stable_expect_fail(f"{DB}.ml_mid")

        # Grandparent has middle → cannot drop
        self._drop_stable_expect_fail(f"{DB}.ml_gp")

        # Drop leaf first → middle becomes droppable
        tdSql.execute(f"drop stable {DB}.ml_leaf")
        tdSql.execute(f"drop stable {DB}.ml_mid")
        tdSql.execute(f"drop stable {DB}.ml_gp")

    # ============================================================
    # Test 9: Multiple children — drop one, parent still protected
    # ============================================================
    def test_multiple_children_partial_drop(self):
        """hasChildren: multiple children, partial drop

        A parent with 3 children: dropping 2 should still protect
        the parent. Only after the last child is dropped can the
        parent be dropped.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, hasChildren

        Jira: None

        History:
            - 2025-06-02 Created
        """
        tdSql.execute(f"use {DB}")

        self._create_vst("mc_parent", "mc_col int", "mc_tag int")
        for i in range(3):
            self._create_vst(
                f"mc_child{i}", f"cc{i}_col int", f"cc{i}_tag int",
                parents=f"{DB}.mc_parent"
            )

        self._assert_has_inheritors("mc_parent", True)
        self._drop_stable_expect_fail(f"{DB}.mc_parent")

        # Drop child 0 — parent still has children 1, 2
        tdSql.execute(f"drop stable {DB}.mc_child0")
        self._assert_has_inheritors("mc_parent", True)
        self._drop_stable_expect_fail(f"{DB}.mc_parent")

        # Drop child 1 — parent still has child 2
        tdSql.execute(f"drop stable {DB}.mc_child1")
        self._drop_stable_expect_fail(f"{DB}.mc_parent")

        # Drop last child — parent is now droppable
        tdSql.execute(f"drop stable {DB}.mc_child2")
        self._assert_has_inheritors("mc_parent", False)
        tdSql.execute(f"drop stable {DB}.mc_parent")

    # ============================================================
    # Test 10: Repeated add/drop cycles maintain correct state
    # ============================================================
    def test_repeated_add_drop_cycles(self):
        """hasChildren: repeated ADD/DROP cycles

        Repeatedly add and remove parents from a child VST.
        After each operation, verify the parent's hasChildren
        cache is correct.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, hasChildren

        Jira: None

        History:
            - 2025-06-02 Created
        """
        tdSql.execute(f"use {DB}")

        self._create_vst("cyc_parent1", "cp1_col int", "cp1_tag int")
        self._create_vst("cyc_parent2", "cp2_col int", "cp2_tag int")
        self._create_vst("cyc_child", "cy_col int", "cy_tag int")

        for i in range(3):
            tdLog.info(f"cycle {i + 1}: add parent1")
            tdSql.execute(f"alter stable {DB}.cyc_child add base on {DB}.cyc_parent1")
            self._assert_has_inheritors("cyc_parent1", True)
            self._drop_stable_expect_fail(f"{DB}.cyc_parent1")

            tdLog.info(f"cycle {i + 1}: add parent2")
            tdSql.execute(f"alter stable {DB}.cyc_child add base on {DB}.cyc_parent2")
            self._assert_has_inheritors("cyc_parent2", True)

            tdLog.info(f"cycle {i + 1}: drop parent1")
            tdSql.execute(f"alter stable {DB}.cyc_child drop base on {DB}.cyc_parent1")
            self._assert_has_inheritors("cyc_parent1", False)

            tdLog.info(f"cycle {i + 1}: drop parent2")
            tdSql.execute(f"alter stable {DB}.cyc_child drop base on {DB}.cyc_parent2")
            self._assert_has_inheritors("cyc_parent2", False)

        # After all cycles, verify final state. Note: ALTER DROP BASE ON
        # is async (transaction-based), so we verify via ins_vstable_inherits
        # which reflects committed state. Cleanup happens via database drop.
        self._assert_has_inheritors("cyc_parent1", False)
        self._assert_has_inheritors("cyc_parent2", False)

    # ============================================================
    # Test 11: Non-virtual STB is always droppable
    # ============================================================
    def test_non_virtual_always_droppable(self):
        """hasChildren: non-virtual STB not affected by cache

        A regular (non-virtual) STB should always be droppable
        regardless of any VST inheritance. The hasChildren cache
        only applies to virtual STBs.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, hasChildren

        Jira: None

        History:
            - 2025-06-02 Created
        """
        tdSql.execute(f"use {DB}")

        # Regular STB — not virtual, always droppable
        tdSql.execute(
            f"create stable regular (ts timestamp, r_col int) tags (r_tag int)"
        )
        tdSql.execute(f"drop stable {DB}.regular")

    # ============================================================
    # Test 12: DROP child with VCT, then verify parent state
    # ============================================================
    def test_drop_child_with_vct_then_parent_state(self):
        """hasChildren: DROP child with VCT, parent becomes droppable

        Create a parent VST, add a child VST with VCT, verify parent
        is not droppable. Drop the child VST (must succeed since it
        has no child VSTs of its own). Verify parent is now droppable.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, hasChildren

        Jira: None

        History:
            - 2025-06-02 Created
        """
        tdSql.execute(f"use {DB}")

        self._create_vst(
            "vct_parent", "vp_col int", "vp_tag int"
        )
        self._create_vst(
            "vct_child", "vc_col int", "vc_tag int",
            parents=f"{DB}.vct_parent"
        )

        # Create VCT on child (both cols reference c1 which is int, matching vp_col and vc_col)
        self._create_vct(
            "vct_on_child", "vct_child",
            [("vp_col", "c1"), ("vc_col", "c1")],
            "1, 100"
        )

        self._assert_has_inheritors("vct_parent", True)
        self._drop_stable_expect_fail(f"{DB}.vct_parent")

        # Child is a leaf (no child VSTs), so it can be dropped
        # even though it has VCTs
        tdSql.execute(f"drop table vct_on_child")
        tdSql.execute(f"drop stable {DB}.vct_child")

        # Parent is now droppable
        self._assert_has_inheritors("vct_parent", False)
        tdSql.execute(f"drop stable {DB}.vct_parent")

    # ============================================================
    # Test 13: Diamond — drop one leaf, shared parent still protected
    # ============================================================
    def test_diamond_partial_drop(self):
        """hasChildren: diamond, drop one leaf, parent still protected

        Two parents (A, B) each have two children (leaf1, leaf2).
        Drop leaf1. Both parents should still be protected by leaf2.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, hasChildren

        Jira: None

        History:
            - 2025-06-02 Created
        """
        tdSql.execute(f"use {DB}")

        self._create_vst("dia_a", "da_col int", "da_tag int")
        self._create_vst("dia_b", "db_col int", "db_tag int")

        self._create_vst(
            "dia_leaf1", "dl1_col int", "dl1_tag int",
            parents=f"{DB}.dia_a, {DB}.dia_b"
        )
        self._create_vst(
            "dia_leaf2", "dl2_col int", "dl2_tag int",
            parents=f"{DB}.dia_a, {DB}.dia_b"
        )

        self._assert_has_inheritors("dia_a", True)
        self._assert_has_inheritors("dia_b", True)

        # Drop leaf1 — parents still protected by leaf2
        tdSql.execute(f"drop stable {DB}.dia_leaf1")
        self._drop_stable_expect_fail(f"{DB}.dia_a")
        self._drop_stable_expect_fail(f"{DB}.dia_b")

        # Drop leaf2 — parents now droppable
        tdSql.execute(f"drop stable {DB}.dia_leaf2")
        tdSql.execute(f"drop stable {DB}.dia_a")
        tdSql.execute(f"drop stable {DB}.dia_b")

    # ============================================================
    # Test 14: Multi-parent child — drop one parent link
    # ============================================================
    def test_multi_parent_alter_drop_one(self):
        """hasChildren: multi-parent child, ALTER DROP one parent

        Child has 2 parents. ALTER DROP BASE ON removes one parent.
        The dropped parent should no longer be protected by this child.
        The remaining parent should still be protected.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, hasChildren

        Jira: None

        History:
            - 2025-06-02 Created
        """
        tdSql.execute(f"use {DB}")

        self._create_vst("mp_a", "mpa_col int", "mpa_tag int")
        self._create_vst("mp_b", "mpb_col int", "mpb_tag int")
        self._create_vst(
            "mp_child", "mpc_col int", "mpc_tag int",
            parents=f"{DB}.mp_a, {DB}.mp_b"
        )

        self._assert_has_inheritors("mp_a", True)
        self._assert_has_inheritors("mp_b", True)

        # Drop parent B — parent A still protected
        tdSql.execute(f"alter stable {DB}.mp_child drop base on {DB}.mp_b")
        self._assert_has_inheritors("mp_b", False)
        self._assert_has_inheritors("mp_a", True)
        self._drop_stable_expect_fail(f"{DB}.mp_a")

        # Parent B is now droppable
        tdSql.execute(f"drop stable {DB}.mp_b")

        # Drop parent A link — parent A becomes droppable
        tdSql.execute(f"alter stable {DB}.mp_child drop base on {DB}.mp_a")
        self._assert_has_inheritors("mp_a", False)
        self._assert_has_inheritors("mp_b", False)

    # ============================================================
    # Test 15: Query parent meta after cascading changes
    # ============================================================
    def test_query_meta_after_cascade(self):
        """hasChildren: query parent meta after cascading DDL

        Perform a sequence of DDL operations and verify that
        querying the parent's meta (via ins_vstable_inherits)
        always returns the correct child count.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, hasChildren

        Jira: None

        History:
            - 2025-06-02 Created
        """
        tdSql.execute(f"use {DB}")

        self._create_vst("casc_parent", "casp_col int", "casp_tag int")

        # Step 1: CREATE child1 → parent has 1 child
        self._create_vst(
            "casc_child1", "cc1 int", "ct1 int",
            parents=f"{DB}.casc_parent"
        )
        tdSql.query(
            f"select count(*) from information_schema.ins_vstable_inherits "
            f"where parent_stable_name = 'casc_parent'"
        )
        assert tdSql.queryResult[0][0] == 1

        # Step 2: CREATE child2 → parent has 2 children
        self._create_vst(
            "casc_child2", "cc2 int", "ct2 int",
            parents=f"{DB}.casc_parent"
        )
        tdSql.query(
            f"select count(*) from information_schema.ins_vstable_inherits "
            f"where parent_stable_name = 'casc_parent'"
        )
        assert tdSql.queryResult[0][0] == 2

        # Step 3: DROP child1 → parent has 1 child
        tdSql.execute(f"drop stable {DB}.casc_child1")
        tdSql.query(
            f"select count(*) from information_schema.ins_vstable_inherits "
            f"where parent_stable_name = 'casc_parent'"
        )
        assert tdSql.queryResult[0][0] == 1

        # Step 4: DROP child2 → parent has 0 children
        tdSql.execute(f"drop stable {DB}.casc_child2")
        tdSql.query(
            f"select count(*) from information_schema.ins_vstable_inherits "
            f"where parent_stable_name = 'casc_parent'"
        )
        assert tdSql.queryResult[0][0] == 0

        # Step 5: Parent is now droppable
        tdSql.execute(f"drop stable {DB}.casc_parent")
