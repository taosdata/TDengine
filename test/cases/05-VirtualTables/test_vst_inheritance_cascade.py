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
from new_test_framework.utils import tdLog, tdSql, etool, tdCom


DB = "test_cascade"


class TestVstInheritanceCascade:

    def setup_class(cls):
        tdLog.info("prepare database and source tables")
        tdSql.execute(f"drop database if exists {DB}")
        tdSql.execute(f"create database {DB}")
        tdSql.execute(f"use {DB}")

        # Source tables for VCT column references
        tdSql.execute(
            f"create stable src_stb (ts timestamp, c1 int, c2 float, c3 double) "
            f"tags (loc int)"
        )
        tdSql.execute(f"create table src_t1 using src_stb tags (1)")
        tdSql.execute(f"insert into src_t1 values (now, 10, 1.5, 3.14)")
        tdSql.execute(f"insert into src_t1 values (now+1s, 20, 2.5, 6.28)")
        tdSql.execute(f"insert into src_t1 values (now+2s, 30, 3.5, 9.42)")

        tdSql.execute(f"create table src_t2 using src_stb tags (2)")
        tdSql.execute(f"insert into src_t2 values (now, 100, 10.0, 99.9)")
        tdSql.execute(f"insert into src_t2 values (now+1s, 200, 20.0, 88.8)")

        # Parent VSTs
        tdSql.execute(
            f"create stable p_device (ts timestamp, status int, temp float) "
            f"tags (region int, site binary(32)) virtual 1"
        )
        tdSql.execute(
            f"create stable p_metric (ts timestamp, val double) "
            f"tags (unit nchar(8)) virtual 1"
        )

    # -- helpers -------------------------------------------------

    @staticmethod
    def _check_inherit_rows(child_name, expected):
        tdSql.query(
            f"select * from information_schema.ins_vstable_inherits "
            f"where child_stable_name = '{child_name}'"
        )
        tdSql.checkRows(expected)

    @staticmethod
    def _check_show_create(stb_name, should_have_base_on, parents=None):
        tdSql.query(f"show create stable {DB}.{stb_name}")
        stmt = tdSql.queryResult[0][1]
        tdLog.info(f"SHOW CREATE {stb_name}: {stmt}")
        if should_have_base_on:
            assert "BASE ON" in stmt, f"Expected BASE ON in: {stmt}"
            if parents:
                for p in parents:
                    assert p in stmt, f"Expected '{p}' in: {stmt}"
        else:
            assert "BASE ON" not in stmt, f"Unexpected BASE ON in: {stmt}"

    # ============================================================
    # Test 1: CREATE with single / multi parent
    # ============================================================
    def test_create_with_inheritance(self):
        """VST Inheritance: CREATE with BASE ON

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance

        Jira: None

        History:
            - 2025-5-10 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable leaf_a (ts timestamp, accuracy int) "
            f"tags (sensor_id int) base on {DB}.p_device virtual 1"
        )
        self._check_inherit_rows("leaf_a", 1)

        tdSql.execute(
            f"create stable leaf_b (ts timestamp, quality int) "
            f"tags (device_id int) base on {DB}.p_device, {DB}.p_metric virtual 1"
        )
        self._check_inherit_rows("leaf_b", 2)

    # ============================================================
    # Test 2: ALTER ADD BASE ON
    # ============================================================
    def test_alter_add_base_on(self):
        """VST Inheritance: ALTER ADD BASE ON

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, alter

        Jira: None

        History:
            - 2025-5-10 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable standalone (ts timestamp, own_col int) "
            f"tags (own_tag int) virtual 1"
        )
        self._check_inherit_rows("standalone", 0)

        tdSql.execute(f"alter stable {DB}.standalone add base on {DB}.p_device")
        self._check_inherit_rows("standalone", 1)
        self._check_show_create("standalone", True, ["p_device"])

        tdSql.execute(f"alter stable {DB}.standalone add base on {DB}.p_metric")
        self._check_inherit_rows("standalone", 2)
        self._check_show_create("standalone", True, ["p_device", "p_metric"])

    # ============================================================
    # Test 3: ALTER DROP BASE ON
    # ============================================================
    def test_alter_drop_base_on(self):
        """VST Inheritance: ALTER DROP BASE ON

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, alter

        Jira: None

        History:
            - 2025-5-10 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(f"alter stable {DB}.standalone drop base on {DB}.p_metric")
        self._check_inherit_rows("standalone", 1)
        self._check_show_create("standalone", True, ["p_device"])

        tdSql.execute(f"alter stable {DB}.standalone drop base on {DB}.p_device")
        self._check_inherit_rows("standalone", 0)

    # ============================================================
    # Test 4: Repeated ADD / DROP cycles
    # ============================================================
    def test_add_drop_cycles(self):
        """VST Inheritance: repeated ADD/DROP cycles

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance

        Jira: None

        History:
            - 2025-5-10 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable cycled (ts timestamp, c1 int) tags (t1 int) virtual 1"
        )

        for i in range(3):
            tdLog.info(f"cycle {i + 1}")
            tdSql.execute(f"alter stable {DB}.cycled add base on {DB}.p_device")
            self._check_inherit_rows("cycled", 1)

            tdSql.execute(f"alter stable {DB}.cycled add base on {DB}.p_metric")
            self._check_inherit_rows("cycled", 2)

            tdSql.execute(f"alter stable {DB}.cycled drop base on {DB}.p_device")
            self._check_inherit_rows("cycled", 1)

            tdSql.execute(f"alter stable {DB}.cycled drop base on {DB}.p_metric")
            self._check_inherit_rows("cycled", 0)

    # ============================================================
    # Test 5: Column conflict on ADD BASE ON
    # ============================================================
    def test_add_conflict(self):
        """VST Inheritance: column name conflict

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, error

        Jira: None

        History:
            - 2025-5-10 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable p_conflict (ts timestamp, status int) "
            f"tags (conf_tag int) virtual 1"
        )
        tdSql.error(f"alter stable {DB}.leaf_a add base on {DB}.p_conflict")
        self._check_inherit_rows("leaf_a", 1)

    # ============================================================
    # Test 6: Tag conflict on ADD BASE ON
    # ============================================================
    def test_add_tag_conflict(self):
        """VST Inheritance: tag name conflict

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, error

        Jira: None

        History:
            - 2025-5-10 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable p_tag_conflict (ts timestamp, tc_col int) "
            f"tags (region int) virtual 1"
        )
        tdSql.error(f"alter stable {DB}.leaf_a add base on {DB}.p_tag_conflict")
        self._check_inherit_rows("leaf_a", 1)

    # ============================================================
    # Test 7: Circular inheritance detection
    # ============================================================
    def test_circular_detection(self):
        """VST Inheritance: circular dependency detection

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, error

        Jira: None

        History:
            - 2025-5-10 Created
        """
        tdSql.execute(f"use {DB}")

        # Direct cycle: leaf_a → p_device, try p_device → leaf_a
        tdSql.error(f"alter stable {DB}.p_device add base on {DB}.leaf_a")

        # Indirect cycle: A→B→C, try C→A
        tdSql.execute(
            f"create stable chain_a (ts timestamp, ca int) tags (ta int) virtual 1"
        )
        tdSql.execute(
            f"create stable chain_b (ts timestamp, cb int) tags (tb int) "
            f"base on {DB}.chain_a virtual 1"
        )
        tdSql.execute(
            f"create stable chain_c (ts timestamp, cc int) tags (tc int) "
            f"base on {DB}.chain_b virtual 1"
        )
        tdSql.error(f"alter stable {DB}.chain_a add base on {DB}.chain_c")

    # ============================================================
    # Test 8: Max parents limit (10)
    # ============================================================
    def test_max_parents(self):
        """VST Inheritance: max 10 parents

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, error

        Jira: None

        History:
            - 2025-5-10 Created
        """
        tdSql.execute(f"use {DB}")

        for i in range(10):
            tdSql.execute(
                f"create stable mp_{i} (ts timestamp, mp_c{i} int) "
                f"tags (mp_t{i} int) virtual 1"
            )

        parent_list = ", ".join([f"{DB}.mp_{i}" for i in range(10)])
        tdSql.execute(
            f"create stable max_child (ts timestamp, mc int) "
            f"tags (mt int) base on {parent_list} virtual 1"
        )
        self._check_inherit_rows("max_child", 10)

        tdSql.execute(
            f"create stable mp_extra (ts timestamp, mp_extra_c int) "
            f"tags (mp_extra_t int) virtual 1"
        )
        tdSql.error(f"alter stable {DB}.max_child add base on {DB}.mp_extra")

    # ============================================================
    # Test 9: Non-leaf cannot have VCT
    # ============================================================
    def test_nonleaf_no_vct(self):
        """VST Inheritance: non-leaf rejects VCT creation

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, error

        Jira: None

        History:
            - 2025-5-10 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.error(
            f"create vtable nonleaf_vct "
            f"({DB}.src_t1.ts, {DB}.src_t1.c1, {DB}.src_t1.c2) "
            f"using {DB}.p_device "
            f"tags (1, 'test')"
        )

    # ============================================================
    # Test 10: VCT on leaf, insert data, query
    # ============================================================
    def test_leaf_vct_query(self):
        """VST Inheritance: leaf VCT query

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, query

        Jira: None

        History:
            - 2025-5-10 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create vtable vct_a1 "
            f"(status FROM {DB}.src_t1.c1, "
            f" temp FROM {DB}.src_t1.c2, "
            f" accuracy FROM {DB}.src_t1.c1) "
            f"using {DB}.leaf_a "
            f"tags (1, 'beijing', 100)"
        )

        tdSql.query(f"select * from {DB}.leaf_a")
        tdSql.checkRows(3)

        tdSql.query(f"select status, temp from {DB}.leaf_a")
        tdSql.checkRows(3)

        tdSql.query(f"select accuracy from {DB}.leaf_a")
        tdSql.checkRows(3)

        tdSql.query(f"select sensor_id, region from {DB}.leaf_a limit 1")
        tdSql.checkRows(1)

    # ============================================================
    # Test 11: Query leaf_b with VCT
    # ============================================================
    def test_parent_vst_query(self):
        """VST Inheritance: leaf_b VCT query

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, query

        Jira: None

        History:
            - 2025-5-10 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create vtable vct_b1 "
            f"(status FROM {DB}.src_t2.c1, "
            f" temp FROM {DB}.src_t2.c2, "
            f" val FROM {DB}.src_t2.c3, "
            f" quality FROM {DB}.src_t2.c1) "
            f"using {DB}.leaf_b "
            f"tags (2, 'shanghai', 'celsius', 200)"
        )

        tdSql.query(f"select * from {DB}.leaf_b")
        tdSql.checkRows(2)

    # ============================================================
    # Test 12: ADD parent then query
    # ============================================================
    def test_add_parent_then_query(self):
        """VST Inheritance: add parent and verify query

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, query

        Jira: None

        History:
            - 2025-5-10 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable p_extra (ts timestamp, extra_val int) "
            f"tags (extra_tag binary(16)) virtual 1"
        )

        tdSql.execute(f"alter stable {DB}.leaf_a add base on {DB}.p_extra")
        self._check_inherit_rows("leaf_a", 2)

        tdSql.query(f"select ts, status, accuracy from {DB}.leaf_a")
        tdSql.checkRows(3)

    # ============================================================
    # Test 13: DROP parent then query
    # ============================================================
    def test_drop_parent_then_query(self):
        """VST Inheritance: drop parent and verify query

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, query

        Jira: None

        History:
            - 2025-5-10 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(f"alter stable {DB}.leaf_a drop base on {DB}.p_extra")
        self._check_inherit_rows("leaf_a", 1)

        tdSql.query(f"select ts, status, accuracy from {DB}.leaf_a")
        tdSql.checkRows(3)

    # ============================================================
    # Test 14: DROP BASE ON with VCT — colRef cascade
    # ============================================================
    def test_drop_base_on_with_vct(self):
        """VST Inheritance: drop parent cascades column removal

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, alter

        Jira: None

        History:
            - 2025-5-10 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(f"alter stable {DB}.leaf_b drop base on {DB}.p_metric")
        self._check_inherit_rows("leaf_b", 1)

        tdSql.query(f"select ts, status, quality from {DB}.leaf_b")
        tdSql.checkRows(2)

        tdSql.error(f"select val from {DB}.leaf_b")

    # ============================================================
    # Test 15: Re-add dropped parent
    # ============================================================
    def test_readd_parent(self):
        """VST Inheritance: re-add previously dropped parent

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, alter

        Jira: None

        History:
            - 2025-5-10 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(f"alter stable {DB}.leaf_b add base on {DB}.p_metric")
        self._check_inherit_rows("leaf_b", 2)

        tdSql.query(f"select ts, status, quality from {DB}.leaf_b")
        tdSql.checkRows(2)

    # ============================================================
    # Test 16: Non-virtual parent rejected
    # ============================================================
    def test_add_non_virtual_parent(self):
        """VST Inheritance: reject non-virtual parent

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, error

        Jira: None

        History:
            - 2025-5-10 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable regular_stb (ts timestamp, c1 int) tags (t1 int)"
        )
        tdSql.error(f"alter stable {DB}.standalone add base on {DB}.regular_stb")

    # ============================================================
    # Test 17: Parent with VCT cannot be inherited (full coverage)
    # ============================================================
    def test_parent_with_vct(self):
        """VST Inheritance: parent with VCT rejected

        Tests all scenarios:
        a) ALTER ADD BASE ON parent with VCT
        b) CREATE STABLE BASE ON parent with VCT
        c) CREATE multi-parent, one has VCT
        d) ALTER multi-parent, one has VCT
        e) Drop partial VCTs, still has VCT
        f) Drop ALL VCTs, CREATE succeeds
        g) Drop ALL VCTs, ALTER succeeds

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, error

        Jira: None

        History:
            - 2025-5-10 Created
        """
        tdSql.execute(f"use {DB}")

        ERR_MSG = "Parent VST already has VCT, cannot be inherited"

        tdSql.execute(
            f"create stable vct_parent (ts timestamp, vp_col int) "
            f"tags (vp_tag int) virtual 1"
        )
        tdSql.execute(
            f"create vtable vct_on_parent1 "
            f"(vp_col FROM {DB}.src_t1.c1) "
            f"using {DB}.vct_parent tags (1)"
        )
        tdSql.execute(
            f"create vtable vct_on_parent2 "
            f"(vp_col FROM {DB}.src_t1.c1) "
            f"using {DB}.vct_parent tags (2)"
        )

        # 17a: ALTER path
        tdSql.execute(
            f"create stable attempt_alter (ts timestamp, aa_col int) "
            f"tags (aa_tag int) virtual 1"
        )
        tdSql.error(
            f"alter stable {DB}.attempt_alter add base on {DB}.vct_parent",
            expectErrInfo=ERR_MSG,
        )

        # 17b: CREATE path
        tdSql.error(
            f"create stable attempt_create (ts timestamp, ac_col int) "
            f"tags (ac_tag int) base on {DB}.vct_parent virtual 1",
            expectErrInfo=ERR_MSG,
        )

        # 17c: CREATE multi-parent
        tdSql.execute(
            f"create stable clean_parent (ts timestamp, cp_col float) "
            f"tags (cp_tag int) virtual 1"
        )
        tdSql.error(
            f"create stable attempt_multi (ts timestamp, am_col int) "
            f"tags (am_tag int) base on {DB}.clean_parent, {DB}.vct_parent virtual 1",
            expectErrInfo=ERR_MSG,
        )

        # 17d: ALTER multi-parent
        tdSql.execute(
            f"create stable attempt_alter_multi (ts timestamp, aam_col int) "
            f"tags (aam_tag int) virtual 1"
        )
        tdSql.error(
            f"alter stable {DB}.attempt_alter_multi "
            f"add base on {DB}.clean_parent, {DB}.vct_parent",
            expectErrInfo=ERR_MSG,
        )

        # 17e: Drop one VCT, still has another
        tdSql.execute(f"drop table vct_on_parent1")
        tdSql.error(
            f"create stable attempt_partial (ts timestamp, ap_col int) "
            f"tags (ap_tag int) base on {DB}.vct_parent virtual 1",
            expectErrInfo=ERR_MSG,
        )

        # 17f: Drop ALL VCTs, CREATE succeeds
        tdSql.execute(f"drop table vct_on_parent2")
        tdSql.execute(
            f"create stable child_after_drop (ts timestamp, cad_col int) "
            f"tags (cad_tag int) base on {DB}.vct_parent virtual 1"
        )
        self._check_inherit_rows("child_after_drop", 1)

        # 17g: ALTER succeeds after all VCTs dropped
        tdSql.execute(
            f"create stable alt_after_drop (ts timestamp, aad_col int) "
            f"tags (aad_tag int) virtual 1"
        )
        tdSql.execute(
            f"alter stable {DB}.alt_after_drop add base on {DB}.vct_parent"
        )
        self._check_inherit_rows("alt_after_drop", 1)

    # ============================================================
    # Test 18: Cross-DB inheritance rejected
    # ============================================================
    def test_cross_db(self):
        """VST Inheritance: cross-database rejected

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, error

        Jira: None

        History:
            - 2025-5-10 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(f"create database if not exists cross_db_test")
        tdSql.execute(
            f"create stable cross_db_test.xdb_parent (ts timestamp, xc int) "
            f"tags (xt int) virtual 1"
        )
        tdSql.error(
            f"alter stable {DB}.standalone add base on cross_db_test.xdb_parent"
        )
        tdSql.execute(f"drop database cross_db_test")

    # ============================================================
    # Test 19: Multi-level inheritance (grandparent → parent → leaf)
    # ============================================================
    def test_multi_level_query(self):
        """VST Inheritance: multi-level chain query

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, query

        Jira: None

        History:
            - 2025-5-10 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable gp (ts timestamp, gp_col int) "
            f"tags (gp_tag int) virtual 1"
        )
        tdSql.execute(
            f"create stable mid (ts timestamp, mid_col int) "
            f"tags (mid_tag int) base on {DB}.gp virtual 1"
        )
        tdSql.execute(
            f"create stable leaf_deep (ts timestamp, ld_col int) "
            f"tags (ld_tag int) base on {DB}.mid virtual 1"
        )

        tdSql.execute(f"create table src_deep using src_stb tags (99)")
        tdSql.execute(f"insert into src_deep values (now, 1, 2.0, 3.0)")
        tdSql.execute(f"insert into src_deep values (now+1s, 4, 5.0, 6.0)")

        tdSql.execute(
            f"create vtable vct_deep "
            f"(gp_col FROM {DB}.src_deep.c1, "
            f" mid_col FROM {DB}.src_deep.c1, "
            f" ld_col FROM {DB}.src_deep.c1) "
            f"using {DB}.leaf_deep "
            f"tags (3, 2, 1)"
        )

        tdSql.query(f"select * from {DB}.leaf_deep")
        tdSql.checkRows(2)

    # ============================================================
    # Test 20: Diamond inheritance
    # ============================================================
    def test_diamond_inheritance(self):
        """VST Inheritance: diamond topology

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance

        Jira: None

        History:
            - 2025-5-10 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable dia_a (ts timestamp, da_col int) "
            f"tags (da_tag int) virtual 1"
        )
        tdSql.execute(
            f"create stable dia_b (ts timestamp, db_col int) "
            f"tags (db_tag int) virtual 1"
        )
        tdSql.execute(
            f"create stable dia_leaf1 (ts timestamp, dl1_col int) "
            f"tags (dl1_tag int) base on {DB}.dia_a, {DB}.dia_b virtual 1"
        )
        tdSql.execute(
            f"create stable dia_leaf2 (ts timestamp, dl2_col int) "
            f"tags (dl2_tag int) base on {DB}.dia_a, {DB}.dia_b virtual 1"
        )

        self._check_inherit_rows("dia_leaf1", 2)
        self._check_inherit_rows("dia_leaf2", 2)

        tdSql.execute(f"create table src_dia1 using src_stb tags (10)")
        tdSql.execute(f"insert into src_dia1 values (now, 111, 1.1, 11.1)")

        tdSql.execute(f"create table src_dia2 using src_stb tags (20)")
        tdSql.execute(f"insert into src_dia2 values (now, 222, 2.2, 22.2)")
        tdSql.execute(f"insert into src_dia2 values (now+1s, 333, 3.3, 33.3)")

        tdSql.execute(
            f"create vtable vct_dia1 "
            f"(da_col FROM {DB}.src_dia1.c1, "
            f" db_col FROM {DB}.src_dia1.c1, "
            f" dl1_col FROM {DB}.src_dia1.c1) "
            f"using {DB}.dia_leaf1 "
            f"tags (10, 100, 1)"
        )
        tdSql.execute(
            f"create vtable vct_dia2 "
            f"(da_col FROM {DB}.src_dia2.c1, "
            f" db_col FROM {DB}.src_dia2.c1, "
            f" dl2_col FROM {DB}.src_dia2.c1) "
            f"using {DB}.dia_leaf2 "
            f"tags (20, 200, 2)"
        )
