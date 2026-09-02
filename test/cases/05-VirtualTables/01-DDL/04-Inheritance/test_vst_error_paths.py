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

# Test cases for VST inheritance error paths not covered by
# test_vst_inheritance_cascade.py or test_vst_has_children_cache.py.
#
# Covered error codes:
#   - VST_DROP_BASE_MIN_COLS  (0x04A9) -- entirely new
#   - VST_CIRCULAR_INHERIT    (0x04A5) -- self-reference variant
#   - VST_PARENT_NOT_VIRTUAL  (0x04A3) -- nonexistent parent
#   - VST_HAS_CHILDREN        (0x04A2) -- ALTER TAG on parent

from new_test_framework.utils import tdLog, tdSql, etool, tdCom

DB = "test_vst_err"


class TestVstErrorPaths:

    def setup_class(cls):
        tdLog.info("setup database for VST error-path tests")
        tdSql.execute(f"drop database if exists {DB}")
        tdSql.execute(f"create database {DB}")
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable src (ts timestamp, c1 int, c2 float) tags (loc int)"
        )
        tdSql.execute(f"create table src_t1 using src tags (1)")
        tdSql.execute(f"insert into src_t1 values (now, 10, 1.5)")

    # ============================================================
    # Test 1: DROP BASE ON leaves < 2 columns
    # ============================================================
    def test_drop_base_on_minimum_columns(self):
        """VST Error: DROP BASE ON keeps the child's own column

        DROP BASE ON removes the dropped parent's contributed columns but
        keeps the child's own column(s). The minimum-schema guard
        (VST_DROP_BASE_MIN_COLS) only trips when the remaining schema would
        have fewer than 2 columns (ts + 1). Because CREATE already requires
        at least one own data column, a single-parent child always retains
        ts + its own column after the drop, so the drop succeeds.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, error

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable err_parent1 (ts timestamp, pc1 int, pc2 float) "
            f"tags (pt1 int) virtual 1"
        )
        tdSql.execute(
            f"create stable err_child1 (ts timestamp, oc1 int) "
            f"tags (ot1 int) base on {DB}.err_parent1 virtual 1"
        )

        # Drop succeeds: child keeps ts + oc1 (2 columns) and ot1 (1 tag).
        tdSql.execute(
            f"alter stable err_child1 drop base on {DB}.err_parent1"
        )

        # Parent-contributed columns are gone; the own column survives.
        tdSql.error(f"select pc1 from {DB}.err_child1")
        tdSql.query(f"select ts, oc1 from {DB}.err_child1")

        tdSql.execute(f"drop stable err_child1")
        tdSql.execute(f"drop stable err_parent1")

    # ============================================================
    # Test 2: DROP BASE ON leaves 0 tags
    # ============================================================
    def test_drop_base_on_minimum_tags(self):
        """VST Error: DROP BASE ON keeps the child's own tag

        DROP BASE ON removes the dropped parent's contributed tags but keeps
        the child's own tag(s). The minimum-schema guard only trips when the
        remaining schema would have 0 tags. A VST that owns a tag therefore
        survives dropping its parent (its own tag remains), so the drop
        succeeds. (Creating a child with zero own tags requires omitting the
        TAGS clause, which is not a supported BASE ON form.)

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, error

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable err_parent2 (ts timestamp, pc1 int) "
            f"tags (pt1 int) virtual 1"
        )
        tdSql.execute(
            f"create stable err_child2 (ts timestamp, oc1 int, oc2 float) "
            f"tags (ot1 int) base on {DB}.err_parent2 virtual 1"
        )

        # Drop succeeds: child keeps ts + oc1 + oc2 and its own tag ot1.
        tdSql.execute(
            f"alter stable err_child2 drop base on {DB}.err_parent2"
        )

        # Parent-contributed tag is gone; the own tag survives.
        tdSql.error(f"select pt1 from {DB}.err_child2")
        tdSql.query(f"select ts, oc1, oc2, ot1 from {DB}.err_child2")

        tdSql.execute(f"drop stable err_child2")
        tdSql.execute(f"drop stable err_parent2")

    # ============================================================
    # Test 3: Self-inheritance (A inherits A)
    # ============================================================
    def test_self_inheritance(self):
        """VST Error: self-inheritance detected

        ALTER STABLE x ADD BASE ON x should be rejected as circular.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, error

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable err_self (ts timestamp, c1 int) "
            f"tags (t1 int) virtual 1"
        )

        tdSql.error(
            f"alter stable err_self add base on {DB}.err_self"
        )

        tdSql.execute(f"drop stable err_self")

    # ============================================================
    # Test 4: Nonexistent parent in BASE ON
    # ============================================================
    def test_nonexistent_parent(self):
        """VST Error: BASE ON nonexistent stable

        Referencing a stable that does not exist should fail.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, error

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.error(
            f"create stable err_no_parent (ts timestamp, c1 int) "
            f"tags (t1 int) base on {DB}.nonexistent_vst virtual 1"
        )

    # ============================================================
    # Test 5: DROP parent not in inheritance list
    # ============================================================
    def test_drop_parent_not_in_list(self):
        """VST Error: DROP BASE ON for parent never added

        Attempting to drop a parent that was never in the inheritance
        list should fail.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, error

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable err_p5a (ts timestamp, c1 int) "
            f"tags (t1 int) virtual 1"
        )
        tdSql.execute(
            f"create stable err_p5b (ts timestamp, c2 float) "
            f"tags (t2 int) virtual 1"
        )
        tdSql.execute(
            f"create stable err_ch5 (ts timestamp, c3 double) "
            f"tags (t3 int) base on {DB}.err_p5a virtual 1"
        )

        tdSql.error(
            f"alter stable err_ch5 drop base on {DB}.err_p5b"
        )

        tdSql.execute(f"drop stable err_ch5")
        tdSql.execute(f"drop stable err_p5a")
        tdSql.execute(f"drop stable err_p5b")

    # ============================================================
    # Test 6: ALTER ADD TAG on parent with children
    # ============================================================
    def test_alter_add_tag_on_parent_with_children(self):
        """VST Error: ADD TAG on parent that has children

        ALTER STABLE parent ADD TAG should be refused when the parent
        has child VSTs inheriting from it.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, error

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable err_ptag (ts timestamp, c1 int) "
            f"tags (t1 int) virtual 1"
        )
        tdSql.execute(
            f"create stable err_ctag (ts timestamp, c2 float) "
            f"tags (t2 int) base on {DB}.err_ptag virtual 1"
        )

        tdSql.error(
            f"alter stable err_ptag add tag new_tag binary(16)"
        )

        tdSql.execute(f"drop stable err_ctag")
        tdSql.execute(f"drop stable err_ptag")

    # ============================================================
    # Test 7: ALTER DROP TAG on parent with children
    # ============================================================
    def test_alter_drop_tag_on_parent_with_children(self):
        """VST Error: DROP TAG on parent that has children

        ALTER STABLE parent DROP TAG should be refused when the parent
        has child VSTs inheriting from it.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, error

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable err_ptag2 (ts timestamp, c1 int) "
            f"tags (t1 int, t2 float) virtual 1"
        )
        tdSql.execute(
            f"create stable err_ctag2 (ts timestamp, c2 double) "
            f"tags (t3 int) base on {DB}.err_ptag2 virtual 1"
        )

        tdSql.error(
            f"alter stable err_ptag2 drop tag t2"
        )

        tdSql.execute(f"drop stable err_ctag2")
        tdSql.execute(f"drop stable err_ptag2")

    # ============================================================
    # Test 8: Indirect circular inheritance A -> B -> A
    # ============================================================
    def test_indirect_circular_inheritance(self):
        """VST Error: indirect circular inheritance

        Create A, create B BASE ON A, then try to ALTER A ADD BASE ON B.
        This creates a cycle A -> B -> A which must be rejected.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, error

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable err_cyc_a (ts timestamp, ca int) "
            f"tags (ta int) virtual 1"
        )
        tdSql.execute(
            f"create stable err_cyc_b (ts timestamp, cb float) "
            f"tags (tb int) base on {DB}.err_cyc_a virtual 1"
        )

        tdSql.error(
            f"alter stable err_cyc_a add base on {DB}.err_cyc_b"
        )

        tdSql.execute(f"drop stable err_cyc_b")
        tdSql.execute(f"drop stable err_cyc_a")

    # ============================================================
    # Test 9: Non-virtual stable as BASE ON target
    # ============================================================
    def test_non_virtual_as_parent(self):
        """VST Error: regular stable used as BASE ON parent

        A regular (non-virtual) stable cannot be used in BASE ON.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, error

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable err_regular (ts timestamp, c1 int) "
            f"tags (t1 int)"
        )

        tdSql.error(
            f"create stable err_child_nv (ts timestamp, c2 float) "
            f"tags (t2 int) base on {DB}.err_regular virtual 1"
        )

        tdSql.execute(f"drop stable err_regular")

    # ============================================================
    # Test 10: Three-level circular A -> B -> C -> A
    # ============================================================
    def test_three_level_circular_inheritance(self):
        """VST Error: three-level circular inheritance

        Create chain A -> B -> C, then try to ALTER A ADD BASE ON C
        to create A -> B -> C -> A cycle.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, error

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable err_3a (ts timestamp, ca int) "
            f"tags (ta int) virtual 1"
        )
        tdSql.execute(
            f"create stable err_3b (ts timestamp, cb float) "
            f"tags (tb int) base on {DB}.err_3a virtual 1"
        )
        tdSql.execute(
            f"create stable err_3c (ts timestamp, cc double) "
            f"tags (tc int) base on {DB}.err_3b virtual 1"
        )

        tdSql.error(
            f"alter stable err_3a add base on {DB}.err_3c"
        )

        tdSql.execute(f"drop stable err_3c")
        tdSql.execute(f"drop stable err_3b")
        tdSql.execute(f"drop stable err_3a")
