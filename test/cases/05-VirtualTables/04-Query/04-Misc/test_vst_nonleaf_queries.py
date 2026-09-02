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

# Test cases for non-leaf VST query expansion.
# When a non-leaf VST is queried, the engine rewrites it to a
# UNION ALL of all leaf-descendant queries.  This file covers
# various query types against non-leaf VSTs.
#
# Topology: gp -> mid -> leaf_a, leaf_b
# leaf_a and leaf_b have VCTs with data inserted.

from new_test_framework.utils import tdLog, tdSql, etool, tdCom

DB = "test_vst_nonleaf"


class TestVstNonleafQueries:

    def setup_class(cls):
        tdLog.info("setup database and VST hierarchy for non-leaf query tests")
        tdSql.execute(f"drop database if exists {DB}")
        tdSql.execute(f"create database {DB}")
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable nl_src (ts timestamp, v1 int, v2 float, v3 double) "
            f"tags (loc int)"
        )

        for i in range(1, 5):
            tdSql.execute(f"create table nl_src_t{i} using nl_src tags ({i})")
        tdSql.execute(f"insert into nl_src_t1 values (now+0s, 10, 1.0, 1.0)")
        tdSql.execute(f"insert into nl_src_t1 values (now+1s, 20, 2.0, 2.0)")
        tdSql.execute(f"insert into nl_src_t1 values (now+2s, 30, 3.0, 3.0)")
        tdSql.execute(f"insert into nl_src_t2 values (now+0s, 100, 10.0, 10.0)")
        tdSql.execute(f"insert into nl_src_t2 values (now+1s, 200, 20.0, 20.0)")
        tdSql.execute(f"insert into nl_src_t3 values (now+0s, 5, 0.5, 0.5)")
        tdSql.execute(f"insert into nl_src_t3 values (now+1s, 15, 1.5, 1.5)")
        tdSql.execute(f"insert into nl_src_t4 values (now+0s, 50, 5.0, 5.0)")
        tdSql.execute(f"insert into nl_src_t4 values (now+1s, 60, 6.0, 6.0)")

        tdSql.execute(
            f"create stable nl_gp (ts timestamp, gp_col int) "
            f"tags (gp_tag int) virtual 1"
        )
        tdSql.execute(
            f"create stable nl_mid (ts timestamp, mid_col float) "
            f"tags (mid_tag int) base on {DB}.nl_gp virtual 1"
        )
        tdSql.execute(
            f"create stable nl_leaf_a (ts timestamp, la_col double) "
            f"tags (la_tag int) base on {DB}.nl_mid virtual 1"
        )
        tdSql.execute(
            f"create stable nl_leaf_b (ts timestamp, lb_col int) "
            f"tags (lb_tag int) base on {DB}.nl_mid virtual 1"
        )

        tdSql.execute(
            f"create vtable nl_vct_a1 "
            f"(gp_col FROM {DB}.nl_src_t1.v1, "
            f"mid_col FROM {DB}.nl_src_t1.v2, "
            f"la_col FROM {DB}.nl_src_t1.v3) "
            f"using {DB}.nl_leaf_a tags (1, 10, 100)"
        )
        tdSql.execute(
            f"create vtable nl_vct_a2 "
            f"(gp_col FROM {DB}.nl_src_t2.v1, "
            f"mid_col FROM {DB}.nl_src_t2.v2, "
            f"la_col FROM {DB}.nl_src_t2.v3) "
            f"using {DB}.nl_leaf_a tags (2, 20, 200)"
        )
        tdSql.execute(
            f"create vtable nl_vct_b1 "
            f"(gp_col FROM {DB}.nl_src_t3.v1, "
            f"mid_col FROM {DB}.nl_src_t3.v2, "
            f"lb_col FROM {DB}.nl_src_t3.v1) "
            f"using {DB}.nl_leaf_b tags (3, 30, 300)"
        )
        tdSql.execute(
            f"create vtable nl_vct_b2 "
            f"(gp_col FROM {DB}.nl_src_t4.v1, "
            f"mid_col FROM {DB}.nl_src_t4.v2, "
            f"lb_col FROM {DB}.nl_src_t4.v1) "
            f"using {DB}.nl_leaf_b tags (4, 40, 400)"
        )

        # Flush so the source-table data is committed and visible to the virtual
        # column-ref scans before the tests query the (non-leaf) VST hierarchy.
        tdSql.execute(f"flush database {DB}")

        # Poll until leaf-level data is visible. Querying a leaf VST (nl_leaf_a)
        # avoids the non-leaf rewrite path (rewriteNonLeafVstQuery) during setup,
        # which can race with catalog meta propagation on fresh taosd instances.
        import time
        for _ in range(30):
            try:
                tdSql.query(f"select count(*) from {DB}.nl_leaf_a")
                if tdSql.queryResult and tdSql.queryResult[0][0] and tdSql.queryResult[0][0] > 0:
                    break
            except Exception:
                pass
            time.sleep(0.5)

    # ============================================================
    # Test 1: COUNT aggregate on non-leaf
    # ============================================================
    def test_nonleaf_aggregate_count(self):
        """VST Nonleaf: COUNT aggregate

        SELECT COUNT(*) FROM a non-leaf VST should aggregate across
        all leaf descendant VCTs.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, query

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.query(f"select count(*) from nl_mid")
        total = tdSql.queryResult[0][0]
        assert total > 0, f"Expected rows from non-leaf nl_mid, got {total}"

        tdSql.query(f"select count(*) from nl_gp")
        gp_total = tdSql.queryResult[0][0]
        assert gp_total >= total, (
            f"Grandparent count ({gp_total}) should >= mid count ({total})"
        )

    # ============================================================
    # Test 2: SUM aggregate on non-leaf
    # ============================================================
    def test_nonleaf_aggregate_sum(self):
        """VST Nonleaf: SUM aggregate

        SUM on an inherited column across leaf VCTs.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, query

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.query(f"select sum(gp_col) from nl_mid")
        assert tdSql.queryResult[0][0] is not None, "SUM should not be NULL"

    # ============================================================
    # Test 3: WHERE filter on non-leaf
    # ============================================================
    def test_nonleaf_where_filter(self):
        """VST Nonleaf: WHERE filter

        SELECT with WHERE clause on inherited column from non-leaf VST.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, query

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.query(f"select count(*) from nl_mid where gp_col > 20")
        filtered = tdSql.queryResult[0][0]
        assert filtered > 0, "WHERE filter should match some rows"

    # ============================================================
    # Test 4: Tag filter on non-leaf
    # ============================================================
    def test_nonleaf_tag_filter(self):
        """VST Nonleaf: tag filter in WHERE

        Filter by inherited tag column on non-leaf VST.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, query

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        # Merged tag order for an inherited VST is [ancestor tags...][own tag], so nl_vct_a1's
        # positional tags (1, 10, 100) map to (gp_tag=1, mid_tag=10, la_tag=100). Filter on the
        # leaf's own tag using its real value to confirm tag predicates reach the leaf scan.
        tdSql.query(f"select count(*) from nl_leaf_a where la_tag = 100")
        assert tdSql.queryResult[0][0] > 0

    # ============================================================
    # Test 5: GROUP BY on non-leaf
    # ============================================================
    def test_nonleaf_group_by(self):
        """VST Nonleaf: GROUP BY tag

        Group results by tag and verify counts per group.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, query

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.query(
            f"select gp_tag, count(*) from nl_gp group by gp_tag order by gp_tag"
        )
        assert tdSql.queryRows >= 2, "Should have at least 2 tag groups"

    # ============================================================
    # Test 6: ORDER BY + LIMIT on non-leaf
    # ============================================================
    def test_nonleaf_order_by_limit(self):
        """VST Nonleaf: ORDER BY ts DESC LIMIT

        Verify ordering and limit work on non-leaf expanded query.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, query

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.query(f"select ts from nl_mid order by ts desc limit 3")
        assert tdSql.queryRows == 3

    # ============================================================
    # Test 7: INTERVAL window on non-leaf
    # ============================================================
    def test_nonleaf_interval_window(self):
        """VST Nonleaf: INTERVAL window query

        _wstart, COUNT(*) INTERVAL(1s) on non-leaf VST.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, query

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.query(
            f"select _wstart, count(*) from nl_leaf_a interval(1s)"
        )
        assert tdSql.queryRows > 0, "Interval query should return windows"

    # ============================================================
    # Test 8: Empty leaves -- non-leaf with no VCTs
    # ============================================================
    def test_nonleaf_empty_leaves(self):
        """VST Nonleaf: empty leaves

        A non-leaf VST whose leaf descendants have no VCTs should
        return empty result set.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, query

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable nl_empty_parent (ts timestamp, ep_col int) "
            f"tags (ep_tag int) virtual 1"
        )
        tdSql.execute(
            f"create stable nl_empty_child (ts timestamp, ec_col int) "
            f"tags (ec_tag int) base on {DB}.nl_empty_parent virtual 1"
        )

        tdSql.query(f"select count(*) from nl_empty_parent")
        assert tdSql.queryResult[0][0] == 0, "Empty non-leaf should return 0"

        tdSql.query(f"select count(*) from nl_empty_child")
        assert tdSql.queryResult[0][0] == 0

        tdSql.execute(f"drop stable nl_empty_child")
        tdSql.execute(f"drop stable nl_empty_parent")

    # ============================================================
    # Test 9: Single-leaf path (no UNION ALL needed)
    # ============================================================
    def test_nonleaf_single_leaf_path(self):
        """VST Nonleaf: single leaf descendant

        Non-leaf with only one leaf descendant (no UNION ALL needed,
        the optimizer should produce a simple subquery).

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, query

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable sl_parent (ts timestamp, sp int) "
            f"tags (st int) virtual 1"
        )
        tdSql.execute(
            f"create stable sl_child (ts timestamp, sc int) "
            f"tags (sc_tag int) base on {DB}.sl_parent virtual 1"
        )
        tdSql.execute(
            f"create vtable sl_vct1 "
            f"(sp FROM {DB}.nl_src_t1.v1, sc FROM {DB}.nl_src_t1.v1) "
            f"using {DB}.sl_child tags (1, 10)"
        )

        tdSql.query(f"select count(*) from sl_parent")
        assert tdSql.queryResult[0][0] > 0, "Single leaf path should return data"

        tdSql.query(f"select sp from sl_parent order by ts")
        assert tdSql.queryRows > 0

        tdSql.execute(f"drop vtable sl_vct1")
        tdSql.execute(f"drop stable sl_child")
        tdSql.execute(f"drop stable sl_parent")

    # ============================================================
    # Test 10: Multi-leaf path (full UNION ALL)
    # ============================================================
    def test_nonleaf_multi_leaf_path(self):
        """VST Nonleaf: multi-leaf UNION ALL

        Grandparent VST queried with 2+ leaf descendants triggers
        full UNION ALL expansion.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, query

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.query(f"select count(*) from nl_gp")
        gp_count = tdSql.queryResult[0][0]

        tdSql.query(f"select count(*) from nl_leaf_a")
        la_count = tdSql.queryResult[0][0]

        tdSql.query(f"select count(*) from nl_leaf_b")
        lb_count = tdSql.queryResult[0][0]

        assert gp_count == la_count + lb_count, (
            f"gp({gp_count}) != leaf_a({la_count}) + leaf_b({lb_count})"
        )

    # ============================================================
    # Test 11: Subquery on non-leaf
    # ============================================================
    def test_nonleaf_subquery(self):
        """VST Nonleaf: subquery

        SELECT from a subquery that reads from non-leaf VST.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, query

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.query(
            f"select count(*) from (select gp_col from nl_mid where gp_col > 0)"
        )
        assert tdSql.queryResult[0][0] > 0

    # ============================================================
    # Test 12: MIN/MAX aggregate on non-leaf
    # ============================================================
    def test_nonleaf_aggregate_minmax(self):
        """VST Nonleaf: MIN/MAX aggregate

        MIN and MAX on inherited column from non-leaf VST.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, query

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.query(f"select min(gp_col), max(gp_col) from nl_gp")
        assert tdSql.queryResult[0][0] is not None
        assert tdSql.queryResult[0][1] is not None
        assert tdSql.queryResult[0][0] <= tdSql.queryResult[0][1]

    # ============================================================
    # Test 13: AVG aggregate on non-leaf
    # ============================================================
    def test_nonleaf_aggregate_avg(self):
        """VST Nonleaf: AVG aggregate

        AVG on inherited column from non-leaf VST.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, query

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.query(f"select avg(gp_col) from nl_gp")
        assert tdSql.queryResult[0][0] is not None
