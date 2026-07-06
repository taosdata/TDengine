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

# Test cases for VST schema merge column/tag ordering.
# The VST inheritance merge follows:
#   columns: [ts][inherited_parent1_cols][inherited_parent2_cols][own_cols]
#   tags:    [inherited_parent1_tags][inherited_parent2_tags][own_tags]
# This file verifies the ordering is correct after various DDL.

from new_test_framework.utils import tdLog, tdSql, etool, tdCom

DB = "test_vst_schema"


class TestVstSchemaMerge:

    def setup_class(cls):
        tdLog.info("setup database for VST schema merge tests")
        tdSql.execute(f"drop database if exists {DB}")
        tdSql.execute(f"create database {DB}")
        tdSql.execute(f"use {DB}")

    # ============================================================
    # Test 1: Column order after multi-parent CREATE
    # ============================================================
    def test_column_order_after_multi_parent_create(self):
        """VST Schema: column order after multi-parent CREATE

        Verify columns appear in order:
        ts, parent1_cols, parent2_cols, own_cols

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, schema

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable sm_p1 (ts timestamp, p1_c1 int, p1_c2 float) "
            f"tags (p1_t1 int) virtual 1"
        )
        tdSql.execute(
            f"create stable sm_p2 (ts timestamp, p2_c1 double) "
            f"tags (p2_t1 int) virtual 1"
        )
        tdSql.execute(
            f"create stable sm_child1 "
            f"(ts timestamp, own_c1 bigint) "
            f"tags (own_t1 int) "
            f"base on {DB}.sm_p1, {DB}.sm_p2 virtual 1"
        )

        tdSql.query(f"describe sm_child1")
        col_names = [tdSql.getData(i, 0) for i in range(tdSql.queryRows)]

        assert col_names[0] == "ts", f"First column should be ts, got {col_names[0]}"
        ts_idx = 0
        p1_indices = [col_names.index(c) for c in ["p1_c1", "p1_c2"] if c in col_names]
        p2_indices = [col_names.index(c) for c in ["p2_c1"] if c in col_names]
        own_indices = [col_names.index(c) for c in ["own_c1"] if c in col_names]

        if p1_indices and p2_indices:
            assert max(p1_indices) < min(p2_indices), (
                f"Parent1 cols should come before parent2: {col_names}"
            )
        if p2_indices and own_indices:
            assert max(p2_indices) < min(own_indices), (
                f"Parent2 cols should come before own: {col_names}"
            )

        tdSql.execute(f"drop stable sm_child1")
        tdSql.execute(f"drop stable sm_p2")
        tdSql.execute(f"drop stable sm_p1")

    # ============================================================
    # Test 2: Tag order after multi-parent CREATE
    # ============================================================
    def test_tag_order_after_multi_parent_create(self):
        """VST Schema: tag order after multi-parent CREATE

        Verify tags appear in order: parent1_tags, parent2_tags, own_tags.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, schema

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable sm_tp1 (ts timestamp, c1 int) "
            f"tags (tp1_a int, tp1_b float) virtual 1"
        )
        tdSql.execute(
            f"create stable sm_tp2 (ts timestamp, c2 int) "
            f"tags (tp2_a double) virtual 1"
        )
        tdSql.execute(
            f"create stable sm_tchild "
            f"(ts timestamp, c3 int) "
            f"tags (own_tag int) "
            f"base on {DB}.sm_tp1, {DB}.sm_tp2 virtual 1"
        )

        tdSql.query(f"describe sm_tchild")
        rows = [(tdSql.getData(i, 0), tdSql.getData(i, 3)) for i in range(tdSql.queryRows)]
        tags = [(name, note) for name, note in rows if note == "TAG"]

        tag_names = [t[0] for t in tags]
        tdLog.info(f"Tag order: {tag_names}")

        if "tp1_a" in tag_names and "tp2_a" in tag_names:
            assert tag_names.index("tp1_a") < tag_names.index("tp2_a"), (
                f"Parent1 tags should come before parent2: {tag_names}"
            )
        if "tp2_a" in tag_names and "own_tag" in tag_names:
            assert tag_names.index("tp2_a") < tag_names.index("own_tag"), (
                f"Parent2 tags should come before own: {tag_names}"
            )

        tdSql.execute(f"drop stable sm_tchild")
        tdSql.execute(f"drop stable sm_tp2")
        tdSql.execute(f"drop stable sm_tp1")

    # ============================================================
    # Test 3: Schema after sequential ADD then DROP
    # ============================================================
    def test_schema_after_add_drop(self):
        """VST Schema: schema after ADD then DROP parent

        Add parent A, add parent B, drop parent A -- verify remaining
        schema only has parent B columns and own columns.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, schema

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable sm_ad_p1 (ts timestamp, ad_c1 int) "
            f"tags (ad_t1 int) virtual 1"
        )
        tdSql.execute(
            f"create stable sm_ad_p2 (ts timestamp, ad_c2 float) "
            f"tags (ad_t2 int) virtual 1"
        )
        tdSql.execute(
            f"create stable sm_ad_child "
            f"(ts timestamp, own_c int, extra_c double) "
            f"tags (own_t int, extra_t int) "
            f"base on {DB}.sm_ad_p1 virtual 1"
        )

        tdSql.execute(
            f"alter stable sm_ad_child add base on {DB}.sm_ad_p2"
        )

        tdSql.query(f"describe sm_ad_child")
        col_names = [tdSql.getData(i, 0) for i in range(tdSql.queryRows)]
        assert "ad_c1" in col_names, "Should have ad_c1 from parent 1"
        assert "ad_c2" in col_names, "Should have ad_c2 from parent 2"

        tdSql.execute(
            f"alter stable sm_ad_child drop base on {DB}.sm_ad_p1"
        )

        tdSql.query(f"describe sm_ad_child")
        col_names_after = [tdSql.getData(i, 0) for i in range(tdSql.queryRows)]
        assert "ad_c1" not in col_names_after, "ad_c1 should be removed"
        assert "ad_c2" in col_names_after, "ad_c2 should remain"
        assert "own_c" in col_names_after, "own_c should remain"

        tdSql.execute(
            f"alter stable sm_ad_child drop base on {DB}.sm_ad_p2"
        )
        tdSql.execute(f"drop stable sm_ad_child")
        tdSql.execute(f"drop stable sm_ad_p2")
        tdSql.execute(f"drop stable sm_ad_p1")

    # ============================================================
    # Test 4: Parent tags appear alongside child's own tags
    # ============================================================
    def test_create_no_tags_inherits_parent_tags(self):
        """VST Schema: parent tags appear alongside child own tags

        CREATE STABLE x (cols) TAGS (own) BASE ON parent VIRTUAL 1 --
        the child's describe should include both the parent tag and the
        child's own tag.

        Note: CREATE STABLE without any TAGS clause is not supported by
        the parser (TAGS is mandatory); the parent-tag inheritance feature
        requires at least one own tag in the TAGS clause.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, schema

        Jira: None

        History:
            - 2026-06-09 Created
            - 2026-06-13 Revised: TAGS clause is required; test verifies
              parent tag appears alongside own tag after inheritance.
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable sm_notag_p (ts timestamp, nc1 int) "
            f"tags (nt_tag int) virtual 1"
        )
        tdSql.execute(
            f"create stable sm_notag_child "
            f"(ts timestamp, nc2 float) "
            f"tags (own_tag int) "
            f"base on {DB}.sm_notag_p virtual 1"
        )

        tdSql.query(f"describe sm_notag_child")
        rows = [(tdSql.getData(i, 0), tdSql.getData(i, 3)) for i in range(tdSql.queryRows)]
        tags = [name for name, note in rows if note == "TAG"]

        assert len(tags) >= 2, f"Should have parent tag + own tag (>=2), got {tags}"
        assert "nt_tag" in tags, f"Should inherit nt_tag from parent, got {tags}"
        assert "own_tag" in tags, f"Should have own own_tag, got {tags}"

        tdSql.execute(f"drop stable sm_notag_child")
        tdSql.execute(f"drop stable sm_notag_p")

    # ============================================================
    # Test 5: Max parents schema verification
    # ============================================================
    def test_max_parents_schema(self):
        """VST Schema: max 10 parents schema

        Create a VST with 10 parents, verify all inherited columns
        appear in the schema.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, schema

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        for i in range(10):
            tdSql.execute(
                f"create stable sm_mp{i} (ts timestamp, mp{i}_col int) "
                f"tags (mp{i}_tag int) virtual 1"
            )

        parents = ", ".join(f"{DB}.sm_mp{i}" for i in range(10))
        tdSql.execute(
            f"create stable sm_max_child "
            f"(ts timestamp, own_col int) "
            f"tags (own_tag int) "
            f"base on {parents} virtual 1"
        )

        tdSql.query(f"describe sm_max_child")
        col_names = [tdSql.getData(i, 0) for i in range(tdSql.queryRows)]

        for i in range(10):
            expected = f"mp{i}_col"
            assert expected in col_names, f"Missing column {expected}"

        tdSql.execute(f"drop stable sm_max_child")
        for i in range(10):
            tdSql.execute(f"drop stable sm_mp{i}")
