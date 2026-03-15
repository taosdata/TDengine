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


DB = "test_vtable_ref_vtable"


class TestVtableRefVtable:
    """Tests for virtual table referencing virtual table (vtable chain).

    Covers: vtable->vtable chain creation, query, aggregation, window,
    deep chains up to TSDB_MAX_VTABLE_REF_DEPTH=5, vstb with vtable children,
    mixed references, depth limit enforcement, NULL propagation, and DROP behavior.
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.execute(f"drop database if exists {DB}")
        tdSql.execute(f"create database {DB} vgroups 4")
        tdSql.execute(f"use {DB}")

        tdSql.execute("CREATE TABLE org_ntb (ts timestamp, int_col int, bigint_col bigint, "
                       "float_col float, double_col double, bool_col bool, "
                       "binary_col binary(32), nchar_col nchar(32))")
        for i in range(10):
            bval = "true" if i % 2 == 0 else "false"
            tdSql.execute(f"INSERT INTO org_ntb VALUES ('2025-01-01 00:00:0{i}', {i}, {i*100}, "
                          f"{i*1.1}, {i*2.2}, {bval}, 'binary_{i}', 'nchar_{i}')")

        tdSql.execute("CREATE TABLE org_ntb2 (ts timestamp, val int, info binary(32))")
        for i in range(3):
            tdSql.execute(f"INSERT INTO org_ntb2 VALUES ('2025-01-01 00:00:0{i}', {(i+1)*10}, 'info_{i}')")

        tdSql.execute("CREATE TABLE org_ntb_empty (ts timestamp, int_col int, double_col double)")

        tdSql.execute("CREATE TABLE org_ntb_null (ts timestamp, int_col int, bigint_col bigint, "
                       "float_col float, double_col double, binary_col binary(32))")
        tdSql.execute("INSERT INTO org_ntb_null VALUES ('2025-01-01 00:00:00', NULL, 100, NULL, 1.1, NULL)")
        tdSql.execute("INSERT INTO org_ntb_null VALUES ('2025-01-01 00:00:01', 1, NULL, 2.2, NULL, 'val_1')")
        tdSql.execute("INSERT INTO org_ntb_null VALUES ('2025-01-01 00:00:02', NULL, NULL, NULL, NULL, NULL)")
        tdSql.execute("INSERT INTO org_ntb_null VALUES ('2025-01-01 00:00:03', 3, 300, 3.3, 6.6, 'val_3')")

        tdSql.execute("CREATE STABLE org_stb (ts timestamp, int_col int, bigint_col bigint, "
                       "float_col float, double_col double) TAGS (int_tag int, binary_tag binary(16))")
        for i in range(3):
            tdSql.execute(f"CREATE TABLE org_ctb_{i} USING org_stb TAGS ({i}, 'tag_{i}')")
        tdSql.execute("INSERT INTO org_ctb_0 VALUES ('2025-01-01 00:00:00', 0, 0, 0.0, 0.0)")
        tdSql.execute("INSERT INTO org_ctb_0 VALUES ('2025-01-01 00:00:01', 1, 100, 1.1, 2.2)")
        tdSql.execute("INSERT INTO org_ctb_0 VALUES ('2025-01-01 00:00:02', 2, 200, 2.2, 4.4)")
        tdSql.execute("INSERT INTO org_ctb_1 VALUES ('2025-01-01 00:00:00', 100, 10000, 110.0, 220.0)")
        tdSql.execute("INSERT INTO org_ctb_1 VALUES ('2025-01-01 00:00:01', 101, 10100, 111.1, 222.2)")
        tdSql.execute("INSERT INTO org_ctb_1 VALUES ('2025-01-01 00:00:02', 102, 10200, 112.2, 224.4)")
        tdSql.execute("INSERT INTO org_ctb_2 VALUES ('2025-01-01 00:00:00', 200, 20000, 210.0, 420.0)")
        tdSql.execute("INSERT INTO org_ctb_2 VALUES ('2025-01-01 00:00:01', 201, 20100, 211.1, 422.2)")

    # ================================================================
    # Layer 1: vtable -> physical table (baseline)
    # ================================================================

    def test_vtable_ref_physical(self):
        """Create: vtable referencing physical table (L1 baseline)

        1. Create vtable vntb1 referencing org_ntb columns
        2. Verify SELECT * returns same data as org_ntb
        3. Verify aggregation and WHERE filters work

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test vtable ref physical table ===")
        tdSql.execute(f"use {DB}")
        tdSql.execute("CREATE VTABLE vntb1 (ts timestamp, "
                       "int_col int from org_ntb.int_col, "
                       "bigint_col bigint from org_ntb.bigint_col, "
                       "float_col float from org_ntb.float_col, "
                       "double_col double from org_ntb.double_col)")

        tdSql.query("SELECT COUNT(*) FROM vntb1")
        tdSql.checkData(0, 0, 10)

        tdSql.query("SELECT SUM(int_col) FROM vntb1")
        tdSql.checkData(0, 0, 45)

        tdSql.query("SELECT * FROM vntb1 WHERE int_col > 5 ORDER BY ts")
        tdSql.checkRows(4)

        tdSql.query("SELECT * FROM vntb1 WHERE ts >= '2025-01-01 00:00:03' AND ts <= '2025-01-01 00:00:06' ORDER BY ts")
        tdSql.checkRows(4)

    # ================================================================
    # Layer 2: vtable -> vtable -> physical (core feature)
    # ================================================================

    def test_vtable_ref_vtable_layer2(self):
        """Chain: vtable -> vtable -> physical (L2)

        1. Create vtable vntb2 referencing vntb1 (vtable -> vtable -> physical)
        2. Verify data consistency with org_ntb
        3. Verify aggregation, WHERE, MIN/MAX/FIRST/LAST

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test vtable ref vtable layer 2 ===")
        tdSql.execute(f"use {DB}")
        tdSql.execute("CREATE VTABLE vntb2 (ts timestamp, "
                       "int_col int from vntb1.int_col, "
                       "bigint_col bigint from vntb1.bigint_col, "
                       "float_col float from vntb1.float_col, "
                       "double_col double from vntb1.double_col)")

        tdSql.query("SELECT COUNT(*) FROM vntb2")
        tdSql.checkData(0, 0, 10)

        tdSql.query("SELECT SUM(int_col) FROM vntb2")
        tdSql.checkData(0, 0, 45)

        tdSql.query("SELECT * FROM vntb2 WHERE int_col > 5 ORDER BY ts")
        tdSql.checkRows(4)

        tdSql.query("SELECT * FROM vntb2 WHERE ts >= '2025-01-01 00:00:03' AND ts <= '2025-01-01 00:00:06' ORDER BY ts")
        tdSql.checkRows(4)

        tdSql.query("SELECT MIN(int_col) FROM vntb2")
        tdSql.checkData(0, 0, 0)
        tdSql.query("SELECT MAX(int_col) FROM vntb2")
        tdSql.checkData(0, 0, 9)

    # ================================================================
    # vstb with child referencing physical tables
    # ================================================================

    def test_vstb_ref_physical(self):
        """VStb: virtual super table with children referencing physical tables

        1. Create vstb + vctb children referencing org_ctb_0 and org_ctb_1
        2. Query individual children and super table scan
        3. Verify GROUP BY tag, tag filter

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test vstb ref physical ===")
        tdSql.execute(f"use {DB}")
        tdSql.execute("CREATE STABLE vstb (ts timestamp, int_col int, bigint_col bigint, "
                       "float_col float, double_col double) TAGS (int_tag int, binary_tag binary(16)) VIRTUAL 1")
        tdSql.execute("CREATE VTABLE vctb_0 (int_col from org_ctb_0.int_col, bigint_col from org_ctb_0.bigint_col, "
                       "float_col from org_ctb_0.float_col, double_col from org_ctb_0.double_col) "
                       "USING vstb TAGS (0, 'vtag_0')")
        tdSql.execute("CREATE VTABLE vctb_1 (int_col from org_ctb_1.int_col, bigint_col from org_ctb_1.bigint_col, "
                       "float_col from org_ctb_1.float_col, double_col from org_ctb_1.double_col) "
                       "USING vstb TAGS (1, 'vtag_1')")

        tdSql.query("SELECT * FROM vctb_0 ORDER BY ts")
        tdSql.checkRows(3)

        tdSql.query("SELECT * FROM vctb_1 ORDER BY ts")
        tdSql.checkRows(3)

        tdSql.query("SELECT COUNT(*) FROM vstb")
        tdSql.checkData(0, 0, 6)

        tdSql.query("SELECT * FROM vstb WHERE int_tag = 0 ORDER BY ts")
        tdSql.checkRows(3)

        tdSql.query("SELECT int_tag, COUNT(*) AS cnt FROM vstb GROUP BY int_tag ORDER BY int_tag")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 3)

    # ================================================================
    # vstb with children referencing vtables (key feature)
    # ================================================================

    def test_vstb_ref_vtable(self):
        """VStb: virtual super table with children referencing vtables

        1. Create vstb2 with child vctb2_0 referencing vntb1 (vtable chain)
        2. Verify child query and super table scan
        3. Add second child referencing another vtable chain
        4. Verify GROUP BY tag, aggregation

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test vstb ref vtable ===")
        tdSql.execute(f"use {DB}")
        tdSql.execute("CREATE STABLE vstb2 (ts timestamp, int_col int, bigint_col bigint, "
                       "float_col float, double_col double) TAGS (int_tag int, binary_tag binary(16)) VIRTUAL 1")

        tdSql.execute("CREATE VTABLE vctb2_0 (int_col from vntb1.int_col, bigint_col from vntb1.bigint_col, "
                       "float_col from vntb1.float_col, double_col from vntb1.double_col) "
                       "USING vstb2 TAGS (0, 'vtag2_0')")

        tdSql.query("SELECT * FROM vctb2_0 ORDER BY ts")
        tdSql.checkRows(10)

        tdSql.query("SELECT COUNT(*) FROM vstb2")
        tdSql.checkData(0, 0, 10)

        tdSql.query("SELECT * FROM vstb2 WHERE int_col >= 5 ORDER BY ts")
        tdSql.checkRows(5)

        tdSql.execute("CREATE VTABLE vntb_from_ctb1 (ts timestamp, "
                       "int_col int from org_ctb_1.int_col, bigint_col bigint from org_ctb_1.bigint_col, "
                       "float_col float from org_ctb_1.float_col, double_col double from org_ctb_1.double_col)")
        tdSql.execute("CREATE VTABLE vctb2_1 (int_col from vntb_from_ctb1.int_col, "
                       "bigint_col from vntb_from_ctb1.bigint_col, "
                       "float_col from vntb_from_ctb1.float_col, double_col from vntb_from_ctb1.double_col) "
                       "USING vstb2 TAGS (1, 'vtag2_1')")

        tdSql.query("SELECT COUNT(*) FROM vstb2")
        tdSql.checkData(0, 0, 13)

        tdSql.query("SELECT int_tag, COUNT(*) AS cnt FROM vstb2 GROUP BY int_tag ORDER BY int_tag")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(1, 1, 3)

    # ================================================================
    # Deep chain: layers 3, 4, 5
    # ================================================================

    def test_deep_chain_layer3(self):
        """DeepChain: 3-layer vtable chain (L3 -> L2 -> L1 -> physical)

        1. Create vntb_L3 referencing vntb2
        2. Verify data consistency across all layers
        3. Verify aggregation works at depth 3

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test deep chain layer 3 ===")
        tdSql.execute(f"use {DB}")
        tdSql.execute("CREATE VTABLE vntb_L3 (ts timestamp, "
                       "int_col int from vntb2.int_col, bigint_col bigint from vntb2.bigint_col)")

        tdSql.query("SELECT COUNT(*) FROM vntb_L3")
        tdSql.checkData(0, 0, 10)
        tdSql.query("SELECT SUM(int_col) FROM vntb_L3")
        tdSql.checkData(0, 0, 45)

    def test_deep_chain_layer4(self):
        """DeepChain: 4-layer vtable chain

        1. Create vntb_L4 referencing vntb_L3
        2. Verify data consistency

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test deep chain layer 4 ===")
        tdSql.execute(f"use {DB}")
        tdSql.execute("CREATE VTABLE vntb_L4 (ts timestamp, "
                       "int_col int from vntb_L3.int_col, bigint_col bigint from vntb_L3.bigint_col)")

        tdSql.query("SELECT COUNT(*) FROM vntb_L4")
        tdSql.checkData(0, 0, 10)
        tdSql.query("SELECT SUM(int_col) FROM vntb_L4")
        tdSql.checkData(0, 0, 45)

    def test_deep_chain_layer5(self):
        """DeepChain: 5-layer vtable chain (max depth)

        1. Create vntb_L5 referencing vntb_L4 (5th layer, maximum allowed)
        2. Verify full data consistency from physical through 5 layers
        3. Verify aggregation, WHERE, MIN/MAX at deepest level

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test deep chain layer 5 ===")
        tdSql.execute(f"use {DB}")
        tdSql.execute("CREATE VTABLE vntb_L5 (ts timestamp, "
                       "int_col int from vntb_L4.int_col, bigint_col bigint from vntb_L4.bigint_col)")

        tdSql.query("SELECT COUNT(*) FROM vntb_L5")
        tdSql.checkData(0, 0, 10)
        tdSql.query("SELECT SUM(int_col) FROM vntb_L5")
        tdSql.checkData(0, 0, 45)
        tdSql.query("SELECT MIN(int_col) FROM vntb_L5")
        tdSql.checkData(0, 0, 0)
        tdSql.query("SELECT MAX(int_col) FROM vntb_L5")
        tdSql.checkData(0, 0, 9)
        tdSql.query("SELECT * FROM vntb_L5 WHERE int_col >= 7 ORDER BY ts")
        tdSql.checkRows(3)

    # ================================================================
    # Depth exceeded: layer 6 should fail
    # ================================================================

    def test_depth_exceeded(self):
        """DepthLimit: 6th layer creation should fail

        1. Attempt to create vntb_L6 referencing vntb_L5 (exceeds max depth 5)
        2. Verify creation fails with appropriate error
        3. Also verify vstb child referencing L5 fails

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test depth exceeded ===")
        tdSql.execute(f"use {DB}")

        tdSql.error("CREATE VTABLE vntb_L6 (ts timestamp, int_col int from vntb_L5.int_col)")

        tdSql.execute("CREATE STABLE vstb_deep (ts timestamp, int_col int, bigint_col bigint) "
                       "TAGS (depth int) VIRTUAL 1")
        tdSql.error("CREATE VTABLE vctb_deep_L5_fail (int_col from vntb_L5.int_col, "
                     "bigint_col from vntb_L5.bigint_col) USING vstb_deep TAGS (5)")

    # ================================================================
    # Data consistency across all layers
    # ================================================================

    def test_data_consistency_across_layers(self):
        """Consistency: verify same data through all 5 layers

        1. Check COUNT(*) and SUM(int_col) match across org_ntb, vntb1..vntb_L5
        2. All should return COUNT=10, SUM=45

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test data consistency across layers ===")
        tdSql.execute(f"use {DB}")
        for tbl in ["org_ntb", "vntb1", "vntb2", "vntb_L3", "vntb_L4", "vntb_L5"]:
            tdSql.query(f"SELECT COUNT(*) FROM {tbl}")
            tdSql.checkData(0, 0, 10)
            tdSql.query(f"SELECT SUM(int_col) FROM {tbl}")
            tdSql.checkData(0, 0, 45)

    # ================================================================
    # vstb with children at different chain depths
    # ================================================================

    def test_vstb_mixed_depth_children(self):
        """MixedDepth: vstb with children from different vtable chain depths

        1. Create vstb_deep with children referencing L2, L3, L4
        2. Verify super table scan returns combined data
        3. Verify GROUP BY depth tag

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test vstb mixed depth children ===")
        tdSql.execute(f"use {DB}")

        tdSql.execute("CREATE VTABLE vctb_deep_L2 (int_col from vntb2.int_col, "
                       "bigint_col from vntb2.bigint_col) USING vstb_deep TAGS (2)")
        tdSql.execute("CREATE VTABLE vctb_deep_L3 (int_col from vntb_L3.int_col, "
                       "bigint_col from vntb_L3.bigint_col) USING vstb_deep TAGS (3)")
        tdSql.execute("CREATE VTABLE vctb_deep_L4 (int_col from vntb_L4.int_col, "
                       "bigint_col from vntb_L4.bigint_col) USING vstb_deep TAGS (4)")

        tdSql.query("SELECT COUNT(*) FROM vstb_deep")
        tdSql.checkData(0, 0, 30)

        tdSql.query("SELECT depth, COUNT(*) AS cnt FROM vstb_deep GROUP BY depth ORDER BY depth")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(1, 1, 10)
        tdSql.checkData(2, 1, 10)

    # ================================================================
    # Mixed physical + vtable references under same vstb
    # ================================================================

    def test_vstb_mixed_physical_vtable(self):
        """MixedRef: vstb with children referencing both physical and vtable sources

        1. Create vstb_mix with children from physical, L1 vtable, and L2 vtable
        2. Verify super table scan combines all sources
        3. Verify GROUP BY source tag

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test vstb mixed physical and vtable refs ===")
        tdSql.execute(f"use {DB}")
        tdSql.execute("CREATE STABLE vstb_mix (ts timestamp, int_col int, bigint_col bigint, "
                       "float_col float, double_col double) TAGS (src_type int, src_name binary(32)) VIRTUAL 1")
        tdSql.execute("CREATE VTABLE vctb_mix_phys (int_col from org_ctb_0.int_col, "
                       "bigint_col from org_ctb_0.bigint_col, float_col from org_ctb_0.float_col, "
                       "double_col from org_ctb_0.double_col) USING vstb_mix TAGS (0, 'physical')")
        tdSql.execute("CREATE VTABLE vctb_mix_virt (int_col from vntb1.int_col, "
                       "bigint_col from vntb1.bigint_col, float_col from vntb1.float_col, "
                       "double_col from vntb1.double_col) USING vstb_mix TAGS (1, 'vtable_L1')")
        tdSql.execute("CREATE VTABLE vctb_mix_virt2 (int_col from vntb2.int_col, "
                       "bigint_col from vntb2.bigint_col, float_col from vntb2.float_col, "
                       "double_col from vntb2.double_col) USING vstb_mix TAGS (2, 'vtable_L2')")

        tdSql.query("SELECT COUNT(*) FROM vstb_mix")
        tdSql.checkData(0, 0, 23)

        tdSql.query("SELECT src_type, COUNT(*) AS cnt FROM vstb_mix GROUP BY src_type ORDER BY src_type")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 10)
        tdSql.checkData(2, 1, 10)

    # ================================================================
    # Partial column reference through vtable chain
    # ================================================================

    def test_partial_column_ref(self):
        """PartialCol: vtable chain with subset of columns

        1. Create vntb_partial referencing only int_col and double_col from org_ntb
        2. Create vntb_partial2 referencing vntb_partial (chain)
        3. Verify queries work with partial columns

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test partial column ref ===")
        tdSql.execute(f"use {DB}")
        tdSql.execute("CREATE VTABLE vntb_partial (ts timestamp, "
                       "int_col int from org_ntb.int_col, double_col double from org_ntb.double_col)")
        tdSql.execute("CREATE VTABLE vntb_partial2 (ts timestamp, "
                       "int_col int from vntb_partial.int_col, double_col double from vntb_partial.double_col)")

        tdSql.query("SELECT COUNT(*) FROM vntb_partial2")
        tdSql.checkData(0, 0, 10)
        tdSql.query("SELECT SUM(int_col) FROM vntb_partial2")
        tdSql.checkData(0, 0, 45)

    # ================================================================
    # All data types through vtable chain
    # ================================================================

    def test_all_data_types_chain(self):
        """DataTypes: all column types through 2-layer vtable chain

        1. Create vntb_alltype referencing all columns of org_ntb
        2. Create vntb_alltype2 referencing vntb_alltype (chain)
        3. Verify bool, binary, nchar values propagate correctly
        4. Verify WHERE on string and bool columns

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test all data types chain ===")
        tdSql.execute(f"use {DB}")
        tdSql.execute("CREATE VTABLE vntb_alltype (ts timestamp, "
                       "int_col int from org_ntb.int_col, bigint_col bigint from org_ntb.bigint_col, "
                       "float_col float from org_ntb.float_col, double_col double from org_ntb.double_col, "
                       "bool_col bool from org_ntb.bool_col, binary_col binary(32) from org_ntb.binary_col, "
                       "nchar_col nchar(32) from org_ntb.nchar_col)")
        tdSql.execute("CREATE VTABLE vntb_alltype2 (ts timestamp, "
                       "int_col int from vntb_alltype.int_col, bigint_col bigint from vntb_alltype.bigint_col, "
                       "float_col float from vntb_alltype.float_col, double_col double from vntb_alltype.double_col, "
                       "bool_col bool from vntb_alltype.bool_col, binary_col binary(32) from vntb_alltype.binary_col, "
                       "nchar_col nchar(32) from vntb_alltype.nchar_col)")

        tdSql.query("SELECT COUNT(*) FROM vntb_alltype2")
        tdSql.checkData(0, 0, 10)

        tdSql.query("SELECT * FROM vntb_alltype2 WHERE binary_col = 'binary_3'")
        tdSql.checkRows(1)
        tdSql.query("SELECT * FROM vntb_alltype2 WHERE nchar_col = 'nchar_7'")
        tdSql.checkRows(1)
        tdSql.query("SELECT * FROM vntb_alltype2 WHERE bool_col = true ORDER BY ts")
        tdSql.checkRows(5)

    # ================================================================
    # NULL value propagation through vtable chain
    # ================================================================

    def test_null_propagation(self):
        """NULL: null values propagate correctly through vtable chain

        1. Create vtable chain on org_ntb_null (has NULL values)
        2. Verify NULLs preserved through 2-layer chain
        3. Verify COUNT(col) vs COUNT(*) reflects NULLs
        4. Verify IS NULL / IS NOT NULL filters

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test null propagation ===")
        tdSql.execute(f"use {DB}")
        tdSql.execute("CREATE VTABLE vntb_null1 (ts timestamp, "
                       "int_col int from org_ntb_null.int_col, bigint_col bigint from org_ntb_null.bigint_col, "
                       "float_col float from org_ntb_null.float_col, double_col double from org_ntb_null.double_col, "
                       "binary_col binary(32) from org_ntb_null.binary_col)")
        tdSql.execute("CREATE VTABLE vntb_null2 (ts timestamp, "
                       "int_col int from vntb_null1.int_col, bigint_col bigint from vntb_null1.bigint_col, "
                       "float_col float from vntb_null1.float_col, double_col double from vntb_null1.double_col, "
                       "binary_col binary(32) from vntb_null1.binary_col)")

        tdSql.query("SELECT COUNT(*) FROM vntb_null2")
        tdSql.checkData(0, 0, 4)

        tdSql.query("SELECT COUNT(int_col) FROM vntb_null2")
        tdSql.checkData(0, 0, 2)

        tdSql.query("SELECT * FROM vntb_null2 WHERE int_col IS NULL ORDER BY ts")
        tdSql.checkRows(2)
        tdSql.query("SELECT * FROM vntb_null2 WHERE int_col IS NOT NULL ORDER BY ts")
        tdSql.checkRows(2)

    # ================================================================
    # Empty source table through vtable chain
    # ================================================================

    def test_empty_source(self):
        """Empty: empty source table through vtable chain

        1. Create vtable chain on org_ntb_empty (no data)
        2. Verify SELECT * and COUNT return empty/0
        3. Verify vstb with empty child works

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test empty source ===")
        tdSql.execute(f"use {DB}")
        tdSql.execute("CREATE VTABLE vntb_empty1 (ts timestamp, "
                       "int_col int from org_ntb_empty.int_col, double_col double from org_ntb_empty.double_col)")
        tdSql.execute("CREATE VTABLE vntb_empty2 (ts timestamp, "
                       "int_col int from vntb_empty1.int_col, double_col double from vntb_empty1.double_col)")

        tdSql.query("SELECT * FROM vntb_empty2")
        tdSql.checkRows(0)
        tdSql.query("SELECT COUNT(*) FROM vntb_empty2")
        tdSql.checkData(0, 0, 0)

        tdSql.execute("CREATE STABLE vstb_empty (ts timestamp, int_col int, double_col double) "
                       "TAGS (t1 int) VIRTUAL 1")
        tdSql.execute("CREATE VTABLE vctb_empty_0 (int_col from vntb_empty1.int_col, "
                       "double_col from vntb_empty1.double_col) USING vstb_empty TAGS (0)")
        tdSql.query("SELECT COUNT(*) FROM vstb_empty")
        tdSql.checkData(0, 0, 0)

    # ================================================================
    # Window queries on vtable chain
    # ================================================================

    def test_interval_window_on_chain(self):
        """Window: interval window on vtable chain

        1. Interval window on vntb2 (2-layer chain)
        2. Interval window on vstb2 (vstb with vtable children)

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test interval window on chain ===")
        tdSql.execute(f"use {DB}")

        tdSql.query("SELECT _wstart, COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vntb2 INTERVAL(3s)")
        assert tdSql.getRows() > 0

        tdSql.query("SELECT _wstart, COUNT(*) AS cnt, SUM(int_col) AS sum_int FROM vstb2 INTERVAL(3s)")
        assert tdSql.getRows() > 0

    def test_state_session_window_on_chain(self):
        """Window: state and session window on vtable chain

        1. State window on vntb_alltype2 using bool_col
        2. Session window on vntb_alltype2

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test state/session window on chain ===")
        tdSql.execute(f"use {DB}")

        tdSql.query("SELECT _wstart, _wend, COUNT(*) AS cnt FROM vntb_alltype2 STATE_WINDOW(bool_col)")
        assert tdSql.getRows() > 0

        tdSql.query("SELECT _wstart, _wend, COUNT(*) AS cnt FROM vntb_alltype2 SESSION(ts, 2s)")
        assert tdSql.getRows() > 0

    # ================================================================
    # LIMIT / OFFSET / ORDER BY
    # ================================================================

    def test_limit_offset_orderby(self):
        """LimitOrder: LIMIT, OFFSET, ORDER BY on vtable chain

        1. LIMIT/OFFSET on vntb2
        2. LIMIT/OFFSET on vstb2
        3. ORDER BY non-ts column

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test limit offset orderby ===")
        tdSql.execute(f"use {DB}")

        tdSql.query("SELECT * FROM vntb2 ORDER BY ts LIMIT 3")
        tdSql.checkRows(3)

        tdSql.query("SELECT * FROM vntb2 ORDER BY ts LIMIT 3 OFFSET 5")
        tdSql.checkRows(3)

        tdSql.query("SELECT * FROM vstb2 ORDER BY ts LIMIT 5")
        tdSql.checkRows(5)

        tdSql.query("SELECT * FROM vntb2 ORDER BY int_col DESC LIMIT 5")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 9)

    # ================================================================
    # Subquery / Nested query
    # ================================================================

    def test_subquery_on_chain(self):
        """Subquery: nested queries on vtable chain

        1. Subquery with WHERE on vntb2
        2. Subquery with GROUP BY on vstb2
        3. AVG in subquery

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test subquery on chain ===")
        tdSql.execute(f"use {DB}")

        tdSql.query("SELECT * FROM (SELECT ts, int_col, double_col FROM vntb2 WHERE int_col > 3) ORDER BY ts")
        tdSql.checkRows(6)

        tdSql.query("SELECT * FROM (SELECT int_tag, COUNT(*) AS cnt FROM vstb2 GROUP BY int_tag) ORDER BY int_tag")
        tdSql.checkRows(2)

        tdSql.query("SELECT AVG(int_col) FROM (SELECT int_col FROM vntb2 WHERE int_col BETWEEN 2 AND 7)")
        tdSql.checkRows(1)

    # ================================================================
    # UNION ALL
    # ================================================================

    def test_union_on_chain(self):
        """Union: UNION ALL on vtable chain

        1. UNION ALL between vntb1 and vntb2 with different filters
        2. UNION ALL between vstb children

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test union on chain ===")
        tdSql.execute(f"use {DB}")

        tdSql.query("SELECT int_col FROM vntb1 WHERE int_col <= 2 "
                     "UNION ALL SELECT int_col FROM vntb2 WHERE int_col >= 8 ORDER BY int_col")
        tdSql.checkRows(5)

    # ================================================================
    # JOIN with vtable chain
    # ================================================================

    def test_join_on_chain(self):
        """Join: JOIN between vtables in chain

        1. JOIN vntb1 and vntb2 on timestamp
        2. JOIN vtable with physical table

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test join on chain ===")
        tdSql.execute(f"use {DB}")

        tdSql.query("SELECT a.ts, a.int_col AS a_int, b.int_col AS b_int "
                     "FROM vntb1 a, vntb2 b WHERE a.ts = b.ts ORDER BY a.ts LIMIT 5")
        tdSql.checkRows(5)

        tdSql.query("SELECT a.ts, a.int_col, b.val "
                     "FROM vntb1 a, org_ntb2 b WHERE a.ts = b.ts ORDER BY a.ts")
        tdSql.checkRows(3)

    # ================================================================
    # TAG / PARTITION BY on vstb with vtable children
    # ================================================================

    def test_tag_partition_on_vstb(self):
        """TagPartition: tag filter and PARTITION BY on vstb with vtable children

        1. WHERE tag filter on vstb2
        2. PARTITION BY tag
        3. TBNAME filter

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test tag partition on vstb ===")
        tdSql.execute(f"use {DB}")

        tdSql.query("SELECT * FROM vstb2 WHERE int_tag = 0 ORDER BY ts LIMIT 5")
        tdSql.checkRows(5)

        tdSql.query("SELECT * FROM vstb2 WHERE int_tag = 1 ORDER BY ts")
        tdSql.checkRows(3)

        tdSql.query("SELECT int_tag, COUNT(*) AS cnt FROM vstb2 PARTITION BY int_tag")
        tdSql.checkRows(2)

    # ================================================================
    # Cross-table column reference through chain
    # ================================================================

    def test_cross_table_ref_chain(self):
        """CrossTable: columns from different physical tables through chain

        1. Create vntb_cross with columns from org_ntb and org_ntb2
        2. Create vntb_cross2 referencing vntb_cross (chain)
        3. Verify data

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test cross table ref chain ===")
        tdSql.execute(f"use {DB}")
        tdSql.execute("CREATE VTABLE vntb_cross (ts timestamp, "
                       "a_int int from org_ntb.int_col, b_val int from org_ntb2.val)")
        tdSql.execute("CREATE VTABLE vntb_cross2 (ts timestamp, "
                       "a_int int from vntb_cross.a_int, b_val int from vntb_cross.b_val)")

        tdSql.query("SELECT * FROM vntb_cross2 ORDER BY ts")
        assert tdSql.getRows() > 0

    # ================================================================
    # Advanced functions on deepest chain
    # ================================================================

    def test_advanced_functions_on_deep_chain(self):
        """Functions: advanced functions on deep vtable chain

        1. SPREAD, DIFF, TOP, BOTTOM on vntb2
        2. FIRST/LAST/LAST_ROW on deep chain
        3. Interval + FILL on chain

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test advanced functions on deep chain ===")
        tdSql.execute(f"use {DB}")

        tdSql.query("SELECT SPREAD(int_col) FROM vntb2")
        tdSql.checkData(0, 0, 9)

        tdSql.query("SELECT SPREAD(int_col) FROM vstb2")
        assert tdSql.getRows() > 0

        tdSql.query("SELECT TOP(int_col, 3) FROM vntb2")
        tdSql.checkRows(3)
        tdSql.query("SELECT BOTTOM(int_col, 3) FROM vntb2")
        tdSql.checkRows(3)

        tdSql.query("SELECT FIRST(int_col) FROM vntb_L5")
        tdSql.checkData(0, 0, 0)
        tdSql.query("SELECT LAST(int_col) FROM vntb_L5")
        tdSql.checkData(0, 0, 9)

        tdSql.query("SELECT LAST_ROW(*) FROM vntb2")
        tdSql.checkRows(1)
        tdSql.query("SELECT LAST_ROW(*) FROM vstb2")
        tdSql.checkRows(1)

    # ================================================================
    # Arithmetic and CAST on vtable chain
    # ================================================================

    def test_arithmetic_cast_on_chain(self):
        """Expr: arithmetic expressions and CAST on vtable chain

        1. Arithmetic expressions in SELECT
        2. CAST between types

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test arithmetic and cast on chain ===")
        tdSql.execute(f"use {DB}")

        tdSql.query("SELECT ts, int_col, int_col * 2 AS doubled FROM vntb2 ORDER BY ts LIMIT 3")
        tdSql.checkRows(3)
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 2, 4)

        tdSql.query("SELECT ts, CAST(int_col AS BIGINT) AS bg FROM vntb2 ORDER BY ts LIMIT 3")
        tdSql.checkRows(3)

    # ================================================================
    # DROP intermediate vtable, query should fail
    # ================================================================

    def test_drop_intermediate_vtable(self):
        """Drop: drop intermediate vtable in chain, query should fail

        1. Create temp chain: org_ntb -> vntb_tmp1 -> vntb_tmp2
        2. Verify vntb_tmp2 works
        3. Drop vntb_tmp1
        4. Verify vntb_tmp2 query fails

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test drop intermediate vtable ===")
        tdSql.execute(f"use {DB}")
        tdSql.execute("CREATE VTABLE vntb_tmp1 (ts timestamp, int_col int from org_ntb.int_col)")
        tdSql.execute("CREATE VTABLE vntb_tmp2 (ts timestamp, int_col int from vntb_tmp1.int_col)")

        tdSql.query("SELECT COUNT(*) FROM vntb_tmp2")
        tdSql.checkData(0, 0, 10)

        tdSql.execute("DROP TABLE vntb_tmp1")
        tdSql.error("SELECT * FROM vntb_tmp2")

    # ================================================================
    # Multiple vstbs sharing same vtable source
    # ================================================================

    def test_multiple_vstbs_share_source(self):
        """SharedSource: multiple vstbs with children referencing same vtable

        1. Create two vstbs with children both referencing vntb1
        2. Verify both return correct data independently

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test multiple vstbs share source ===")
        tdSql.execute(f"use {DB}")
        tdSql.execute("CREATE STABLE vstb_share_a (ts timestamp, int_col int, double_col double) "
                       "TAGS (t1 int) VIRTUAL 1")
        tdSql.execute("CREATE STABLE vstb_share_b (ts timestamp, int_col int, double_col double) "
                       "TAGS (t1 int) VIRTUAL 1")
        tdSql.execute("CREATE VTABLE vctb_share_a0 (int_col from vntb1.int_col, "
                       "double_col from vntb1.double_col) USING vstb_share_a TAGS (0)")
        tdSql.execute("CREATE VTABLE vctb_share_b0 (int_col from vntb1.int_col, "
                       "double_col from vntb1.double_col) USING vstb_share_b TAGS (0)")

        tdSql.query("SELECT COUNT(*), SUM(int_col) FROM vstb_share_a")
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(0, 1, 45)

        tdSql.query("SELECT COUNT(*), SUM(int_col) FROM vstb_share_b")
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(0, 1, 45)

    # ================================================================
    # Single column vtable chain
    # ================================================================

    def test_single_column_chain(self):
        """SingleCol: single column vtable chain

        1. Create 2-layer chain with only int_col
        2. Verify data

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test single column chain ===")
        tdSql.execute(f"use {DB}")
        tdSql.execute("CREATE VTABLE vntb_1col_a (ts timestamp, int_col int from org_ntb.int_col)")
        tdSql.execute("CREATE VTABLE vntb_1col_b (ts timestamp, int_col int from vntb_1col_a.int_col)")

        tdSql.query("SELECT COUNT(*), SUM(int_col) FROM vntb_1col_b")
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(0, 1, 45)

    # ================================================================
    # Many children under vstb via vtable chain
    # ================================================================

    def test_many_children_via_chain(self):
        """ManyChildren: multiple children under vstb, each via vtable chain

        1. Create 5 physical tables, 5 L1 vtables, 5 vstb children
        2. Verify super table scan returns combined data

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test many children via chain ===")
        tdSql.execute(f"use {DB}")

        tdSql.execute("CREATE STABLE org_stb_many (ts timestamp, val int) TAGS (idx int)")
        for i in range(5):
            tdSql.execute(f"CREATE TABLE org_ctb_m{i} USING org_stb_many TAGS ({i})")
            tdSql.execute(f"INSERT INTO org_ctb_m{i} VALUES ('2025-01-01 00:00:00', {(i+1)*10})")

        for i in range(5):
            tdSql.execute(f"CREATE VTABLE vntb_m{i} (ts timestamp, val int from org_ctb_m{i}.val)")

        tdSql.execute("CREATE STABLE vstb_many (ts timestamp, val int) TAGS (idx int) VIRTUAL 1")
        for i in range(5):
            tdSql.execute(f"CREATE VTABLE vctb_many_{i} (val from vntb_m{i}.val) USING vstb_many TAGS ({i})")

        tdSql.query("SELECT COUNT(*) FROM vstb_many")
        tdSql.checkData(0, 0, 5)
        tdSql.query("SELECT SUM(val) FROM vstb_many")
        tdSql.checkData(0, 0, 150)

    # ================================================================
    # Complex WHERE conditions on vstb chain
    # ================================================================

    def test_complex_where_on_vstb(self):
        """ComplexWhere: complex WHERE conditions on vstb with vtable children

        1. AND conditions
        2. OR conditions
        3. BETWEEN
        4. Timestamp + tag combined filter

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test complex where on vstb ===")
        tdSql.execute(f"use {DB}")

        tdSql.query("SELECT * FROM vstb2 WHERE int_col > 0 AND int_col < 5 ORDER BY ts")
        tdSql.checkRows(4)

        tdSql.query("SELECT * FROM vstb2 WHERE int_col > 100 OR int_col < 2 ORDER BY ts")
        assert tdSql.getRows() > 0

        tdSql.query("SELECT * FROM vstb2 WHERE int_tag = 0 AND int_col BETWEEN 3 AND 7 ORDER BY ts")
        tdSql.checkRows(5)

    # ================================================================
    # information_schema queries for vtables
    # ================================================================

    def test_information_schema(self):
        """Schema: verify vtables appear in information_schema

        1. Query ins_tables for VIRTUAL_NORMAL_TABLE
        2. Query ins_tables for VIRTUAL_CHILD_TABLE
        3. Query ins_stables for virtual super tables

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2026-3-10 Created for vtable-ref-vtable feature

        """
        tdLog.info("=== test information_schema ===")

        tdSql.query(f"SELECT table_name FROM information_schema.ins_tables "
                     f"WHERE db_name = '{DB}' AND type = 'VIRTUAL_NORMAL_TABLE' ORDER BY table_name LIMIT 10")
        assert tdSql.getRows() > 0

        tdSql.query(f"SELECT table_name FROM information_schema.ins_tables "
                     f"WHERE db_name = '{DB}' AND type = 'VIRTUAL_CHILD_TABLE' ORDER BY table_name LIMIT 10")
        assert tdSql.getRows() > 0

        tdSql.query(f"SELECT stable_name FROM information_schema.ins_stables "
                     f"WHERE db_name = '{DB}' AND stable_name LIKE 'vstb%' ORDER BY stable_name")
        assert tdSql.getRows() > 0

    def cleanup_class(cls):
        tdLog.info(f"cleanup {__file__}")
        tdSql.execute(f"drop database if exists {DB}")
