###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved
#
#  This file is proprietary and confidential to TAOS Technologies, Inc.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission of Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
"""Depth-boundary test for vntb tag-ref chains.

NOT registered in cases.task on purpose: building a 33-edge chain costs O(n^2)
catalog round-trips during DDL translation (each CREATE re-walks the whole
existing chain), which exceeds the CI worker timeout on a multi-node cluster
under ASan. The boundary semantics are verified here for local runs; see
test_vtable_tag_ref_vntb_src.py for the CI-registered suite.
"""

from new_test_framework.utils import tdLog, tdSql

DB = "td_trvns_depth"
TS0 = 1700000000000
TSDB_CODE_VTABLE_REF_DEPTH_EXCEEDED = -2147458548


class TestTagRefDepthBoundary:
    """33 ref edges are the legal cap; the 34th edge must be rejected."""

    @classmethod
    def setup_class(cls):
        tdLog.info("=== setup: depth-boundary chain env ===")
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB};")
        tdSql.execute(f"CREATE DATABASE {DB} BUFFER 16;")
        tdSql.execute(f"USE {DB};")
        tdSql.execute("CREATE STABLE src_stb (ts TIMESTAMP, val INT) "
                      "TAGS (city NCHAR(20), code INT);")
        tdSql.execute(f"CREATE TABLE c1 USING src_stb TAGS ('beijing', 100);")
        for j, v in enumerate([1, 2, 3]):
            tdSql.execute(f"INSERT INTO c1 VALUES ({TS0 + j * 1000}, {v});")
        tdLog.info("=== setup complete ===")

    def test_chain_depth_boundary(self):
        """Chain depth boundary: 33 ref edges pass, the 34th is rejected.

        Builds c1 -> d_1 -> ... -> d_33 (33 edges, legal cap, matching the shipped
        vchild max-depth behavior) and asserts d_34 is rejected with the
        depth-exceeded error.

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: virtual, negative

        Jira: None

        History:
            - 2026-09-16 Created
        """
        prev = "c1"
        prev_tag = "city"
        prev_col = "val"
        for i in range(1, 34):
            tdSql.execute(f"CREATE VTABLE d_{i} (ts TIMESTAMP, vv INT FROM {prev}.{prev_col}) "
                          f"TAGS (ct_{i} NCHAR(20) FROM {prev}.{prev_tag});")
            prev = f"d_{i}"
            prev_tag = f"ct_{i}"
            prev_col = "vv"
        # the 34th edge must be rejected — pinned to the depth-exceeded error code so a
        # different failure (syntax, name clash, ...) can never masquerade as the boundary
        tdSql.error(f"CREATE VTABLE d_34 (ts TIMESTAMP, vv INT FROM d_33.vv) "
                    f"TAGS (ct_34 NCHAR(20) FROM d_33.ct_33);",
                    expectedErrno=TSDB_CODE_VTABLE_REF_DEPTH_EXCEEDED)
        # value still resolves through the full legal depth (33 edges)
        tdSql.query(f"SELECT ct_33 FROM {DB}.d_33;")
        assert tdSql.queryRows >= 1
        assert tdSql.getData(0, 0) == "beijing", \
            f"d_33 tag should resolve to c1.city, got {tdSql.getData(0, 0)}"
        # no per-table DROP cleanup: chain-linked drops re-walk the chain per DROP and
        # dominate wall time; setup_class drops the whole database on the next run
