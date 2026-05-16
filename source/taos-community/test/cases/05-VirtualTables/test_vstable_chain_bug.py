###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
###################################################################

# -*- coding: utf-8 -*-
"""Minimal repro: vstable->vstable L2+ SELECT * returns Invalid parameters."""

from new_test_framework.utils import tdLog, tdSql


class TestVstableChainBug:

    def setup_class(cls):
        tdSql.execute("DROP DATABASE IF EXISTS bug_vstchain;")
        tdSql.execute("CREATE DATABASE bug_vstchain VGROUPS 2;")
        tdSql.execute("USE bug_vstchain;")

        # source stable + 2 children
        tdSql.execute("CREATE STABLE src_stb (ts TIMESTAMP, v1 INT, v2 INT) "
                      "TAGS (gid INT, region NCHAR(16));")
        tdSql.execute("CREATE TABLE src_c0 USING src_stb TAGS (0, 'r0');")
        tdSql.execute("CREATE TABLE src_c1 USING src_stb TAGS (1, 'r1');")
        tdSql.execute("INSERT INTO src_c0 VALUES "
                      "(1700000000000, 10, 100)(1700000001000, 20, 200);")
        tdSql.execute("INSERT INTO src_c1 VALUES "
                      "(1700000000000, 15, 150)(1700000001000, 25, 250);")

        # L1: vstable -> src_stb
        tdSql.execute("CREATE STABLE vstb_d1 (ts TIMESTAMP, v1 INT, v2 INT) "
                      "TAGS (gid INT, region NCHAR(16)) VIRTUAL 1;")
        tdSql.execute("CREATE VTABLE vc1_c0 (v1 FROM src_c0.v1, v2 FROM src_c0.v2) "
                      "USING vstb_d1 TAGS (10, 'v1_r0');")
        tdSql.execute("CREATE VTABLE vc1_c1 (v1 FROM src_c1.v1, v2 FROM src_c1.v2) "
                      "USING vstb_d1 TAGS (11, 'v1_r1');")

        # L2: vstable -> vstable(L1)
        tdSql.execute("CREATE STABLE vstb_d2 (ts TIMESTAMP, v1 INT, v2 INT) "
                      "TAGS (gid INT, region NCHAR(16)) VIRTUAL 1;")
        tdSql.execute("CREATE VTABLE vc2_c0 (v1 FROM vc1_c0.v1, v2 FROM vc1_c0.v2) "
                      "USING vstb_d2 TAGS (20, 'v2_r0');")
        tdSql.execute("CREATE VTABLE vc2_c1 (v1 FROM vc1_c1.v1, v2 FROM vc1_c1.v2) "
                      "USING vstb_d2 TAGS (21, 'v2_r1');")

    def test_vstable_l2_queries(self):
        """Bug repro: vstable chain L2 query failures

        L1 queries should all work. L2 queries fail with Invalid parameters
        for certain query types.

        Catalog:
            - VirtualTable

        Since: v3.4.0.0

        Labels: virtual, bug

        Jira: None

        History:
            - 2026-04-17 Created
        """
        tdSql.execute("USE bug_vstchain;")

        # L1 queries - all should work
        tdLog.info("--- L1 queries ---")
        tdSql.query("SELECT * FROM vstb_d1;")
        tdLog.info(f"  L1 SELECT *: {tdSql.queryRows} rows")
        tdSql.checkRows(4)

        tdSql.query("SELECT COUNT(*) FROM vstb_d1;")
        tdLog.info(f"  L1 COUNT: {tdSql.queryResult[0][0]}")
        tdSql.checkData(0, 0, 4)

        tdSql.query("SELECT * FROM vstb_d1 WHERE gid = 10;")
        tdLog.info(f"  L1 tag filter: {tdSql.queryRows} rows")
        tdSql.checkRows(2)

        # L2 queries - test each type
        tdLog.info("--- L2 queries ---")

        # Child table query (should work)
        tdLog.info("  L2 child query...")
        tdSql.query("SELECT * FROM vc2_c0;")
        tdLog.info(f"  L2 child SELECT *: {tdSql.queryRows} rows")
        tdSql.checkRows(2)

        # Tag filter on super table (should work per benchmark)
        tdLog.info("  L2 tag filter...")
        tdSql.query("SELECT * FROM vstb_d2 WHERE gid = 20;")
        tdLog.info(f"  L2 tag filter: {tdSql.queryRows} rows")
        tdSql.checkRows(2)

        # COUNT on super table
        tdLog.info("  L2 COUNT...")
        tdSql.query("SELECT COUNT(*) FROM vstb_d2;")
        tdLog.info(f"  L2 COUNT: {tdSql.queryResult[0][0]}")
        tdSql.checkData(0, 0, 4)

        # SELECT * on super table - THIS IS THE BUG
        tdLog.info("  L2 SELECT * (super table)...")
        tdSql.query("SELECT * FROM vstb_d2;")
        tdLog.info(f"  L2 SELECT *: {tdSql.queryRows} rows")
        tdSql.checkRows(4)
