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
import pytest
from new_test_framework.utils import tdLog, tdSql, clusterComCheck


class TestBalanceVgroupLeaderDbName:
    """Test that BALANCE VGROUP LEADER DATABASE handles backtick-quoted and
    uppercase database names correctly.

    Regression test for the bug where createBalanceVgroupLeaderDBNameStmt used
    COPY_STRING_FORM_ID_TOKEN (which preserves backticks) instead of calling
    checkDbName first (which strips them via trimEscape).  This caused the
    server-side strcmp(req.db, name.dbname) to always fail when the SQL used
    backtick-quoted identifiers, returning "No VGroup's leader need to be
    balanced" even though the database existed.
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def _count_leaders(self, db: str, vgroup_rows: int) -> int:
        """Return how many vgroups for *db* have a leader on any replica slot."""
        tdSql.query(f"show {db}.vgroups", show=True)
        leaders = 0
        for i in range(vgroup_rows):
            if (
                tdSql.checkDataV2(i, 4, "leader", False)
                or tdSql.checkDataV2(i, 7, "leader", False)
                or tdSql.checkDataV2(i, 10, "leader", False)
            ):
                leaders += 1
        return leaders

    @pytest.mark.cluster
    def test_balance_vgroup_leader_db_name(self):
        """BALANCE VGROUP LEADER DATABASE with backtick-quoted db names

        1. Create 3-dnode cluster
        2. Create database with uppercase name (TEST_BIG) replica 3 vgroups 2
        3. Create database with lowercase name (test_lower) replica 3 vgroups 2
        4. BALANCE VGROUP LEADER DATABASE `TEST_BIG`  -- backtick uppercase
        5. BALANCE VGROUP LEADER DATABASE TEST_BIG    -- plain  uppercase
        6. BALANCE VGROUP LEADER DATABASE `test_lower` -- backtick lowercase
        7. BALANCE VGROUP LEADER DATABASE test_lower   -- plain  lowercase
        8. Verify each balance succeeds and every vgroup still has a leader

        Since: v3.3.7.0

        Labels: cluster,ci

        Jira: None

        History:
            - 2026-05-14 GitHub Copilot Created

        """
        clusterComCheck.checkDnodes(3)

        tdLog.info("========== create databases")
        tdSql.execute("create database `TEST_BIG` replica 3 vgroups 2")
        tdSql.execute("create database `test_lower` replica 3 vgroups 2")
        clusterComCheck.checkDbReady("`TEST_BIG`")
        clusterComCheck.checkDbReady("`test_lower`")

        # ------------------------------------------------------------------
        # Case 1: backtick-quoted uppercase name -- the original bug scenario
        # ------------------------------------------------------------------
        tdLog.info("========== step1: balance vgroup leader database `TEST_BIG`")
        tdSql.execute("balance vgroup leader database `TEST_BIG`")
        clusterComCheck.checkTransactions()
        clusterComCheck.checkDbReady("`TEST_BIG`")
        leaders = self._count_leaders("`TEST_BIG`", 2)
        if leaders != 2:
            tdLog.exit(f"step1 failed: expected 2 leaders, got {leaders}")

        # ------------------------------------------------------------------
        # Case 2: plain uppercase name (no backticks) -- control path
        # ------------------------------------------------------------------
        tdLog.info("========== step2: balance vgroup leader database TEST_BIG")
        tdSql.execute("balance vgroup leader database `TEST_BIG`")
        clusterComCheck.checkTransactions()
        clusterComCheck.checkDbReady("`TEST_BIG`")
        leaders = self._count_leaders("`TEST_BIG`", 2)
        if leaders != 2:
            tdLog.exit(f"step2 failed: expected 2 leaders, got {leaders}")

        # ------------------------------------------------------------------
        # Case 3: backtick-quoted lowercase name
        # ------------------------------------------------------------------
        tdLog.info("========== step3: balance vgroup leader database `test_lower`")
        tdSql.execute("balance vgroup leader database `test_lower`")
        clusterComCheck.checkTransactions()
        clusterComCheck.checkDbReady("`test_lower`")
        leaders = self._count_leaders("`test_lower`", 2)
        if leaders != 2:
            tdLog.exit(f"step3 failed: expected 2 leaders, got {leaders}")

        # ------------------------------------------------------------------
        # Case 4: plain lowercase name -- control path
        # ------------------------------------------------------------------
        tdLog.info("========== step4: balance vgroup leader database `test_lower`")
        tdSql.execute("balance vgroup leader database `test_lower`")
        clusterComCheck.checkTransactions()
        clusterComCheck.checkDbReady("`test_lower`")
        leaders = self._count_leaders("`test_lower`", 2)
        if leaders != 2:
            tdLog.exit(f"step4 failed: expected 2 leaders, got {leaders}")

        tdLog.info("========== all balance vgroup leader database name cases passed")
