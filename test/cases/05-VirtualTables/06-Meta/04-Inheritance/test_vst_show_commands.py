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

# Test cases for SHOW VTABLE INHERITS command and SHOW CREATE STABLE
# output verification for VST inheritance.

from new_test_framework.utils import tdLog, tdSql, etool, tdCom

DB = "test_vst_show"


class TestVstShowCommands:

    def setup_class(cls):
        tdLog.info("setup database for SHOW command tests")
        tdSql.execute(f"drop database if exists {DB}")
        tdSql.execute(f"create database {DB}")
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable src (ts timestamp, c1 int, c2 float) tags (loc int)"
        )
        tdSql.execute(f"create table src_t1 using src tags (1)")
        tdSql.execute(f"insert into src_t1 values (now, 10, 1.5)")

        tdSql.execute(
            f"create stable show_p1 (ts timestamp, p1_col int) "
            f"tags (p1_tag int) virtual 1"
        )
        tdSql.execute(
            f"create stable show_p2 (ts timestamp, p2_col float) "
            f"tags (p2_tag int) virtual 1"
        )
        tdSql.execute(
            f"create stable show_child "
            f"(ts timestamp, own_col double) "
            f"tags (own_tag int) "
            f"base on {DB}.show_p1, {DB}.show_p2 virtual 1"
        )

    # ============================================================
    # Test 1: SHOW VTABLE INHERITS returns correct rows
    # ============================================================
    def test_show_vtable_inherits_basic(self):
        """VST Show: SHOW VTABLE INHERITS basic

        Verify SHOW VTABLE INHERITS returns rows for VST inheritance.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, show

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.query(f"show vtable inherits")
        assert tdSql.queryRows >= 2, (
            f"Expected >= 2 rows, got {tdSql.queryRows}"
        )

    # ============================================================
    # Test 2: ins_vstable_inherits column values
    # ============================================================
    def test_ins_vstable_inherits_column_values(self):
        """VST Show: ins_vstable_inherits column values

        Verify that information_schema.ins_vstable_inherits contains
        correct db_name, parent_stable_name, and child_stable_name.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, show

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.query(
            f"select db_name, parent_stable_name, child_stable_name "
            f"from information_schema.ins_vstable_inherits "
            f"where child_stable_name = 'show_child'"
        )
        assert tdSql.queryRows == 2, f"Expected 2 rows, got {tdSql.queryRows}"

        parents = set()
        for i in range(tdSql.queryRows):
            db = tdSql.getData(i, 0)
            parent = tdSql.getData(i, 1)
            child = tdSql.getData(i, 2)
            assert db == DB, f"db_name should be {DB}, got {db}"
            assert child == "show_child"
            parents.add(parent)

        assert "show_p1" in parents, "Missing parent show_p1"
        assert "show_p2" in parents, "Missing parent show_p2"

    # ============================================================
    # Test 3: SHOW CREATE STABLE with BASE ON
    # ============================================================
    def test_show_create_with_base_on(self):
        """VST Show: SHOW CREATE STABLE with BASE ON

        Verify SHOW CREATE STABLE output contains BASE ON clause
        with parent names.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, show

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.query(f"show create stable {DB}.show_child")
        stmt = str(tdSql.queryResult[0][1])
        assert "BASE ON" in stmt, f"Expected BASE ON in: {stmt}"
        assert "show_p1" in stmt, f"Expected show_p1 in: {stmt}"
        assert "show_p2" in stmt, f"Expected show_p2 in: {stmt}"

    # ============================================================
    # Test 4: SHOW CREATE STABLE contains VIRTUAL 1
    # ============================================================
    def test_show_create_virtual_flag(self):
        """VST Show: SHOW CREATE STABLE contains VIRTUAL 1

        Verify SHOW CREATE STABLE output contains VIRTUAL 1 for VST.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, show

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.query(f"show create stable {DB}.show_p1")
        stmt = str(tdSql.queryResult[0][1])
        assert "virtual" in stmt.lower(), f"Expected 'virtual' in: {stmt}"

    # ============================================================
    # Test 5: SHOW CREATE STABLE without BASE ON
    # ============================================================
    def test_show_create_without_base_on(self):
        """VST Show: SHOW CREATE STABLE without BASE ON

        A root VST (no parents) should NOT have BASE ON in its
        SHOW CREATE output.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, show

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.query(f"show create stable {DB}.show_p1")
        stmt = str(tdSql.queryResult[0][1])
        assert "BASE ON" not in stmt, f"Unexpected BASE ON in: {stmt}"

    # ============================================================
    # Test 6: SHOW CREATE after DROP BASE ON
    # ============================================================
    def test_show_create_after_drop_base_on(self):
        """VST Show: SHOW CREATE after DROP BASE ON

        After ALTER DROP BASE ON, the SHOW CREATE output should no
        longer contain the dropped parent name.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, show

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable show_tmp (ts timestamp, tc1 int, tc2 float, tc3 double) "
            f"tags (tt1 int, tt2 int) "
            f"base on {DB}.show_p1 virtual 1"
        )

        tdSql.query(f"show create stable {DB}.show_tmp")
        stmt_before = str(tdSql.queryResult[0][1])
        assert "BASE ON" in stmt_before

        tdSql.execute(f"alter stable show_tmp drop base on {DB}.show_p1")

        tdSql.query(f"show create stable {DB}.show_tmp")
        stmt_after = str(tdSql.queryResult[0][1])
        assert "BASE ON" not in stmt_after, (
            f"BASE ON should be removed: {stmt_after}"
        )

        tdSql.execute(f"drop stable show_tmp")

    # ============================================================
    # Test 7: ins_vstable_inherits after ALTER ADD BASE ON
    # ============================================================
    def test_inherits_after_alter_add(self):
        """VST Show: ins_vstable_inherits after ALTER ADD

        Verify that after ALTER ADD BASE ON, the system table
        reflects the new parent.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, show

        Jira: None

        History:
            - 2026-06-09 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable show_add_ch (ts timestamp, ac1 int, ac2 float) "
            f"tags (at1 int) "
            f"base on {DB}.show_p1 virtual 1"
        )

        tdSql.query(
            f"select count(*) from information_schema.ins_vstable_inherits "
            f"where child_stable_name = 'show_add_ch'"
        )
        assert tdSql.queryResult[0][0] == 1

        tdSql.execute(
            f"alter stable show_add_ch add base on {DB}.show_p2"
        )

        tdSql.query(
            f"select count(*) from information_schema.ins_vstable_inherits "
            f"where child_stable_name = 'show_add_ch'"
        )
        assert tdSql.queryResult[0][0] == 2

        tdSql.execute(
            f"alter stable show_add_ch drop base on {DB}.show_p1"
        )
        tdSql.execute(
            f"alter stable show_add_ch drop base on {DB}.show_p2"
        )
        tdSql.execute(f"drop stable show_add_ch")

    # ============================================================
    # Test 8: SHOW CREATE STABLE DDL is replayable
    # ============================================================
    def test_show_create_replay(self):
        """VST Show: SHOW CREATE STABLE DDL replay

        The DDL emitted by SHOW CREATE STABLE for an inherited VST
        must be re-executable: drop the child, run the captured DDL,
        and verify the BASE ON inheritance is reconstructed. This
        locks down backup/restore and taosdump replay correctness.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, show, replay

        Jira: None

        History:
            - 2026-06-24 Created
        """
        tdSql.execute(f"use {DB}")

        tdSql.execute(
            f"create stable replay_child (ts timestamp, own_c int) "
            f"tags (own_t int) "
            f"base on {DB}.show_p1 virtual 1"
        )

        tdSql.query(f"show create stable {DB}.replay_child")
        ddl = str(tdSql.queryResult[0][1]).strip()
        assert "BASE ON" in ddl, f"replay DDL missing BASE ON: {ddl}"

        tdSql.execute(f"drop stable replay_child")

        tdSql.execute(ddl)

        tdSql.query(f"show create stable {DB}.replay_child")
        ddl2 = str(tdSql.queryResult[0][1]).strip()
        assert ddl == ddl2, f"replayed SHOW CREATE differs:\n{ddl}\nvs\n{ddl2}"

        tdSql.query(
            f"select count(*) from information_schema.ins_vstable_inherits "
            f"where child_stable_name = 'replay_child'"
        )
        assert tdSql.queryResult[0][0] == 1

        tdSql.execute(f"drop stable replay_child")

    # ============================================================
    # Test 9: SHOW CREATE STABLE for zero-own-tags BASE ON VST
    # ============================================================
    def test_show_create_no_own_tags(self):
        """VST Show: SHOW CREATE STABLE for BASE ON VST with no own tags

        When all tags are inherited from parents, SHOW CREATE STABLE must
        omit the TAGS clause entirely (not emit "TAGS ()"), matching the
        grammar rule:
          CREATE STABLE ... (cols) BASE ON parents VIRTUAL 1
        The resulting DDL must be replayable.

        Catalog:
            - VirtualTable

        Since: v3.4.1.0

        Labels: virtual, inheritance, show, replay

        Jira: None

        History:
            - 2026-06-24 Created
        """
        tdSql.execute(f"use {DB}")

        # Create a VST with no own tags — tags come entirely from show_p1 (p1_tag int)
        tdSql.execute(
            f"create stable notags_child "
            f"(ts timestamp, own_col int) "
            f"base on {DB}.show_p1 virtual 1"
        )

        tdSql.query(f"show create stable {DB}.notags_child")
        ddl = str(tdSql.queryResult[0][1]).strip()

        assert "BASE ON" in ddl, f"Expected BASE ON in DDL: {ddl}"
        assert "TAGS ()" not in ddl, f"DDL must not contain empty 'TAGS ()': {ddl}"

        # Drop and replay the DDL — it must parse and re-create the VST cleanly
        tdSql.execute(f"drop stable notags_child")
        tdSql.execute(ddl)

        tdSql.query(f"show create stable {DB}.notags_child")
        ddl2 = str(tdSql.queryResult[0][1]).strip()
        assert ddl == ddl2, f"Replayed SHOW CREATE differs:\n{ddl}\nvs\n{ddl2}"

        # Inheritance record must survive the drop+replay
        tdSql.query(
            f"select count(*) from information_schema.ins_vstable_inherits "
            f"where child_stable_name = 'notags_child'"
        )
        assert tdSql.queryResult[0][0] == 1

        tdSql.execute(f"drop stable notags_child")
