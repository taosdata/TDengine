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


DB_NAME = "test_vtag_ref"
CROSS_DB = "test_vtag_ref_cross"


class TestVtableTagRef:
    """Test cases for virtual table tag column references.

    Tests the new unified syntax for tag references in virtual child tables:
    - Old syntax:          TAGS (FROM table.tag, ...)
    - Specific tag ref:    TAGS (tag_name FROM table.tag, ...)
    - Positional tag ref:  TAGS (table.tag, ...)  or  TAGS (db.table.tag, ...)
    - Mixed tag ref:       TAGS (literal, tag_name FROM table.tag, table.tag, FROM table.tag, ...)
    """

    def setup_class(cls):
        """Prepare org tables for tag reference tests."""
        tdLog.info("=== setup: prepare databases and tables for virtual table tag ref tests ===")

        tdSql.execute(f"drop database if exists {DB_NAME};")
        tdSql.execute(f"drop database if exists {CROSS_DB};")
        tdSql.execute(f"create database {DB_NAME};")
        tdSql.execute(f"use {DB_NAME};")

        # --- Source super table with various tag types ---
        tdLog.info("prepare org super table.")
        tdSql.execute("CREATE STABLE `org_stb` ("
                      "ts timestamp, "
                      "int_col int, "
                      "bigint_col bigint, "
                      "float_col float, "
                      "double_col double, "
                      "binary_32_col binary(32), "
                      "nchar_32_col nchar(32)"
                      ") TAGS ("
                      "int_tag int, "
                      "bool_tag bool, "
                      "float_tag float, "
                      "double_tag double, "
                      "nchar_32_tag nchar(32), "
                      "binary_32_tag binary(32))")

        # --- Source child tables ---
        tdLog.info("prepare org child tables.")
        for i in range(5):
            tdSql.execute(f"CREATE TABLE `org_child_{i}` USING `org_stb` "
                          f"TAGS ({i}, {'true' if i % 2 == 0 else 'false'}, {i}.{i}, {i}.{i}{i}, "
                          f"'nchar_child{i}', 'bin_child{i}');")

        # --- Source normal tables ---
        tdLog.info("prepare org normal tables.")
        for i in range(3):
            tdSql.execute(f"CREATE TABLE `org_normal_{i}` ("
                          "ts timestamp, "
                          "int_col int, "
                          "bigint_col bigint, "
                          "float_col float, "
                          "double_col double, "
                          "binary_32_col binary(32), "
                          "nchar_32_col nchar(32))")

        # --- Virtual super table ---
        tdLog.info("prepare virtual super table.")
        tdSql.execute("CREATE STABLE `vstb` ("
                      "ts timestamp, "
                      "int_col int, "
                      "bigint_col bigint, "
                      "float_col float, "
                      "double_col double, "
                      "binary_32_col binary(32), "
                      "nchar_32_col nchar(32)"
                      ") TAGS ("
                      "int_tag int, "
                      "bool_tag bool, "
                      "float_tag float, "
                      "double_tag double, "
                      "nchar_32_tag nchar(32), "
                      "binary_32_tag binary(32))"
                      " VIRTUAL 1")

        # --- Insert data into org tables ---
        tdLog.info("insert data into org tables.")
        for i in range(5):
            for j in range(10):
                ts = 1700000000000 + j * 1000
                tdSql.execute(f"INSERT INTO org_child_{i} VALUES ({ts}, {j}, {j*100}, {j}.{j}, {j}.{j}{j}, "
                              f"'bin_{i}_{j}', 'nchar_{i}_{j}');")
        for i in range(3):
            for j in range(10):
                ts = 1700000000000 + j * 1000
                tdSql.execute(f"INSERT INTO org_normal_{i} VALUES ({ts}, {j}, {j*100}, {j}.{j}, {j}.{j}{j}, "
                              f"'bin_{i}_{j}', 'nchar_{i}_{j}');")

        # --- Cross-database setup ---
        tdLog.info("prepare cross-db source tables.")
        tdSql.execute(f"create database {CROSS_DB};")
        tdSql.execute(f"use {CROSS_DB};")
        tdSql.execute("CREATE STABLE `cross_stb` ("
                      "ts timestamp, "
                      "int_col int, "
                      "bigint_col bigint"
                      ") TAGS ("
                      "int_tag int, "
                      "bool_tag bool, "
                      "float_tag float, "
                      "double_tag double, "
                      "nchar_32_tag nchar(32), "
                      "binary_32_tag binary(32))")
        for i in range(3):
            tdSql.execute(f"CREATE TABLE `cross_child_{i}` USING `cross_stb` "
                          f"TAGS ({i+10}, {'true' if i % 2 == 0 else 'false'}, {i+10}.{i}, {i+10}.{i}{i}, "
                          f"'cross_nchar{i}', 'cross_bin{i}');")
            for j in range(10):
                ts = 1700000000000 + j * 1000
                tdSql.execute(f"INSERT INTO cross_child_{i} VALUES ({ts}, {j}, {j*100});")

        tdSql.execute(f"use {DB_NAME};")
        tdLog.info("=== setup complete ===")

    # =========================================================================
    # Helper
    # =========================================================================
    def check_vtable_count(self, vctable_num, vntable_num):
        tdSql.query(f"show {DB_NAME}.vtables;")
        tdSql.checkRows(vctable_num + vntable_num)
        tdSql.query(f"show child {DB_NAME}.vtables;")
        tdSql.checkRows(vctable_num)
        tdSql.query(f"show normal {DB_NAME}.vtables;")
        tdSql.checkRows(vntable_num)

    # =========================================================================
    # 1. Old FROM syntax for tag references
    # =========================================================================
    def test_tag_ref_old_syntax(self):
        """Create: virtual child table with old tag ref syntax (FROM table.tag)

        Test the legacy tag reference syntax where FROM precedes the column_ref
        in the TAGS clause: TAGS (FROM table.tag, ...)

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, create, tag_ref

        Jira: None

        History:
            - 2026-2-11 Created

        """
        tdLog.info("=== Test: old tag reference syntax: FROM column_ref ===")

        tdSql.execute(f"use {DB_NAME};")

        tdSql.execute("CREATE VTABLE `vctb_old_0` ("
                      "int_col FROM org_child_0.int_col, "
                      "bigint_col FROM org_child_1.bigint_col"
                      ") USING vstb TAGS ("
                      "FROM org_child_0.int_tag, "
                      "FROM org_child_0.bool_tag, "
                      "FROM org_child_0.float_tag, "
                      "FROM org_child_0.double_tag, "
                      "FROM org_child_0.nchar_32_tag, "
                      "FROM org_child_0.binary_32_tag)")

        self.check_vtable_count(1, 0)
        tdLog.info("old tag reference syntax test passed")

    # =========================================================================
    # 2. New specific syntax: tag_name FROM table.tag
    # =========================================================================
    def test_tag_ref_specific_syntax(self):
        """Create: virtual child table with specific tag ref syntax (tag_name FROM table.tag)

        Test the new specific tag reference syntax where the tag name is explicitly
        specified: TAGS (tag_name FROM table.tag, ...)

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, create, tag_ref

        Jira: None

        History:
            - 2026-2-11 Created

        """
        tdLog.info("=== Test: new specific tag reference syntax ===")

        tdSql.execute(f"use {DB_NAME};")

        # 2.1 All tags ref from same child table
        tdSql.execute("CREATE VTABLE `vctb_specific_0` ("
                      "int_col FROM org_child_0.int_col, "
                      "bigint_col FROM org_child_1.bigint_col"
                      ") USING vstb TAGS ("
                      "int_tag FROM org_child_0.int_tag, "
                      "bool_tag FROM org_child_0.bool_tag, "
                      "float_tag FROM org_child_0.float_tag, "
                      "double_tag FROM org_child_0.double_tag, "
                      "nchar_32_tag FROM org_child_0.nchar_32_tag, "
                      "binary_32_tag FROM org_child_0.binary_32_tag)")

        self.check_vtable_count(2, 0)

        # 2.2 Tags ref from different child tables
        tdSql.execute("CREATE VTABLE `vctb_specific_1` ("
                      "int_col FROM org_child_0.int_col, "
                      "bigint_col FROM org_child_1.bigint_col"
                      ") USING vstb TAGS ("
                      "int_tag FROM org_child_0.int_tag, "
                      "bool_tag FROM org_child_1.bool_tag, "
                      "float_tag FROM org_child_2.float_tag, "
                      "double_tag FROM org_child_3.double_tag, "
                      "nchar_32_tag FROM org_child_4.nchar_32_tag, "
                      "binary_32_tag FROM org_child_0.binary_32_tag)")

        self.check_vtable_count(3, 0)

        # 2.3 Partial tag references (some literal, some reference)
        tdSql.execute("CREATE VTABLE `vctb_specific_2` ("
                      "int_col FROM org_child_0.int_col, "
                      "bigint_col FROM org_child_1.bigint_col"
                      ") USING vstb TAGS ("
                      "int_tag FROM org_child_0.int_tag, "
                      "false, "
                      "float_tag FROM org_child_2.float_tag, "
                      "3.14, "
                      "'literal_nchar', "
                      "binary_32_tag FROM org_child_0.binary_32_tag)")

        self.check_vtable_count(4, 0)

        tdLog.info("new specific tag reference syntax test passed")

    # =========================================================================
    # 3. New positional syntax: table.tag or db.table.tag
    # =========================================================================
    def test_tag_ref_positional_syntax(self):
        """Create: virtual child table with positional tag ref syntax (table.tag / db.table.tag)

        Test the new positional tag reference syntax where the tag column is
        specified by table.tag (2-part) or db.table.tag (3-part).

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, create, tag_ref

        Jira: None

        History:
            - 2026-2-11 Created

        """
        tdLog.info("=== Test: new positional tag reference syntax ===")

        tdSql.execute(f"use {DB_NAME};")

        # 3.1 2-part name: table.tag
        tdSql.execute("CREATE VTABLE `vctb_pos_0` ("
                      "int_col FROM org_child_0.int_col, "
                      "bigint_col FROM org_child_1.bigint_col"
                      ") USING vstb TAGS ("
                      "org_child_0.int_tag, "
                      "org_child_0.bool_tag, "
                      "org_child_0.float_tag, "
                      "org_child_0.double_tag, "
                      "org_child_0.nchar_32_tag, "
                      "org_child_0.binary_32_tag)")

        self.check_vtable_count(5, 0)

        # 3.2 Positional from different child tables
        tdSql.execute("CREATE VTABLE `vctb_pos_1` ("
                      "int_col FROM org_child_0.int_col, "
                      "bigint_col FROM org_child_1.bigint_col"
                      ") USING vstb TAGS ("
                      "org_child_0.int_tag, "
                      "org_child_1.bool_tag, "
                      "org_child_2.float_tag, "
                      "org_child_3.double_tag, "
                      "org_child_4.nchar_32_tag, "
                      "org_child_0.binary_32_tag)")

        self.check_vtable_count(6, 0)

        # 3.3 3-part name: db.table.tag
        tdSql.execute(f"CREATE VTABLE `vctb_pos_2` ("
                      "int_col FROM org_child_0.int_col, "
                      "bigint_col FROM org_child_1.bigint_col"
                      f") USING vstb TAGS ("
                      f"{DB_NAME}.org_child_0.int_tag, "
                      f"{DB_NAME}.org_child_0.bool_tag, "
                      f"{DB_NAME}.org_child_0.float_tag, "
                      f"{DB_NAME}.org_child_0.double_tag, "
                      f"{DB_NAME}.org_child_0.nchar_32_tag, "
                      f"{DB_NAME}.org_child_0.binary_32_tag)")

        self.check_vtable_count(7, 0)

        tdLog.info("new positional tag reference syntax test passed")

    # =========================================================================
    # 4. Mixed syntax: literal + old FROM + specific + positional
    # =========================================================================
    def test_tag_ref_mixed_syntax(self):
        """Create: virtual child table with mixed tag ref syntax

        Test mixing literal values, old FROM syntax, new specific syntax,
        and new positional syntax in a single TAGS clause.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, create, tag_ref

        Jira: None

        History:
            - 2026-2-11 Created

        """
        tdLog.info("=== Test: mixed tag reference syntax ===")

        tdSql.execute(f"use {DB_NAME};")

        # 4.1 Mix of literal, old FROM, specific, positional
        tdSql.execute("CREATE VTABLE `vctb_mixed_0` ("
                      "int_col FROM org_child_0.int_col, "
                      "bigint_col FROM org_child_1.bigint_col"
                      ") USING vstb TAGS ("
                      "100, "                                                # literal
                      "bool_tag FROM org_child_1.bool_tag, "                 # new specific
                      "org_child_2.float_tag, "                              # new positional
                      "FROM org_child_3.double_tag, "                        # old FROM
                      "'mixed_nchar', "                                      # literal
                      "binary_32_tag FROM org_child_0.binary_32_tag)")       # new specific

        self.check_vtable_count(8, 0)

        # 4.2 Mix with 3-part positional names
        tdSql.execute(f"CREATE VTABLE `vctb_mixed_1` ("
                      "int_col FROM org_child_0.int_col, "
                      "bigint_col FROM org_child_1.bigint_col"
                      f") USING vstb TAGS ("
                      f"int_tag FROM {DB_NAME}.org_child_0.int_tag, "        # specific with 3-part ref
                      "false, "                                               # literal
                      f"{DB_NAME}.org_child_2.float_tag, "                   # positional 3-part
                      "3.14, "                                                # literal
                      "nchar_32_tag FROM org_child_4.nchar_32_tag, "         # specific with 2-part ref
                      "'literal_bin')")                                        # literal

        self.check_vtable_count(9, 0)

        tdLog.info("mixed tag reference syntax test passed")

    # =========================================================================
    # 5. Tag refs combined with different column reference styles
    # =========================================================================
    def test_tag_ref_with_column_ref(self):
        """Create: virtual child table with tag refs + different col ref styles

        Test tag references combined with positional column references
        (no FROM keyword for columns).

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, create, tag_ref

        Jira: None

        History:
            - 2026-2-11 Created

        """
        tdLog.info("=== Test: tag refs with different column ref styles ===")

        tdSql.execute(f"use {DB_NAME};")

        # 5.1 Positional column refs + specific tag refs
        tdSql.execute("CREATE VTABLE `vctb_colref_0` ("
                      "org_child_0.int_col, "
                      "org_child_1.bigint_col, "
                      "org_child_2.float_col"
                      ") USING vstb TAGS ("
                      "int_tag FROM org_child_0.int_tag, "
                      "bool_tag FROM org_child_0.bool_tag, "
                      "float_tag FROM org_child_0.float_tag, "
                      "double_tag FROM org_child_0.double_tag, "
                      "nchar_32_tag FROM org_child_0.nchar_32_tag, "
                      "binary_32_tag FROM org_child_0.binary_32_tag)")

        self.check_vtable_count(10, 0)

        # 5.2 FROM column refs + positional tag refs
        tdSql.execute("CREATE VTABLE `vctb_colref_1` ("
                      "int_col FROM org_child_0.int_col, "
                      "bigint_col FROM org_child_1.bigint_col"
                      ") USING vstb TAGS ("
                      "org_child_0.int_tag, "
                      "org_child_1.bool_tag, "
                      "org_child_2.float_tag, "
                      "org_child_3.double_tag, "
                      "org_child_4.nchar_32_tag, "
                      "org_child_0.binary_32_tag)")

        self.check_vtable_count(11, 0)

        tdLog.info("tag refs with column ref styles test passed")

    # =========================================================================
    # 6. Cross-database tag references
    # =========================================================================
    def test_tag_ref_cross_db(self):
        """Create: virtual child table with cross-database tag references

        Test tag references that point to child tables in a different database.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, create, tag_ref

        Jira: None

        History:
            - 2026-2-11 Created

        """
        tdLog.info("=== Test: cross-database tag references ===")

        tdSql.execute(f"use {DB_NAME};")

        # 6.1 Specific syntax with cross-db ref
        tdSql.execute(f"CREATE VTABLE `vctb_crossdb_0` ("
                      f"int_col FROM {CROSS_DB}.cross_child_0.int_col, "
                      f"bigint_col FROM {CROSS_DB}.cross_child_1.bigint_col"
                      f") USING vstb TAGS ("
                      f"int_tag FROM {CROSS_DB}.cross_child_0.int_tag, "
                      f"bool_tag FROM {CROSS_DB}.cross_child_0.bool_tag, "
                      f"float_tag FROM {CROSS_DB}.cross_child_0.float_tag, "
                      f"double_tag FROM {CROSS_DB}.cross_child_0.double_tag, "
                      f"nchar_32_tag FROM {CROSS_DB}.cross_child_0.nchar_32_tag, "
                      f"binary_32_tag FROM {CROSS_DB}.cross_child_0.binary_32_tag)")

        self.check_vtable_count(12, 0)

        # 6.2 Positional syntax with cross-db ref (3-part name)
        tdSql.execute(f"CREATE VTABLE `vctb_crossdb_1` ("
                      f"int_col FROM {CROSS_DB}.cross_child_1.int_col"
                      f") USING vstb TAGS ("
                      f"{CROSS_DB}.cross_child_1.int_tag, "
                      f"{CROSS_DB}.cross_child_1.bool_tag, "
                      f"{CROSS_DB}.cross_child_1.float_tag, "
                      f"{CROSS_DB}.cross_child_1.double_tag, "
                      f"{CROSS_DB}.cross_child_1.nchar_32_tag, "
                      f"{CROSS_DB}.cross_child_1.binary_32_tag)")

        self.check_vtable_count(13, 0)

        # 6.3 Mixed: some tags from cross-db, some literal
        tdSql.execute(f"CREATE VTABLE `vctb_crossdb_2` ("
                      f"int_col FROM {CROSS_DB}.cross_child_2.int_col"
                      f") USING vstb TAGS ("
                      f"int_tag FROM {CROSS_DB}.cross_child_2.int_tag, "
                      "true, "
                      f"float_tag FROM {CROSS_DB}.cross_child_2.float_tag, "
                      "9.99, "
                      "'cross_nchar_val', "
                      "'cross_bin_val')")

        self.check_vtable_count(14, 0)

        tdLog.info("cross-database tag reference test passed")

    # =========================================================================
    # 7. SHOW CREATE VTABLE verification for tag refs
    # =========================================================================
    def test_tag_ref_show_create_vtable(self):
        """Create: verify tag references via SHOW CREATE VTABLE

        Verify that tag references are correctly stored and returned by
        SHOW CREATE VTABLE command.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, create, tag_ref

        Jira: None

        History:
            - 2026-2-11 Created

        """
        tdLog.info("=== Test: tag reference storage via SHOW CREATE VTABLE ===")

        tdSql.execute(f"use {DB_NAME};")

        # Create a vtable with specific tag refs
        tdSql.execute("CREATE VTABLE `vctb_verify_0` ("
                      "int_col FROM org_child_0.int_col"
                      ") USING vstb TAGS ("
                      "int_tag FROM org_child_2.int_tag, "
                      "bool_tag FROM org_child_2.bool_tag, "
                      "float_tag FROM org_child_2.float_tag, "
                      "double_tag FROM org_child_2.double_tag, "
                      "nchar_32_tag FROM org_child_2.nchar_32_tag, "
                      "binary_32_tag FROM org_child_2.binary_32_tag)")

        self.check_vtable_count(15, 0)

        tdSql.query(f"SHOW CREATE VTABLE {DB_NAME}.vctb_verify_0;")
        tdSql.checkRows(1)
        create_sql = tdSql.getData(0, 1)
        tdLog.info(f"SHOW CREATE VTABLE vctb_verify_0: {create_sql}")
        assert 'vctb_verify_0' in create_sql, "Table name should be in create SQL"

        # Create with positional syntax
        tdSql.execute("CREATE VTABLE `vctb_verify_1` ("
                      "int_col FROM org_child_0.int_col"
                      ") USING vstb TAGS ("
                      "org_child_3.int_tag, "
                      "org_child_3.bool_tag, "
                      "org_child_3.float_tag, "
                      "org_child_3.double_tag, "
                      "org_child_3.nchar_32_tag, "
                      "org_child_3.binary_32_tag)")

        self.check_vtable_count(16, 0)

        tdSql.query(f"SHOW CREATE VTABLE {DB_NAME}.vctb_verify_1;")
        tdSql.checkRows(1)
        create_sql = tdSql.getData(0, 1)
        tdLog.info(f"SHOW CREATE VTABLE vctb_verify_1: {create_sql}")
        assert 'vctb_verify_1' in create_sql, "Table name should be in create SQL"

        # Create with mixed syntax: some literal, some ref
        tdSql.execute("CREATE VTABLE `vctb_verify_2` ("
                      "int_col FROM org_child_0.int_col"
                      ") USING vstb TAGS ("
                      "int_tag FROM org_child_4.int_tag, "
                      "true, "
                      "float_tag FROM org_child_1.float_tag, "
                      "9.99, "
                      "'my_nchar_val', "
                      "binary_32_tag FROM org_child_0.binary_32_tag)")

        self.check_vtable_count(17, 0)

        tdSql.query(f"SHOW CREATE VTABLE {DB_NAME}.vctb_verify_2;")
        tdSql.checkRows(1)
        create_sql = tdSql.getData(0, 1)
        tdLog.info(f"SHOW CREATE VTABLE vctb_verify_2: {create_sql}")
        assert 'vctb_verify_2' in create_sql, "Table name should be in create SQL"
        assert 'my_nchar_val' in create_sql, "Literal nchar tag value should be in create SQL"

        tdLog.info("SHOW CREATE VTABLE tag ref verification passed")

    # =========================================================================
    # 8. DESCRIBE verification
    # =========================================================================
    def test_tag_ref_describe(self):
        """Create: verify tag references via DESCRIBE

        Verify that DESCRIBE on a virtual child table with tag references
        returns the expected schema.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, create, tag_ref

        Jira: None

        History:
            - 2026-2-11 Created

        """
        tdLog.info("=== Test: DESCRIBE virtual child table with tag refs ===")

        tdSql.execute(f"use {DB_NAME};")

        # vctb_verify_0 was created in the previous test with all tag refs
        tdSql.query(f"DESCRIBE {DB_NAME}.vctb_verify_0;")
        tdLog.info(f"DESCRIBE vctb_verify_0 rows: {tdSql.queryRows}")

        # Should have: ts(1) + 6 data cols + 6 tags = 13 rows
        # (vstb has ts + 6 data cols + 6 tags)
        tdSql.checkRows(13)

        tdLog.info("DESCRIBE tag ref verification passed")

    # =========================================================================
    # 9. Tag ref with specific_cols_opt
    # =========================================================================
    def test_tag_ref_with_specific_tags(self):
        """Create: virtual child table with specific tag names in TAGS clause

        Test tag references when specific tag names are listed before TAGS
        keyword: USING vstb (tag1, tag2, ...) TAGS (...)

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, create, tag_ref

        Jira: None

        History:
            - 2026-2-11 Created

        """
        tdLog.info("=== Test: tag refs with specific tag names ===")

        tdSql.execute(f"use {DB_NAME};")

        tdSql.execute("CREATE VTABLE `vctb_spectag_0` ("
                      "int_col FROM org_child_0.int_col"
                      ") USING vstb (int_tag, bool_tag, float_tag, double_tag, nchar_32_tag, binary_32_tag) TAGS ("
                      "int_tag FROM org_child_1.int_tag, "
                      "false, "
                      "float_tag FROM org_child_2.float_tag, "
                      "3.14, "
                      "'nchar_val', "
                      "'bin_val')")

        self.check_vtable_count(18, 0)

        tdSql.query(f"SHOW CREATE VTABLE {DB_NAME}.vctb_spectag_0;")
        tdSql.checkRows(1)
        create_sql = tdSql.getData(0, 1)
        tdLog.info(f"SHOW CREATE VTABLE vctb_spectag_0: {create_sql}")
        assert 'vctb_spectag_0' in create_sql, "Table name should be in create SQL"
        assert 'nchar_val' in create_sql, "Literal nchar tag value should be in create SQL"

        tdLog.info("tag refs with specific tag names test passed")

    # =========================================================================
    # 10. Error cases
    # =========================================================================
    def test_tag_ref_error_cases(self):
        """Create: virtual child table tag ref error cases

        Test various error conditions for tag column references:
        1. Tag ref column does not exist in source table
        2. Tag ref table does not exist
        3. Tag ref type mismatch (non-tag column used as tag)
        4. Positional tag ref - column does not exist
        5. Positional tag ref - table does not exist
        6. Tag type mismatch between virtual and source
        7. 3-part positional with non-existent db
        8. Tag ref from normal table (not child table)

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, create, tag_ref, negative

        Jira: None

        History:
            - 2026-2-11 Created

        """
        tdLog.info("=== Test: tag reference error cases ===")

        tdSql.execute(f"use {DB_NAME};")

        # 10.1 Tag ref column does not exist in source table
        tdSql.error("CREATE VTABLE `vctb_err_0` ("
                    "int_col FROM org_child_0.int_col"
                    ") USING vstb TAGS ("
                    "int_tag FROM org_child_0.not_exist_tag, "
                    "false, 1.0, 2.0, 'nchar', 'bin')")

        # 10.2 Tag ref table does not exist
        tdSql.error("CREATE VTABLE `vctb_err_1` ("
                    "int_col FROM org_child_0.int_col"
                    ") USING vstb TAGS ("
                    "int_tag FROM not_exist_table.int_tag, "
                    "false, 1.0, 2.0, 'nchar', 'bin')")

        # 10.3 Tag ref type mismatch: referencing a non-tag column as tag
        tdSql.error("CREATE VTABLE `vctb_err_2` ("
                    "int_col FROM org_child_0.int_col"
                    ") USING vstb TAGS ("
                    "int_tag FROM org_child_0.int_col, "
                    "false, 1.0, 2.0, 'nchar', 'bin')")

        # 10.4 Positional tag ref - column does not exist
        tdSql.error("CREATE VTABLE `vctb_err_3` ("
                    "int_col FROM org_child_0.int_col"
                    ") USING vstb TAGS ("
                    "org_child_0.not_exist_tag, "
                    "false, 1.0, 2.0, 'nchar', 'bin')")

        # 10.5 Positional tag ref - table does not exist
        tdSql.error("CREATE VTABLE `vctb_err_4` ("
                    "int_col FROM org_child_0.int_col"
                    ") USING vstb TAGS ("
                    "not_exist_table.int_tag, "
                    "false, 1.0, 2.0, 'nchar', 'bin')")

        # 10.6 Tag type mismatch - referencing int_tag for bool_tag position
        tdSql.error("CREATE VTABLE `vctb_err_5` ("
                    "int_col FROM org_child_0.int_col"
                    ") USING vstb TAGS ("
                    "0, "
                    "bool_tag FROM org_child_0.int_tag, "
                    "1.0, 2.0, 'nchar', 'bin')")

        # 10.7 3-part positional with non-existent db
        tdSql.error("CREATE VTABLE `vctb_err_6` ("
                    "int_col FROM org_child_0.int_col"
                    ") USING vstb TAGS ("
                    "nonexist_db.org_child_0.int_tag, "
                    "false, 1.0, 2.0, 'nchar', 'bin')")

        # 10.8 Tag ref from normal table (not child table - has no tags)
        tdSql.error("CREATE VTABLE `vctb_err_7` ("
                    "int_col FROM org_child_0.int_col"
                    ") USING vstb TAGS ("
                    "int_tag FROM org_normal_0.int_col, "
                    "false, 1.0, 2.0, 'nchar', 'bin')")

        tdLog.info("tag reference error cases test passed")

    def test_tag_ref_must_reference_tag_column(self):
        """Create: tag ref must point to actual tag column, not data column

        Verify that when a tag reference points to a column that exists
        but is not a tag column, the error message clearly indicates
        that the referenced column is not a tag column.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, create, tag_ref, negative

        Jira: None

        History:
            - 2026-2-12 Created

        """
        tdLog.info("=== Test: tag ref must point to tag column, not data column ===")

        tdSql.execute(f"use {DB_NAME};")

        err = tdSql.error("CREATE VTABLE `vctb_not_tag_0` ("
                    "int_col FROM org_child_0.int_col"
                    ") USING vstb TAGS ("
                    "int_tag FROM org_child_0.int_col, "
                    "false, 1.0, 2.0, 'nchar', 'bin')")
        assert "not a tag column" in err, f"Expected 'not a tag column' in error, got: {err}"

        err = tdSql.error("CREATE VTABLE `vctb_not_tag_1` ("
                    "int_col FROM org_child_0.int_col"
                    ") USING vstb TAGS ("
                    "int_tag FROM org_child_0.ts, "
                    "false, 1.0, 2.0, 'nchar', 'bin')")
        assert "not a tag column" in err, f"Expected 'not a tag column' in error, got: {err}"

        err = tdSql.error("CREATE VTABLE `vctb_not_tag_2` ("
                    "int_col FROM org_child_0.int_col"
                    ") USING vstb TAGS ("
                    "org_child_0.bigint_col, "
                    "false, 1.0, 2.0, 'nchar', 'bin')")
        assert "not a tag column" in err, f"Expected 'not a tag column' in error, got: {err}"

        err = tdSql.error("CREATE VTABLE `vctb_not_tag_3` ("
                    "int_col FROM org_child_0.int_col"
                    ") USING vstb TAGS ("
                    "int_tag FROM org_child_0.nonexistent_col, "
                    "false, 1.0, 2.0, 'nchar', 'bin')")
        assert "non-existent tag" in err, f"Expected 'non-existent tag' in error, got: {err}"

        err = tdSql.error("CREATE VTABLE `vctb_not_tag_4` ("
                    "int_col FROM org_child_0.int_col"
                    ") USING vstb TAGS ("
                    "int_tag FROM org_child_0.int_col, "
                    "bool_tag FROM org_child_0.bigint_col, "
                    "1.0, 2.0, 'nchar', 'bin')")
        assert "not a tag column" in err, f"Expected 'not a tag column' in error, got: {err}"

        tdLog.info("tag ref must point to tag column test passed")

    # =========================================================================
    # 11. Query data from virtual child tables with tag refs
    # =========================================================================
    def test_tag_ref_query_data(self):
        """Query: select data from virtual child table with tag references

        Verify that data can be queried from virtual child tables that
        were created with tag column references.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, query, tag_ref

        Jira: None

        History:
            - 2026-2-11 Created

        """
        tdLog.info("=== Test: query data from vtables with tag refs ===")

        tdSql.execute(f"use {DB_NAME};")

        # Query from vctb_specific_0 (all tags ref from org_child_0)
        tdSql.query(f"select * from {DB_NAME}.vctb_specific_0 limit 5;")
        tdLog.info(f"vctb_specific_0 query returned {tdSql.queryRows} rows")
        assert tdSql.queryRows <= 5, "Should return at most 5 rows"

        # Query with tag filter
        tdSql.query(f"select int_col, bigint_col from {DB_NAME}.vctb_specific_0;")
        tdLog.info(f"vctb_specific_0 col query returned {tdSql.queryRows} rows")
        tdSql.checkRows(10)

        # Query from virtual super table (all child tables)
        tdSql.query(f"select count(*) from {DB_NAME}.vstb;")
        tdLog.info(f"vstb total count: {tdSql.getData(0, 0)}")

        tdLog.info("query data from vtables with tag refs test passed")

    # =========================================================================
    # 12. Show tags for virtual child table with tag refs
    # =========================================================================
    def test_tag_ref_show_tags(self):
        """Query: show tags from virtual child table with tag references

        Verify that SHOW TAGS works correctly for virtual child tables
        that have tag column references.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, query, tag_ref

        Jira: None

        History:
            - 2026-2-11 Created

        """
        tdLog.info("=== Test: show tags from vtable with tag refs ===")

        tdSql.execute(f"use {DB_NAME};")

        # Show tags from vctb_old_0 (old FROM syntax, all tags ref from org_child_0)
        tdSql.query(f"show tags from {DB_NAME}.vctb_old_0;")
        tdLog.info(f"vctb_old_0 show tags returned {tdSql.queryRows} rows")
        tdSql.checkRows(6)  # 6 tags

        # Show tags from vctb_verify_2 (mixed literal + ref)
        tdSql.query(f"show tags from {DB_NAME}.vctb_verify_2;")
        tdLog.info(f"vctb_verify_2 show tags returned {tdSql.queryRows} rows")
        tdSql.checkRows(6)  # 6 tags

        tdLog.info("show tags test passed")

    # =========================================================================
    # 13. Drop and recreate with tag refs
    # =========================================================================
    def test_tag_ref_drop_recreate(self):
        """Create: drop and recreate virtual child table with tag refs

        Verify that virtual child tables with tag references can be
        dropped and recreated without issues.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, create, drop, tag_ref

        Jira: None

        History:
            - 2026-2-11 Created

        """
        tdLog.info("=== Test: drop and recreate vtable with tag refs ===")

        tdSql.execute(f"use {DB_NAME};")

        # Create a temporary vtable
        tdSql.execute("CREATE VTABLE `vctb_temp` ("
                      "int_col FROM org_child_0.int_col"
                      ") USING vstb TAGS ("
                      "int_tag FROM org_child_0.int_tag, "
                      "bool_tag FROM org_child_0.bool_tag, "
                      "float_tag FROM org_child_0.float_tag, "
                      "double_tag FROM org_child_0.double_tag, "
                      "nchar_32_tag FROM org_child_0.nchar_32_tag, "
                      "binary_32_tag FROM org_child_0.binary_32_tag)")

        tdSql.query(f"SHOW CREATE VTABLE {DB_NAME}.vctb_temp;")
        tdSql.checkRows(1)

        # Drop it
        tdSql.execute(f"DROP VTABLE {DB_NAME}.vctb_temp;")

        # Verify it's gone
        tdSql.error(f"SHOW CREATE VTABLE {DB_NAME}.vctb_temp;")

        # Recreate with different tag refs
        tdSql.execute("CREATE VTABLE `vctb_temp` ("
                      "int_col FROM org_child_0.int_col"
                      ") USING vstb TAGS ("
                      "org_child_4.int_tag, "
                      "org_child_4.bool_tag, "
                      "org_child_4.float_tag, "
                      "org_child_4.double_tag, "
                      "org_child_4.nchar_32_tag, "
                      "org_child_4.binary_32_tag)")

        tdSql.query(f"SHOW CREATE VTABLE {DB_NAME}.vctb_temp;")
        tdSql.checkRows(1)

        # Clean up
        tdSql.execute(f"DROP VTABLE {DB_NAME}.vctb_temp;")

        tdLog.info("drop and recreate with tag refs test passed")

    # =========================================================================
    # 14. All literal tags (no tag refs) - backward compatibility
    # =========================================================================
    def test_tag_ref_all_literal(self):
        """Create: virtual child table with all literal tags (no tag refs)

        Verify backward compatibility: creating virtual child tables with
        only literal tag values (no tag references) still works correctly.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, create, tag_ref

        Jira: None

        History:
            - 2026-2-11 Created

        """
        tdLog.info("=== Test: all literal tags (backward compatibility) ===")

        tdSql.execute(f"use {DB_NAME};")

        # Create with all literal tags (no references at all)
        tdSql.execute("CREATE VTABLE `vctb_literal_0` ("
                      "int_col FROM org_child_0.int_col, "
                      "bigint_col FROM org_child_1.bigint_col"
                      ") USING vstb TAGS (42, true, 3.14, 2.718, 'hello_nchar', 'hello_bin')")

        tdSql.query(f"SHOW CREATE VTABLE {DB_NAME}.vctb_literal_0;")
        tdSql.checkRows(1)
        create_sql = tdSql.getData(0, 1)
        tdLog.info(f"SHOW CREATE VTABLE vctb_literal_0: {create_sql}")
        assert 'vctb_literal_0' in create_sql

        # Show tags and verify values
        tdSql.query(f"show tags from {DB_NAME}.vctb_literal_0;")
        tdSql.checkRows(6)

        tdLog.info("all literal tags backward compatibility test passed")
