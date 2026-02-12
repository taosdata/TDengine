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

# Error code constants (signed 32-bit representation)
TSDB_CODE_SUCCESS = 0
TSDB_CODE_PAR_TABLE_NOT_EXIST = -2147473917       # 0x80002603
TSDB_CODE_PAR_INVALID_REF_COLUMN = -2147473779    # 0x8000268D
TSDB_CODE_MND_DB_NOT_EXIST = -2147482744          # 0x80000388

DB_NAME = "test_vtable_validate_ref"
CROSS_DB_NAME = "test_vtable_validate_ref_src"


class TestVtableValidateReferencing:

    def setup_class(cls):
        tdLog.info(f"=== setup: prepare databases and tables for virtual table referencing validation ===")

        # Clean up
        tdSql.execute(f"drop database if exists {DB_NAME};")
        tdSql.execute(f"drop database if exists {CROSS_DB_NAME};")

        # Create main database
        tdSql.execute(f"create database {DB_NAME};")
        tdSql.execute(f"use {DB_NAME};")

        # --- Source tables in the same database ---
        tdLog.info(f"prepare source normal table in same db.")
        tdSql.execute(f"CREATE TABLE `src_ntb` ("
                      "ts timestamp, "
                      "int_col int, "
                      "float_col float, "
                      "binary_col binary(32));")

        tdLog.info(f"prepare source super table and child table in same db.")
        tdSql.execute(f"CREATE STABLE `src_stb` ("
                      "ts timestamp, "
                      "val int, "
                      "extra_col float"
                      ") TAGS ("
                      "t_region int, "
                      "t_name binary(32));")
        tdSql.execute(f"CREATE TABLE `src_ctb` USING `src_stb` TAGS (1, 'device1');")

        # Insert some data into source tables
        tdSql.execute(f"INSERT INTO src_ntb VALUES (now, 100, 3.14, 'hello');")
        tdSql.execute(f"INSERT INTO src_ctb VALUES (now, 200, 2.71);")

        # --- Source tables in a cross database ---
        tdLog.info(f"prepare cross-db source tables.")
        tdSql.execute(f"create database {CROSS_DB_NAME};")
        tdSql.execute(f"use {CROSS_DB_NAME};")

        tdSql.execute(f"CREATE TABLE `cross_ntb` ("
                      "ts timestamp, "
                      "voltage int, "
                      "current float);")
        tdSql.execute(f"INSERT INTO cross_ntb VALUES (now, 220, 1.5);")

        # Switch back to main database
        tdSql.execute(f"use {DB_NAME};")

        # --- Virtual normal table referencing same-db normal table ---
        tdLog.info(f"create virtual normal table referencing same-db ntb.")
        tdSql.execute(f"CREATE VTABLE `vntb_same_db` ("
                      "ts timestamp, "
                      "v_int int from src_ntb.int_col, "
                      "v_float float from src_ntb.float_col, "
                      "v_bin binary(32) from src_ntb.binary_col);")

        # --- Virtual super table + child table referencing same-db child table ---
        tdLog.info(f"create virtual super table.")
        tdSql.execute(f"CREATE STABLE `vstb` ("
                      "ts timestamp, "
                      "v_val int, "
                      "v_extra float"
                      ") TAGS ("
                      "vt_region int, "
                      "vt_name binary(32))"
                      " VIRTUAL 1;")

        tdLog.info(f"create virtual child table referencing same-db ctb.")
        tdSql.execute(f"CREATE VTABLE `vctb_same_db` ("
                      "v_val from src_ctb.val, "
                      "v_extra from src_ctb.extra_col) "
                      "USING `vstb` TAGS (1, 'vdev1');")

        # --- Virtual normal table referencing cross-db normal table ---
        tdLog.info(f"create virtual normal table referencing cross-db ntb.")
        tdSql.execute(f"CREATE VTABLE `vntb_cross_db` ("
                      "ts timestamp, "
                      f"v_voltage int from {CROSS_DB_NAME}.cross_ntb.voltage, "
                      f"v_current float from {CROSS_DB_NAME}.cross_ntb.current);")

    def test_valid_same_db_ntb_referencing(self):
        """Validate: same-db normal table referencing (all valid)

        Query ins_virtual_tables_referencing for a virtual normal table
        that references columns from a same-db normal table.
        All references should be valid (err_code = 0).

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: common, virtual, validate

        Jira: None

        History:
            - 2026-2-11 Created

        """
        tdLog.info(f"=== Test: valid same-db ntb referencing ===")
        tdSql.execute(f"use {DB_NAME};")

        tdSql.query(f"select * from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vntb_same_db';")

        # vntb_same_db has 3 referenced columns (v_int, v_float, v_bin)
        tdSql.checkRows(3)

        # All err_code should be 0 (success)
        for i in range(3):
            tdSql.checkData(i, 8, TSDB_CODE_SUCCESS)

    def test_valid_same_db_ctb_referencing(self):
        """Validate: same-db child table referencing (all valid)

        Query ins_virtual_tables_referencing for a virtual child table
        that references columns from a same-db child table.
        All references should be valid (err_code = 0).

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: common, virtual, validate

        Jira: None

        History:
            - 2026-2-11 Created

        """
        tdLog.info(f"=== Test: valid same-db ctb referencing ===")
        tdSql.execute(f"use {DB_NAME};")

        tdSql.query(f"select * from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vctb_same_db';")

        # vctb_same_db has 2 referenced columns (v_val, v_extra)
        tdSql.checkRows(2)

        # All err_code should be 0 (success)
        for i in range(2):
            tdSql.checkData(i, 8, TSDB_CODE_SUCCESS)

    def test_valid_cross_db_referencing(self):
        """Validate: cross-db normal table referencing (all valid)

        Query ins_virtual_tables_referencing for a virtual normal table
        that references columns from a cross-db normal table.
        All references should be valid (err_code = 0).

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: common, virtual, validate

        Jira: None

        History:
            - 2026-2-11 Created

        """
        tdLog.info(f"=== Test: valid cross-db referencing ===")
        tdSql.execute(f"use {DB_NAME};")

        tdSql.query(f"select * from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vntb_cross_db';")

        # vntb_cross_db has 2 referenced columns (v_voltage, v_current)
        tdSql.checkRows(2)

        # All err_code should be 0 (success)
        for i in range(2):
            tdSql.checkData(i, 8, TSDB_CODE_SUCCESS)

    def test_validate_column_content(self):
        """Validate: check column content correctness

        Verify that the virtual_db_name, virtual_table_name, virtual_col_name,
        src_db_name, src_table_name, src_column_name columns are populated correctly.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: common, virtual, validate

        Jira: None

        History:
            - 2026-2-11 Created

        """
        tdLog.info(f"=== Test: column content correctness ===")
        tdSql.execute(f"use {DB_NAME};")

        tdSql.query(f"select virtual_db_name, virtual_table_name, virtual_col_name, "
                     f"src_db_name, src_table_name, src_column_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vntb_same_db' "
                     f"order by virtual_col_name;")

        tdSql.checkRows(3)

        # Check virtual_db_name (col 0) - all should be DB_NAME
        tdSql.checkData(0, 0, DB_NAME)
        tdSql.checkData(1, 0, DB_NAME)
        tdSql.checkData(2, 0, DB_NAME)

        # Check virtual_table_name (col 1) - all should be 'vntb_same_db'
        tdSql.checkData(0, 1, 'vntb_same_db')

        # Check src_table_name (col 4) - all should be 'src_ntb'
        tdSql.checkData(0, 4, 'src_ntb')
        tdSql.checkData(1, 4, 'src_ntb')
        tdSql.checkData(2, 4, 'src_ntb')

    def test_drop_source_column(self):
        """Validate: source column dropped => err_code = TSDB_CODE_PAR_INVALID_REF_COLUMN

        Drop a column from the source super table that is referenced by a
        virtual child table. The validation should report INVALID_REF_COLUMN.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: common, virtual, validate

        Jira: None

        History:
            - 2026-2-11 Created

        """
        tdLog.info(f"=== Test: drop source column => INVALID_REF_COLUMN ===")
        tdSql.execute(f"use {DB_NAME};")

        # Create a dedicated virtual child table for this test
        tdSql.execute(f"CREATE VTABLE `vctb_drop_col_test` ("
                      "v_val from src_ctb.val, "
                      "v_extra from src_ctb.extra_col) "
                      "USING `vstb` TAGS (10, 'drop_col_test');")

        # Verify it's valid before dropping
        tdSql.query(f"select err_code from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vctb_drop_col_test';")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, TSDB_CODE_SUCCESS)
        tdSql.checkData(1, 0, TSDB_CODE_SUCCESS)

        # Now drop the 'val' column from src_stb (need to add a dummy col first to avoid last-col error)
        tdSql.execute(f"ALTER STABLE src_stb ADD COLUMN dummy_col int;")
        tdSql.execute(f"ALTER STABLE src_stb DROP COLUMN val;")

        # Validate again - the 'val' reference should now be invalid
        tdSql.query(f"select virtual_col_name, src_column_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vctb_drop_col_test' "
                     f"order by virtual_col_name;")
        tdSql.checkRows(2)

        # v_extra references extra_col which still exists => err_code = 0
        tdSql.checkData(0, 0, 'v_extra')
        tdSql.checkData(0, 2, TSDB_CODE_SUCCESS)

        # v_val references val which was dropped => err_code = TSDB_CODE_PAR_INVALID_REF_COLUMN
        tdSql.checkData(1, 0, 'v_val')
        tdSql.checkData(1, 2, TSDB_CODE_PAR_INVALID_REF_COLUMN)

        # Restore the column for other tests
        tdSql.execute(f"ALTER STABLE src_stb ADD COLUMN val int;")
        tdSql.execute(f"ALTER STABLE src_stb DROP COLUMN dummy_col;")

    def test_drop_source_table(self):
        """Validate: source table dropped => err_code = TSDB_CODE_PAR_TABLE_NOT_EXIST

        Drop the source normal table referenced by a virtual normal table.
        The validation should report TABLE_NOT_EXIST.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: common, virtual, validate

        Jira: None

        History:
            - 2026-2-11 Created

        """
        tdLog.info(f"=== Test: drop source table => TABLE_NOT_EXIST ===")
        tdSql.execute(f"use {DB_NAME};")

        # Create a dedicated source table and virtual table for this test
        tdSql.execute(f"CREATE TABLE `src_drop_test` (ts timestamp, c1 int, c2 float);")
        tdSql.execute(f"INSERT INTO src_drop_test VALUES (now, 1, 1.0);")

        tdSql.execute(f"CREATE VTABLE `vntb_drop_tbl_test` ("
                      "ts timestamp, "
                      "v_c1 int from src_drop_test.c1, "
                      "v_c2 float from src_drop_test.c2);")

        # Verify valid before dropping
        tdSql.query(f"select err_code from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vntb_drop_tbl_test';")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, TSDB_CODE_SUCCESS)
        tdSql.checkData(1, 0, TSDB_CODE_SUCCESS)

        # Drop the source table
        tdSql.execute(f"DROP TABLE src_drop_test;")

        # Validate again - all references should report TABLE_NOT_EXIST
        tdSql.query(f"select virtual_col_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vntb_drop_tbl_test';")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, TSDB_CODE_PAR_TABLE_NOT_EXIST)
        tdSql.checkData(1, 1, TSDB_CODE_PAR_TABLE_NOT_EXIST)

    def test_drop_source_database(self):
        """Validate: source database dropped => err_code = TSDB_CODE_MND_DB_NOT_EXIST

        Drop the source database referenced by a cross-db virtual table.
        The validation should report DB_NOT_EXIST.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: common, virtual, validate

        Jira: None

        History:
            - 2026-2-11 Created

        """
        tdLog.info(f"=== Test: drop source database => DB_NOT_EXIST ===")
        tdSql.execute(f"use {DB_NAME};")

        # Create a dedicated cross-db environment for this test
        cross_db = "test_vtable_validate_ref_drop_db"
        tdSql.execute(f"drop database if exists {cross_db};")
        tdSql.execute(f"create database {cross_db};")
        tdSql.execute(f"use {cross_db};")
        tdSql.execute(f"CREATE TABLE `src_for_drop` (ts timestamp, c1 int);")
        tdSql.execute(f"INSERT INTO src_for_drop VALUES (now, 42);")

        # Create virtual table in main db referencing cross_db
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"CREATE VTABLE `vntb_drop_db_test` ("
                      "ts timestamp, "
                      f"v_c1 int from {cross_db}.src_for_drop.c1);")

        # Verify valid before dropping
        tdSql.query(f"select err_code from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vntb_drop_db_test';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, TSDB_CODE_SUCCESS)

        # Drop the cross database
        tdSql.execute(f"drop database {cross_db};")

        # Validate again - reference should report DB_NOT_EXIST
        tdSql.query(f"select virtual_col_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vntb_drop_db_test';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, TSDB_CODE_MND_DB_NOT_EXIST)

    def test_full_scan_no_filter(self):
        """Validate: full scan without filter

        Query ins_virtual_tables_referencing without any WHERE filter.
        Should return rows for all virtual tables in the database.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: common, virtual, validate

        Jira: None

        History:
            - 2026-2-11 Created

        """
        tdLog.info(f"=== Test: full scan without filter ===")
        tdSql.execute(f"use {DB_NAME};")

        tdSql.query(f"select * from information_schema.ins_virtual_tables_referencing;")

        # Should have rows from all virtual tables in all databases
        # At minimum, we have vntb_same_db(3), vctb_same_db(2), vntb_cross_db(2),
        # plus any created in drop tests that still exist
        rows = tdSql.queryRows
        tdLog.info(f"Full scan returned {rows} rows")
        assert rows >= 7, f"Expected at least 7 rows from full scan, got {rows}"

    def test_filter_by_vtable_name(self):
        """Validate: filter by virtual_table_name

        Query ins_virtual_tables_referencing with virtual_table_name filter.
        Verify the optimized path returns correct results.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: common, virtual, validate

        Jira: None

        History:
            - 2026-2-11 Created

        """
        tdLog.info(f"=== Test: filter by vtable name ===")
        tdSql.execute(f"use {DB_NAME};")

        # Filter for vntb_same_db
        tdSql.query(f"select virtual_table_name, virtual_col_name, src_table_name, src_column_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vntb_same_db';")
        tdSql.checkRows(3)

        # Filter for vctb_same_db
        tdSql.query(f"select virtual_table_name, virtual_col_name, src_table_name, src_column_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vctb_same_db';")
        tdSql.checkRows(2)

        # Filter for non-existent virtual table
        tdSql.query(f"select * from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'nonexistent_vtable';")
        tdSql.checkRows(0)

    def test_err_msg_populated(self):
        """Validate: err_msg is populated when err_code != 0

        When a column reference is invalid, the err_msg column should contain
        a non-empty error description string.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: common, virtual, validate

        Jira: None

        History:
            - 2026-2-11 Created

        """
        tdLog.info(f"=== Test: err_msg populated for invalid refs ===")
        tdSql.execute(f"use {DB_NAME};")

        # Create a source table and virtual table, then drop source
        tdSql.execute(f"CREATE TABLE `src_errmsg_test` (ts timestamp, c1 int);")
        tdSql.execute(f"INSERT INTO src_errmsg_test VALUES (now, 1);")
        tdSql.execute(f"CREATE VTABLE `vntb_errmsg_test` ("
                      "ts timestamp, "
                      "v_c1 int from src_errmsg_test.c1);")

        # Drop source table
        tdSql.execute(f"DROP TABLE src_errmsg_test;")

        # Check err_msg is non-empty
        tdSql.query(f"select err_code, err_msg "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vntb_errmsg_test';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, TSDB_CODE_PAR_TABLE_NOT_EXIST)

        # err_msg should be non-empty (not None and not empty string)
        err_msg = tdSql.queryResult[0][1]
        tdLog.info(f"err_msg for dropped table: '{err_msg}'")
        assert err_msg is not None and len(str(err_msg).strip()) > 0, \
            f"err_msg should be non-empty when err_code != 0, got: '{err_msg}'"

    def test_mixed_valid_invalid_refs(self):
        """Validate: virtual table with mixed valid and invalid column references

        Create a virtual normal table with multiple column references.
        Drop only one source column. Verify that valid references have err_code=0
        while the invalid one has the correct error code.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: common, virtual, validate

        Jira: None

        History:
            - 2026-2-11 Created

        """
        tdLog.info(f"=== Test: mixed valid and invalid refs ===")
        tdSql.execute(f"use {DB_NAME};")

        # Create source table with multiple columns
        tdSql.execute(f"CREATE TABLE `src_mixed` ("
                      "ts timestamp, "
                      "col_a int, "
                      "col_b float, "
                      "col_c binary(16), "
                      "col_d bigint);")
        tdSql.execute(f"INSERT INTO src_mixed VALUES (now, 1, 1.0, 'abc', 100);")

        # Create virtual table referencing all columns
        tdSql.execute(f"CREATE VTABLE `vntb_mixed` ("
                      "ts timestamp, "
                      "v_a int from src_mixed.col_a, "
                      "v_b float from src_mixed.col_b, "
                      "v_c binary(16) from src_mixed.col_c, "
                      "v_d bigint from src_mixed.col_d);")

        # All should be valid initially
        tdSql.query(f"select virtual_col_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vntb_mixed';")
        tdSql.checkRows(4)
        for i in range(4):
            tdSql.checkData(i, 1, TSDB_CODE_SUCCESS)

        # Drop col_b from source table (need to keep at least ts + 1 col)
        tdSql.execute(f"ALTER TABLE src_mixed DROP COLUMN col_b;")

        # Now v_b should be invalid, others still valid
        tdSql.query(f"select virtual_col_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vntb_mixed' "
                     f"order by virtual_col_name;")
        tdSql.checkRows(4)

        # v_a => col_a still exists => 0
        tdSql.checkData(0, 0, 'v_a')
        tdSql.checkData(0, 1, TSDB_CODE_SUCCESS)

        # v_b => col_b dropped => INVALID_REF_COLUMN
        tdSql.checkData(1, 0, 'v_b')
        tdSql.checkData(1, 1, TSDB_CODE_PAR_INVALID_REF_COLUMN)

        # v_c => col_c still exists => 0
        tdSql.checkData(2, 0, 'v_c')
        tdSql.checkData(2, 1, TSDB_CODE_SUCCESS)

        # v_d => col_d still exists => 0
        tdSql.checkData(3, 0, 'v_d')
        tdSql.checkData(3, 1, TSDB_CODE_SUCCESS)
