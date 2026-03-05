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

        # --- Multiple source tables for complex reference tests ---
        tdLog.info(f"prepare multiple source tables for multi-source tests.")
        tdSql.execute(f"CREATE TABLE `src_multi_1` (ts timestamp, c1 int);")
        tdSql.execute(f"INSERT INTO src_multi_1 VALUES (now, 10);")
        
        tdSql.execute(f"CREATE TABLE `src_multi_2` (ts timestamp, c2 float);")
        tdSql.execute(f"INSERT INTO src_multi_2 VALUES (now, 2.5);")
        
        tdSql.execute(f"CREATE TABLE `src_multi_3` (ts timestamp, c3 binary(16));")
        tdSql.execute(f"INSERT INTO src_multi_3 VALUES (now, 'test');")

        # --- Source table with many columns ---
        tdLog.info(f"prepare source table with many columns.")
        tdSql.execute(f"CREATE TABLE `src_many_cols` ("
                      "ts timestamp, "
                      "col1 int, "
                      "col2 float, "
                      "col3 bigint, "
                      "col4 binary(16), "
                      "col5 double, "
                      "col6 smallint);")
        tdSql.execute(f"INSERT INTO src_many_cols VALUES (now, 1, 1.1, 100, 'abc', 3.14, 10);")

        # --- Source table with complex column types ---
        tdLog.info(f"prepare source table with complex column types.")
        tdSql.execute(f"CREATE TABLE `src_complex_types` ("
                      "ts timestamp, "
                      "geo_col geometry(32), "
                      "varbin_col varbinary(64), "
                      "nchar_col nchar(64));")
        tdSql.execute(f"INSERT INTO src_complex_types VALUES (now, NULL, NULL, 'nchar_test');")

        # --- Multiple super tables for combination tests ---
        tdLog.info(f"prepare multiple source super tables for combination tests.")
        tdSql.execute(f"CREATE STABLE `src_stb_A` ("
                      "ts timestamp, "
                      "val_a int"
                      ") TAGS ("
                      "tag_a int);")
        tdSql.execute(f"CREATE TABLE `src_ctb_A1` USING `src_stb_A` TAGS (1);")
        tdSql.execute(f"CREATE TABLE `src_ctb_A2` USING `src_stb_A` TAGS (2);")
        tdSql.execute(f"INSERT INTO src_ctb_A1 VALUES (now, 100);")
        
        tdSql.execute(f"CREATE STABLE `src_stb_B` ("
                      "ts timestamp, "
                      "val_b float"
                      ") TAGS ("
                      "tag_b int);")
        tdSql.execute(f"CREATE TABLE `src_ctb_B1` USING `src_stb_B` TAGS (10);")
        tdSql.execute(f"INSERT INTO src_ctb_B1 VALUES (now, 1.5);")

        # --- Additional child tables for multi-child tests ---
        tdLog.info(f"prepare additional child tables for multi-child tests.")
        tdSql.execute(f"CREATE TABLE `src_ctb_multi_1` USING `src_stb` TAGS (100, 'multi1');")
        tdSql.execute(f"CREATE TABLE `src_ctb_multi_2` USING `src_stb` TAGS (101, 'multi2');")
        tdSql.execute(f"CREATE TABLE `src_ctb_multi_3` USING `src_stb` TAGS (102, 'multi3');")
        tdSql.execute(f"INSERT INTO src_ctb_multi_1 VALUES (now, 1000, 10.0);")
        tdSql.execute(f"INSERT INTO src_ctb_multi_2 VALUES (now, 2000, 20.0);")
        tdSql.execute(f"INSERT INTO src_ctb_multi_3 VALUES (now, 3000, 30.0);")

        # --- Mixed type source table ---
        tdLog.info(f"prepare mixed type source table.")
        tdSql.execute(f"CREATE TABLE `src_ntb_mixed` (ts timestamp, mixed_col int);")
        tdSql.execute(f"INSERT INTO src_ntb_mixed VALUES (now, 42);")

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

    def test_multiple_source_tables(self):
        """Validate: virtual table referencing multiple source tables

        Create a virtual normal table that references columns from 3 different
        source tables. Verify all references are valid, then drop source tables
        one by one and verify corresponding references become invalid.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, complex

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: multiple source tables ===")
        tdSql.execute(f"use {DB_NAME};")

        # Create virtual table referencing 3 different source tables
        tdSql.execute(f"CREATE VTABLE `vntb_multi_src` ("
                      "ts timestamp, "
                      "v_c1 int from src_multi_1.c1, "
                      "v_c2 float from src_multi_2.c2, "
                      "v_c3 binary(16) from src_multi_3.c3);")

        # Verify all references are valid initially
        tdSql.query(f"select virtual_col_name, src_table_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vntb_multi_src' "
                     f"order by virtual_col_name;")
        tdSql.checkRows(3)

        # All should be valid (err_code = 0)
        tdSql.checkData(0, 2, TSDB_CODE_SUCCESS)  # v_c1
        tdSql.checkData(1, 2, TSDB_CODE_SUCCESS)  # v_c2
        tdSql.checkData(2, 2, TSDB_CODE_SUCCESS)  # v_c3

        # Drop first source table
        tdSql.execute(f"DROP TABLE src_multi_1;")

        # Verify v_c1 now has TABLE_NOT_EXIST error
        tdSql.query(f"select virtual_col_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vntb_multi_src' "
                     f"order by virtual_col_name;")
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, 'v_c1')
        tdSql.checkData(0, 1, TSDB_CODE_PAR_TABLE_NOT_EXIST)

        # v_c2 and v_c3 should still be valid
        tdSql.checkData(1, 1, TSDB_CODE_SUCCESS)
        tdSql.checkData(2, 1, TSDB_CODE_SUCCESS)

        # Drop second source table
        tdSql.execute(f"DROP TABLE src_multi_2;")

        # Verify v_c2 also has TABLE_NOT_EXIST error
        tdSql.query(f"select virtual_col_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vntb_multi_src' "
                     f"order by virtual_col_name;")
        tdSql.checkRows(3)

        tdSql.checkData(1, 0, 'v_c2')
        tdSql.checkData(1, 1, TSDB_CODE_PAR_TABLE_NOT_EXIST)

        # v_c3 should still be valid
        tdSql.checkData(2, 1, TSDB_CODE_SUCCESS)

        # Restore tables for other tests
        tdSql.execute(f"CREATE TABLE `src_multi_1` (ts timestamp, c1 int);")
        tdSql.execute(f"INSERT INTO src_multi_1 VALUES (now, 10);")
        tdSql.execute(f"CREATE TABLE `src_multi_2` (ts timestamp, c2 float);")
        tdSql.execute(f"INSERT INTO src_multi_2 VALUES (now, 2.5);")

    def test_virtual_stb_multiple_children(self):
        """Validate: virtual super table with multiple virtual child tables

        Create multiple virtual child tables under the same virtual super table,
        each referencing different source child tables. Verify all references
        are correct and independent.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, complex

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: virtual stb with multiple child tables ===")
        tdSql.execute(f"use {DB_NAME};")

        # Create 3 virtual child tables, each referencing different source child table
        tdSql.execute(f"CREATE VTABLE `vctb_multi_1` ("
                      "v_val from src_ctb_multi_1.val, "
                      "v_extra from src_ctb_multi_1.extra_col) "
                      "USING `vstb` TAGS (100, 'vmulti1');")

        tdSql.execute(f"CREATE VTABLE `vctb_multi_2` ("
                      "v_val from src_ctb_multi_2.val, "
                      "v_extra from src_ctb_multi_2.extra_col) "
                      "USING `vstb` TAGS (101, 'vmulti2');")

        tdSql.execute(f"CREATE VTABLE `vctb_multi_3` ("
                      "v_val from src_ctb_multi_3.val, "
                      "v_extra from src_ctb_multi_3.extra_col) "
                      "USING `vstb` TAGS (102, 'vmulti3');")

        # Verify all 3 virtual child tables have valid references
        for i in range(1, 4):
            vtable_name = f'vctb_multi_{i}'
            src_table_name = f'src_ctb_multi_{i}'
            
            tdSql.query(f"select virtual_col_name, src_table_name, err_code "
                         f"from information_schema.ins_virtual_tables_referencing "
                         f"where virtual_table_name = '{vtable_name}';")
            tdSql.checkRows(2)
            
            # Both columns should reference the correct source table
            for j in range(2):
                tdSql.checkData(j, 1, src_table_name)
                tdSql.checkData(j, 2, TSDB_CODE_SUCCESS)

        # Drop src_ctb_multi_1
        tdSql.execute(f"DROP TABLE src_ctb_multi_1;")

        # Verify vctb_multi_1 now has TABLE_NOT_EXIST error
        tdSql.query(f"select virtual_col_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vctb_multi_1';")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, TSDB_CODE_PAR_TABLE_NOT_EXIST)
        tdSql.checkData(1, 1, TSDB_CODE_PAR_TABLE_NOT_EXIST)

        # vctb_multi_2 and vctb_multi_3 should still be valid
        for vtable_name in ['vctb_multi_2', 'vctb_multi_3']:
            tdSql.query(f"select err_code "
                         f"from information_schema.ins_virtual_tables_referencing "
                         f"where virtual_table_name = '{vtable_name}';")
            tdSql.checkRows(2)
            tdSql.checkData(0, 0, TSDB_CODE_SUCCESS)
            tdSql.checkData(1, 0, TSDB_CODE_SUCCESS)

        # Restore table for other tests
        tdSql.execute(f"CREATE TABLE `src_ctb_multi_1` USING `src_stb` TAGS (100, 'multi1');")
        tdSql.execute(f"INSERT INTO src_ctb_multi_1 VALUES (now, 1000, 10.0);")

    def test_multi_column_ref_same_table(self):
        """Validate: virtual table referencing multiple columns from same source

        Create a virtual table that references 6 columns from same source table.
        Verify all references are valid, then drop columns one by one and verify
        corresponding references become invalid.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, complex

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: multi-column reference from same table ===")
        tdSql.execute(f"use {DB_NAME};")

        # Create virtual table referencing all 6 columns from src_many_cols
        tdSql.execute(f"CREATE VTABLE `vntb_many_cols` ("
                      "ts timestamp, "
                      "v_col1 int from src_many_cols.col1, "
                      "v_col2 float from src_many_cols.col2, "
                      "v_col3 bigint from src_many_cols.col3, "
                      "v_col4 binary(16) from src_many_cols.col4, "
                      "v_col5 double from src_many_cols.col5, "
                      "v_col6 smallint from src_many_cols.col6);")

        # Verify all 6 references are valid
        tdSql.query(f"select virtual_col_name, src_column_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vntb_many_cols' "
                     f"order by virtual_col_name;")
        tdSql.checkRows(6)
        
        # All should be valid
        for i in range(6):
            tdSql.checkData(i, 2, TSDB_CODE_SUCCESS)

        # Drop col3 from source table (need to add dummy col first to avoid last-col error)
        tdSql.execute(f"ALTER TABLE src_many_cols ADD COLUMN dummy_col int;")
        tdSql.execute(f"ALTER TABLE src_many_cols DROP COLUMN col3;")

        # Verify v_col3 now has INVALID_REF_COLUMN error
        tdSql.query(f"select virtual_col_name, src_column_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vntb_many_cols' "
                     f"order by virtual_col_name;")
        tdSql.checkRows(6)
        
        # Find v_col3 and verify it has error
        found_v_col3 = False
        for i in range(6):
            if tdSql.queryResult[i][0] == 'v_col3':
                tdSql.checkData(i, 2, TSDB_CODE_PAR_INVALID_REF_COLUMN)
                found_v_col3 = True
            else:
                # Other columns should still be valid
                tdSql.checkData(i, 2, TSDB_CODE_SUCCESS)
        
        assert found_v_col3, "v_col3 should exist in results"

        # Drop col5
        tdSql.execute(f"ALTER TABLE src_many_cols DROP COLUMN col5;")

        # Verify v_col5 also has INVALID_REF_COLUMN error
        tdSql.query(f"select virtual_col_name, src_column_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vntb_many_cols' "
                     f"order by virtual_col_name;")
        
        # Find v_col5 and verify it has error
        found_v_col5 = False
        for i in range(6):
            col_name = tdSql.queryResult[i][0]
            if col_name == 'v_col3' or col_name == 'v_col5':
                tdSql.checkData(i, 2, TSDB_CODE_PAR_INVALID_REF_COLUMN)
                if col_name == 'v_col5':
                    found_v_col5 = True
            else:
                # Other columns should still be valid
                tdSql.checkData(i, 2, TSDB_CODE_SUCCESS)
        
        assert found_v_col5, "v_col5 should exist in results"

        # Restore columns for other tests
        tdSql.execute(f"ALTER TABLE src_many_cols ADD COLUMN col3 bigint;")
        tdSql.execute(f"ALTER TABLE src_many_cols ADD COLUMN col5 double;")
        tdSql.execute(f"ALTER TABLE src_many_cols DROP COLUMN dummy_col;")

    def test_cross_db_mixed_sources(self):
        """Validate: virtual table with mixed same-db and cross-db sources

        Create a virtual table that references both same-database tables
        and cross-database tables. Verify different error codes when
        dropping same-db tables vs cross-db database.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, cross-db

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: mixed same-db and cross-db sources ===")
        tdSql.execute(f"use {DB_NAME};")

        # Create same-db source table
        tdSql.execute(f"CREATE TABLE `src_ntb_same` (ts timestamp, same_col int);")
        tdSql.execute(f"INSERT INTO src_ntb_same VALUES (now, 99);")

        # Create virtual table referencing same-db + cross-db tables
        tdSql.execute(f"CREATE VTABLE `vntb_mixed_src` ("
                      "ts timestamp, "
                      "v_same int from src_ntb_same.same_col, "
                      f"v_cross int from {CROSS_DB_NAME}.cross_ntb.voltage, "
                      f"v_cross2 float from {CROSS_DB_NAME}.cross_ntb.current);")

        # Verify all references are valid
        tdSql.query(f"select virtual_col_name, src_db_name, src_table_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vntb_mixed_src' "
                     f"order by virtual_col_name;")
        tdSql.checkRows(3)
        
        # All should be valid
        for i in range(3):
            tdSql.checkData(i, 3, TSDB_CODE_SUCCESS)

        # Drop same-db source table
        tdSql.execute(f"DROP TABLE src_ntb_same;")

        # Verify v_same has TABLE_NOT_EXIST error
        tdSql.query(f"select virtual_col_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vntb_mixed_src' "
                     f"order by virtual_col_name;")
        
        tdSql.checkData(0, 0, 'v_same')
        tdSql.checkData(0, 1, TSDB_CODE_PAR_TABLE_NOT_EXIST)
        
        # Cross-db references should still be valid
        tdSql.checkData(1, 1, TSDB_CODE_SUCCESS)
        tdSql.checkData(2, 1, TSDB_CODE_SUCCESS)

        # Restore same-db table
        tdSql.execute(f"CREATE TABLE `src_ntb_same` (ts timestamp, same_col int);")
        tdSql.execute(f"INSERT INTO src_ntb_same VALUES (now, 99);")

        # Drop cross-db database
        tdSql.execute(f"drop database {CROSS_DB_NAME};")

        # Verify cross-db references now have DB_NOT_EXIST error
        tdSql.query(f"select virtual_col_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vntb_mixed_src' "
                     f"order by virtual_col_name;")
        
        # v_same should be valid again (table restored)
        tdSql.checkData(0, 0, 'v_same')
        tdSql.checkData(0, 1, TSDB_CODE_SUCCESS)
        
        # Cross-db references should have DB_NOT_EXIST
        tdSql.checkData(1, 1, TSDB_CODE_MND_DB_NOT_EXIST)
        tdSql.checkData(2, 1, TSDB_CODE_MND_DB_NOT_EXIST)

        # Restore cross-db for other tests
        tdSql.execute(f"create database {CROSS_DB_NAME};")
        tdSql.execute(f"use {CROSS_DB_NAME};")
        tdSql.execute(f"CREATE TABLE `cross_ntb` ("
                      "ts timestamp, "
                      "voltage int, "
                      "current float);")
        tdSql.execute(f"INSERT INTO cross_ntb VALUES (now, 220, 1.5);")
        tdSql.execute(f"use {DB_NAME};")

    def test_cross_db_multiple_dbs(self):
        """Validate: virtual table referencing multiple cross-databases

        Create a virtual table that references tables from 2 different
        cross-databases. Drop one cross-database and verify partial
        references become invalid.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, cross-db

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: multiple cross-databases ===")
        tdSql.execute(f"use {DB_NAME};")

        # Create 2 cross-databases with source tables
        cross_db_1 = "test_vtable_validate_cross_1"
        cross_db_2 = "test_vtable_validate_cross_2"
        
        tdSql.execute(f"drop database if exists {cross_db_1};")
        tdSql.execute(f"drop database if exists {cross_db_2};")
        tdSql.execute(f"create database {cross_db_1};")
        tdSql.execute(f"create database {cross_db_2};")
        
        tdSql.execute(f"use {cross_db_1};")
        tdSql.execute(f"CREATE TABLE `src_in_cross1` (ts timestamp, val1 int);")
        tdSql.execute(f"INSERT INTO src_in_cross1 VALUES (now, 111);")
        
        tdSql.execute(f"use {cross_db_2};")
        tdSql.execute(f"CREATE TABLE `src_in_cross2` (ts timestamp, val2 float);")
        tdSql.execute(f"INSERT INTO src_in_cross2 VALUES (now, 2.22);")

        # Create virtual table in main db referencing both cross-databases
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"CREATE VTABLE `vntb_multi_cross` ("
                      "ts timestamp, "
                      f"v_from_db1 int from {cross_db_1}.src_in_cross1.val1, "
                      f"v_from_db2 float from {cross_db_2}.src_in_cross2.val2);")

        # Verify all references are valid
        tdSql.query(f"select virtual_col_name, src_db_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vntb_multi_cross' "
                     f"order by virtual_col_name;")
        tdSql.checkRows(2)
        
        tdSql.checkData(0, 1, cross_db_1)
        tdSql.checkData(0, 2, TSDB_CODE_SUCCESS)
        tdSql.checkData(1, 1, cross_db_2)
        tdSql.checkData(1, 2, TSDB_CODE_SUCCESS)

        # Drop cross_db_1
        tdSql.execute(f"drop database {cross_db_1};")

        # Verify v_from_db1 has DB_NOT_EXIST, v_from_db2 still valid
        tdSql.query(f"select virtual_col_name, src_db_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vntb_multi_cross' "
                     f"order by virtual_col_name;")
        tdSql.checkRows(2)
        
        tdSql.checkData(0, 0, 'v_from_db1')
        tdSql.checkData(0, 1, cross_db_1)
        tdSql.checkData(0, 2, TSDB_CODE_MND_DB_NOT_EXIST)
        
        tdSql.checkData(1, 0, 'v_from_db2')
        tdSql.checkData(1, 1, cross_db_2)
        tdSql.checkData(1, 2, TSDB_CODE_SUCCESS)

        # Cleanup
        tdSql.execute(f"drop database if exists {cross_db_2};")

    def test_cross_db_stb_child_tables(self):
        """Validate: virtual child table referencing cross-db source child table

        Create a virtual child table that references a source child table
        from a cross-database super table. Verify the reference is valid
        and metadata is correct.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, cross-db

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: cross-db child table reference ===")
        
        # Create cross-database with super table and child table
        cross_stb_db = "test_vtable_validate_cross_stb"
        tdSql.execute(f"drop database if exists {cross_stb_db};")
        tdSql.execute(f"create database {cross_stb_db};")
        tdSql.execute(f"use {cross_stb_db};")
        
        tdSql.execute(f"CREATE STABLE `cross_src_stb` ("
                      "ts timestamp, "
                      "cross_val int, "
                      "cross_extra float"
                      ") TAGS ("
                      "cross_tag int);")
        tdSql.execute(f"CREATE TABLE `cross_src_ctb` USING `cross_src_stb` TAGS (999);")
        tdSql.execute(f"INSERT INTO cross_src_ctb VALUES (now, 888, 8.8);")

        # Switch back to main database
        tdSql.execute(f"use {DB_NAME};")

        # Create virtual child table referencing cross-db child table
        tdSql.execute(f"CREATE VTABLE `vctb_cross` ("
                      f"v_val from {cross_stb_db}.cross_src_ctb.cross_val, "
                      f"v_extra from {cross_stb_db}.cross_src_ctb.cross_extra) "
                      "USING `vstb` TAGS (999, 'vcross');")

        # Verify reference is valid and metadata is correct
        tdSql.query(f"select virtual_db_name, virtual_table_name, virtual_col_name, "
                     f"src_db_name, src_table_name, src_column_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vctb_cross';")
        tdSql.checkRows(2)
        
        # Check metadata for both columns
        for i in range(2):
            tdSql.checkData(i, 0, DB_NAME)  # virtual_db_name
            tdSql.checkData(i, 1, 'vctb_cross')  # virtual_table_name
            tdSql.checkData(i, 3, cross_stb_db)  # src_db_name
            tdSql.checkData(i, 4, 'cross_src_ctb')  # src_table_name
            tdSql.checkData(i, 6, TSDB_CODE_SUCCESS)  # err_code
        
        # Check column names
        tdSql.checkData(0, 2, 'v_val')  # virtual_col_name
        tdSql.checkData(0, 5, 'cross_val')  # src_column_name
        tdSql.checkData(1, 2, 'v_extra')
        tdSql.checkData(1, 5, 'cross_extra')

        # Cleanup
        tdSql.execute(f"drop database {cross_stb_db};")

    def test_virtual_ntb_mixed_stb_ntb_sources(self):
        """Validate: virtual normal table mixing normal table and child table sources

        Create a virtual normal table that references columns from both
        a normal table and a super table's child table. Verify all
        references are valid and source table types are correct.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, combination

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: virtual ntb with mixed stb/ntb sources ===")
        tdSql.execute(f"use {DB_NAME};")

        # Create virtual normal table referencing normal table + child table
        tdSql.execute(f"CREATE VTABLE `vntb_mixed_types` ("
                      "ts timestamp, "
                      "v_from_ntb int from src_ntb_mixed.mixed_col, "
                      "v_from_ctb int from src_ctb.val);")

        # Verify all references are valid
        tdSql.query(f"select virtual_col_name, src_table_name, src_column_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vntb_mixed_types' "
                     f"order by virtual_col_name;")
        tdSql.checkRows(2)
        
        # Both should be valid
        tdSql.checkData(0, 3, TSDB_CODE_SUCCESS)
        tdSql.checkData(1, 3, TSDB_CODE_SUCCESS)
        
        # Check source table names
        tdSql.checkData(0, 1, 'src_ntb_mixed')
        tdSql.checkData(1, 1, 'src_ctb')
        
        # Check column names
        tdSql.checkData(0, 2, 'mixed_col')
        tdSql.checkData(1, 2, 'val')

    def test_virtual_stb_different_source_stbs(self):
        """Validate: virtual super table with children from different source super tables

        Create a virtual super table with multiple virtual child tables,
        each referencing child tables from different source super tables.
        Verify references are independent and correct.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, combination

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: virtual stb with children from different source stbs ===")
        tdSql.execute(f"use {DB_NAME};")

        # Create a new virtual super table for this test
        tdSql.execute(f"CREATE STABLE `vstb_mixed` ("
                      "ts timestamp, "
                      "v_val_a int, "
                      "v_val_b float"
                      ") TAGS ("
                      "vt_tag int)"
                      " VIRTUAL 1;")

        # Create virtual child table 1 referencing src_ctb_A1 (from src_stb_A)
        tdSql.execute(f"CREATE VTABLE `vctb_from_stbA` ("
                      "v_val_a from src_ctb_A1.val_a) "
                      "USING `vstb_mixed` TAGS (1);")

        # Create virtual child table 2 referencing src_ctb_B1 (from src_stb_B)
        tdSql.execute(f"CREATE VTABLE `vctb_from_stbB` ("
                      "v_val_b from src_ctb_B1.val_b) "
                      "USING `vstb_mixed` TAGS (2);")

        # Verify virtual child table 1
        tdSql.query(f"select virtual_table_name, virtual_col_name, src_table_name, src_column_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vctb_from_stbA';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 'v_val_a')
        tdSql.checkData(0, 2, 'src_ctb_A1')
        tdSql.checkData(0, 3, 'val_a')
        tdSql.checkData(0, 4, TSDB_CODE_SUCCESS)

        # Verify virtual child table 2
        tdSql.query(f"select virtual_table_name, virtual_col_name, src_table_name, src_column_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vctb_from_stbB';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 'v_val_b')
        tdSql.checkData(0, 2, 'src_ctb_B1')
        tdSql.checkData(0, 3, 'val_b')
        tdSql.checkData(0, 4, TSDB_CODE_SUCCESS)

        # Drop src_stb_A (and its children)
        tdSql.execute(f"DROP STABLE src_stb_A;")

        # Verify vctb_from_stbA now has error
        tdSql.query(f"select virtual_col_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vctb_from_stbA';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, TSDB_CODE_PAR_TABLE_NOT_EXIST)

        # vctb_from_stbB should still be valid
        tdSql.query(f"select err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vctb_from_stbB';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, TSDB_CODE_SUCCESS)

        # Restore src_stb_A for other tests
        tdSql.execute(f"CREATE STABLE `src_stb_A` (ts timestamp, val_a int) TAGS (tag_a int);")
        tdSql.execute(f"CREATE TABLE `src_ctb_A1` USING `src_stb_A` TAGS (1);")
        tdSql.execute(f"CREATE TABLE `src_ctb_A2` USING `src_stb_A` TAGS (2);")
        tdSql.execute(f"INSERT INTO src_ctb_A1 VALUES (now, 100);")

    def test_complex_column_type_mapping(self):
        """Validate: virtual table with complex column types (GEOMETRY, VARBINARY, NCHAR)

        Create a virtual table that references columns with complex types
        including GEOMETRY, VARBINARY, and NCHAR. Verify all references
        are valid and type mappings are correct.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, types

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: complex column type mapping ===")
        tdSql.execute(f"use {DB_NAME};")

        # Create virtual table referencing complex types
        tdSql.execute(f"CREATE VTABLE `vntb_complex_types` ("
                      "ts timestamp, "
                      "v_geo geometry(32) from src_complex_types.geo_col, "
                      "v_varbin varbinary(64) from src_complex_types.varbin_col, "
                      "v_nchar nchar(64) from src_complex_types.nchar_col);")

        # Verify all references are valid
        tdSql.query(f"select virtual_col_name, src_column_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name = 'vntb_complex_types' "
                     f"order by virtual_col_name;")
        tdSql.checkRows(3)
        
        # All should be valid
        for i in range(3):
            tdSql.checkData(i, 2, TSDB_CODE_SUCCESS)
        
        # Verify column mappings
        tdSql.checkData(0, 0, 'v_geo')
        tdSql.checkData(0, 1, 'geo_col')
        
        tdSql.checkData(1, 0, 'v_nchar')
        tdSql.checkData(1, 1, 'nchar_col')
        
        tdSql.checkData(2, 0, 'v_varbin')
        tdSql.checkData(2, 1, 'varbin_col')

        # Query the virtual table to verify data can be read
        tdSql.query(f"select ts, v_nchar from vntb_complex_types;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 'nchar_test')
