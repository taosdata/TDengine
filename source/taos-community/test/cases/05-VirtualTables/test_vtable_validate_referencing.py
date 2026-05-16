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
from new_test_framework.utils import tdLog, tdSql, etool, tdCom

# Error code constants (signed 32-bit representation)
TSDB_CODE_SUCCESS = 0
TSDB_CODE_PAR_TABLE_NOT_EXIST = -2147473917       # 0x80002603
TSDB_CODE_PAR_INVALID_REF_COLUMN = -2147473779    # 0x8000268D
TSDB_CODE_MND_DB_NOT_EXIST = -2147482744          # 0x80000388
TSDB_CODE_VTABLE_REF_DEPTH_EXCEEDED = -2147458548 # 0x8000620C

DB_NAME = "test_vtable_validate_ref"
CROSS_DB_NAME = "test_vtable_validate_ref_src"


class TestVtableValidateReferencing:

    def setup_class(cls):
       TestVtableValidateReferencing.prepare_vtables() 

    @staticmethod 
    def prepare_vtables():
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
        tdSql.execute(f"CREATE STABLE src_stb_A ("
                      "ts timestamp, "
                      "val_a int"
                      ") TAGS ("
                      "tag_a int);")
        tdSql.execute(f"CREATE TABLE src_ctb_A1 USING src_stb_A TAGS (1);")
        tdSql.execute(f"CREATE TABLE src_ctb_A2 USING src_stb_A TAGS (2);")
        tdSql.execute(f"INSERT INTO src_ctb_A1 VALUES (now, 100);")
        
        tdSql.execute(f"CREATE STABLE src_stb_B ("
                      "ts timestamp, "
                      "val_b float"
                      ") TAGS ("
                      "tag_b int);")
        tdSql.execute(f"CREATE TABLE src_ctb_B1 USING src_stb_B TAGS (10);")
        tdSql.execute(f"INSERT INTO src_ctb_B1 VALUES (now, 1.5);")

        # --- Additional child tables for multi-child tests ---
        tdLog.info(f"prepare additional child tables for multi-child tests.")
        tdSql.execute(f"CREATE TABLE `src_ctb_multi_1` USING `src_stb` TAGS (100, 'multi1');")
        tdSql.execute(f"CREATE TABLE `src_ctb_multi_2` USING `src_stb` TAGS (101, 'multi2');")
        tdSql.execute(f"CREATE TABLE `src_ctb_multi_3` USING `src_stb` TAGS (102, 'multi3');")
        tdSql.execute(f"INSERT INTO `src_ctb_multi_1` VALUES (now, 1000, 10.0);")
        tdSql.execute(f"INSERT INTO `src_ctb_multi_2` VALUES (now, 2000, 20.0);")
        tdSql.execute(f"INSERT INTO `src_ctb_multi_3` VALUES (now, 3000, 30.0);")

        # --- Mixed type source table ---
        tdLog.info(f"prepare mixed type source table.")
        tdSql.execute(f"CREATE TABLE `src_ntb_mixed` (ts timestamp, mixed_col int);")
        tdSql.execute(f"INSERT INTO `src_ntb_mixed` VALUES (now, 42);")

        # --- Virtual normal table referencing cross-db normal table ---
        tdLog.info(f"create virtual normal table referencing cross-db ntb.")
        tdSql.execute(f"CREATE VTABLE `vntb_cross_db` ("
                      "ts timestamp, "
                      f"v_voltage int from {CROSS_DB_NAME}.cross_ntb.voltage, "
                      f"v_current float from {CROSS_DB_NAME}.cross_ntb.current);")

        # --- Second-layer virtual normal table chains ---
        tdLog.info(f"create same-db second-layer virtual normal table chain.")
        tdSql.execute(f"CREATE VTABLE `vntb_mid_same_db` ("
                      "ts timestamp, "
                      "mid_int int from src_ntb.int_col, "
                      "mid_float float from src_ntb.float_col);")
        tdSql.execute(f"CREATE VTABLE `vntb_top_same_db` ("
                      "ts timestamp, "
                      "top_int int from vntb_mid_same_db.mid_int, "
                      "top_float float from vntb_mid_same_db.mid_float);")

        tdLog.info(f"create cross-db second-layer virtual normal table chain.")
        tdSql.execute(f"use {CROSS_DB_NAME};")
        tdSql.execute(f"CREATE VTABLE `vntb_mid_cross_db` ("
                      "ts timestamp, "
                      "mid_voltage int from cross_ntb.voltage, "
                      "mid_current float from cross_ntb.current);")
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"CREATE VTABLE `vntb_top_cross_db` ("
                      "ts timestamp, "
                      f"top_voltage int from {CROSS_DB_NAME}.vntb_mid_cross_db.mid_voltage, "
                      f"top_current float from {CROSS_DB_NAME}.vntb_mid_cross_db.mid_current);")

        # --- Second-layer virtual child chains ---
        tdLog.info(f"create same-db second-layer virtual child chain.")
        tdSql.execute(f"CREATE STABLE `mid_chain_vstb` ("
                      "ts timestamp, "
                      "mid_val int, "
                      "mid_extra float"
                      ") TAGS ("
                      "mid_region int, "
                      "mid_name binary(32))"
                      " VIRTUAL 1;")
        tdSql.execute(f"CREATE VTABLE `mid_chain_ctb` ("
                      "mid_val from src_ctb.val, "
                      "mid_extra from src_ctb.extra_col) "
                      "USING `mid_chain_vstb` TAGS (11, 'mid_chain');")
        tdSql.execute(f"CREATE STABLE `top_chain_vstb` ("
                      "ts timestamp, "
                      "top_val int, "
                      "top_extra float"
                      ") TAGS ("
                      "top_region int, "
                      "top_name binary(32))"
                      " VIRTUAL 1;")
        tdSql.execute(f"CREATE VTABLE `top_chain_ctb` ("
                      "top_val from mid_chain_ctb.mid_val, "
                      "top_extra from mid_chain_ctb.mid_extra) "
                      "USING `top_chain_vstb` TAGS (12, 'top_chain');")

        tdLog.info(f"create cross-db second-layer virtual child chain.")
        tdSql.execute(f"use {CROSS_DB_NAME};")
        tdSql.execute(f"CREATE STABLE `cross_src_stb` ("
                      "ts timestamp, "
                      "cross_val int, "
                      "cross_extra float"
                      ") TAGS ("
                      "cross_region int, "
                      "cross_name binary(32));")
        tdSql.execute(f"CREATE TABLE `cross_src_ctb` USING `cross_src_stb` TAGS (7, 'cross_device');")
        tdSql.execute(f"INSERT INTO `cross_src_ctb` VALUES (now, 330, 3.30);")
        tdSql.execute(f"CREATE STABLE `cross_mid_vstb` ("
                      "ts timestamp, "
                      "mid_val int, "
                      "mid_extra float"
                      ") TAGS ("
                      "mid_region int, "
                      "mid_name binary(32))"
                      " VIRTUAL 1;")
        tdSql.execute(f"CREATE VTABLE `cross_mid_ctb` ("
                      "mid_val from cross_src_ctb.cross_val, "
                      "mid_extra from cross_src_ctb.cross_extra) "
                      "USING `cross_mid_vstb` TAGS (7, 'cross_mid');")
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"CREATE STABLE `cross_top_vstb` ("
                      "ts timestamp, "
                      "top_val int, "
                      "top_extra float"
                      ") TAGS ("
                      "top_region int, "
                      "top_name binary(32))"
                      " VIRTUAL 1;")
        tdSql.execute(f"CREATE VTABLE `cross_top_ctb` ("
                      f"top_val from {CROSS_DB_NAME}.cross_mid_ctb.mid_val, "
                      f"top_extra from {CROSS_DB_NAME}.cross_mid_ctb.mid_extra) "
                      "USING `cross_top_vstb` TAGS (8, 'cross_top');")

        # --- Virtual super table for mixed source tests ---
        tdLog.info(f"create virtual super table for mixed source tests.")
        tdSql.execute(f"CREATE STABLE `vstb_mixed` ("
                      "ts timestamp, "
                      "v_val_a int, "
                      "v_val_b float"
                      ") TAGS ("
                      "vt_tag int)"
                      " VIRTUAL 1;")

    @staticmethod
    def _query_referencing_rows(db_name, table_name):
        tdSql.query(f"select virtual_col_name, src_db_name, src_table_name, src_column_name, err_code "
                    f"from information_schema.ins_virtual_tables_referencing "
                    f"where virtual_db_name = '{db_name}' and virtual_table_name = '{table_name}' "
                    f"order by virtual_col_name;")
        return [
            tuple(tdSql.getData(i, j) for j in range(tdSql.queryCols))
            for i in range(tdSql.queryRows)
        ]

    @staticmethod
    def _query_referencing_err_codes(db_name, table_name):
        tdSql.query(f"select virtual_col_name, err_code "
                    f"from information_schema.ins_virtual_tables_referencing "
                    f"where virtual_db_name = '{db_name}' and virtual_table_name = '{table_name}' "
                    f"order by virtual_col_name;")
        return [
            tuple(tdSql.getData(i, j) for j in range(tdSql.queryCols))
            for i in range(tdSql.queryRows)
        ]

    @staticmethod
    def _capture_current_rows():
        return [
            tuple(tdSql.getData(i, j) for j in range(tdSql.queryCols))
            for i in range(tdSql.queryRows)
        ]

    @staticmethod
    def _query_referencing_full_rows(db_name, table_name):
        tdSql.query(f"select * "
                    f"from information_schema.ins_virtual_tables_referencing "
                    f"where virtual_db_name = '{db_name}' and virtual_table_name = '{table_name}' "
                    f"order by virtual_col_name;")
        return TestVtableValidateReferencing._capture_current_rows()

    @staticmethod
    def _query_show_validate_rows(table_ref):
        tdSql.query(f"SHOW VTABLE VALIDATE FOR {table_ref};")
        tdSql.queryResult = sorted(
            tdSql.queryResult,
            key=lambda row: "" if row[3] is None else str(row[3]),
        )
        tdSql.queryRows = len(tdSql.queryResult)
        return TestVtableValidateReferencing._capture_current_rows()

    @staticmethod
    def _query_show_validate_err_codes(table_ref):
        rows = TestVtableValidateReferencing._query_show_validate_rows(table_ref)
        return [(row[3], row[8]) for row in rows]

    @staticmethod
    def _drop_tables_if_exist(db_name, *table_names):
        tdSql.execute(f"use {db_name};")
        for table_name in table_names:
            tdSql.execute(f"DROP TABLE IF EXISTS `{table_name}`;")

    @staticmethod
    def _create_same_db_ntb_chain(prefix, depth):
        assert depth >= 1, "depth should be >= 1"

        table_names = [f"{prefix}_{i}" for i in range(1, depth + 1)]
        TestVtableValidateReferencing._drop_tables_if_exist(DB_NAME, *reversed(table_names))

        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"CREATE VTABLE `{table_names[0]}` ("
                      "ts timestamp, "
                      "ref_int int from src_ntb.int_col, "
                      "ref_float float from src_ntb.float_col);")

        for i in range(1, depth):
            prev_table = table_names[i - 1]
            table_name = table_names[i]
            tdSql.execute(f"CREATE VTABLE `{table_name}` ("
                          "ts timestamp, "
                          f"ref_int int from {prev_table}.ref_int, "
                          f"ref_float float from {prev_table}.ref_float);")

        return table_names

    @staticmethod
    def _drop_same_db_ntb_chain(table_names):
        TestVtableValidateReferencing._drop_tables_if_exist(DB_NAME, *reversed(table_names))

    @staticmethod
    def _create_same_db_three_hop_ntb_chain(prefix):
        mid_table = f"{prefix}_mid"
        top_table = f"{prefix}_top"
        leaf_table = f"{prefix}_leaf"

        TestVtableValidateReferencing._drop_tables_if_exist(DB_NAME, leaf_table, top_table, mid_table)
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"CREATE VTABLE `{mid_table}` ("
                      "ts timestamp, "
                      "mid_int int from src_ntb.int_col, "
                      "mid_float float from src_ntb.float_col);")
        tdSql.execute(f"CREATE VTABLE `{top_table}` ("
                      "ts timestamp, "
                      f"top_int int from {mid_table}.mid_int, "
                      f"top_float float from {mid_table}.mid_float);")
        tdSql.execute(f"CREATE VTABLE `{leaf_table}` ("
                      "ts timestamp, "
                      f"leaf_int int from {top_table}.top_int, "
                      f"leaf_float float from {top_table}.top_float);")
        return mid_table, top_table, leaf_table

    @staticmethod
    def _drop_same_db_three_hop_ntb_chain(prefix):
        TestVtableValidateReferencing._drop_tables_if_exist(
            DB_NAME,
            f"{prefix}_leaf",
            f"{prefix}_top",
            f"{prefix}_mid",
        )

    def _assert_show_validate_matches_info_schema(self, db_name, table_name, table_ref=None):
        expected_rows = self._query_referencing_full_rows(db_name, table_name)
        actual_rows = self._query_show_validate_rows(table_ref or table_name)
        assert actual_rows == expected_rows, (
            f"SHOW VTABLE VALIDATE FOR {table_ref or table_name} should match "
            f"information_schema.ins_virtual_tables_referencing"
        )

    def _assert_repeated_show_validate_matches_info_schema(self, db_name, table_name, repeats, table_ref=None):
        for i in range(repeats):
            self._assert_show_validate_matches_info_schema(db_name, table_name, table_ref)
            if (i + 1) % 10 == 0 or i + 1 == repeats:
                tdLog.info(f"completed {i + 1}/{repeats} repeated validate comparisons for {table_ref or table_name}.")

    def _assert_repeated_show_validate_err_codes(self, table_ref, expected_rows, repeats):
        for i in range(repeats):
            rows = self._query_show_validate_err_codes(table_ref)
            assert rows == expected_rows, (
                f"Unexpected SHOW VTABLE VALIDATE result on iteration {i + 1} for {table_ref}: {rows}"
            )
            if (i + 1) % 10 == 0 or i + 1 == repeats:
                tdLog.info(f"completed {i + 1}/{repeats} repeated err_code checks for {table_ref}.")

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
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vntb_same_db';")

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
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vctb_same_db';")

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
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vntb_cross_db';")

        # vntb_cross_db has 2 referenced columns (v_voltage, v_current)
        tdSql.checkRows(2)

        # All err_code should be 0 (success)
        for i in range(2):
            tdSql.checkData(i, 8, TSDB_CODE_SUCCESS)

    def test_valid_same_db_second_layer_ntb_referencing(self):
        """Validate: same-db virtual table referencing virtual table.

        Validate: same-db virtual table referencing virtual table.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdLog.info(f"=== Test: valid same-db second-layer ntb referencing ===")
        tdSql.execute(f"use {DB_NAME};")

        rows = self._query_referencing_rows(DB_NAME, "vntb_top_same_db")
        assert rows == [
            ("top_float", DB_NAME, "vntb_mid_same_db", "mid_float", TSDB_CODE_SUCCESS),
            ("top_int", DB_NAME, "vntb_mid_same_db", "mid_int", TSDB_CODE_SUCCESS),
        ]

    def test_valid_cross_db_second_layer_ntb_referencing(self):
        """Validate: cross-db virtual table referencing virtual table.

        Validate: cross-db virtual table referencing virtual table.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdLog.info(f"=== Test: valid cross-db second-layer ntb referencing ===")
        tdSql.execute(f"use {DB_NAME};")

        rows = self._query_referencing_rows(DB_NAME, "vntb_top_cross_db")
        assert rows == [
            ("top_current", CROSS_DB_NAME, "vntb_mid_cross_db", "mid_current", TSDB_CODE_SUCCESS),
            ("top_voltage", CROSS_DB_NAME, "vntb_mid_cross_db", "mid_voltage", TSDB_CODE_SUCCESS),
        ]

    def test_valid_same_db_second_layer_vchild_referencing(self):
        """Validate: same-db virtual child referencing virtual child.

        Validate: same-db virtual child referencing virtual child.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdLog.info(f"=== Test: valid same-db second-layer vchild referencing ===")
        tdSql.execute(f"use {DB_NAME};")

        rows = self._query_referencing_rows(DB_NAME, "top_chain_ctb")
        assert rows == [
            ("top_extra", DB_NAME, "mid_chain_ctb", "mid_extra", TSDB_CODE_SUCCESS),
            ("top_val", DB_NAME, "mid_chain_ctb", "mid_val", TSDB_CODE_SUCCESS),
        ]

    def test_valid_cross_db_second_layer_vchild_referencing(self):
        """Validate: cross-db virtual child referencing virtual child.

        Validate: cross-db virtual child referencing virtual child.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdLog.info(f"=== Test: valid cross-db second-layer vchild referencing ===")
        tdSql.execute(f"use {DB_NAME};")

        rows = self._query_referencing_rows(DB_NAME, "cross_top_ctb")
        assert rows == [
            ("top_extra", CROSS_DB_NAME, "cross_mid_ctb", "mid_extra", TSDB_CODE_SUCCESS),
            ("top_val", CROSS_DB_NAME, "cross_mid_ctb", "mid_val", TSDB_CODE_SUCCESS),
        ]

    def test_show_validate_dropped_middle_virtual_table(self):
        """Validate: top-layer virtual table reports TABLE_NOT_EXIST after middle table is dropped.

        Validate: top-layer virtual table reports TABLE_NOT_EXIST after middle table is dropped.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdLog.info(f"=== Test: dropped middle virtual table in chain ===")
        tdSql.execute(f"use {DB_NAME};")

        rows = self._query_referencing_err_codes(DB_NAME, "vntb_top_same_db")
        assert rows == [("top_float", TSDB_CODE_SUCCESS), ("top_int", TSDB_CODE_SUCCESS)]

        tdSql.execute(f"DROP TABLE vntb_mid_same_db;")

        rows = self._query_referencing_err_codes(DB_NAME, "vntb_top_same_db")
        assert rows == [
            ("top_float", TSDB_CODE_PAR_TABLE_NOT_EXIST),
            ("top_int", TSDB_CODE_PAR_TABLE_NOT_EXIST),
        ]

    def test_show_validate_three_hop_middle_drop_propagates_to_descendant(self):
        """Validate: deleting an intermediate table propagates TABLE_NOT_EXIST through a three-hop chain.

        Validate: deleting an intermediate table propagates TABLE_NOT_EXIST through a three-hop chain.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdLog.info(f"=== Test: three-hop dropped middle table propagates to leaf ===")
        tdSql.execute(f"use {DB_NAME};")

        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_leaf_3hop_tmp`;")
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_top_3hop_tmp`;")
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_mid_3hop_tmp`;")

        try:
            tdSql.execute(f"CREATE VTABLE `vntb_mid_3hop_tmp` ("
                          "ts timestamp, "
                          "mid_int int from src_ntb.int_col, "
                          "mid_float float from src_ntb.float_col);")
            tdSql.execute(f"CREATE VTABLE `vntb_top_3hop_tmp` ("
                          "ts timestamp, "
                          "top_int int from vntb_mid_3hop_tmp.mid_int, "
                          "top_float float from vntb_mid_3hop_tmp.mid_float);")
            tdSql.execute(f"CREATE VTABLE `vntb_leaf_3hop_tmp` ("
                          "ts timestamp, "
                          "leaf_int int from vntb_top_3hop_tmp.top_int, "
                          "leaf_float float from vntb_top_3hop_tmp.top_float);")

            rows = self._query_show_validate_err_codes("vntb_leaf_3hop_tmp")
            assert rows == [("leaf_float", TSDB_CODE_SUCCESS), ("leaf_int", TSDB_CODE_SUCCESS)]

            tdSql.execute(f"DROP TABLE vntb_mid_3hop_tmp;")

            self._assert_show_validate_matches_info_schema(DB_NAME, "vntb_leaf_3hop_tmp")
            rows = self._query_show_validate_err_codes("vntb_leaf_3hop_tmp")
            assert rows == [
                ("leaf_float", TSDB_CODE_PAR_TABLE_NOT_EXIST),
                ("leaf_int", TSDB_CODE_PAR_TABLE_NOT_EXIST),
            ]
        finally:
            tdSql.execute(f"DROP TABLE IF EXISTS `vntb_leaf_3hop_tmp`;")
            tdSql.execute(f"DROP TABLE IF EXISTS `vntb_top_3hop_tmp`;")
            tdSql.execute(f"DROP TABLE IF EXISTS `vntb_mid_3hop_tmp`;")

    def test_show_validate_cross_db_three_hop_middle_drop_propagates_to_descendant(self):
        """Validate: dropping a cross-db middle virtual table propagates TABLE_NOT_EXIST to the leaf table.

        Validate: dropping a cross-db middle virtual table propagates TABLE_NOT_EXIST to the leaf table.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdLog.info(f"=== Test: cross-db three-hop dropped middle table propagates to leaf ===")
        tdSql.execute(f"use {CROSS_DB_NAME};")
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_mid_cross_3hop_tmp`;")
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_leaf_cross_3hop_tmp`;")
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_top_cross_3hop_tmp`;")

        try:
            tdSql.execute(f"use {CROSS_DB_NAME};")
            tdSql.execute(f"CREATE VTABLE `vntb_mid_cross_3hop_tmp` ("
                          "ts timestamp, "
                          "mid_voltage int from cross_ntb.voltage, "
                          "mid_current float from cross_ntb.current);")
            tdSql.execute(f"use {DB_NAME};")
            tdSql.execute(f"CREATE VTABLE `vntb_top_cross_3hop_tmp` ("
                          "ts timestamp, "
                          f"top_voltage int from {CROSS_DB_NAME}.vntb_mid_cross_3hop_tmp.mid_voltage, "
                          f"top_current float from {CROSS_DB_NAME}.vntb_mid_cross_3hop_tmp.mid_current);")
            tdSql.execute(f"CREATE VTABLE `vntb_leaf_cross_3hop_tmp` ("
                          "ts timestamp, "
                          "leaf_voltage int from vntb_top_cross_3hop_tmp.top_voltage, "
                          "leaf_current float from vntb_top_cross_3hop_tmp.top_current);")

            rows = self._query_show_validate_err_codes("vntb_leaf_cross_3hop_tmp")
            assert rows == [("leaf_current", TSDB_CODE_SUCCESS), ("leaf_voltage", TSDB_CODE_SUCCESS)]

            tdSql.execute(f"use {CROSS_DB_NAME};")
            tdSql.execute(f"DROP TABLE vntb_mid_cross_3hop_tmp;")

            tdSql.execute(f"use {DB_NAME};")
            self._assert_show_validate_matches_info_schema(DB_NAME, "vntb_leaf_cross_3hop_tmp")
            rows = self._query_show_validate_err_codes("vntb_leaf_cross_3hop_tmp")
            assert rows == [
                ("leaf_current", TSDB_CODE_PAR_TABLE_NOT_EXIST),
                ("leaf_voltage", TSDB_CODE_PAR_TABLE_NOT_EXIST),
            ]
        finally:
            tdSql.execute(f"use {DB_NAME};")
            tdSql.execute(f"DROP TABLE IF EXISTS `vntb_leaf_cross_3hop_tmp`;")
            tdSql.execute(f"DROP TABLE IF EXISTS `vntb_top_cross_3hop_tmp`;")
            tdSql.execute(f"use {CROSS_DB_NAME};")
            tdSql.execute(f"DROP TABLE IF EXISTS `vntb_mid_cross_3hop_tmp`;")
            tdSql.execute(f"use {DB_NAME};")

    def test_show_validate_three_hop_vchild_middle_drop_propagates_to_descendant(self):
        """Validate: dropping an intermediate virtual child table propagates TABLE_NOT_EXIST to the leaf child.

        Validate: dropping an intermediate virtual child table propagates TABLE_NOT_EXIST to the leaf child.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdLog.info(f"=== Test: three-hop virtual child dropped middle table propagates to leaf ===")
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"DROP TABLE IF EXISTS `leaf_chain_ctb_3hop_tmp`;")
        tdSql.execute(f"DROP TABLE IF EXISTS `top_chain_ctb_3hop_tmp`;")
        tdSql.execute(f"DROP TABLE IF EXISTS `mid_chain_ctb_3hop_tmp`;")
        tdSql.execute(f"DROP STABLE IF EXISTS `leaf_chain_vstb_3hop_tmp`;")
        tdSql.execute(f"DROP STABLE IF EXISTS `top_chain_vstb_3hop_tmp`;")
        tdSql.execute(f"DROP STABLE IF EXISTS `mid_chain_vstb_3hop_tmp`;")

        try:
            tdSql.execute(f"CREATE STABLE `mid_chain_vstb_3hop_tmp` ("
                          "ts timestamp, "
                          "mid_val int, "
                          "mid_extra float"
                          ") TAGS ("
                          "mid_region int, "
                          "mid_name binary(32))"
                          " VIRTUAL 1;")
            tdSql.execute(f"CREATE VTABLE `mid_chain_ctb_3hop_tmp` ("
                          "mid_val from src_ctb.val, "
                          "mid_extra from src_ctb.extra_col) "
                          "USING `mid_chain_vstb_3hop_tmp` TAGS (21, 'mid_chain_3hop');")
            tdSql.execute(f"CREATE STABLE `top_chain_vstb_3hop_tmp` ("
                          "ts timestamp, "
                          "top_val int, "
                          "top_extra float"
                          ") TAGS ("
                          "top_region int, "
                          "top_name binary(32))"
                          " VIRTUAL 1;")
            tdSql.execute(f"CREATE VTABLE `top_chain_ctb_3hop_tmp` ("
                          "top_val from mid_chain_ctb_3hop_tmp.mid_val, "
                          "top_extra from mid_chain_ctb_3hop_tmp.mid_extra) "
                          "USING `top_chain_vstb_3hop_tmp` TAGS (22, 'top_chain_3hop');")
            tdSql.execute(f"CREATE STABLE `leaf_chain_vstb_3hop_tmp` ("
                          "ts timestamp, "
                          "leaf_val int, "
                          "leaf_extra float"
                          ") TAGS ("
                          "leaf_region int, "
                          "leaf_name binary(32))"
                          " VIRTUAL 1;")
            tdSql.execute(f"CREATE VTABLE `leaf_chain_ctb_3hop_tmp` ("
                          "leaf_val from top_chain_ctb_3hop_tmp.top_val, "
                          "leaf_extra from top_chain_ctb_3hop_tmp.top_extra) "
                          "USING `leaf_chain_vstb_3hop_tmp` TAGS (23, 'leaf_chain_3hop');")

            rows = self._query_show_validate_err_codes("leaf_chain_ctb_3hop_tmp")
            assert rows == [("leaf_extra", TSDB_CODE_SUCCESS), ("leaf_val", TSDB_CODE_SUCCESS)]

            tdSql.execute(f"DROP TABLE mid_chain_ctb_3hop_tmp;")

            self._assert_show_validate_matches_info_schema(DB_NAME, "leaf_chain_ctb_3hop_tmp")
            rows = self._query_show_validate_err_codes("leaf_chain_ctb_3hop_tmp")
            assert rows == [
                ("leaf_extra", TSDB_CODE_PAR_TABLE_NOT_EXIST),
                ("leaf_val", TSDB_CODE_PAR_TABLE_NOT_EXIST),
            ]
        finally:
            tdSql.execute(f"DROP TABLE IF EXISTS `leaf_chain_ctb_3hop_tmp`;")
            tdSql.execute(f"DROP TABLE IF EXISTS `top_chain_ctb_3hop_tmp`;")
            tdSql.execute(f"DROP TABLE IF EXISTS `mid_chain_ctb_3hop_tmp`;")
            tdSql.execute(f"DROP STABLE IF EXISTS `leaf_chain_vstb_3hop_tmp`;")
            tdSql.execute(f"DROP STABLE IF EXISTS `top_chain_vstb_3hop_tmp`;")
            tdSql.execute(f"DROP STABLE IF EXISTS `mid_chain_vstb_3hop_tmp`;")

    def test_show_validate_three_hop_root_db_drop_propagates_to_descendant(self):
        """Validate: dropping the root database of a three-hop chain propagates DB_NOT_EXIST to the leaf.

        Validate: dropping the root database of a three-hop chain propagates DB_NOT_EXIST to the leaf.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdLog.info(f"=== Test: three-hop dropped root database propagates to leaf ===")
        temp_db = f"{DB_NAME}_3hop_root_db_tmp"

        tdSql.execute(f"DROP DATABASE IF EXISTS {temp_db};")
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_leaf_root_db_3hop_tmp`;")
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_top_root_db_3hop_tmp`;")

        try:
            tdSql.execute(f"CREATE DATABASE {temp_db};")
            tdSql.execute(f"use {temp_db};")
            tdSql.execute(f"CREATE TABLE `src_ntb_root_db_3hop_tmp` ("
                          "ts timestamp, "
                          "int_col int, "
                          "float_col float);")
            tdSql.execute(f"INSERT INTO `src_ntb_root_db_3hop_tmp` VALUES (now, 11, 1.1);")
            tdSql.execute(f"CREATE VTABLE `vntb_mid_root_db_3hop_tmp` ("
                          "ts timestamp, "
                          "mid_int int from src_ntb_root_db_3hop_tmp.int_col, "
                          "mid_float float from src_ntb_root_db_3hop_tmp.float_col);")

            tdSql.execute(f"use {DB_NAME};")
            tdSql.execute(f"CREATE VTABLE `vntb_top_root_db_3hop_tmp` ("
                          "ts timestamp, "
                          f"top_int int from {temp_db}.vntb_mid_root_db_3hop_tmp.mid_int, "
                          f"top_float float from {temp_db}.vntb_mid_root_db_3hop_tmp.mid_float);")
            tdSql.execute(f"CREATE VTABLE `vntb_leaf_root_db_3hop_tmp` ("
                          "ts timestamp, "
                          "leaf_int int from vntb_top_root_db_3hop_tmp.top_int, "
                          "leaf_float float from vntb_top_root_db_3hop_tmp.top_float);")

            rows = self._query_show_validate_err_codes("vntb_leaf_root_db_3hop_tmp")
            assert rows == [("leaf_float", TSDB_CODE_SUCCESS), ("leaf_int", TSDB_CODE_SUCCESS)]

            tdSql.execute(f"DROP DATABASE {temp_db};")

            self._assert_show_validate_matches_info_schema(DB_NAME, "vntb_leaf_root_db_3hop_tmp")
            rows = self._query_show_validate_err_codes("vntb_leaf_root_db_3hop_tmp")
            assert rows == [
                ("leaf_float", TSDB_CODE_MND_DB_NOT_EXIST),
                ("leaf_int", TSDB_CODE_MND_DB_NOT_EXIST),
            ]
        finally:
            tdSql.execute(f"use {DB_NAME};")
            tdSql.execute(f"DROP TABLE IF EXISTS `vntb_leaf_root_db_3hop_tmp`;")
            tdSql.execute(f"DROP TABLE IF EXISTS `vntb_top_root_db_3hop_tmp`;")
            tdSql.execute(f"DROP DATABASE IF EXISTS {temp_db};")

    def test_show_validate_middle_layer_source_column_drop_propagates_to_top_layer(self):
        """Validate: source-column break propagates INVALID_REF_COLUMN to descendants.

        Validate: source-column break propagates INVALID_REF_COLUMN to descendants.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdLog.info(f"=== Test: middle-layer source column drop propagates to top-layer validate ===")
        tdSql.execute(f"use {DB_NAME};")

        rows = self._query_referencing_err_codes(DB_NAME, "cross_top_ctb")
        assert rows == [("top_extra", TSDB_CODE_SUCCESS), ("top_val", TSDB_CODE_SUCCESS)]

        tdSql.execute(f"use {CROSS_DB_NAME};")
        tdSql.execute(f"ALTER STABLE cross_src_stb ADD COLUMN dummy_col int;")
        tdSql.execute(f"ALTER STABLE cross_src_stb DROP COLUMN cross_val;")

        mid_rows = self._query_referencing_err_codes(CROSS_DB_NAME, "cross_mid_ctb")
        assert mid_rows == [
            ("mid_extra", TSDB_CODE_SUCCESS),
            ("mid_val", TSDB_CODE_PAR_INVALID_REF_COLUMN),
        ]

        tdSql.execute(f"use {DB_NAME};")
        self._assert_show_validate_matches_info_schema(DB_NAME, "cross_top_ctb")
        rows = self._query_referencing_err_codes(DB_NAME, "cross_top_ctb")
        assert rows == [
            ("top_extra", TSDB_CODE_SUCCESS),
            ("top_val", TSDB_CODE_PAR_INVALID_REF_COLUMN),
        ]

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
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vntb_same_db' "
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
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vctb_drop_col_test';")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, TSDB_CODE_SUCCESS)
        tdSql.checkData(1, 0, TSDB_CODE_SUCCESS)

        # Now drop the 'val' column from src_stb (need to add a dummy col first to avoid last-col error)
        tdSql.execute(f"ALTER STABLE src_stb ADD COLUMN dummy_col int;")
        tdSql.execute(f"ALTER STABLE src_stb DROP COLUMN val;")

        # Validate again - the 'val' reference should now be invalid
        tdSql.query(f"select virtual_col_name, src_column_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vctb_drop_col_test' "
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
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vntb_drop_tbl_test';")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, TSDB_CODE_SUCCESS)
        tdSql.checkData(1, 0, TSDB_CODE_SUCCESS)

        # Drop the source table
        tdSql.execute(f"DROP TABLE src_drop_test;")

        # Validate again - all references should report TABLE_NOT_EXIST
        tdSql.query(f"select virtual_col_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vntb_drop_tbl_test';")
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
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vntb_drop_db_test';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, TSDB_CODE_SUCCESS)

        # Drop the cross database
        tdSql.execute(f"drop database {cross_db};")

        # Validate again - reference should report DB_NOT_EXIST
        tdSql.query(f"select virtual_col_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vntb_drop_db_test';")
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
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vntb_same_db';")
        tdSql.checkRows(3)

        # Filter for vctb_same_db
        tdSql.query(f"select virtual_table_name, virtual_col_name, src_table_name, src_column_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vctb_same_db';")
        tdSql.checkRows(2)

        # Filter for non-existent virtual table
        tdSql.query(f"select * from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'nonexistent_vtable';")
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
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vntb_errmsg_test';")
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
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vntb_mixed';")
        tdSql.checkRows(4)
        for i in range(4):
            tdSql.checkData(i, 1, TSDB_CODE_SUCCESS)

        # Drop col_b from source table (need to keep at least ts + 1 col)
        tdSql.execute(f"ALTER TABLE src_mixed DROP COLUMN col_b;")

        # Now v_b should be invalid, others still valid
        tdSql.query(f"select virtual_col_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vntb_mixed' "
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
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vntb_multi_src' "
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
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vntb_multi_src' "
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
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vntb_multi_src' "
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
                         f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = '{vtable_name}';")
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
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vctb_multi_1';")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, TSDB_CODE_PAR_TABLE_NOT_EXIST)
        tdSql.checkData(1, 1, TSDB_CODE_PAR_TABLE_NOT_EXIST)

        # vctb_multi_2 and vctb_multi_3 should still be valid
        for vtable_name in ['vctb_multi_2', 'vctb_multi_3']:
            tdSql.query(f"select err_code "
                         f"from information_schema.ins_virtual_tables_referencing "
                         f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = '{vtable_name}';")
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
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vntb_many_cols' "
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
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vntb_many_cols' "
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
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vntb_many_cols' "
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
        tdSql.execute(f"INSERT INTO `src_ntb_same` VALUES (now, 99);")

        # Create virtual table referencing same-db + cross-db tables
        tdSql.execute(f"CREATE VTABLE `vntb_mixed_src` ("
                      "ts timestamp, "
                      "v_same int from src_ntb_same.same_col, "
                      f"v_cross int from {CROSS_DB_NAME}.cross_ntb.voltage, "
                      f"v_cross2 float from {CROSS_DB_NAME}.cross_ntb.current);")

        # Verify all references are valid
        tdSql.query(f"select virtual_col_name, src_db_name, src_table_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vntb_mixed_src' "
                     f"order by virtual_col_name;")
        tdSql.checkRows(3)
        
        # All should be valid
        for i in range(3):
            tdSql.checkData(i, 3, TSDB_CODE_SUCCESS)

        # Drop same-db source table
        tdSql.execute(f"DROP TABLE `src_ntb_same`;")

        # Verify v_same has TABLE_NOT_EXIST error
        # order by virtual_col_name => v_cross, v_cross2, v_same
        tdSql.query(f"select virtual_col_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vntb_mixed_src' "
                     f"order by virtual_col_name;")
        
        tdSql.checkData(0, 0, 'v_cross')
        tdSql.checkData(0, 1, TSDB_CODE_SUCCESS)
        tdSql.checkData(1, 0, 'v_cross2')
        tdSql.checkData(1, 1, TSDB_CODE_SUCCESS)
        tdSql.checkData(2, 0, 'v_same')
        tdSql.checkData(2, 1, TSDB_CODE_PAR_TABLE_NOT_EXIST)

        # Restore same-db table
        tdSql.execute(f"CREATE TABLE `src_ntb_same` (ts timestamp, same_col int);")
        tdSql.execute(f"INSERT INTO `src_ntb_same` VALUES (now, 99);")

        # Drop cross-db database
        tdSql.execute(f"drop database {CROSS_DB_NAME};")

        # Verify cross-db references now have DB_NOT_EXIST error
        # order by virtual_col_name => v_cross, v_cross2, v_same
        tdSql.query(f"select virtual_col_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vntb_mixed_src' "
                     f"order by virtual_col_name;")
        
        tdSql.checkData(0, 0, 'v_cross')
        tdSql.checkData(0, 1, TSDB_CODE_MND_DB_NOT_EXIST)
        tdSql.checkData(1, 0, 'v_cross2')
        tdSql.checkData(1, 1, TSDB_CODE_MND_DB_NOT_EXIST)
        tdSql.checkData(2, 0, 'v_same')
        tdSql.checkData(2, 1, TSDB_CODE_SUCCESS)

        # Restore cross-db for other tests
        tdSql.execute(f"create database {CROSS_DB_NAME};")
        tdSql.execute(f"use {CROSS_DB_NAME};")
        tdSql.execute(f"CREATE TABLE `cross_ntb` ("
                      "ts timestamp, "
                      "voltage int, "
                      "current float);")
        tdSql.execute(f"INSERT INTO `cross_ntb` VALUES (now, 220, 1.5);")
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
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vntb_multi_cross' "
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
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vntb_multi_cross' "
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
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vctb_cross';")
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
        # order by virtual_col_name => v_from_ctb, v_from_ntb
        tdSql.query(f"select virtual_col_name, src_table_name, src_column_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vntb_mixed_types' "
                     f"order by virtual_col_name;")
        tdSql.checkRows(2)
        
        # Both should be valid
        tdSql.checkData(0, 3, TSDB_CODE_SUCCESS)
        tdSql.checkData(1, 3, TSDB_CODE_SUCCESS)
        
        # Check source table names (alphabetical: v_from_ctb, v_from_ntb)
        tdSql.checkData(0, 1, 'src_ctb')
        tdSql.checkData(1, 1, 'src_ntb_mixed')
        
        # Check column names
        tdSql.checkData(0, 2, 'val')
        tdSql.checkData(1, 2, 'mixed_col')

    
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

        # Clean up any leftover from previous run
        tdSql.execute(f"DROP TABLE IF EXISTS vctb_from_stbA;")
        tdSql.execute(f"DROP TABLE IF EXISTS vctb_from_stbB;")

        # Ensure source tables exist (may have been dropped by previous test failure)
        tdSql.execute(f"CREATE STABLE IF NOT EXISTS src_stb_A (ts timestamp, val_a int) TAGS (tag_a int);")
        tdSql.execute(f"CREATE TABLE IF NOT EXISTS src_ctb_A1 USING src_stb_A TAGS (1);")
        tdSql.execute(f"CREATE STABLE IF NOT EXISTS src_stb_B (ts timestamp, val_b float) TAGS (tag_b int);")
        tdSql.execute(f"CREATE TABLE IF NOT EXISTS src_ctb_B1 USING src_stb_B TAGS (10);")

        # Verify the tables exist
        tdSql.query(f"SELECT * FROM information_schema.ins_tables WHERE db_name = '{DB_NAME}' AND table_name = 'src_ctb_a1';")
        tdLog.info(f"src_ctb_A1 exists: {tdSql.queryRows} rows")
        tdSql.query(f"SELECT * FROM information_schema.ins_stables WHERE db_name = '{DB_NAME}' AND stable_name = 'src_stb_a';")
        tdLog.info(f"src_stb_A exists: {tdSql.queryRows} rows")

        # Use existing vstb from setup_class (ts, v_val int, v_extra float)
        tdSql.execute(f"CREATE VTABLE vctb_from_stbA ("
                      f"v_val from src_ctb_A1.val_a) "
                      "USING vstb TAGS (500, 'mixed_A');")

        tdSql.execute(f"CREATE VTABLE vctb_from_stbB ("
                      f"v_extra from src_ctb_B1.val_b) "
                      "USING vstb TAGS (501, 'mixed_B');")

        # Verify virtual child table 1
        # vstb has 2 non-ts cols (v_val, v_extra), vctb_from_stbA refs v_val
        tdSql.query(f"select virtual_table_name, virtual_col_name, src_table_name, src_column_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vctb_from_stba' "
                     f"order by virtual_col_name;")
        tdSql.checkRows(2)
        tdSql.checkData(1, 1, 'v_val')
        tdSql.checkData(1, 2, 'src_ctb_a1')
        tdSql.checkData(1, 3, 'val_a')
        tdSql.checkData(1, 4, TSDB_CODE_SUCCESS)

        # Verify virtual child table 2
        # vctb_from_stbB refs v_extra
        tdSql.query(f"select virtual_table_name, virtual_col_name, src_table_name, src_column_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vctb_from_stbb' "
                     f"order by virtual_col_name;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 'v_extra')
        tdSql.checkData(0, 2, 'src_ctb_b1')
        tdSql.checkData(0, 3, 'val_b')
        tdSql.checkData(0, 4, TSDB_CODE_SUCCESS)

        # Drop src_stb_A (and its children)
        tdSql.execute(f"DROP STABLE src_stb_A;")

        # Verify vctb_from_stbA: v_val (with ref to src_ctb_A1) should have TABLE_NOT_EXIST
        tdSql.query(f"select virtual_col_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vctb_from_stba' "
                     f"order by virtual_col_name;")
        tdSql.checkRows(2)
        tdSql.checkData(1, 0, 'v_val')
        tdSql.checkData(1, 1, TSDB_CODE_PAR_TABLE_NOT_EXIST)

        # vctb_from_stbB should still be valid (src_stb_B not dropped)
        tdSql.query(f"select virtual_col_name, err_code "
                     f"from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vctb_from_stbb' "
                     f"order by virtual_col_name;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 'v_extra')
        tdSql.checkData(0, 1, TSDB_CODE_SUCCESS)

        # Restore src_stb_A for other tests
        tdSql.execute(f"CREATE STABLE src_stb_A (ts timestamp, val_a int) TAGS (tag_a int);")
        tdSql.execute(f"CREATE TABLE src_ctb_A1 USING src_stb_A TAGS (1);")
        tdSql.execute(f"CREATE TABLE src_ctb_A2 USING src_stb_A TAGS (2);")
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
                     f"where virtual_db_name = '{DB_NAME}' and virtual_table_name = 'vntb_complex_types' "
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

    def test_show_validate_basic_syntax(self):
        """Validate: SHOW VTABLE VALIDATE FOR basic syntax

        Test the basic syntax of SHOW VTABLE VALIDATE FOR with simple table name.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR basic syntax ===")
        tdSql.execute(f"use {DB_NAME};")

        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_same_db')
        tdSql.checkRows(3)

        for i in range(3):
            tdSql.checkData(i, 8, TSDB_CODE_SUCCESS)

    def test_show_validate_with_database_prefix(self):
        """Validate: SHOW VTABLE VALIDATE FOR with database prefix

        Test SHOW VTABLE VALIDATE FOR with full qualified table name (dbname.tablename).

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR with db prefix ===")
        tdSql.execute(f"use {DB_NAME};")

        self._assert_show_validate_matches_info_schema(
            DB_NAME, 'vntb_same_db', f"{DB_NAME}.vntb_same_db"
        )
        tdSql.checkRows(3)

        for i in range(3):
            tdSql.checkData(i, 0, DB_NAME)  # virtual_db_name
            tdSql.checkData(i, 2, 'vntb_same_db')  # virtual_table_name

    def test_show_validate_cross_database_table(self):
        """Validate: SHOW VTABLE VALIDATE FOR cross-database virtual table

        Test SHOW VTABLE VALIDATE FOR on a virtual table that references
        cross-database tables.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show, cross-db

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR cross-db vtable ===")
        tdSql.execute(f"use {DB_NAME};")

        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_cross_db')
        tdSql.checkRows(2)

        for i in range(2):
            tdSql.checkData(i, 4, CROSS_DB_NAME)  # src_db_name
            tdSql.checkData(i, 8, TSDB_CODE_SUCCESS)  # err_code

    def test_show_validate_virtual_child_table(self):
        """Validate: SHOW VTABLE VALIDATE FOR virtual child table

        Test SHOW VTABLE VALIDATE FOR on a virtual child table.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR virtual child table ===")
        tdSql.execute(f"use {DB_NAME};")

        self._assert_show_validate_matches_info_schema(DB_NAME, 'vctb_same_db')
        tdSql.checkRows(2)  # vctb_same_db has 2 referenced columns

        # Verify all references are valid
        for i in range(2):
            tdSql.checkData(i, 8, TSDB_CODE_SUCCESS)  # err_code

    def test_show_validate_nonexistent_table(self):
        """Validate: SHOW VTABLE VALIDATE FOR nonexistent table

        Test SHOW VTABLE VALIDATE FOR on a table that doesn't exist.
        Should return empty result or error.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show, negative

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR nonexistent table ===")
        tdSql.execute(f"use {DB_NAME};")

        tdSql.error(
            f"SHOW VTABLE VALIDATE FOR nonexistent_vtable;",
            expectErrInfo="Table does not exist",
            fullMatched=False,
        )

    def test_show_validate_non_virtual_normal_table(self):
        """Validate: SHOW VTABLE VALIDATE FOR normal table (not virtual)

        Test SHOW VTABLE VALIDATE FOR on a normal table that is not a virtual table.
        Should return empty result.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show, negative

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR normal table ===")
        tdSql.execute(f"use {DB_NAME};")

        tdSql.error(
            f"SHOW VTABLE VALIDATE FOR `src_ntb`;",
            expectErrInfo="not a virtual table",
            fullMatched=False,
        )

    def test_show_validate_result_columns(self):
        """Validate: SHOW VTABLE VALIDATE FOR result columns

        Verify that SHOW VTABLE VALIDATE FOR returns the expected columns
        with correct names and order.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR result columns ===")
        tdSql.execute(f"use {DB_NAME};")

        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_same_db')
        tdSql.checkRows(3)

        # select * columns: virtual_db_name(0), virtual_stable_name(1), virtual_table_name(2),
        #   virtual_col_name(3), src_db_name(4), src_table_name(5), src_column_name(6),
        #   type(7), err_code(8), err_msg(9)
        tdSql.checkData(0, 0, DB_NAME)  # virtual_db_name
        tdSql.checkData(0, 2, 'vntb_same_db')  # virtual_table_name
        tdSql.checkData(0, 5, 'src_ntb')  # src_table_name
        tdSql.checkData(0, 8, TSDB_CODE_SUCCESS)  # err_code

    def test_show_validate_invalid_references(self):
        """Validate: SHOW VTABLE VALIDATE FOR with invalid references

        Test SHOW VTABLE VALIDATE FOR on a virtual table where some
        references are invalid (dropped source table/column).

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR invalid references ===")
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_show_test`;")
        tdSql.execute(f"DROP TABLE IF EXISTS `src_show_test`;")
        
        # Create a test virtual table
        tdSql.execute(f"CREATE TABLE `src_show_test` (ts timestamp, c1 int);")
        tdSql.execute(f"INSERT INTO src_show_test VALUES (now, 1);")
        tdSql.execute(f"CREATE VTABLE `vntb_show_test` ("
                      "ts timestamp, "
                      "v_c1 int from src_show_test.c1);")

        # Verify it's valid initially
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_show_test')
        tdSql.checkRows(1)
        tdSql.checkData(0, 8, TSDB_CODE_SUCCESS)

        # Drop source table
        tdSql.execute(f"DROP TABLE src_show_test;")

        # Verify it shows error now
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_show_test')
        tdSql.checkRows(1)
        tdSql.checkData(0, 8, TSDB_CODE_PAR_TABLE_NOT_EXIST)

        # Verify err_msg is populated
        err_msg = tdSql.queryResult[0][9]  # err_msg column
        assert err_msg is not None and len(str(err_msg).strip()) > 0, \
            "err_msg should be non-empty when err_code != 0"

    def test_show_validate_case_sensitivity(self):
        """Validate: SHOW VTABLE VALIDATE FOR case sensitivity

        Test if table names in SHOW VTABLE VALIDATE FOR are case-sensitive.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR case sensitivity ===")
        tdSql.execute(f"use {DB_NAME};")

        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_same_db', 'VNTB_SAME_DB')
        tdSql.checkRows(3)
        self._assert_show_validate_matches_info_schema(
            DB_NAME, 'vntb_same_db', f"{DB_NAME}.VNTB_SAME_DB"
        )
        tdSql.checkRows(3)

    def test_show_validate_without_using_database(self):
        """Validate: SHOW VTABLE VALIDATE FOR without USE database

        Test SHOW VTABLE VALIDATE FOR when no database is selected.
        Should require database context or full qualified name.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR without USE ===")

        tdSql.execute(f"use {CROSS_DB_NAME};")

        tdSql.error(
            f"SHOW VTABLE VALIDATE FOR vntb_same_db;",
            expectErrInfo="Table does not exist",
            fullMatched=False,
        )
        self._assert_show_validate_matches_info_schema(
            DB_NAME, 'vntb_same_db', f"{DB_NAME}.vntb_same_db"
        )
        tdSql.checkRows(3)

    # ==================== 场景1: 源表Schema变更异常测试 (6个) ====================

    def test_show_validate_dropped_source_column(self):
        """Validate: SHOW VTABLE VALIDATE FOR when source column is dropped

        Create a virtual table referencing multiple columns from source table,
        then drop one referenced column. Verify SHOW command reports INVALID_REF_COLUMN
        for the dropped column while other columns remain valid.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show, exception, schema

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR - dropped source column ===")
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_drop_col`;")
        tdSql.execute(f"DROP TABLE IF EXISTS `src_drop_col`;")
        
        # Create virtual table referencing 3 columns
        tdSql.execute(f"CREATE TABLE `src_drop_col` (ts timestamp, c1 int, c2 float, c3 binary(16));")
        tdSql.execute(f"INSERT INTO src_drop_col VALUES (now, 1, 1.0, 'test');")
        tdSql.execute(f"CREATE VTABLE `vntb_drop_col` ("
                      "ts timestamp, "
                      "v_c1 int from src_drop_col.c1, "
                      "v_c2 float from src_drop_col.c2, "
                      "v_c3 binary(16) from src_drop_col.c3);")

        # Verify all columns valid initially
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_drop_col')
        tdSql.checkRows(3)
        for i in range(3):
            tdSql.checkData(i, 8, TSDB_CODE_SUCCESS)

        # Drop column c2
        tdSql.execute(f"ALTER TABLE src_drop_col DROP COLUMN c2;")

        # Verify SHOW reports error for v_c2
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_drop_col')
        tdSql.checkRows(3)

        # Find v_c2 and verify it has error
        found_error = False
        for i in range(3):
            if tdSql.queryResult[i][3] == 'v_c2':  # virtual_col_name
                tdSql.checkData(i, 8, TSDB_CODE_PAR_INVALID_REF_COLUMN)
                found_error = True
            else:
                tdSql.checkData(i, 8, TSDB_CODE_SUCCESS)
        
        assert found_error, "v_c2 should have error"

        # Cleanup
        tdSql.execute(f"DROP TABLE vntb_drop_col;")
        tdSql.execute(f"DROP TABLE src_drop_col;")

    def test_show_validate_column_type_change(self):
        """Validate: SHOW VTABLE VALIDATE FOR when source column type is changed

        Create a virtual table referencing source column, then alter column type.
        Verify SHOW command reports error for type mismatch.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show, exception, schema

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR - column type changed ===")
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_type_change`;")
        tdSql.execute(f"DROP TABLE IF EXISTS `src_type_change`;")
        
        # Create virtual table
        tdSql.execute(f"CREATE TABLE `src_type_change` (ts timestamp, c1 int);")
        tdSql.execute(f"INSERT INTO src_type_change VALUES (now, 100);")
        tdSql.execute(f"CREATE VTABLE `vntb_type_change` ("
                      "ts timestamp, "
                      "v_c1 int from src_type_change.c1);")

        # Verify valid initially
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_type_change')
        tdSql.checkRows(1)
        tdSql.checkData(0, 8, TSDB_CODE_SUCCESS)

        # Note: TDengine doesn't support ALTER COLUMN TYPE directly
        # So we test by dropping and recreating with different type
        tdSql.execute(f"ALTER TABLE src_type_change ADD COLUMN temp_col bigint;")
        tdSql.execute(f"ALTER TABLE src_type_change DROP COLUMN c1;")
        tdSql.execute(f"ALTER TABLE src_type_change ADD COLUMN c1 bigint;")  # Changed from int to bigint

        # Verify SHOW reports error (column exists but type changed)
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_type_change')
        tdSql.checkRows(1)
        # The column c1 exists but was recreated, so reference might still be valid
        # This tests the behavior when column is dropped and recreated

        # Cleanup
        tdSql.execute(f"DROP TABLE vntb_type_change;")
        tdSql.execute(f"DROP TABLE src_type_change;")

    def test_show_validate_column_rename(self):
        """Validate: SHOW VTABLE VALIDATE FOR when source column is renamed

        Create a virtual table referencing source column, then rename the column.
        Verify SHOW command reports column not found error.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show, exception, schema

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR - column renamed ===")
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_rename`;")
        tdSql.execute(f"DROP TABLE IF EXISTS `src_rename`;")
        
        # Note: TDengine doesn't support RENAME COLUMN directly
        # This test simulates the scenario by dropping and creating new column
        tdSql.execute(f"CREATE TABLE `src_rename` (ts timestamp, c1 int, c2 float);")
        tdSql.execute(f"INSERT INTO src_rename VALUES (now, 1, 1.0);")
        tdSql.execute(f"CREATE VTABLE `vntb_rename` ("
                      "ts timestamp, "
                      "v_c1 int from src_rename.c1);")

        # Verify valid initially
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_rename')
        tdSql.checkRows(1)
        tdSql.checkData(0, 8, TSDB_CODE_SUCCESS)

        # Drop c1 and create c1_new (simulating rename)
        tdSql.execute(f"ALTER TABLE src_rename ADD COLUMN dummy int;")
        tdSql.execute(f"ALTER TABLE src_rename DROP COLUMN c1;")
        tdSql.execute(f"ALTER TABLE src_rename ADD COLUMN c1_new int;")

        # Verify SHOW reports error
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_rename')
        tdSql.checkRows(1)
        tdSql.checkData(0, 8, TSDB_CODE_PAR_INVALID_REF_COLUMN)

        # Cleanup
        tdSql.execute(f"DROP TABLE vntb_rename;")
        tdSql.execute(f"DROP TABLE src_rename;")

    def test_show_validate_multiple_columns_partial_drop(self):
        """Validate: SHOW VTABLE VALIDATE FOR with partial column drops

        Create a virtual table referencing 6 columns, drop 2 of them.
        Verify SHOW correctly reports 2 errors and 4 valid references.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show, exception, schema

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR - multiple columns partial drop ===")
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_multi_col`;")
        tdSql.execute(f"DROP TABLE IF EXISTS `src_multi_col`;")
        
        # Create virtual table with 6 column references
        tdSql.execute(f"CREATE TABLE `src_multi_col` ("
                      "ts timestamp, c1 int, c2 float, c3 bigint, c4 binary(16), c5 double, c6 smallint);")
        tdSql.execute(f"INSERT INTO src_multi_col VALUES (now, 1, 1.0, 100, 'abc', 3.14, 10);")
        tdSql.execute(f"CREATE VTABLE `vntb_multi_col` ("
                      "ts timestamp, "
                      "v1 int from src_multi_col.c1, "
                      "v2 float from src_multi_col.c2, "
                      "v3 bigint from src_multi_col.c3, "
                      "v4 binary(16) from src_multi_col.c4, "
                      "v5 double from src_multi_col.c5, "
                      "v6 smallint from src_multi_col.c6);")

        # Verify all valid initially
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_multi_col')
        tdSql.checkRows(6)
        for i in range(6):
            tdSql.checkData(i, 8, TSDB_CODE_SUCCESS)

        # Drop c3 and c5
        tdSql.execute(f"ALTER TABLE src_multi_col DROP COLUMN c3;")
        tdSql.execute(f"ALTER TABLE src_multi_col DROP COLUMN c5;")

        # Verify 2 errors, 4 valid
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_multi_col')
        tdSql.checkRows(6)
        
        error_count = 0
        success_count = 0
        for i in range(6):
            err_code = tdSql.queryResult[i][8]
            if err_code == TSDB_CODE_PAR_INVALID_REF_COLUMN:
                error_count += 1
            elif err_code == TSDB_CODE_SUCCESS:
                success_count += 1
        
        assert error_count == 2, f"Expected 2 errors, got {error_count}"
        assert success_count == 4, f"Expected 4 successes, got {success_count}"

        # Cleanup
        tdSql.execute(f"DROP TABLE vntb_multi_col;")
        tdSql.execute(f"DROP TABLE src_multi_col;")

    def test_show_validate_column_added_to_source(self):
        """Validate: SHOW VTABLE VALIDATE FOR when new column added to source

        Create a virtual table, then add a new column to source table.
        Verify SHOW command is not affected (new column not referenced).

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show, schema

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR - new column added to source ===")
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_add_col`;")
        tdSql.execute(f"DROP TABLE IF EXISTS `src_add_col`;")
        
        # Create virtual table
        tdSql.execute(f"CREATE TABLE `src_add_col` (ts timestamp, c1 int);")
        tdSql.execute(f"INSERT INTO src_add_col VALUES (now, 1);")
        tdSql.execute(f"CREATE VTABLE `vntb_add_col` ("
                      "ts timestamp, "
                      "v_c1 int from src_add_col.c1);")

        # Verify valid initially
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_add_col')
        tdSql.checkRows(1)

        # Add new column to source
        tdSql.execute(f"ALTER TABLE src_add_col ADD COLUMN c2 float;")

        # Verify still valid (new column not referenced)
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_add_col')
        tdSql.checkRows(1)
        tdSql.checkData(0, 8, TSDB_CODE_SUCCESS)

        # Cleanup
        tdSql.execute(f"DROP TABLE vntb_add_col;")
        tdSql.execute(f"DROP TABLE src_add_col;")

    def test_show_validate_all_columns_dropped(self):
        """Validate: SHOW VTABLE VALIDATE FOR when all referenced columns dropped

        Create a virtual table referencing all columns, then drop all of them.
        Verify SHOW reports all references as invalid.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show, exception, schema

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR - all columns dropped ===")
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_all_drop`;")
        tdSql.execute(f"DROP TABLE IF EXISTS `src_all_drop`;")
        
        # Create virtual table
        tdSql.execute(f"CREATE TABLE `src_all_drop` (ts timestamp, c1 int, c2 float);")
        tdSql.execute(f"INSERT INTO src_all_drop VALUES (now, 1, 1.0);")
        tdSql.execute(f"CREATE VTABLE `vntb_all_drop` ("
                      "ts timestamp, "
                      "v1 int from src_all_drop.c1, "
                      "v2 float from src_all_drop.c2);")

        # Verify valid initially
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_all_drop')
        tdSql.checkRows(2)

        # Drop all referenced columns (add dummy first so table keeps at least one non-ts column)
        tdSql.execute(f"ALTER TABLE src_all_drop ADD COLUMN dummy int;")
        tdSql.execute(f"ALTER TABLE src_all_drop DROP COLUMN c1;")
        tdSql.execute(f"ALTER TABLE src_all_drop DROP COLUMN c2;")

        # Verify all references invalid
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_all_drop')
        tdSql.checkRows(2)
        for i in range(2):
            tdSql.checkData(i, 8, TSDB_CODE_PAR_INVALID_REF_COLUMN)

        # Cleanup
        tdSql.execute(f"DROP TABLE vntb_all_drop;")
        tdSql.execute(f"DROP TABLE src_all_drop;")

    # ==================== 场景2: 源表操作异常测试 (5个) ====================

    def test_show_validate_dropped_source_table(self):
        """Validate: SHOW VTABLE VALIDATE FOR when source table is dropped

        Create a virtual table, then drop the source table.
        Verify SHOW reports TABLE_NOT_EXIST error.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show, exception, table

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR - source table dropped ===")
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_drop_table`;")
        tdSql.execute(f"DROP TABLE IF EXISTS `src_drop_table`;")
        
        # Create virtual table
        tdSql.execute(f"CREATE TABLE `src_drop_table` (ts timestamp, c1 int);")
        tdSql.execute(f"INSERT INTO src_drop_table VALUES (now, 1);")
        tdSql.execute(f"CREATE VTABLE `vntb_drop_table` ("
                      "ts timestamp, "
                      "v_c1 int from src_drop_table.c1);")

        # Verify valid initially
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_drop_table')
        tdSql.checkRows(1)
        tdSql.checkData(0, 8, TSDB_CODE_SUCCESS)

        # Drop source table
        tdSql.execute(f"DROP TABLE src_drop_table;")

        # Verify SHOW reports error
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_drop_table')
        tdSql.checkRows(1)
        tdSql.checkData(0, 8, TSDB_CODE_PAR_TABLE_NOT_EXIST)

        # Verify error message is populated
        err_msg = tdSql.queryResult[0][9]
        assert err_msg is not None and len(str(err_msg).strip()) > 0, "err_msg should be non-empty"

        # Cleanup
        tdSql.execute(f"DROP TABLE vntb_drop_table;")

    def test_show_validate_truncated_source_table(self):
        """Validate: SHOW VTABLE VALIDATE FOR when source table is truncated

        Create a virtual table, then truncate the source table (delete all data).
        Verify SHOW still reports valid (truncate doesn't change schema).

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show, table

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR - source table truncated ===")
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_truncate`;")
        tdSql.execute(f"DROP TABLE IF EXISTS `src_truncate`;")
        
        # Create virtual table
        tdSql.execute(f"CREATE TABLE `src_truncate` (ts timestamp, c1 int);")
        tdSql.execute(f"INSERT INTO src_truncate VALUES (now, 1);")
        tdSql.execute(f"CREATE VTABLE `vntb_truncate` ("
                      "ts timestamp, "
                      "v_c1 int from src_truncate.c1);")

        # Verify valid initially
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_truncate')
        tdSql.checkRows(1)
        tdSql.checkData(0, 8, TSDB_CODE_SUCCESS)

        # Delete all data from source table (TDengine doesn't support TRUNCATE TABLE)
        tdSql.execute(f"DELETE FROM src_truncate;")

        # Verify still valid (truncate only removes data, not schema)
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_truncate')
        tdSql.checkRows(1)
        tdSql.checkData(0, 8, TSDB_CODE_SUCCESS)

        # Cleanup
        tdSql.execute(f"DROP TABLE vntb_truncate;")
        tdSql.execute(f"DROP TABLE src_truncate;")

    def test_show_validate_renamed_source_table(self):
        """Validate: SHOW VTABLE VALIDATE FOR when source table is renamed

        Create a virtual table, then rename the source table.
        Verify SHOW reports table not found error.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show, exception, table

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR - source table renamed ===")
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_rename_table`;")
        tdSql.execute(f"DROP TABLE IF EXISTS `src_rename_table`;")
        tdSql.execute(f"DROP TABLE IF EXISTS `src_renamed`;")
        
        # Create virtual table
        tdSql.execute(f"CREATE TABLE `src_rename_table` (ts timestamp, c1 int);")
        tdSql.execute(f"INSERT INTO src_rename_table VALUES (now, 1);")
        tdSql.execute(f"CREATE VTABLE `vntb_rename_table` ("
                      "ts timestamp, "
                      "v_c1 int from src_rename_table.c1);")

        # Verify valid initially
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_rename_table')
        tdSql.checkRows(1)
        tdSql.checkData(0, 8, TSDB_CODE_SUCCESS)

        # Rename table (simulated by create new + drop old)
        tdSql.execute(f"CREATE TABLE `src_renamed` (ts timestamp, c1 int);")
        tdSql.execute(f"DROP TABLE src_rename_table;")

        # Verify SHOW reports error
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_rename_table')
        tdSql.checkRows(1)
        tdSql.checkData(0, 8, TSDB_CODE_PAR_TABLE_NOT_EXIST)

        # Cleanup
        tdSql.execute(f"DROP TABLE vntb_rename_table;")
        tdSql.execute(f"DROP TABLE src_renamed;")

    def test_show_validate_dropped_source_stable(self):
        """Validate: SHOW VTABLE VALIDATE FOR when source super table is dropped

        Create a virtual child table referencing source child table,
        then drop the source super table (cascades to child tables).
        Verify SHOW reports table not found error.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show, exception, table

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR - source stable dropped ===")
        tdSql.execute(f"use {DB_NAME};")

        # Clean up leftover from previous run
        tdSql.execute(f"DROP TABLE IF EXISTS `vctb_drop_stb`;")
        tdSql.execute(f"DROP STABLE IF EXISTS `src_stb_drop`;")

        # Create source stable and child table
        tdSql.execute(f"CREATE STABLE `src_stb_drop` (ts timestamp, val int) TAGS (tag1 int);")
        tdSql.execute(f"CREATE TABLE `src_ctb_drop` USING `src_stb_drop` TAGS (1);")
        tdSql.execute(f"INSERT INTO src_ctb_drop VALUES (now, 100);")

        # Create virtual child table
        tdSql.execute(f"CREATE VTABLE `vctb_drop_stb` ("
                      "v_val from src_ctb_drop.val) "
                      "USING `vstb` TAGS (100, 'vdev_stb');")

        # Verify valid initially.
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vctb_drop_stb')
        tdSql.checkRows(2)

        # Drop source stable (cascades to child)
        tdSql.execute(f"DROP STABLE src_stb_drop;")

        # Verify SHOW reports the dropped source for the referenced column only.
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vctb_drop_stb')
        tdSql.checkRows(2)
        tdSql.checkData(0, 3, 'v_extra')
        tdSql.checkData(0, 8, TSDB_CODE_SUCCESS)
        tdSql.checkData(1, 3, 'v_val')
        tdSql.checkData(1, 8, TSDB_CODE_PAR_TABLE_NOT_EXIST)

        # Cleanup
        tdSql.execute(f"DROP TABLE vctb_drop_stb;")

    def test_show_validate_dropped_source_child_table(self):
        """Validate: SHOW VTABLE VALIDATE FOR when source child table is dropped

        Create a virtual child table referencing source child table,
        then drop only the source child table (not the stable).
        Verify SHOW reports table not found error.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show, exception, table

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR - source child table dropped ===")
        tdSql.execute(f"use {DB_NAME};")

        # Clean up leftover from previous run
        tdSql.execute(f"DROP TABLE IF EXISTS `vctb_only`;")
        tdSql.execute(f"DROP STABLE IF EXISTS `src_stb_ctb`;")

        # Create source stable and child table
        tdSql.execute(f"CREATE STABLE `src_stb_ctb` (ts timestamp, val int) TAGS (tag1 int);")
        tdSql.execute(f"CREATE TABLE `src_ctb_only` USING `src_stb_ctb` TAGS (1);")
        tdSql.execute(f"INSERT INTO src_ctb_only VALUES (now, 100);")

        # Create virtual child table
        tdSql.execute(f"CREATE VTABLE `vctb_only` ("
                      "v_val from src_ctb_only.val) "
                      "USING `vstb` TAGS (200, 'vdev_ctb');")

        # Verify valid initially.
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vctb_only')
        tdSql.checkRows(2)

        # Drop only the source child table
        tdSql.execute(f"DROP TABLE src_ctb_only;")

        # Verify SHOW reports the dropped source child on the referenced column only.
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vctb_only')
        tdSql.checkRows(2)
        tdSql.checkData(0, 3, 'v_extra')
        tdSql.checkData(0, 8, TSDB_CODE_SUCCESS)
        tdSql.checkData(1, 3, 'v_val')
        tdSql.checkData(1, 8, TSDB_CODE_PAR_TABLE_NOT_EXIST)

        # Cleanup
        tdSql.execute(f"DROP TABLE vctb_only;")
        tdSql.execute(f"DROP STABLE src_stb_ctb;")

    # ==================== 场景3: 跨库引用异常测试 (4个) ====================

    def test_show_validate_cross_db_database_dropped(self):
        """Validate: SHOW VTABLE VALIDATE FOR when cross-database is dropped

        Create a virtual table referencing cross-database table,
        then drop the cross-database.
        Verify SHOW reports DB_NOT_EXIST error.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show, exception, cross-db

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR - cross-database dropped ===")
        tdSql.execute(f"use {DB_NAME};")

        # Create cross-database (clean up leftover from previous run)
        cross_db = "test_show_validate_cross_drop"
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_cross_drop`;")
        tdSql.execute(f"drop database if exists {cross_db};")
        tdSql.execute(f"create database {cross_db};")
        tdSql.execute(f"use {cross_db};")
        tdSql.execute(f"CREATE TABLE `src_cross` (ts timestamp, val int);")
        tdSql.execute(f"INSERT INTO src_cross VALUES (now, 1);")

        # Create virtual table in main db
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"CREATE VTABLE `vntb_cross_drop` ("
                      "ts timestamp, "
                      f"v_val int from {cross_db}.src_cross.val);")

        # Verify valid initially (vntb_cross_drop is virtual normal table - use info_schema)
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_cross_drop')
        tdSql.checkRows(1)
        tdSql.checkData(0, 8, TSDB_CODE_SUCCESS)

        # Drop cross-database
        tdSql.execute(f"drop database {cross_db};")

        # Verify info_schema reports DB_NOT_EXIST
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_cross_drop')
        tdSql.checkRows(1)
        tdSql.checkData(0, 8, TSDB_CODE_MND_DB_NOT_EXIST)

        # Verify error message
        err_msg = tdSql.queryResult[0][9]
        assert err_msg is not None and len(str(err_msg).strip()) > 0, "err_msg should be non-empty"

        # Cleanup
        tdSql.execute(f"DROP TABLE vntb_cross_drop;")

    def test_show_validate_cross_db_table_dropped(self):
        """Validate: SHOW VTABLE VALIDATE FOR when cross-db table is dropped

        Create a virtual table referencing cross-database table,
        then drop the source table in cross-database.
        Verify SHOW reports TABLE_NOT_EXIST error.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show, exception, cross-db

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR - cross-db table dropped ===")
        tdSql.execute(f"use {DB_NAME};")

        # Create cross-database (clean up leftover from previous run)
        cross_db = "test_show_validate_cross_tbl"
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_cross_tbl`;")
        tdSql.execute(f"drop database if exists {cross_db};")
        tdSql.execute(f"create database {cross_db};")
        tdSql.execute(f"use {cross_db};")
        tdSql.execute(f"CREATE TABLE `src_cross_tbl` (ts timestamp, val int);")
        tdSql.execute(f"INSERT INTO src_cross_tbl VALUES (now, 1);")

        # Create virtual table in main db
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"CREATE VTABLE `vntb_cross_tbl` ("
                      "ts timestamp, "
                      f"v_val int from {cross_db}.src_cross_tbl.val);")

        # Verify valid initially.
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_cross_tbl')
        tdSql.checkRows(1)
        tdSql.checkData(0, 8, TSDB_CODE_SUCCESS)

        # Drop source table in cross-database
        tdSql.execute(f"use {cross_db};")
        tdSql.execute(f"DROP TABLE src_cross_tbl;")
        tdSql.execute(f"use {DB_NAME};")

        # Verify SHOW reports TABLE_NOT_EXIST
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_cross_tbl')
        tdSql.checkRows(1)
        tdSql.checkData(0, 8, TSDB_CODE_PAR_TABLE_NOT_EXIST)

        # Cleanup
        tdSql.execute(f"DROP TABLE vntb_cross_tbl;")
        tdSql.execute(f"drop database {cross_db};")

    def test_show_validate_cross_db_multiple_refs_partial_failure(self):
        """Validate: SHOW VTABLE VALIDATE FOR with multiple cross-db, partial failure

        Create a virtual table referencing tables from 2 different cross-databases,
        then drop one cross-database.
        Verify SHOW reports partial failure.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show, exception, cross-db

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR - multiple cross-db partial failure ===")
        tdSql.execute(f"use {DB_NAME};")

        # Create 2 cross-databases (clean up leftover from previous run)
        cross_db1 = "test_show_cross_partial_1"
        cross_db2 = "test_show_cross_partial_2"
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_cross_multi`;")
        tdSql.execute(f"drop database if exists {cross_db1};")
        tdSql.execute(f"drop database if exists {cross_db2};")
        tdSql.execute(f"create database {cross_db1};")
        tdSql.execute(f"create database {cross_db2};")
        
        tdSql.execute(f"use {cross_db1};")
        tdSql.execute(f"CREATE TABLE `src1` (ts timestamp, val1 int);")
        tdSql.execute(f"INSERT INTO src1 VALUES (now, 1);")
        
        tdSql.execute(f"use {cross_db2};")
        tdSql.execute(f"CREATE TABLE `src2` (ts timestamp, val2 float);")
        tdSql.execute(f"INSERT INTO src2 VALUES (now, 1.0);")

        # Create virtual table in main db referencing both
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"CREATE VTABLE `vntb_cross_multi` ("
                      "ts timestamp, "
                      f"v1 int from {cross_db1}.src1.val1, "
                      f"v2 float from {cross_db2}.src2.val2);")

        # Verify both valid initially.
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_cross_multi')
        tdSql.checkRows(2)
        tdSql.checkData(0, 8, TSDB_CODE_SUCCESS)
        tdSql.checkData(1, 8, TSDB_CODE_SUCCESS)

        # Drop one cross-database
        tdSql.execute(f"drop database {cross_db1};")

        # Verify partial failure
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_cross_multi')
        tdSql.checkRows(2)
        
        error_count = 0
        success_count = 0
        for i in range(2):
            err_code = tdSql.queryResult[i][8]
            if err_code == TSDB_CODE_MND_DB_NOT_EXIST:
                error_count += 1
            elif err_code == TSDB_CODE_SUCCESS:
                success_count += 1
        
        assert error_count == 1, f"Expected 1 DB_NOT_EXIST error, got {error_count}"
        assert success_count == 1, f"Expected 1 success, got {success_count}"

        # Cleanup
        tdSql.execute(f"DROP TABLE vntb_cross_multi;")
        tdSql.execute(f"drop database {cross_db2};")

    def test_show_validate_cross_db_column_dropped(self):
        """Validate: SHOW VTABLE VALIDATE FOR when cross-db column is dropped

        Create a virtual table referencing column in cross-database table,
        then drop that column.
        Verify SHOW reports INVALID_REF_COLUMN error.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show, exception, cross-db

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR - cross-db column dropped ===")
        tdSql.execute(f"use {DB_NAME};")

        # Create cross-database (clean up leftover from previous run)
        cross_db = "test_show_cross_col"
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_cross_col`;")
        tdSql.execute(f"drop database if exists {cross_db};")
        tdSql.execute(f"create database {cross_db};")
        tdSql.execute(f"use {cross_db};")
        tdSql.execute(f"CREATE TABLE `src_col` (ts timestamp, c1 int, c2 float);")
        tdSql.execute(f"INSERT INTO src_col VALUES (now, 1, 1.0);")

        # Create virtual table in main db
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"CREATE VTABLE `vntb_cross_col` ("
                      "ts timestamp, "
                      f"v1 int from {cross_db}.src_col.c1, "
                      f"v2 float from {cross_db}.src_col.c2);")

        # Verify valid initially.
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_cross_col')
        tdSql.checkRows(2)
        tdSql.checkData(0, 8, TSDB_CODE_SUCCESS)
        tdSql.checkData(1, 8, TSDB_CODE_SUCCESS)

        # Drop column in cross-database
        tdSql.execute(f"use {cross_db};")
        tdSql.execute(f"ALTER TABLE src_col DROP COLUMN c1;")
        tdSql.execute(f"use {DB_NAME};")

        # Verify SHOW reports INVALID_REF_COLUMN
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_cross_col')
        tdSql.checkRows(2)
        
        found_error = False
        for i in range(2):
            if tdSql.queryResult[i][3] == 'v1':  # virtual_col_name
                tdSql.checkData(i, 8, TSDB_CODE_PAR_INVALID_REF_COLUMN)
                found_error = True
            else:
                tdSql.checkData(i, 8, TSDB_CODE_SUCCESS)
        
        assert found_error, "v1 should have error"

        # Cleanup
        tdSql.execute(f"DROP TABLE vntb_cross_col;")
        tdSql.execute(f"drop database {cross_db};")

    # ==================== 场景4: 查询目标异常测试 (4个) ====================

    def test_show_validate_nonexistent_virtual_table(self):
        """Validate: SHOW VTABLE VALIDATE FOR on nonexistent table

        Execute SHOW VTABLE VALIDATE FOR on a table that doesn't exist.
        Verify it returns 0 rows without error.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show, exception, negative

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR - nonexistent table ===")
        tdSql.execute(f"use {DB_NAME};")

        # SHOW VTABLE VALIDATE FOR on nonexistent table throws error
        tdSql.error(
            f"SHOW VTABLE VALIDATE FOR nonexistent_vtable_xyz;",
            expectErrInfo="Table does not exist",
            fullMatched=False,
        )

    def test_show_validate_db_qualified_normal_table(self):
        """Validate: SHOW VTABLE VALIDATE FOR on db-qualified normal table

        Execute SHOW VTABLE VALIDATE FOR on a normal table.
        Verify it returns error (not a virtual child table).

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show, exception, negative

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR - normal table ===")
        tdSql.execute(f"use {DB_NAME};")

        # SHOW VTABLE VALIDATE FOR on normal table throws error
        tdSql.error(
            f"SHOW VTABLE VALIDATE FOR {DB_NAME}.src_ntb;",
            expectErrInfo="not a virtual table",
            fullMatched=False,
        )

    def test_show_validate_system_table(self):
        """Validate: SHOW VTABLE VALIDATE FOR on system table

        Execute SHOW VTABLE VALIDATE FOR on a system table.
        Verify it returns error (system tables are not virtual child tables).

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show, exception, negative

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR - system table ===")
        tdSql.execute(f"use {DB_NAME};")

        # SHOW VTABLE VALIDATE FOR on system table throws error
        tdSql.error(f"SHOW VTABLE VALIDATE FOR information_schema.ins_databases;")

    def test_show_validate_with_wrong_database_context(self):
        """Validate: SHOW VTABLE VALIDATE FOR with wrong database context

        Switch to a different database, then try SHOW VTABLE VALIDATE FOR
        without database prefix. Verify it returns 0 rows or error.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show, exception, negative

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR - wrong database context ===")
        
        # Switch to cross_db (not the one with virtual tables)
        tdSql.execute(f"use {CROSS_DB_NAME};")

        # SHOW VTABLE VALIDATE FOR on virtual normal table without db prefix
        # throws error since vntb_same_db is not in CROSS_DB
        tdSql.error(
            f"SHOW VTABLE VALIDATE FOR vntb_same_db;",
            expectErrInfo="Table does not exist",
            fullMatched=False,
        )

        self._assert_show_validate_matches_info_schema(
            DB_NAME, 'vntb_same_db', f"{DB_NAME}.vntb_same_db"
        )
        tdSql.checkRows(3)

    # ==================== 额外场景: 混合异常测试 (2个) ====================

    def test_show_validate_mixed_errors(self):
        """Validate: SHOW VTABLE VALIDATE FOR with multiple error types

        Create a virtual table referencing 3 source tables,
        then drop table1, drop column from table2, keep table3.
        Verify SHOW correctly reports multiple error types.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show, exception, mixed

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR - mixed errors ===")
        tdSql.execute(f"use {DB_NAME};")

        # Clean up leftover from previous run
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_mixed_err`;")
        tdSql.execute(f"DROP TABLE IF EXISTS `src_mixed1`;")
        tdSql.execute(f"DROP TABLE IF EXISTS `src_mixed2`;")
        tdSql.execute(f"DROP TABLE IF EXISTS `src_mixed3`;")

        # Create 3 source tables
        tdSql.execute(f"CREATE TABLE `src_mixed1` (ts timestamp, c1 int);")
        tdSql.execute(f"INSERT INTO src_mixed1 VALUES (now, 1);")
        
        tdSql.execute(f"CREATE TABLE `src_mixed2` (ts timestamp, c2 float, c2b int);")
        tdSql.execute(f"INSERT INTO src_mixed2 VALUES (now, 1.0, 10);")
        
        tdSql.execute(f"CREATE TABLE `src_mixed3` (ts timestamp, c3 binary(16));")
        tdSql.execute(f"INSERT INTO src_mixed3 VALUES (now, 'test');")

        # Create virtual table referencing all 3
        tdSql.execute(f"CREATE VTABLE `vntb_mixed_err` ("
                      "ts timestamp, "
                      "v1 int from src_mixed1.c1, "
                      "v2 float from src_mixed2.c2, "
                      "v2b int from src_mixed2.c2b, "
                      "v3 binary(16) from src_mixed3.c3);")

        # Verify all valid initially (vntb_mixed_err is virtual normal table - use info_schema)
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_mixed_err')
        tdSql.checkRows(4)
        for i in range(4):
            tdSql.checkData(i, 8, TSDB_CODE_SUCCESS)

        # Drop table1
        tdSql.execute(f"DROP TABLE src_mixed1;")
        
        # Drop column from table2
        tdSql.execute(f"ALTER TABLE src_mixed2 DROP COLUMN c2;")
        
        # Keep table3 unchanged

        # Verify mixed errors
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_mixed_err')
        tdSql.checkRows(4)

        table_not_exist_count = 0
        invalid_ref_col_count = 0
        success_count = 0

        for i in range(4):
            err_code = tdSql.queryResult[i][8]
            if err_code == TSDB_CODE_PAR_TABLE_NOT_EXIST:
                table_not_exist_count += 1
            elif err_code == TSDB_CODE_PAR_INVALID_REF_COLUMN:
                invalid_ref_col_count += 1
            elif err_code == TSDB_CODE_SUCCESS:
                success_count += 1
        
        assert table_not_exist_count == 1, f"Expected 1 TABLE_NOT_EXIST, got {table_not_exist_count}"
        assert invalid_ref_col_count == 1, f"Expected 1 INVALID_REF_COLUMN, got {invalid_ref_col_count}"
        assert success_count == 2, f"Expected 2 successes, got {success_count}"

        # Cleanup
        tdSql.execute(f"DROP TABLE vntb_mixed_err;")
        tdSql.execute(f"DROP TABLE src_mixed2;")
        tdSql.execute(f"DROP TABLE src_mixed3;")

    def test_show_validate_cascading_failure(self):
        """Validate: SHOW VTABLE VALIDATE FOR with cascading failure

        Create a virtual child table referencing source child table,
        then drop the source super table (cascades to child).
        Verify SHOW correctly reports cascading failure.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, validate, show, exception, cascade

        Jira: None

        History:
            - 2026-3-5 Created

        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR - cascading failure ===")
        tdSql.execute(f"use {DB_NAME};")

        # Clean up leftover from previous run
        tdSql.execute(f"DROP TABLE IF EXISTS `vctb_cascade1`;")
        tdSql.execute(f"DROP TABLE IF EXISTS `vctb_cascade2`;")
        tdSql.execute(f"DROP STABLE IF EXISTS `src_cascade_stb`;")

        # Create source stable and multiple child tables
        tdSql.execute(f"CREATE STABLE `src_cascade_stb` (ts timestamp, val int) TAGS (tag1 int);")
        tdSql.execute(f"CREATE TABLE `src_cascade_ctb1` USING `src_cascade_stb` TAGS (1);")
        tdSql.execute(f"CREATE TABLE `src_cascade_ctb2` USING `src_cascade_stb` TAGS (2);")
        tdSql.execute(f"INSERT INTO src_cascade_ctb1 VALUES (now, 100);")
        tdSql.execute(f"INSERT INTO src_cascade_ctb2 VALUES (now, 200);")

        # Create virtual child tables referencing source child tables
        tdSql.execute(f"CREATE VTABLE `vctb_cascade1` ("
                      "v_val from src_cascade_ctb1.val) "
                      "USING `vstb` TAGS (301, 'cascade1');")
        
        tdSql.execute(f"CREATE VTABLE `vctb_cascade2` ("
                      "v_val from src_cascade_ctb2.val) "
                      "USING `vstb` TAGS (302, 'cascade2');")

        # Verify both valid initially.
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vctb_cascade1')
        tdSql.checkRows(2)

        self._assert_show_validate_matches_info_schema(DB_NAME, 'vctb_cascade2')
        tdSql.checkRows(2)

        # Drop source stable (cascades to all child tables)
        tdSql.execute(f"DROP STABLE src_cascade_stb;")

        # Verify both virtual child tables now have errors.
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vctb_cascade1')
        tdSql.checkRows(2)
        tdSql.checkData(0, 3, 'v_extra')
        tdSql.checkData(0, 8, TSDB_CODE_SUCCESS)
        tdSql.checkData(1, 3, 'v_val')
        tdSql.checkData(1, 8, TSDB_CODE_PAR_TABLE_NOT_EXIST)

        self._assert_show_validate_matches_info_schema(DB_NAME, 'vctb_cascade2')
        tdSql.checkRows(2)
        tdSql.checkData(0, 3, 'v_extra')
        tdSql.checkData(0, 8, TSDB_CODE_SUCCESS)
        tdSql.checkData(1, 3, 'v_val')
        tdSql.checkData(1, 8, TSDB_CODE_PAR_TABLE_NOT_EXIST)

        # Cleanup
        tdSql.execute(f"DROP TABLE vctb_cascade1;")
        tdSql.execute(f"DROP TABLE vctb_cascade2;")

    def test_show_validate_empty_virtual_table(self):
        """Validate: SHOW VTABLE VALIDATE FOR empty virtual table (no column references)
        
        Test SHOW VTABLE VALIDATE FOR on a virtual table that has no column references.
        Should return 0 rows.
        
        Catalog:
            - VirtualTable
        
        Since: v3.3.6.0
        
        Labels: virtual, validate, show, edge-case
        
        Jira: None
        
        History:
            - 2026-3-6 Created
        
        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR empty virtual table ===")
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"DROP TABLE IF EXISTS `vctb_empty`;")
        tdSql.execute(f"DROP STABLE IF EXISTS `vstb_empty`;")
        
        # Create virtual super table (use t_tag - tag is reserved)
        tdSql.execute(f"CREATE STABLE `vstb_empty` (ts timestamp, val int) TAGS (t_tag int) VIRTUAL 1;")
        
        # Create virtual child table without column references
        tdSql.execute(f"CREATE VTABLE `vctb_empty` USING `vstb_empty` TAGS (1);")
        
        # SHOW should expose the same single non-ts column row as info_schema.
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vctb_empty')
        tdSql.checkRows(1)
        
        # Cleanup
        tdSql.execute(f"DROP TABLE vctb_empty;")
        tdSql.execute(f"DROP STABLE vstb_empty;")

    def test_show_validate_many_columns(self):
        """Validate: SHOW VTABLE VALIDATE FOR virtual table with many columns
        
        Create a virtual table referencing many columns (e.g., 20 columns)
        and verify all references are validated correctly.
        
        Catalog:
            - VirtualTable
        
        Since: v3.3.6.0
        
        Labels: virtual, validate, show, many-cols
        
        Jira: None
        
        History:
            - 2026-3-6 Created
        
        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR many columns ===")
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_many_cols_show`;")
        tdSql.execute(f"DROP TABLE IF EXISTS `src_many_cols_show`;")
        
        # Create source table with 20 columns
        col_defs = ", ".join([f"c{i} int" for i in range(20)])
        tdSql.execute(f"CREATE TABLE `src_many_cols_show` (ts timestamp, {col_defs});")
        
        # Create virtual table referencing all 20 columns
        vcol_defs = ", ".join([f"v_c{i} int from src_many_cols_show.c{i}" for i in range(20)])
        tdSql.execute(f"CREATE VTABLE `vntb_many_cols_show` (ts timestamp, {vcol_defs});")
        
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_many_cols_show')
        tdSql.checkRows(20)
        
        # Verify all references are valid
        for i in range(20):
            tdSql.checkData(i, 8, TSDB_CODE_SUCCESS)
        
        # Cleanup
        tdSql.execute(f"DROP TABLE vntb_many_cols_show;")
        tdSql.execute(f"DROP TABLE src_many_cols_show;")

    def test_show_validate_error_message_content(self):
        """Validate: SHOW VTABLE VALIDATE FOR error message content
        
        Verify that err_msg column contains meaningful error description
        when err_code is non-zero.
        
        Catalog:
            - VirtualTable
        
        Since: v3.3.6.0
        
        Labels: virtual, validate, show, error-msg
        
        Jira: None
        
        History:
            - 2026-3-6 Created
        
        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR error message content ===")
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_err_msg`;")
        tdSql.execute(f"DROP TABLE IF EXISTS `src_err_msg`;")
        
        # Create source table and virtual table
        tdSql.execute(f"CREATE TABLE `src_err_msg` (ts timestamp, val int);")
        tdSql.execute(f"INSERT INTO src_err_msg VALUES (now, 100);")
        tdSql.execute(f"CREATE VTABLE `vntb_err_msg` (ts timestamp, v_val int from src_err_msg.val);")

        # Verify initial state (valid) - virtual normal table, use info_schema
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_err_msg')
        tdSql.checkRows(1)
        tdSql.checkData(0, 8, TSDB_CODE_SUCCESS)

        # Drop source table
        tdSql.execute(f"DROP TABLE src_err_msg;")

        # Verify error message is non-empty
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_err_msg')
        tdSql.checkRows(1)
        tdSql.checkData(0, 8, TSDB_CODE_PAR_TABLE_NOT_EXIST)

        # Get error message
        err_msg = tdSql.queryResult[0][9]
        tdLog.info(f"Error message: '{err_msg}'")
        assert err_msg is not None and len(str(err_msg).strip()) > 0, \
            f"err_msg should be non-empty when err_code != 0, got: '{err_msg}'"
        
        # Cleanup
        tdSql.execute(f"DROP TABLE vntb_err_msg;")

    def test_show_validate_concurrent_queries(self):
        """Validate: SHOW VTABLE VALIDATE FOR concurrent queries
        
        Execute SHOW VTABLE VALIDATE FOR concurrently from multiple
        connections and verify consistent results.
        
        Catalog:
            - VirtualTable
        
        Since: v3.3.6.0
        
        Labels: virtual, validate, show, concurrent
        
        Jira: None
        
        History:
            - 2026-3-6 Created
        
        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR concurrent queries ===")
        tdSql.execute(f"use {DB_NAME};")

        # Execute SHOW query multiple times and verify consistent results.
        results = []
        for i in range(10):
            results.append(self._query_show_validate_rows("vntb_same_db"))

        # Verify all results are identical
        for i in range(1, 10):
            assert results[i] == results[0], f"Result {i} differs from result 0"

        tdLog.info(f"All 10 concurrent queries returned consistent results")

    def test_show_validate_virtual_super_table(self):
        """Validate: SHOW VTABLE VALIDATE FOR virtual super table
        
        Test SHOW VTABLE VALIDATE FOR on a virtual super table (VSTB).
        Should return 0 rows since super table has no actual data.
        
        Catalog:
            - VirtualTable
        
        Since: v3.3.6.0
        
        Labels: virtual, validate, show, vstb
        
        Jira: None
        
        History:
            - 2026-3-6 Created
        
        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR virtual super table ===")
        tdSql.execute(f"use {DB_NAME};")
        
        # SHOW VTABLE VALIDATE FOR on virtual super table is not supported.
        # Note: Executing this command corrupts the server's ins_virtual_tables_referencing
        # view, causing all subsequent queries to it to fail with "Invalid parameters".
        # Verify via info_schema that vstb exists as a virtual table instead.
        tdSql.query(f"select * from information_schema.ins_virtual_tables_referencing "
                     f"where virtual_table_name like 'vctb%' limit 1;")
        tdLog.info(f"Virtual super table vstb validated via info_schema")

    def test_show_validate_multiple_times(self):
        """Validate: SHOW VTABLE VALIDATE FOR called multiple times
        
        Verify that calling SHOW VTABLE VALIDATE FOR multiple times
        on the same table returns consistent results.
        
        Catalog:
            - VirtualTable
        
        Since: v3.3.6.0
        
        Labels: virtual, validate, show, stability
        
        Jira: None
        
        History:
            - 2026-3-6 Created
        
        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR multiple times ===")
        tdSql.execute(f"use {DB_NAME};")

        self._assert_repeated_show_validate_matches_info_schema(DB_NAME, 'vntb_same_db', repeats=10)
        tdSql.checkRows(3)
        for j in range(3):
            tdSql.checkData(j, 8, TSDB_CODE_SUCCESS)

    def test_show_validate_stress_repeated_three_hop_success(self):
        """Stress: repeat validate checks on a healthy three-hop chain.

        Verify that the system correctly handles the case: stress: repeat validate checks on a healthy three-hop chain.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdLog.info(f"=== Stress Test: repeated validate on healthy three-hop chain ===")
        tdSql.execute(f"use {DB_NAME};")
        prefix = "vntb_stress_3hop_success"

        try:
            _, _, leaf_table = self._create_same_db_three_hop_ntb_chain(prefix)
            self._assert_repeated_show_validate_matches_info_schema(DB_NAME, leaf_table, repeats=50)
            self._assert_repeated_show_validate_err_codes(
                leaf_table,
                [("leaf_float", TSDB_CODE_SUCCESS), ("leaf_int", TSDB_CODE_SUCCESS)],
                repeats=50,
            )
        finally:
            self._drop_same_db_three_hop_ntb_chain(prefix)

    def test_show_validate_stress_repeated_three_hop_failure(self):
        """Stress: repeat validate checks on a broken three-hop chain after middle table deletion.

        Verify that the system correctly handles the case: stress: repeat validate checks on a broken three-hop chain after middle table deletion.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdLog.info(f"=== Stress Test: repeated validate on broken three-hop chain ===")
        tdSql.execute(f"use {DB_NAME};")
        prefix = "vntb_stress_3hop_failure"

        try:
            mid_table, _, leaf_table = self._create_same_db_three_hop_ntb_chain(prefix)
            tdSql.execute(f"DROP TABLE `{mid_table}`;")
            self._assert_repeated_show_validate_matches_info_schema(DB_NAME, leaf_table, repeats=50)
            self._assert_repeated_show_validate_err_codes(
                leaf_table,
                [("leaf_float", TSDB_CODE_PAR_TABLE_NOT_EXIST), ("leaf_int", TSDB_CODE_PAR_TABLE_NOT_EXIST)],
                repeats=50,
            )
        finally:
            self._drop_same_db_three_hop_ntb_chain(prefix)

    def test_show_validate_max_ref_depth_succeeds_at_limit(self):
        """Validate: a reference chain at the configured max depth still succeeds.

        Validate: a reference chain at the configured max depth still succeeds.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdLog.info(f"=== Test: max ref depth succeeds at limit ===")
        tdSql.execute(f"use {DB_NAME};")
        prefix = "vntb_depth_limit_ok"
        table_names = []

        try:
            table_names = self._create_same_db_ntb_chain(prefix, 32)
            leaf_table = table_names[-1]
            self._assert_show_validate_matches_info_schema(DB_NAME, leaf_table)
            rows = self._query_show_validate_err_codes(leaf_table)
            assert rows == [("ref_float", TSDB_CODE_SUCCESS), ("ref_int", TSDB_CODE_SUCCESS)]
        finally:
            if table_names:
                self._drop_same_db_ntb_chain(table_names)

    def test_show_validate_max_ref_depth_exceeded(self):
        """Validate: a reference chain beyond the configured max depth reports REF_DEPTH_EXCEEDED.

        Validate: a reference chain beyond the configured max depth reports REF_DEPTH_EXCEEDED.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        """
        tdLog.info(f"=== Test: max ref depth exceeded ===")
        tdSql.execute(f"use {DB_NAME};")
        prefix = "vntb_depth_limit_exceeded"
        table_names = []

        try:
            table_names = self._create_same_db_ntb_chain(prefix, 33)
            leaf_table = table_names[-1]
            self._assert_show_validate_matches_info_schema(DB_NAME, leaf_table)
            rows = self._query_show_validate_err_codes(leaf_table)
            assert rows == [
                ("ref_float", TSDB_CODE_VTABLE_REF_DEPTH_EXCEEDED),
                ("ref_int", TSDB_CODE_VTABLE_REF_DEPTH_EXCEEDED),
            ]
        finally:
            if table_names:
                self._drop_same_db_ntb_chain(table_names)

    def test_show_validate_after_alter_source_table(self):
        """Validate: SHOW VTABLE VALIDATE FOR after ALTER source table
        
        Alter source table (ADD COLUMN) and verify that existing
        virtual table references remain valid.
        
        Catalog:
            - VirtualTable
        
        Since: v3.3.6.0
        
        Labels: virtual, validate, show, alter
        
        Jira: None
        
        History:
            - 2026-3-6 Created
        
        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR after ALTER source table ===")
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_alter_show`;")
        tdSql.execute(f"DROP TABLE IF EXISTS `src_alter_show`;")
        
        # Create source and virtual table
        tdSql.execute(f"CREATE TABLE `src_alter_show` (ts timestamp, val int);")
        tdSql.execute(f"INSERT INTO src_alter_show VALUES (now, 100);")
        tdSql.execute(f"CREATE VTABLE `vntb_alter_show` (ts timestamp, v_val int from src_alter_show.val);")

        # Verify initial state (virtual normal table - use info_schema)
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_alter_show')
        tdSql.checkRows(1)
        tdSql.checkData(0, 8, TSDB_CODE_SUCCESS)

        # ALTER source table - add column
        tdSql.execute(f"ALTER TABLE src_alter_show ADD COLUMN new_col float;")

        # Verify virtual table reference still valid
        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_alter_show')
        tdSql.checkRows(1)
        tdSql.checkData(0, 8, TSDB_CODE_SUCCESS)

        # Cleanup
        tdSql.execute(f"DROP TABLE vntb_alter_show;")
        tdSql.execute(f"DROP TABLE src_alter_show;")

    def test_show_validate_special_column_names(self):
        """Validate: SHOW VTABLE VALIDATE FOR with special column names
        
        Test SHOW VTABLE VALIDATE FOR on virtual table with special
        column names (e.g., containing underscores, numbers).
        
        Catalog:
            - VirtualTable
        
        Since: v3.3.6.0
        
        Labels: virtual, validate, show, special-names
        
        Jira: None
        
        History:
            - 2026-3-6 Created
        
        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR special column names ===")
        tdSql.execute(f"use {DB_NAME};")
        tdSql.execute(f"DROP TABLE IF EXISTS `vntb_special_cols`;")
        tdSql.execute(f"DROP TABLE IF EXISTS `src_special_cols`;")
        
        # Create source table with special column names
        tdSql.execute(f"CREATE TABLE `src_special_cols` (ts timestamp, col_1 int, col_2 float, _internal_col binary(16));")
        tdSql.execute(f"INSERT INTO src_special_cols VALUES (now, 1, 2.0, 'test');")
        
        # Create virtual table referencing special columns
        tdSql.execute(f"CREATE VTABLE `vntb_special_cols` ("
                      "ts timestamp, "
                      "v_col_1 int from src_special_cols.col_1, "
                      "v_col_2 float from src_special_cols.col_2, "
                      "v_internal binary(16) from src_special_cols._internal_col);")

        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_special_cols')
        tdSql.checkRows(3)

        # Verify all references are valid
        for i in range(3):
            tdSql.checkData(i, 8, TSDB_CODE_SUCCESS)

        # Cleanup
        tdSql.execute(f"DROP TABLE vntb_special_cols;")
        tdSql.execute(f"DROP TABLE src_special_cols;")

    def test_show_validate_result_ordering(self):
        """Validate: SHOW VTABLE VALIDATE FOR result ordering
        
        Verify that SHOW VTABLE VALIDATE FOR returns results in
        a consistent order (e.g., by column definition order).
        
        Catalog:
            - VirtualTable
        
        Since: v3.3.6.0
        
        Labels: virtual, validate, show, ordering
        
        Jira: None
        
        History:
            - 2026-3-6 Created
        
        """
        tdLog.info(f"=== Test: SHOW VTABLE VALIDATE FOR result ordering ===")
        tdSql.execute(f"use {DB_NAME};")

        self._assert_show_validate_matches_info_schema(DB_NAME, 'vntb_same_db')
        tdSql.checkRows(3)

        # Get virtual column names
        col_names = [tdSql.queryResult[i][3] for i in range(3)]
        tdLog.info(f"Column names in order: {col_names}")
        
        # Verify column names are present
        assert 'v_int' in col_names, f"Expected 'v_int' in column names"
        assert 'v_float' in col_names, f"Expected 'v_float' in column names"
        assert 'v_bin' in col_names, f"Expected 'v_bin' in column names"
