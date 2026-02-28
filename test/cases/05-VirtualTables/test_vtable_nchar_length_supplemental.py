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
"""
Supplemental test for NCHAR/BINARY actual length (no truncation) feature.

This test covers scenarios not fully tested in existing test_vtable_nchar_length.py:
  - Edge cases: empty string, single char, NULL handling
  - Unicode special characters: emoji, symbols, mixed encoding
  - VARCHAR and VARBINARY types
  - Virtual super table with NCHAR/BINARY columns
  - More string function combinations
  - Data consistency validation
"""

from new_test_framework.utils import tdLog, tdSql, etool, tdCom


class TestVtableNcharLengthSupplemental:

    DB_NAME = "test_vtb_nchar_supp"

    def setup_class(cls):
        """Setup test environment."""
        tdLog.info("=== Setup: Creating databases and tables for supplemental tests ===")
        
        tdSql.execute(f"DROP DATABASE IF EXISTS {cls.DB_NAME};")
        tdSql.execute(f"CREATE DATABASE {cls.DB_NAME} KEEP 3650 DURATION 10 BUFFER 16;")
        tdSql.execute(f"USE {cls.DB_NAME};")

        # Source table for basic tests
        tdSql.execute(
            "CREATE TABLE src_basic ("
            "ts TIMESTAMP, bin_col BINARY(64), nch_col NCHAR(64), "
            "vc_col VARCHAR(64), vb_col VARBINARY(64))"
        )
        tdSql.execute(
            "INSERT INTO src_basic VALUES "
            "('2024-01-01 00:00:00', 'Hello World - Test String', "
            "'你好世界 - 测试字符串', 'VARCHAR Test', 'VARBINARY Test')"
        )
        tdSql.execute(
            "INSERT INTO src_basic VALUES "
            "('2024-01-01 00:00:01', 'short', '短', 'a', 'b')"
        )
        tdSql.execute(
            "INSERT INTO src_basic VALUES "
            "('2024-01-01 00:00:02', NULL, NULL, NULL, NULL)"
        )

        # Virtual table with equal length
        tdSql.execute(
            f"CREATE VTABLE {cls.DB_NAME}.vtb_basic ("
            "ts TIMESTAMP, "
            "bin_col BINARY(64) FROM src_basic.bin_col, "
            "nch_col NCHAR(64) FROM src_basic.nch_col, "
            "vc_col VARCHAR(64) FROM src_basic.vc_col, "
            "vb_col VARBINARY(64) FROM src_basic.vb_col)"
        )

        # Source table for edge cases
        tdSql.execute(
            "CREATE TABLE src_edge ("
            "ts TIMESTAMP, bin_col BINARY(256), nch_col NCHAR(256))"
        )
        # Empty string
        tdSql.execute(
            "INSERT INTO src_edge VALUES ('2024-01-01 00:00:00', '', '')"
        )
        # Single char
        tdSql.execute(
            "INSERT INTO src_edge VALUES ('2024-01-01 00:00:01', 'a', '中')"
        )
        # NULL
        tdSql.execute(
            "INSERT INTO src_edge VALUES ('2024-01-01 00:00:02', NULL, NULL)"
        )

        # Virtual table for edge cases
        tdSql.execute(
            f"CREATE VTABLE {cls.DB_NAME}.vtb_edge ("
            "ts TIMESTAMP, "
            "bin_col BINARY(256) FROM src_edge.bin_col, "
            "nch_col NCHAR(256) FROM src_edge.nch_col)"
        )

        # Source table for Unicode special characters
        tdSql.execute(
            "CREATE TABLE src_unicode ("
            "ts TIMESTAMP, bin_col BINARY(128), nch_col NCHAR(128))"
        )
        tdSql.execute(
            "INSERT INTO src_unicode VALUES "
            "('2024-01-01 00:00:00', 'emoji_test', '🎉🎊🎈🎁')"
        )
        tdSql.execute(
            "INSERT INTO src_unicode VALUES "
            "('2024-01-01 00:00:01', 'symbols', '★☆♠♣♥♦')"
        )
        tdSql.execute(
            "INSERT INTO src_unicode VALUES "
            "('2024-01-01 00:00:02', 'mixed', 'Hello你好World世界')"
        )

        # Virtual table for Unicode
        tdSql.execute(
            f"CREATE VTABLE {cls.DB_NAME}.vtb_unicode ("
            "ts TIMESTAMP, "
            "bin_col BINARY(128) FROM src_unicode.bin_col, "
            "nch_col NCHAR(128) FROM src_unicode.nch_col)"
        )

        # Source super table
        tdSql.execute(
            "CREATE STABLE src_stb ("
            "ts TIMESTAMP, bin_col BINARY(64), val INT) "
            "TAGS (region NCHAR(16))"
        )
        tdSql.execute(
            "CREATE TABLE src_ct1 USING src_stb TAGS ('east')"
        )
        tdSql.execute(
            "CREATE TABLE src_ct2 USING src_stb TAGS ('west')"
        )
        tdSql.execute(
            "INSERT INTO src_ct1 VALUES "
            "('2024-01-01 00:00:00', 'East Region Data String', 10)"
        )
        tdSql.execute(
            "INSERT INTO src_ct2 VALUES "
            "('2024-01-01 00:00:00', 'West Region Data String', 20)"
        )

        # Virtual super table
        tdSql.execute(
            "CREATE STABLE vstb ("
            "ts TIMESTAMP, bin_col BINARY(64), val INT) "
            "TAGS (region NCHAR(16)) VIRTUAL 1"
        )
        tdSql.execute(
            "CREATE VTABLE vct1 "
            "(bin_col FROM src_ct1.bin_col, val FROM src_ct1.val) "
            "USING vstb TAGS ('east')"
        )
        tdSql.execute(
            "CREATE VTABLE vct2 "
            "(bin_col FROM src_ct2.bin_col, val FROM src_ct2.val) "
            "USING vstb TAGS ('west')"
        )

        tdLog.info("=== Setup complete ===")

    # ===================== EDGE CASE TESTS =====================

    def test_edge_empty_string(self):
        """Edge case: empty string handling

        Verify that empty strings are handled correctly in virtual tables.
        Empty strings should have LENGTH=0 and CHAR_LENGTH=0.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, edge, empty_string

        Jira: None

        History:
            - 2026-2-28 Created
        """
        tdLog.info("=== Test: Empty string handling ===")
        db = self.DB_NAME
        
        tdSql.query(f"SELECT bin_col, LENGTH(bin_col), nch_col, CHAR_LENGTH(nch_col) "
                    f"FROM {db}.vtb_edge WHERE ts = '2024-01-01 00:00:00';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '')
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(0, 2, '')
        tdSql.checkData(0, 3, 0)

    def test_edge_single_char(self):
        """Edge case: single character handling

        Verify that single characters are handled correctly in virtual tables.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, edge, single_char

        Jira: None

        History:
            - 2026-2-28 Created
        """
        tdLog.info("=== Test: Single character handling ===")
        db = self.DB_NAME
        
        tdSql.query(f"SELECT bin_col, LENGTH(bin_col), nch_col, CHAR_LENGTH(nch_col) "
                    f"FROM {db}.vtb_edge WHERE ts = '2024-01-01 00:00:01';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'a')
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, '中')
        tdSql.checkData(0, 3, 1)

    def test_edge_null_value(self):
        """Edge case: NULL value handling

        Verify that NULL values are handled correctly in virtual tables.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, edge, null

        Jira: None

        History:
            - 2026-2-28 Created
        """
        tdLog.info("=== Test: NULL value handling ===")
        db = self.DB_NAME
        
        tdSql.query(f"SELECT bin_col, LENGTH(bin_col), nch_col, CHAR_LENGTH(nch_col) "
                    f"FROM {db}.vtb_edge WHERE ts = '2024-01-01 00:00:02';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, None)

    # ===================== UNICODE TESTS =====================

    def test_unicode_emoji(self):
        """Unicode: emoji characters

        Verify that emoji characters are handled correctly in virtual tables.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, unicode, emoji

        Jira: None

        History:
            - 2026-2-28 Created
        """
        tdLog.info("=== Test: Emoji characters ===")
        db = self.DB_NAME
        
        tdSql.query(f"SELECT nch_col, CHAR_LENGTH(nch_col) "
                    f"FROM {db}.vtb_unicode WHERE bin_col = 'emoji_test';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '🎉🎊🎈🎁')
        tdSql.checkData(0, 1, 4)

    def test_unicode_symbols(self):
        """Unicode: special symbols

        Verify that special symbols are handled correctly in virtual tables.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, unicode, symbols

        Jira: None

        History:
            - 2026-2-28 Created
        """
        tdLog.info("=== Test: Special symbols ===")
        db = self.DB_NAME
        
        tdSql.query(f"SELECT nch_col, CHAR_LENGTH(nch_col) "
                    f"FROM {db}.vtb_unicode WHERE bin_col = 'symbols';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '★☆♠♣♥♦')
        tdSql.checkData(0, 1, 6)

    def test_unicode_mixed(self):
        """Unicode: mixed ASCII and CJK characters

        Verify that mixed ASCII and CJK characters are handled correctly.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, unicode, mixed

        Jira: None

        History:
            - 2026-2-28 Created
        """
        tdLog.info("=== Test: Mixed ASCII and CJK ===")
        db = self.DB_NAME
        
        tdSql.query(f"SELECT nch_col, CHAR_LENGTH(nch_col) "
                    f"FROM {db}.vtb_unicode WHERE bin_col = 'mixed';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'Hello你好World世界')
        tdSql.checkData(0, 1, 14)

    # ===================== VARCHAR/VARBINARY TESTS =====================

    def test_varchar_no_truncation(self):
        """VARCHAR: no truncation in virtual table

        Verify that VARCHAR data is not truncated in virtual tables.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, varchar

        Jira: None

        History:
            - 2026-2-28 Created
        """
        tdLog.info("=== Test: VARCHAR no truncation ===")
        db = self.DB_NAME
        
        tdSql.query(f"SELECT vc_col, LENGTH(vc_col) "
                    f"FROM {db}.vtb_basic WHERE vc_col IS NOT NULL ORDER BY ts;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 'VARCHAR Test')
        tdSql.checkData(0, 1, 12)

    def test_varbinary_no_truncation(self):
        """VARBINARY: no truncation in virtual table

        Verify that VARBINARY data is not truncated in virtual tables.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, varbinary

        Jira: None

        History:
            - 2026-2-28 Created
        """
        tdLog.info("=== Test: VARBINARY no truncation ===")
        db = self.DB_NAME
        
        # VARBINARY data is stored as bytes, check it exists and has correct length
        tdSql.query(f"SELECT vb_col, LENGTH(vb_col) "
                    f"FROM {db}.vtb_basic WHERE vb_col IS NOT NULL ORDER BY ts;")
        tdSql.checkRows(2)
        # First row has 'VARBINARY Test' = 14 bytes
        tdSql.checkData(0, 1, 14)

    # ===================== VIRTUAL SUPER TABLE TESTS =====================

    def test_vstb_data_consistency(self):
        """Virtual super table: data consistency

        Verify that virtual super table returns correct data without truncation.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, super_table, consistency

        Jira: None

        History:
            - 2026-2-28 Created
        """
        tdLog.info("=== Test: Virtual super table data consistency ===")
        db = self.DB_NAME
        
        # Query virtual super table
        tdSql.query(f"SELECT bin_col, LENGTH(bin_col), val, region "
                    f"FROM {db}.vstb ORDER BY region;")
        tdSql.checkRows(2)
        
        # Check east region
        tdSql.checkData(0, 0, 'East Region Data String')
        tdSql.checkData(0, 1, 23)
        tdSql.checkData(0, 2, 10)
        tdSql.checkData(0, 3, 'east')
        
        # Check west region
        tdSql.checkData(1, 0, 'West Region Data String')
        tdSql.checkData(1, 1, 23)
        tdSql.checkData(1, 2, 20)
        tdSql.checkData(1, 3, 'west')

    def test_vstb_aggregate(self):
        """Virtual super table: aggregate functions

        Verify aggregate functions work correctly on virtual super tables.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, super_table, aggregate

        Jira: None

        History:
            - 2026-2-28 Created
        """
        tdLog.info("=== Test: Virtual super table aggregate ===")
        db = self.DB_NAME
        
        tdSql.query(f"SELECT COUNT(*), COUNT(bin_col), SUM(val), "
                    f"FIRST(bin_col), LAST(bin_col) FROM {db}.vstb;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 30)  # 10 + 20

    # ===================== DATA CONSISTENCY TESTS =====================

    def test_consistency_source_vs_vtable(self):
        """Consistency: source table vs virtual table

        Verify that data is identical between source and virtual table.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, consistency

        Jira: None

        History:
            - 2026-2-28 Created
        """
        tdLog.info("=== Test: Data consistency source vs vtable ===")
        db = self.DB_NAME
        
        # Query source table
        tdSql.query(f"SELECT bin_col, nch_col FROM {db}.src_basic ORDER BY ts;")
        src_data = [(tdSql.getData(i, 0), tdSql.getData(i, 1)) 
                    for i in range(tdSql.queryRows)]
        
        # Query virtual table
        tdSql.query(f"SELECT bin_col, nch_col FROM {db}.vtb_basic ORDER BY ts;")
        vtb_data = [(tdSql.getData(i, 0), tdSql.getData(i, 1)) 
                    for i in range(tdSql.queryRows)]
        
        assert src_data == vtb_data, \
            f"Data mismatch: src={src_data}, vtb={vtb_data}"

    # ===================== STRING FUNCTION COMBINATION TESTS =====================

    def test_string_func_on_unicode(self):
        """String function: operations on Unicode data

        Verify string functions work correctly on Unicode data.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, string, unicode

        Jira: None

        History:
            - 2026-2-28 Created
        """
        tdLog.info("=== Test: String functions on Unicode ===")
        db = self.DB_NAME
        
        # SUBSTR on Unicode
        tdSql.query(f"SELECT SUBSTR(nch_col, 1, 3) FROM {db}.vtb_unicode "
                    f"WHERE bin_col = 'mixed' ORDER BY ts;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'Hel')

    def test_string_func_concat(self):
        """String function: CONCAT with NCHAR/BINARY

        Verify CONCAT works correctly with NCHAR/BINARY columns.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, string, concat

        Jira: None

        History:
            - 2026-2-28 Created
        """
        tdLog.info("=== Test: CONCAT function ===")
        db = self.DB_NAME
        
        tdSql.query(f"SELECT CONCAT(bin_col, '-suffix') FROM {db}.vtb_basic "
                    f"WHERE bin_col IS NOT NULL ORDER BY ts LIMIT 1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'Hello World - Test String-suffix')

    # ===================== NULL HANDLING COMBINATION TESTS =====================

    def test_null_handling_with_functions(self):
        """NULL handling: with string functions

        Verify string functions handle NULL values correctly.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, null, string

        Jira: None

        History:
            - 2026-2-28 Created
        """
        tdLog.info("=== Test: NULL handling with string functions ===")
        db = self.DB_NAME
        
        # String function on NULL should return NULL
        tdSql.query(f"SELECT LOWER(bin_col) FROM {db}.vtb_basic ORDER BY ts;")
        tdSql.checkRows(3)
        tdSql.checkData(2, 0, None)  # Third row is NULL

    def test_null_handling_aggregate(self):
        """NULL handling: with aggregate functions

        Verify aggregate functions handle NULL values correctly.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, null, aggregate

        Jira: None

        History:
            - 2026-2-28 Created
        """
        tdLog.info("=== Test: NULL handling with aggregate ===")
        db = self.DB_NAME
        
        # COUNT should exclude NULLs
        tdSql.query(f"SELECT COUNT(*), COUNT(bin_col), COUNT(nch_col) "
                    f"FROM {db}.vtb_basic;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)  # COUNT(*) includes all rows
        tdSql.checkData(0, 1, 2)  # COUNT(bin_col) excludes NULL
        tdSql.checkData(0, 2, 2)  # COUNT(nch_col) excludes NULL
