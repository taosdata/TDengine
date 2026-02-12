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
import os


class TestVtableNcharLength:

    DB_NAME = "test_vtable_nchar_len"

    def setup_class(cls):
        tdLog.info(f"prepare org tables for nchar/binary length test.")

        tdSql.execute(f"drop database if exists {cls.DB_NAME};")
        tdSql.execute(f"create database {cls.DB_NAME} vgroups 2;")
        tdSql.execute(f"use {cls.DB_NAME};")

        # Source table with binary(32) and nchar(32) columns
        tdSql.execute("CREATE TABLE `src_ntb_32` ("
                      "ts timestamp, "
                      "binary_col binary(32), "
                      "nchar_col nchar(32), "
                      "varchar_col varchar(32), "
                      "int_col int)")

        # Insert data with various lengths
        tdSql.execute("INSERT INTO src_ntb_32 VALUES "
                      "('2024-01-01 00:00:00.000', 'Shanghai - Los Angles', "
                      "'圣克拉拉 - Santa Clara', 'Hello World Test', 1)")
        tdSql.execute("INSERT INTO src_ntb_32 VALUES "
                      "('2024-01-01 00:00:01.000', 'short', '短', 'a', 2)")
        tdSql.execute("INSERT INTO src_ntb_32 VALUES "
                      "('2024-01-01 00:00:02.000', 'Palo Alto - Mountain View', "
                      "'库比蒂诺 - Cupertino City', 'Medium Length Str', 3)")
        tdSql.execute("INSERT INTO src_ntb_32 VALUES "
                      "('2024-01-01 00:00:03.000', NULL, NULL, NULL, 4)")
        tdSql.execute("INSERT INTO src_ntb_32 VALUES "
                      "('2024-01-01 00:00:04.000', 'San Francisco - Cupertino', "
                      "'旧金山 - San Francisco City', 'Test String Value', 5)")

        # Source table with binary(16) and nchar(16) columns
        tdSql.execute("CREATE TABLE `src_ntb_16` ("
                      "ts timestamp, "
                      "binary_col binary(16), "
                      "nchar_col nchar(16), "
                      "int_col int)")

        tdSql.execute("INSERT INTO src_ntb_16 VALUES "
                      "('2024-01-01 00:00:00.000', 'Palo Alto', '帕洛阿托', 10)")
        tdSql.execute("INSERT INTO src_ntb_16 VALUES "
                      "('2024-01-01 00:00:01.000', 'San Jose', '圣何塞', 20)")
        tdSql.execute("INSERT INTO src_ntb_16 VALUES "
                      "('2024-01-01 00:00:02.000', 'Campbell', '坎贝尔', 30)")
        tdSql.execute("INSERT INTO src_ntb_16 VALUES "
                      "('2024-01-01 00:00:03.000', NULL, NULL, 40)")
        tdSql.execute("INSERT INTO src_ntb_16 VALUES "
                      "('2024-01-01 00:00:04.000', 'Mountain View', '山景城 - MV', 50)")

        # Case 1: vtable col len > src col len
        # binary(64)/nchar(64) referencing binary(32)/nchar(32)
        tdSql.execute("CREATE VTABLE `vtb_nchar_gt` ("
                      "ts timestamp, "
                      "binary_col binary(64) from src_ntb_32.binary_col, "
                      "nchar_col nchar(64) from src_ntb_32.nchar_col, "
                      "varchar_col varchar(64) from src_ntb_32.varchar_col, "
                      "int_col int from src_ntb_32.int_col)")

        # Case 2: vtable col len = src col len
        # binary(32)/nchar(32) referencing binary(32)/nchar(32)
        tdSql.execute("CREATE VTABLE `vtb_nchar_eq` ("
                      "ts timestamp, "
                      "binary_col binary(32) from src_ntb_32.binary_col, "
                      "nchar_col nchar(32) from src_ntb_32.nchar_col, "
                      "varchar_col varchar(32) from src_ntb_32.varchar_col, "
                      "int_col int from src_ntb_32.int_col)")

        # Case 3: vtable col len < src col len (KEY SCENARIO)
        # binary(8)/nchar(8) referencing binary(32)/nchar(32)
        tdSql.execute("CREATE VTABLE `vtb_nchar_lt` ("
                      "ts timestamp, "
                      "binary_col binary(8) from src_ntb_32.binary_col, "
                      "nchar_col nchar(8) from src_ntb_32.nchar_col, "
                      "varchar_col varchar(8) from src_ntb_32.varchar_col, "
                      "int_col int from src_ntb_32.int_col)")

        # Case 4: vtable with mixed references
        # binary(16) referencing src_ntb_32.binary_col(32) and
        # binary(32) referencing src_ntb_16.binary_col(16)
        tdSql.execute("CREATE VTABLE `vtb_nchar_mix` ("
                      "ts timestamp, "
                      "binary_32_col binary(16) from src_ntb_32.binary_col, "
                      "nchar_32_col nchar(16) from src_ntb_32.nchar_col, "
                      "binary_16_col binary(32) from src_ntb_16.binary_col, "
                      "nchar_16_col nchar(32) from src_ntb_16.nchar_col, "
                      "int_col int from src_ntb_32.int_col)")

    # Expected data from src_ntb_32
    SRC_32_BINARY = [
        'Shanghai - Los Angles',
        'short',
        'Palo Alto - Mountain View',
        None,
        'San Francisco - Cupertino',
    ]

    SRC_32_NCHAR = [
        '圣克拉拉 - Santa Clara',
        '短',
        '库比蒂诺 - Cupertino City',
        None,
        '旧金山 - San Francisco City',
    ]

    SRC_32_VARCHAR = [
        'Hello World Test',
        'a',
        'Medium Length Str',
        None,
        'Test String Value',
    ]

    SRC_32_INT = [1, 2, 3, 4, 5]

    # Expected data from src_ntb_16
    SRC_16_BINARY = [
        'Palo Alto',
        'San Jose',
        'Campbell',
        None,
        'Mountain View',
    ]

    SRC_16_NCHAR = [
        '帕洛阿托',
        '圣何塞',
        '坎贝尔',
        None,
        '山景城 - MV',
    ]

    def _check_projection(self, vtable_name, binary_data, nchar_data, varchar_data, int_data):
        """Helper to check projection queries on a virtual table."""
        db = self.DB_NAME

        # Check binary_col
        tdSql.query(f"select binary_col from {db}.{vtable_name} order by ts;")
        tdSql.checkRows(len(binary_data))
        for i, val in enumerate(binary_data):
            tdSql.checkData(i, 0, val)

        # Check nchar_col
        tdSql.query(f"select nchar_col from {db}.{vtable_name} order by ts;")
        tdSql.checkRows(len(nchar_data))
        for i, val in enumerate(nchar_data):
            tdSql.checkData(i, 0, val)

        # Check varchar_col
        if varchar_data is not None:
            tdSql.query(f"select varchar_col from {db}.{vtable_name} order by ts;")
            tdSql.checkRows(len(varchar_data))
            for i, val in enumerate(varchar_data):
                tdSql.checkData(i, 0, val)

        # Check combined projection
        if varchar_data is not None:
            tdSql.query(f"select binary_col, nchar_col, varchar_col, int_col from {db}.{vtable_name} order by ts;")
            tdSql.checkRows(len(binary_data))
            for i in range(len(binary_data)):
                tdSql.checkData(i, 0, binary_data[i])
                tdSql.checkData(i, 1, nchar_data[i])
                tdSql.checkData(i, 2, varchar_data[i])
                tdSql.checkData(i, 3, int_data[i])

    def _check_length_functions(self, vtable_name, binary_data, nchar_data):
        """Helper to check length/char_length functions."""
        db = self.DB_NAME

        # Check length(binary_col) - returns byte length
        tdSql.query(f"select length(binary_col) from {db}.{vtable_name} order by ts;")
        tdSql.checkRows(len(binary_data))
        for i, val in enumerate(binary_data):
            if val is None:
                tdSql.checkData(i, 0, None)
            else:
                tdSql.checkData(i, 0, len(val))

        # Check char_length(binary_col) - for binary, same as length
        tdSql.query(f"select char_length(binary_col) from {db}.{vtable_name} order by ts;")
        tdSql.checkRows(len(binary_data))
        for i, val in enumerate(binary_data):
            if val is None:
                tdSql.checkData(i, 0, None)
            else:
                tdSql.checkData(i, 0, len(val))

        # Check char_length(nchar_col) - returns character count
        tdSql.query(f"select char_length(nchar_col) from {db}.{vtable_name} order by ts;")
        tdSql.checkRows(len(nchar_data))
        for i, val in enumerate(nchar_data):
            if val is None:
                tdSql.checkData(i, 0, None)
            else:
                tdSql.checkData(i, 0, len(val))

    def _check_string_functions(self, vtable_name, binary_data):
        """Helper to check string functions on binary column."""
        db = self.DB_NAME

        # lower
        tdSql.query(f"select lower(binary_col) from {db}.{vtable_name} order by ts;")
        tdSql.checkRows(len(binary_data))
        for i, val in enumerate(binary_data):
            if val is None:
                tdSql.checkData(i, 0, None)
            else:
                tdSql.checkData(i, 0, val.lower())

        # upper
        tdSql.query(f"select upper(binary_col) from {db}.{vtable_name} order by ts;")
        tdSql.checkRows(len(binary_data))
        for i, val in enumerate(binary_data):
            if val is None:
                tdSql.checkData(i, 0, None)
            else:
                tdSql.checkData(i, 0, val.upper())

        # ltrim (data has no leading spaces, so same as original)
        tdSql.query(f"select ltrim(binary_col) from {db}.{vtable_name} order by ts;")
        tdSql.checkRows(len(binary_data))
        for i, val in enumerate(binary_data):
            if val is None:
                tdSql.checkData(i, 0, None)
            else:
                tdSql.checkData(i, 0, val.lstrip())

        # rtrim (data has no trailing spaces, so same as original)
        tdSql.query(f"select rtrim(binary_col) from {db}.{vtable_name} order by ts;")
        tdSql.checkRows(len(binary_data))
        for i, val in enumerate(binary_data):
            if val is None:
                tdSql.checkData(i, 0, None)
            else:
                tdSql.checkData(i, 0, val.rstrip())

    def _check_concat_function(self, vtable_name, binary_data, nchar_data):
        """Helper to check concat functions."""
        db = self.DB_NAME

        # concat binary with suffix
        tdSql.query(f"select concat(binary_col, '-suffix') from {db}.{vtable_name} order by ts;")
        tdSql.checkRows(len(binary_data))
        for i, val in enumerate(binary_data):
            if val is None:
                tdSql.checkData(i, 0, None)
            else:
                tdSql.checkData(i, 0, val + '-suffix')

        # concat nchar with suffix
        tdSql.query(f"select concat(nchar_col, '-后缀') from {db}.{vtable_name} order by ts;")
        tdSql.checkRows(len(nchar_data))
        for i, val in enumerate(nchar_data):
            if val is None:
                tdSql.checkData(i, 0, None)
            else:
                tdSql.checkData(i, 0, val + '-后缀')

    def _check_substring_function(self, vtable_name, binary_data, nchar_data):
        """Helper to check substring functions."""
        db = self.DB_NAME

        # substring(binary_col, 1, 10) - 1-based index
        tdSql.query(f"select substring(binary_col, 1, 10) from {db}.{vtable_name} order by ts;")
        tdSql.checkRows(len(binary_data))
        for i, val in enumerate(binary_data):
            if val is None:
                tdSql.checkData(i, 0, None)
            else:
                tdSql.checkData(i, 0, val[:10])

        # substring(nchar_col, 1, 5) - 1-based index
        tdSql.query(f"select substring(nchar_col, 1, 5) from {db}.{vtable_name} order by ts;")
        tdSql.checkRows(len(nchar_data))
        for i, val in enumerate(nchar_data):
            if val is None:
                tdSql.checkData(i, 0, None)
            else:
                tdSql.checkData(i, 0, val[:5])

    def _check_replace_function(self, vtable_name, binary_data):
        """Helper to check replace function."""
        db = self.DB_NAME

        tdSql.query(f"select replace(binary_col, ' ', '_') from {db}.{vtable_name} order by ts;")
        tdSql.checkRows(len(binary_data))
        for i, val in enumerate(binary_data):
            if val is None:
                tdSql.checkData(i, 0, None)
            else:
                tdSql.checkData(i, 0, val.replace(' ', '_'))

    def _check_ascii_function(self, vtable_name, binary_data):
        """Helper to check ascii function."""
        db = self.DB_NAME

        tdSql.query(f"select ascii(binary_col) from {db}.{vtable_name} order by ts;")
        tdSql.checkRows(len(binary_data))
        for i, val in enumerate(binary_data):
            if val is None:
                tdSql.checkData(i, 0, None)
            else:
                tdSql.checkData(i, 0, ord(val[0]))

    def _check_position_function(self, vtable_name, binary_data):
        """Helper to check position function."""
        db = self.DB_NAME

        tdSql.query(f"select position('-' IN binary_col) from {db}.{vtable_name} order by ts;")
        tdSql.checkRows(len(binary_data))
        for i, val in enumerate(binary_data):
            if val is None:
                tdSql.checkData(i, 0, None)
            else:
                pos = val.find('-')
                # TDengine position returns 1-based index, 0 if not found
                tdSql.checkData(i, 0, pos + 1 if pos >= 0 else 0)

    def test_vtable_nchar_len_gt(self):
        """Query: vtable col len > src col len

        Virtual table binary(64)/nchar(64) referencing source binary(32)/nchar(32).
        Should return actual source data without any issue.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, nchar, binary, length

        Jira: None

        History:
            - 2026-2-11 Created
        """
        tdLog.info("Case 1: vtable col len > src col len")

        self._check_projection("vtb_nchar_gt",
                               self.SRC_32_BINARY, self.SRC_32_NCHAR,
                               self.SRC_32_VARCHAR, self.SRC_32_INT)
        self._check_length_functions("vtb_nchar_gt",
                                     self.SRC_32_BINARY, self.SRC_32_NCHAR)
        self._check_string_functions("vtb_nchar_gt", self.SRC_32_BINARY)
        self._check_concat_function("vtb_nchar_gt",
                                    self.SRC_32_BINARY, self.SRC_32_NCHAR)
        self._check_substring_function("vtb_nchar_gt",
                                       self.SRC_32_BINARY, self.SRC_32_NCHAR)
        self._check_replace_function("vtb_nchar_gt", self.SRC_32_BINARY)
        self._check_ascii_function("vtb_nchar_gt", self.SRC_32_BINARY)
        self._check_position_function("vtb_nchar_gt", self.SRC_32_BINARY)

    def test_vtable_nchar_len_eq(self):
        """Query: vtable col len = src col len

        Virtual table binary(32)/nchar(32) referencing source binary(32)/nchar(32).
        Should return actual source data.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, nchar, binary, length

        Jira: None

        History:
            - 2026-2-11 Created
        """
        tdLog.info("Case 2: vtable col len = src col len")

        self._check_projection("vtb_nchar_eq",
                               self.SRC_32_BINARY, self.SRC_32_NCHAR,
                               self.SRC_32_VARCHAR, self.SRC_32_INT)
        self._check_length_functions("vtb_nchar_eq",
                                     self.SRC_32_BINARY, self.SRC_32_NCHAR)
        self._check_string_functions("vtb_nchar_eq", self.SRC_32_BINARY)
        self._check_concat_function("vtb_nchar_eq",
                                    self.SRC_32_BINARY, self.SRC_32_NCHAR)
        self._check_substring_function("vtb_nchar_eq",
                                       self.SRC_32_BINARY, self.SRC_32_NCHAR)
        self._check_replace_function("vtb_nchar_eq", self.SRC_32_BINARY)
        self._check_ascii_function("vtb_nchar_eq", self.SRC_32_BINARY)
        self._check_position_function("vtb_nchar_eq", self.SRC_32_BINARY)

    def test_vtable_nchar_len_lt(self):
        """Query: vtable col len < src col len (KEY SCENARIO - no truncation)

        Virtual table binary(8)/nchar(8) referencing source binary(32)/nchar(32).
        Should return actual source data WITHOUT truncation.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, nchar, binary, length

        Jira: None

        History:
            - 2026-2-11 Created
        """
        tdLog.info("Case 3: vtable col len < src col len - must NOT truncate")

        self._check_projection("vtb_nchar_lt",
                               self.SRC_32_BINARY, self.SRC_32_NCHAR,
                               self.SRC_32_VARCHAR, self.SRC_32_INT)
        self._check_length_functions("vtb_nchar_lt",
                                     self.SRC_32_BINARY, self.SRC_32_NCHAR)
        self._check_string_functions("vtb_nchar_lt", self.SRC_32_BINARY)
        self._check_concat_function("vtb_nchar_lt",
                                    self.SRC_32_BINARY, self.SRC_32_NCHAR)
        self._check_substring_function("vtb_nchar_lt",
                                       self.SRC_32_BINARY, self.SRC_32_NCHAR)
        self._check_replace_function("vtb_nchar_lt", self.SRC_32_BINARY)
        self._check_ascii_function("vtb_nchar_lt", self.SRC_32_BINARY)
        self._check_position_function("vtb_nchar_lt", self.SRC_32_BINARY)

    def test_vtable_nchar_len_mix(self):
        """Query: vtable with mixed references and different lengths

        Virtual table with binary(16) referencing src_ntb_32.binary_col(32)
        and binary(32) referencing src_ntb_16.binary_col(16).

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, nchar, binary, length

        Jira: None

        History:
            - 2026-2-11 Created
        """
        tdLog.info("Case 4: mixed references with different lengths")
        db = self.DB_NAME

        # Check combined projection
        tdSql.query(f"select binary_32_col, nchar_32_col, binary_16_col, nchar_16_col "
                    f"from {db}.vtb_nchar_mix order by ts;")
        tdSql.checkRows(5)
        for i in range(5):
            tdSql.checkData(i, 0, self.SRC_32_BINARY[i])
            tdSql.checkData(i, 1, self.SRC_32_NCHAR[i])
            tdSql.checkData(i, 2, self.SRC_16_BINARY[i])
            tdSql.checkData(i, 3, self.SRC_16_NCHAR[i])

        # int_col (fixed-length) should also work
        tdSql.query(f"select int_col from {db}.vtb_nchar_mix order by ts;")
        tdSql.checkRows(5)
        for i in range(5):
            tdSql.checkData(i, 0, self.SRC_32_INT[i])

    def test_vtable_nchar_len_consistency(self):
        """Query: data consistency across all three vtable definitions

        All three virtual tables (gt, eq, lt) reference the same source data
        and should return identical results.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, nchar, binary, length

        Jira: None

        History:
            - 2026-2-11 Created
        """
        tdLog.info("Case 5: data consistency across gt/eq/lt definitions")
        db = self.DB_NAME

        # Count should be the same
        for vtable in ["vtb_nchar_gt", "vtb_nchar_eq", "vtb_nchar_lt"]:
            tdSql.query(f"select count(*) from {db}.{vtable};")
            tdSql.checkData(0, 0, 5)

        # first(binary_col) should be the same
        for vtable in ["vtb_nchar_gt", "vtb_nchar_eq", "vtb_nchar_lt"]:
            tdSql.query(f"select first(binary_col) from {db}.{vtable};")
            tdSql.checkData(0, 0, 'Shanghai - Los Angles')

        # last(nchar_col) should be the same
        for vtable in ["vtb_nchar_gt", "vtb_nchar_eq", "vtb_nchar_lt"]:
            tdSql.query(f"select last(nchar_col) from {db}.{vtable};")
            tdSql.checkData(0, 0, '旧金山 - San Francisco City')

        # Filter on character columns
        for vtable in ["vtb_nchar_gt", "vtb_nchar_eq", "vtb_nchar_lt"]:
            tdSql.query(f"select binary_col, int_col from {db}.{vtable} "
                        f"where binary_col = 'short' order by ts;")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, 'short')
            tdSql.checkData(0, 1, 2)

        for vtable in ["vtb_nchar_gt", "vtb_nchar_eq", "vtb_nchar_lt"]:
            tdSql.query(f"select nchar_col, int_col from {db}.{vtable} "
                        f"where nchar_col = '短' order by ts;")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, '短')
            tdSql.checkData(0, 1, 2)

    def test_vtable_nchar_len_cast(self):
        """Query: cast function on virtual table with mismatched lengths

        Test cast function works correctly on virtual tables
        with different column length definitions.

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, nchar, binary, length

        Jira: None

        History:
            - 2026-2-11 Created
        """
        tdLog.info("Case 6: cast function on virtual table")
        db = self.DB_NAME

        # Cast int to binary
        tdSql.query(f"select cast(int_col as binary(16)) from {db}.vtb_nchar_lt order by ts;")
        tdSql.checkRows(5)
        for i, val in enumerate(self.SRC_32_INT):
            tdSql.checkData(i, 0, str(val))

        # Cast int to nchar
        tdSql.query(f"select cast(int_col as nchar(16)) from {db}.vtb_nchar_lt order by ts;")
        tdSql.checkRows(5)
        for i, val in enumerate(self.SRC_32_INT):
            tdSql.checkData(i, 0, str(val))
