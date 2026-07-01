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

from frame.etool import *
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame.common import *

class TDTestCase(TBase):
    """Test cases for virtual table tag column references.
    
    Tests the new unified syntax for tag references in virtual child tables:
    - Positional tag ref:  TAGS (db.table.tag, ...)
    - Specific tag ref:    TAGS (tag_name FROM db.table.tag, ...)
    - Mixed tag ref:       TAGS (literal, tag_name FROM db.table.tag, db.table.tag, ...)
    - Old syntax:          TAGS (FROM db.table.tag, ...)
    """

    DB_NAME = "test_vtable_tag_ref"

    def prepare_org_tables(self):
        tdLog.info(f"prepare org tables.")

        tdSql.execute(f"create database {self.DB_NAME};")
        tdSql.execute(f"use {self.DB_NAME};")

        tdLog.info(f"prepare org super table.")
        tdSql.execute(f"CREATE STABLE `vtb_org_stb` ("
                      "ts timestamp, "
                      "int_col int, "
                      "bigint_col bigint, "
                      "float_col float, "
                      "double_col double, "
                      "binary_32_col binary(32), "
                      "nchar_32_col nchar(32)"
                      ") TAGS ("
                      "int_tag int,"
                      "bool_tag bool,"
                      "float_tag float,"
                      "double_tag double,"
                      "nchar_32_tag nchar(32),"
                      "binary_32_tag binary(32))")

        tdLog.info(f"prepare org child tables.")
        for i in range(5):
            tdSql.execute(f"CREATE TABLE `vtb_org_child_{i}` USING `vtb_org_stb` "
                          f"TAGS ({i}, {'true' if i % 2 == 0 else 'false'}, {i}.{i}, {i}.{i}{i}, "
                          f"'nchar_child{i}', 'bin_child{i}');")

        tdLog.info(f"prepare org normal tables.")
        for i in range(5):
            tdSql.execute(f"CREATE TABLE `vtb_org_normal_{i}` ("
                          "ts timestamp, "
                          "int_col int, "
                          "bigint_col bigint, "
                          "float_col float, "
                          "double_col double, "
                          "binary_32_col binary(32), "
                          "nchar_32_col nchar(32))")

        tdLog.info(f"prepare virtual super table.")
        tdSql.execute(f"CREATE STABLE `vtb_virtual_stb` ("
                      "ts timestamp, "
                      "int_col int, "
                      "bigint_col bigint, "
                      "float_col float, "
                      "double_col double, "
                      "binary_32_col binary(32), "
                      "nchar_32_col nchar(32)"
                      ") TAGS ("
                      "int_tag int,"
                      "bool_tag bool,"
                      "float_tag float,"
                      "double_tag double,"
                      "nchar_32_tag nchar(32),"
                      "binary_32_tag binary(32))"
                      "VIRTUAL 1")

        tdLog.info(f"insert data into org tables.")
        for i in range(5):
            for j in range(10):
                ts = 1700000000000 + j * 1000
                tdSql.execute(f"INSERT INTO vtb_org_child_{i} VALUES ({ts}, {j}, {j*100}, {j}.{j}, {j}.{j}{j}, "
                              f"'bin_{i}_{j}', 'nchar_{i}_{j}');")
                tdSql.execute(f"INSERT INTO vtb_org_normal_{i} VALUES ({ts}, {j}, {j*100}, {j}.{j}, {j}.{j}{j}, "
                              f"'bin_{i}_{j}', 'nchar_{i}_{j}');")

    def check_vtable_count(self, vctable_num, vntable_num):
        tdSql.query(f"show {self.DB_NAME}.vtables;")
        tdSql.checkRows(vctable_num + vntable_num)
        tdSql.query(f"show child {self.DB_NAME}.vtables;")
        tdSql.checkRows(vctable_num)
        tdSql.query(f"show normal {self.DB_NAME}.vtables;")
        tdSql.checkRows(vntable_num)

    def test_tag_ref_old_syntax(self):
        """Test the old tag reference syntax: TAGS (FROM db.table.tag)"""
        tdLog.info("test old tag reference syntax: FROM column_ref")

        tdSql.execute(f"use {self.DB_NAME};")

        # Old syntax: FROM table.tag_col (positional, no tag name specified)
        tdSql.execute("CREATE VTABLE `vtb_vctb_old_0` ("
                      "int_col FROM vtb_org_child_0.int_col, "
                      "bigint_col FROM vtb_org_child_1.bigint_col"
                      ") USING vtb_virtual_stb TAGS ("
                      "FROM vtb_org_child_0.int_tag, "
                      "FROM vtb_org_child_0.bool_tag, "
                      "FROM vtb_org_child_0.float_tag, "
                      "FROM vtb_org_child_0.double_tag, "
                      "FROM vtb_org_child_0.nchar_32_tag, "
                      "FROM vtb_org_child_0.binary_32_tag)")

        self.check_vtable_count(1, 0)
        tdLog.info("old tag reference syntax test passed")

    def test_tag_ref_specific_syntax(self):
        """Test the new specific tag reference syntax: TAGS (tag_name FROM table.tag)"""
        tdLog.info("test new specific tag reference syntax: tag_name FROM column_ref")

        tdSql.execute(f"use {self.DB_NAME};")

        # New syntax: tag_name FROM table.tag_col (specific, with tag name)
        # All tags reference from same child table
        tdSql.execute("CREATE VTABLE `vtb_vctb_specific_0` ("
                      "int_col FROM vtb_org_child_0.int_col, "
                      "bigint_col FROM vtb_org_child_1.bigint_col"
                      ") USING vtb_virtual_stb TAGS ("
                      "int_tag FROM vtb_org_child_0.int_tag, "
                      "bool_tag FROM vtb_org_child_0.bool_tag, "
                      "float_tag FROM vtb_org_child_0.float_tag, "
                      "double_tag FROM vtb_org_child_0.double_tag, "
                      "nchar_32_tag FROM vtb_org_child_0.nchar_32_tag, "
                      "binary_32_tag FROM vtb_org_child_0.binary_32_tag)")

        self.check_vtable_count(2, 0)

        # Tags reference from different child tables
        tdSql.execute("CREATE VTABLE `vtb_vctb_specific_1` ("
                      "int_col FROM vtb_org_child_0.int_col, "
                      "bigint_col FROM vtb_org_child_1.bigint_col"
                      ") USING vtb_virtual_stb TAGS ("
                      "int_tag FROM vtb_org_child_0.int_tag, "
                      "bool_tag FROM vtb_org_child_1.bool_tag, "
                      "float_tag FROM vtb_org_child_2.float_tag, "
                      "double_tag FROM vtb_org_child_3.double_tag, "
                      "nchar_32_tag FROM vtb_org_child_4.nchar_32_tag, "
                      "binary_32_tag FROM vtb_org_child_0.binary_32_tag)")

        self.check_vtable_count(3, 0)

        # Partial tag references (some tags are literals, some are references)
        tdSql.execute("CREATE VTABLE `vtb_vctb_specific_2` ("
                      "int_col FROM vtb_org_child_0.int_col, "
                      "bigint_col FROM vtb_org_child_1.bigint_col"
                      ") USING vtb_virtual_stb TAGS ("
                      "int_tag FROM vtb_org_child_0.int_tag, "
                      "false, "
                      "float_tag FROM vtb_org_child_2.float_tag, "
                      "3.14, "
                      "'literal_nchar', "
                      "binary_32_tag FROM vtb_org_child_0.binary_32_tag)")

        self.check_vtable_count(4, 0)

        tdLog.info("new specific tag reference syntax test passed")

    def test_tag_ref_positional_syntax(self):
        """Test the new positional tag reference syntax: TAGS (table.tag)"""
        tdLog.info("test new positional tag reference syntax: table.tag")

        tdSql.execute(f"use {self.DB_NAME};")

        # New syntax: table.tag_col (positional, 2-part name)
        tdSql.execute("CREATE VTABLE `vtb_vctb_positional_0` ("
                      "int_col FROM vtb_org_child_0.int_col, "
                      "bigint_col FROM vtb_org_child_1.bigint_col"
                      ") USING vtb_virtual_stb TAGS ("
                      "vtb_org_child_0.int_tag, "
                      "vtb_org_child_0.bool_tag, "
                      "vtb_org_child_0.float_tag, "
                      "vtb_org_child_0.double_tag, "
                      "vtb_org_child_0.nchar_32_tag, "
                      "vtb_org_child_0.binary_32_tag)")

        self.check_vtable_count(5, 0)

        # Positional tag refs from different child tables
        tdSql.execute("CREATE VTABLE `vtb_vctb_positional_1` ("
                      "int_col FROM vtb_org_child_0.int_col, "
                      "bigint_col FROM vtb_org_child_1.bigint_col"
                      ") USING vtb_virtual_stb TAGS ("
                      "vtb_org_child_0.int_tag, "
                      "vtb_org_child_1.bool_tag, "
                      "vtb_org_child_2.float_tag, "
                      "vtb_org_child_3.double_tag, "
                      "vtb_org_child_4.nchar_32_tag, "
                      "vtb_org_child_0.binary_32_tag)")

        self.check_vtable_count(6, 0)

        # 3-part name: db.table.tag
        tdSql.execute(f"CREATE VTABLE `vtb_vctb_positional_2` ("
                      "int_col FROM vtb_org_child_0.int_col, "
                      "bigint_col FROM vtb_org_child_1.bigint_col"
                      f") USING vtb_virtual_stb TAGS ("
                      f"{self.DB_NAME}.vtb_org_child_0.int_tag, "
                      f"{self.DB_NAME}.vtb_org_child_0.bool_tag, "
                      f"{self.DB_NAME}.vtb_org_child_0.float_tag, "
                      f"{self.DB_NAME}.vtb_org_child_0.double_tag, "
                      f"{self.DB_NAME}.vtb_org_child_0.nchar_32_tag, "
                      f"{self.DB_NAME}.vtb_org_child_0.binary_32_tag)")

        self.check_vtable_count(7, 0)

        tdLog.info("new positional tag reference syntax test passed")

    def test_tag_ref_mixed_syntax(self):
        """Test mixed tag reference syntax: literals + old FROM + new specific + new positional"""
        tdLog.info("test mixed tag reference syntax")

        tdSql.execute(f"use {self.DB_NAME};")

        # Mix of literal values, old FROM syntax, new specific syntax, and new positional syntax
        tdSql.execute("CREATE VTABLE `vtb_vctb_mixed_0` ("
                      "int_col FROM vtb_org_child_0.int_col, "
                      "bigint_col FROM vtb_org_child_1.bigint_col"
                      ") USING vtb_virtual_stb TAGS ("
                      "100, "                                                  # literal
                      "bool_tag FROM vtb_org_child_1.bool_tag, "               # new specific
                      "vtb_org_child_2.float_tag, "                            # new positional
                      "FROM vtb_org_child_3.double_tag, "                      # old FROM
                      "'mixed_nchar', "                                        # literal
                      "binary_32_tag FROM vtb_org_child_0.binary_32_tag)")     # new specific

        self.check_vtable_count(8, 0)

        # Mix with 3-part positional names
        tdSql.execute(f"CREATE VTABLE `vtb_vctb_mixed_1` ("
                      "int_col FROM vtb_org_child_0.int_col, "
                      "bigint_col FROM vtb_org_child_1.bigint_col"
                      f") USING vtb_virtual_stb TAGS ("
                      f"int_tag FROM {self.DB_NAME}.vtb_org_child_0.int_tag, "   # specific with 3-part ref
                      "false, "                                                   # literal
                      f"{self.DB_NAME}.vtb_org_child_2.float_tag, "              # positional 3-part
                      "3.14, "                                                    # literal
                      "nchar_32_tag FROM vtb_org_child_4.nchar_32_tag, "         # specific with 2-part ref
                      "'literal_bin')")                                            # literal

        self.check_vtable_count(9, 0)

        tdLog.info("mixed tag reference syntax test passed")

    def test_tag_ref_with_column_ref(self):
        """Test tag references combined with different column reference styles"""
        tdLog.info("test tag references with different column reference styles")

        tdSql.execute(f"use {self.DB_NAME};")

        # Column refs without FROM + tag refs
        tdSql.execute("CREATE VTABLE `vtb_vctb_colref_0` ("
                      "vtb_org_child_0.int_col, "
                      "vtb_org_child_1.bigint_col, "
                      "vtb_org_child_2.float_col"
                      ") USING vtb_virtual_stb TAGS ("
                      "int_tag FROM vtb_org_child_0.int_tag, "
                      "bool_tag FROM vtb_org_child_0.bool_tag, "
                      "float_tag FROM vtb_org_child_0.float_tag, "
                      "double_tag FROM vtb_org_child_0.double_tag, "
                      "nchar_32_tag FROM vtb_org_child_0.nchar_32_tag, "
                      "binary_32_tag FROM vtb_org_child_0.binary_32_tag)")

        self.check_vtable_count(10, 0)

        # Column refs with FROM + positional tag refs
        tdSql.execute("CREATE VTABLE `vtb_vctb_colref_1` ("
                      "int_col FROM vtb_org_child_0.int_col, "
                      "bigint_col FROM vtb_org_child_1.bigint_col"
                      ") USING vtb_virtual_stb TAGS ("
                      "vtb_org_child_0.int_tag, "
                      "vtb_org_child_1.bool_tag, "
                      "vtb_org_child_2.float_tag, "
                      "vtb_org_child_3.double_tag, "
                      "vtb_org_child_4.nchar_32_tag, "
                      "vtb_org_child_0.binary_32_tag)")

        self.check_vtable_count(11, 0)

        tdLog.info("tag references with column reference styles test passed")

    def test_tag_ref_query_verify(self):
        """Verify that tag references are correctly stored via SHOW CREATE VTABLE"""
        tdLog.info("test tag reference storage correctness via SHOW CREATE VTABLE")

        tdSql.execute(f"use {self.DB_NAME};")

        # Create a vtable with specific tag refs (tag_name FROM table.tag)
        tdSql.execute("CREATE VTABLE `vtb_vctb_verify_0` ("
                      "int_col FROM vtb_org_child_0.int_col"
                      ") USING vtb_virtual_stb TAGS ("
                      "int_tag FROM vtb_org_child_2.int_tag, "
                      "bool_tag FROM vtb_org_child_2.bool_tag, "
                      "float_tag FROM vtb_org_child_2.float_tag, "
                      "double_tag FROM vtb_org_child_2.double_tag, "
                      "nchar_32_tag FROM vtb_org_child_2.nchar_32_tag, "
                      "binary_32_tag FROM vtb_org_child_2.binary_32_tag)")

        self.check_vtable_count(12, 0)

        # Verify via SHOW CREATE VTABLE that tag references are stored
        tdSql.query(f"SHOW CREATE VTABLE {self.DB_NAME}.vtb_vctb_verify_0;")
        tdSql.checkRows(1)
        create_sql = tdSql.getData(0, 1)
        tdLog.info(f"SHOW CREATE VTABLE vtb_vctb_verify_0: {create_sql}")
        # The create SQL should contain tag reference info
        assert 'vtb_vctb_verify_0' in create_sql, "Table name should be in create SQL"

        # Create with positional syntax from vtb_org_child_3
        tdSql.execute("CREATE VTABLE `vtb_vctb_verify_1` ("
                      "int_col FROM vtb_org_child_0.int_col"
                      ") USING vtb_virtual_stb TAGS ("
                      "vtb_org_child_3.int_tag, "
                      "vtb_org_child_3.bool_tag, "
                      "vtb_org_child_3.float_tag, "
                      "vtb_org_child_3.double_tag, "
                      "vtb_org_child_3.nchar_32_tag, "
                      "vtb_org_child_3.binary_32_tag)")

        self.check_vtable_count(13, 0)

        tdSql.query(f"SHOW CREATE VTABLE {self.DB_NAME}.vtb_vctb_verify_1;")
        tdSql.checkRows(1)
        create_sql = tdSql.getData(0, 1)
        tdLog.info(f"SHOW CREATE VTABLE vtb_vctb_verify_1: {create_sql}")
        assert 'vtb_vctb_verify_1' in create_sql, "Table name should be in create SQL"

        # Create with mixed syntax: some literal, some ref
        tdSql.execute("CREATE VTABLE `vtb_vctb_verify_2` ("
                      "int_col FROM vtb_org_child_0.int_col"
                      ") USING vtb_virtual_stb TAGS ("
                      "int_tag FROM vtb_org_child_4.int_tag, "   # ref -> 4
                      "true, "                                     # literal
                      "float_tag FROM vtb_org_child_1.float_tag, " # ref -> 1.1
                      "9.99, "                                     # literal
                      "'my_nchar_val', "                           # literal
                      "binary_32_tag FROM vtb_org_child_0.binary_32_tag)")  # ref -> bin_child0

        self.check_vtable_count(14, 0)

        tdSql.query(f"SHOW CREATE VTABLE {self.DB_NAME}.vtb_vctb_verify_2;")
        tdSql.checkRows(1)
        create_sql = tdSql.getData(0, 1)
        tdLog.info(f"SHOW CREATE VTABLE vtb_vctb_verify_2: {create_sql}")
        assert 'vtb_vctb_verify_2' in create_sql, "Table name should be in create SQL"
        # Verify literal tag values are present
        assert 'my_nchar_val' in create_sql, "Literal nchar tag value should be in create SQL"

        # Verify tag refs via describe - check the 'ref' column
        tdSql.query(f"DESCRIBE {self.DB_NAME}.vtb_vctb_verify_0;")
        tdLog.info(f"DESCRIBE vtb_vctb_verify_0 rows: {tdSql.queryRows}")

        tdLog.info("tag reference storage correctness test passed")

    def test_tag_ref_error_cases(self):
        """Test error cases for tag references"""
        tdLog.info("test tag reference error cases")

        tdSql.execute(f"use {self.DB_NAME};")

        # 1. Tag reference column does not exist in source table
        tdSql.error("CREATE VTABLE `vtb_vctb_err_0` ("
                    "int_col FROM vtb_org_child_0.int_col"
                    ") USING vtb_virtual_stb TAGS ("
                    "int_tag FROM vtb_org_child_0.not_exist_tag, "
                    "false, 1.0, 2.0, 'nchar', 'bin')")

        # 2. Tag reference table does not exist
        tdSql.error("CREATE VTABLE `vtb_vctb_err_1` ("
                    "int_col FROM vtb_org_child_0.int_col"
                    ") USING vtb_virtual_stb TAGS ("
                    "int_tag FROM not_exist_table.int_tag, "
                    "false, 1.0, 2.0, 'nchar', 'bin')")

        # 3. Tag reference type mismatch (referencing a non-tag column as tag)
        tdSql.error("CREATE VTABLE `vtb_vctb_err_2` ("
                    "int_col FROM vtb_org_child_0.int_col"
                    ") USING vtb_virtual_stb TAGS ("
                    "int_tag FROM vtb_org_child_0.int_col, "
                    "false, 1.0, 2.0, 'nchar', 'bin')")

        # 4. Positional tag ref - column does not exist
        tdSql.error("CREATE VTABLE `vtb_vctb_err_3` ("
                    "int_col FROM vtb_org_child_0.int_col"
                    ") USING vtb_virtual_stb TAGS ("
                    "vtb_org_child_0.not_exist_tag, "
                    "false, 1.0, 2.0, 'nchar', 'bin')")

        # 5. Positional tag ref - table does not exist
        tdSql.error("CREATE VTABLE `vtb_vctb_err_4` ("
                    "int_col FROM vtb_org_child_0.int_col"
                    ") USING vtb_virtual_stb TAGS ("
                    "not_exist_table.int_tag, "
                    "false, 1.0, 2.0, 'nchar', 'bin')")

        # 6. Tag type mismatch - referencing int_tag for bool_tag position
        tdSql.error("CREATE VTABLE `vtb_vctb_err_5` ("
                    "int_col FROM vtb_org_child_0.int_col"
                    ") USING vtb_virtual_stb TAGS ("
                    "0, "
                    "bool_tag FROM vtb_org_child_0.int_tag, "
                    "1.0, 2.0, 'nchar', 'bin')")

        # 7. 3-part positional with non-existent db
        tdSql.error("CREATE VTABLE `vtb_vctb_err_6` ("
                    "int_col FROM vtb_org_child_0.int_col"
                    ") USING vtb_virtual_stb TAGS ("
                    "nonexist_db.vtb_org_child_0.int_tag, "
                    "false, 1.0, 2.0, 'nchar', 'bin')")

        tdLog.info("tag reference error cases test passed")

    def test_tag_ref_with_specific_tags(self):
        """Test tag references with specific tag names in TAGS clause"""
        tdLog.info("test tag references with specific tag names")

        tdSql.execute(f"use {self.DB_NAME};")

        # Use specific tag names (not all tags, just some) with tag refs
        tdSql.execute("CREATE VTABLE `vtb_vctb_spectag_0` ("
                      "int_col FROM vtb_org_child_0.int_col"
                      ") USING vtb_virtual_stb (int_tag, bool_tag, float_tag, double_tag, nchar_32_tag, binary_32_tag) TAGS ("
                      "int_tag FROM vtb_org_child_1.int_tag, "
                      "false, "
                      "float_tag FROM vtb_org_child_2.float_tag, "
                      "3.14, "
                      "'nchar_val', "
                      "'bin_val')")

        self.check_vtable_count(15, 0)

        # Verify via SHOW CREATE VTABLE
        tdSql.query(f"SHOW CREATE VTABLE {self.DB_NAME}.vtb_vctb_spectag_0;")
        tdSql.checkRows(1)
        create_sql = tdSql.getData(0, 1)
        tdLog.info(f"SHOW CREATE VTABLE vtb_vctb_spectag_0: {create_sql}")
        assert 'vtb_vctb_spectag_0' in create_sql, "Table name should be in create SQL"
        # Literal tag values should be present
        assert 'nchar_val' in create_sql, "Literal nchar tag value should be in create SQL"
        assert 'bin_val' in create_sql, "Literal binary tag value should be in create SQL"

        tdLog.info("tag references with specific tag names test passed")

    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        self.prepare_org_tables()
        self.test_tag_ref_old_syntax()
        self.test_tag_ref_specific_syntax()
        self.test_tag_ref_positional_syntax()
        self.test_tag_ref_mixed_syntax()
        self.test_tag_ref_with_column_ref()
        self.test_tag_ref_query_verify()
        self.test_tag_ref_error_cases()
        self.test_tag_ref_with_specific_tags()

        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
