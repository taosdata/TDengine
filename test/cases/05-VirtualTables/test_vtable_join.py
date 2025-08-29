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

class TestVtableJoin:

    def setup_class(cls):
        tdLog.info(f"prepare org tables.")

        tdSql.execute("drop database if exists test_vtable_join;")
        tdSql.execute("create database test_vtable_join;")
        tdSql.execute("use test_vtable_join;")

        tdLog.info(f"prepare org super table.")
        tdSql.execute("select database();")
        tdSql.execute(f"CREATE STABLE `vtb_org_stb` ("
                      "ts timestamp, "
                      "u_tinyint_col tinyint unsigned, "
                      "u_smallint_col smallint unsigned, "
                      "u_int_col int unsigned, "
                      "u_bigint_col bigint unsigned, "
                      "tinyint_col tinyint, "
                      "smallint_col smallint, "
                      "int_col int, "
                      "bigint_col bigint, "
                      "float_col float, "
                      "double_col double, "
                      "bool_col bool, "
                      "binary_16_col binary(16),"
                      "binary_32_col binary(32),"
                      "nchar_16_col nchar(16),"
                      "nchar_32_col nchar(32)"
                      ") TAGS ("
                      "int_tag int,"
                      "bool_tag bool,"
                      "float_tag float,"
                      "double_tag double,"
                      "nchar_32_tag nchar(32),"
                      "binary_32_tag binary(32))")

        tdLog.info(f"prepare org child table.")
        for i in range(3):
            tdSql.execute(f"CREATE TABLE `vtb_org_child_{i}` USING `vtb_org_stb` TAGS ({i}, false, {i}, {i}, 'child{i}', 'child{i}');")

        tdLog.info(f"prepare org normal table.")
        for i in range(3):
            tdSql.execute(f"CREATE TABLE `vtb_org_normal_{i}` (ts timestamp, u_tinyint_col tinyint unsigned, u_smallint_col smallint unsigned, u_int_col int unsigned, u_bigint_col bigint unsigned, tinyint_col tinyint, smallint_col smallint, int_col int, bigint_col bigint, float_col float, double_col double, bool_col bool, binary_16_col binary(16), binary_32_col binary(32), nchar_16_col nchar(16), nchar_32_col nchar(32))")

        tdLog.info(f"insert data into org tables.")
        datafile = etool.curFile(__file__, "data/data1.csv")
        tdSql.execute("insert into vtb_org_normal_0 file '%s';" % datafile)
        tdSql.execute("insert into vtb_org_child_0 file '%s';" % datafile)

        datafile = etool.curFile(__file__, "data/data2.csv")
        tdSql.execute("insert into vtb_org_normal_1 file '%s';" % datafile)
        tdSql.execute("insert into vtb_org_child_1 file '%s';" % datafile)

        datafile = etool.curFile(__file__, "data/data3.csv")
        tdSql.execute("insert into vtb_org_normal_2 file '%s';" % datafile)
        tdSql.execute("insert into vtb_org_child_2 file '%s';" % datafile)

        tdLog.info(f"prepare virtual normal table.")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ntb_1` ("
                      "ts timestamp, "
                      "u_tinyint_col tinyint unsigned from vtb_org_normal_0.u_tinyint_col, "
                      "u_smallint_col smallint unsigned from vtb_org_normal_1.u_smallint_col, "
                      "u_int_col int unsigned from vtb_org_normal_2.u_int_col, "
                      "u_bigint_col bigint unsigned from vtb_org_normal_0.u_bigint_col, "
                      "tinyint_col tinyint from vtb_org_normal_1.tinyint_col, "
                      "smallint_col smallint from vtb_org_normal_2.smallint_col, "
                      "int_col int from vtb_org_normal_0.int_col, "
                      "bigint_col bigint from vtb_org_normal_1.bigint_col, "
                      "float_col float from vtb_org_normal_2.float_col, "
                      "double_col double from vtb_org_normal_0.double_col, "
                      "bool_col bool from vtb_org_normal_1.bool_col, "
                      "binary_16_col binary(16) from vtb_org_normal_2.binary_16_col,"
                      "binary_32_col binary(32) from vtb_org_normal_0.binary_32_col,"
                      "nchar_16_col nchar(16) from vtb_org_normal_1.nchar_16_col,"
                      "nchar_32_col nchar(32) from vtb_org_normal_2.nchar_32_col)")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ntb_2` ("
                      "ts timestamp, "
                      "u_tinyint_col tinyint unsigned from vtb_org_normal_1.u_tinyint_col, "
                      "u_smallint_col smallint unsigned from vtb_org_normal_2.u_smallint_col, "
                      "u_int_col int unsigned from vtb_org_normal_0.u_int_col, "
                      "u_bigint_col bigint unsigned from vtb_org_normal_1.u_bigint_col, "
                      "tinyint_col tinyint from vtb_org_normal_2.tinyint_col, "
                      "smallint_col smallint from vtb_org_normal_0.smallint_col, "
                      "int_col int from vtb_org_normal_1.int_col, "
                      "bigint_col bigint from vtb_org_normal_2.bigint_col, "
                      "float_col float from vtb_org_normal_0.float_col, "
                      "double_col double from vtb_org_normal_1.double_col, "
                      "bool_col bool from vtb_org_normal_2.bool_col, "
                      "binary_16_col binary(16) from vtb_org_normal_0.binary_16_col,"
                      "binary_32_col binary(32) from vtb_org_normal_1.binary_32_col,"
                      "nchar_16_col nchar(16) from vtb_org_normal_2.nchar_16_col,"
                      "nchar_32_col nchar(32) from vtb_org_normal_0.nchar_32_col)")

        tdSql.execute(f"CREATE STABLE `vtb_virtual_stb` ("
                      "ts timestamp, "
                      "u_tinyint_col tinyint unsigned, "
                      "u_smallint_col smallint unsigned, "
                      "u_int_col int unsigned, "
                      "u_bigint_col bigint unsigned, "
                      "tinyint_col tinyint, "
                      "smallint_col smallint, "
                      "int_col int, "
                      "bigint_col bigint, "
                      "float_col float, "
                      "double_col double, "
                      "bool_col bool, "
                      "binary_16_col binary(16),"
                      "binary_32_col binary(32),"
                      "nchar_16_col nchar(16),"
                      "nchar_32_col nchar(32)"
                      ") TAGS ("
                      "int_tag int,"
                      "bool_tag bool,"
                      "float_tag float,"
                      "double_tag double,"
                      "nchar_32_tag nchar(32),"
                      "binary_32_tag binary(32))"
                      "VIRTUAL 1")

        tdLog.info(f"prepare virtual child table.")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ctb_1` ("
                      "u_tinyint_col from vtb_org_normal_0.u_tinyint_col, "
                      "u_smallint_col from vtb_org_normal_1.u_smallint_col, "
                      "u_int_col from vtb_org_normal_2.u_int_col, "
                      "u_bigint_col from vtb_org_normal_0.u_bigint_col, "
                      "tinyint_col from vtb_org_normal_1.tinyint_col, "
                      "smallint_col from vtb_org_normal_2.smallint_col, "
                      "int_col from vtb_org_normal_0.int_col, "
                      "bigint_col from vtb_org_normal_1.bigint_col, "
                      "float_col from vtb_org_normal_2.float_col, "
                      "double_col from vtb_org_normal_0.double_col, "
                      "bool_col from vtb_org_normal_1.bool_col, "
                      "binary_16_col from vtb_org_normal_2.binary_16_col,"
                      "binary_32_col from vtb_org_normal_0.binary_32_col,"
                      "nchar_16_col from vtb_org_normal_1.nchar_16_col,"
                      "nchar_32_col from vtb_org_normal_2.nchar_32_col)"
                      "USING `vtb_virtual_stb` TAGS (0, false, 0, 0, 'child0', 'child0')")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ctb_2` ("
                      "u_tinyint_col from vtb_org_normal_1.u_tinyint_col, "
                      "u_smallint_col from vtb_org_normal_2.u_smallint_col, "
                      "u_int_col from vtb_org_normal_0.u_int_col, "
                      "u_bigint_col from vtb_org_normal_1.u_bigint_col, "
                      "tinyint_col from vtb_org_normal_2.tinyint_col, "
                      "smallint_col from vtb_org_normal_0.smallint_col, "
                      "int_col from vtb_org_normal_1.int_col, "
                      "bigint_col from vtb_org_normal_2.bigint_col, "
                      "float_col from vtb_org_normal_0.float_col, "
                      "double_col from vtb_org_normal_1.double_col, "
                      "bool_col from vtb_org_normal_2.bool_col, "
                      "binary_16_col from vtb_org_normal_0.binary_16_col,"
                      "binary_32_col from vtb_org_normal_1.binary_32_col,"
                      "nchar_16_col from vtb_org_normal_2.nchar_16_col,"
                      "nchar_32_col from vtb_org_normal_0.nchar_32_col)"
                      "USING `vtb_virtual_stb` TAGS (1, false, 1, 1, 'child1', 'child1')")

    def test_vtable_join(self):
        """Query: join

        test query virtual tables join

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, join

        Jira: None

        History:
            - 2025-3-15 Jing Sima Created
            - 2025-5-6 Huo Hong Migrated to new test framework

        """
        # read sql from .sql file and execute
        testCase = "test_vtable_join"
        tdLog.info(f"test case : {testCase}.")
        self.sqlFile = etool.curFile(__file__, f"in/{testCase}.in")
        self.ansFile = etool.curFile(__file__, f"ans/{testCase}.ans")

        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)


