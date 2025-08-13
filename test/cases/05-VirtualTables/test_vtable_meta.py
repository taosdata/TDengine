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


class TestVtableMeta:

    def setup_class(cls):
        tdLog.info(f"prepare org tables.")
        tdSql.execute("drop database if exists test_vtable_meta;")
        tdSql.execute("create database test_vtable_meta;")
        tdSql.execute("use test_vtable_meta;")

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
                      "nchar_32_col nchar(32),"
                      "varbinary_16_col varbinary(16),"
                      "varbinary_32_col varbinary(32),"
                      "geo_16_col geometry(16),"
                      "geo_32_col geometry(32)"
                      ") TAGS ("
                      "int_tag int,"
                      "bool_tag bool,"
                      "float_tag float,"
                      "double_tag double,"
                      "nchar_32_tag nchar(32),"
                      "binary_32_tag binary(32))")

        tdLog.info(f"prepare org child table.")
        for i in range(30):
            tdSql.execute(f"CREATE TABLE `vtb_org_child_{i}` USING `vtb_org_stb` TAGS ({i}, false, {i}, {i}, 'child{i}', 'child{i}');")

        tdLog.info(f"prepare org normal table.")
        for i in range(30):
            tdSql.execute(f"CREATE TABLE `vtb_org_normal_{i}` (ts timestamp, u_tinyint_col tinyint unsigned, u_smallint_col smallint unsigned, u_int_col int unsigned, u_bigint_col bigint unsigned, tinyint_col tinyint, smallint_col smallint, int_col int, bigint_col bigint, float_col float, double_col double, bool_col bool, binary_16_col binary(16), binary_32_col binary(32), nchar_16_col nchar(16), nchar_32_col nchar(32), varbinary_16_col varbinary(16), varbinary_32_col varbinary(32), geo_16_col geometry(16), geo_32_col geometry(32))")

        tdLog.info(f"prepare virtual super tables.")
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
                      "nchar_32_col nchar(32),"
                      "varbinary_16_col varbinary(16),"
                      "varbinary_32_col varbinary(32),"
                      "geo_16_col geometry(16),"
                      "geo_32_col geometry(32)"
                      ") TAGS ("
                      "int_tag int,"
                      "bool_tag bool,"
                      "float_tag float,"
                      "double_tag double,"
                      "nchar_32_tag nchar(32),"
                      "binary_32_tag binary(32))"
                      "VIRTUAL 1")

        tdLog.info(f"prepare virtual child tables.")
        for i in range(30):
            tdSql.execute(f"CREATE VTABLE `vtb_virtual_ctb{i}` "
                          f"(vtb_org_child_0.u_tinyint_col, "
                          f"vtb_org_child_1.u_smallint_col, "
                          f"vtb_org_child_2.u_int_col, "
                          f"vtb_org_child_3.u_bigint_col,"
                          f"vtb_org_child_4.tinyint_col) "
                          f"USING vtb_virtual_stb TAGS ({i}, false, {i}, {i}, 'vchild{i}', 'vchild{i}')")

        tdLog.info(f"prepare virtual normal tables.")
        for i in range(30):
            tdSql.execute(f"CREATE VTABLE `vtb_virtual_ntb{i}` ("
                          "ts timestamp, "
                          "u_tinyint_col tinyint unsigned from vtb_org_child_0.u_tinyint_col, "
                          "u_smallint_col smallint unsigned from vtb_org_child_1.u_smallint_col, "
                          "u_int_col int unsigned, "
                          "u_bigint_col bigint unsigned from vtb_org_child_3.u_bigint_col, "
                          "tinyint_col tinyint from vtb_org_child_4.tinyint_col, "
                          "smallint_col smallint, "
                          "int_col int, "
                          "bigint_col bigint, "
                          "float_col float from vtb_org_child_8.float_col, "
                          "double_col double from vtb_org_child_9.double_col, "
                          "bool_col bool from vtb_org_child_10.bool_col, "
                          "binary_16_col binary(16),"
                          "binary_32_col binary(32),"
                          "nchar_16_col nchar(16),"
                          "nchar_32_col nchar(32) from vtb_org_child_14.nchar_32_col,"
                          "varbinary_16_col varbinary(16),"
                          "varbinary_32_col varbinary(32),"
                          "geo_16_col geometry(16) from vtb_org_child_17.geo_16_col,"
                          "geo_32_col geometry(32) from vtb_org_child_18.geo_32_col)")

    def test_normal_query_new(self):
        """test virtual table normal query.

        test virtual table normal query.

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2025-3-15 Jing Sima Created
            - 2025-5-6 Huo Hong Migrated to new test framework

        """
        # read sql from .sql file and execute
        tdLog.info("test normal query.")
        testCase = "test_vtable_meta"
        self.sqlFile = os.path.join(os.path.dirname(__file__), "in", f"{testCase}.in")
        self.ansFile = os.path.join(os.path.dirname(__file__), "ans", f"{testCase}.ans")

        tdCom.compare_testcase_result(self.sqlFile, self.ansFile, testCase)

