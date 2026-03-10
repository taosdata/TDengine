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

class TestVTableQuery:

    def setup_class(cls):
        tdLog.info(f"prepare org tables.")

        tdSql.execute("drop database if exists test_vtable_select_with_null;")
        tdSql.execute("create database test_vtable_select_with_null;")
        tdSql.execute("use test_vtable_select_with_null;")

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
        for i in range(15):
            tdSql.execute(f"CREATE TABLE `vtb_org_child_{i}` USING `vtb_org_stb` TAGS ({i}, false, {i}, {i}, 'child{i}', 'child{i}');")

        tdLog.info(f"prepare org normal table.")
        for i in range(15):
            tdSql.execute(f"CREATE TABLE `vtb_org_normal_{i}` (ts timestamp, u_tinyint_col tinyint unsigned, u_smallint_col smallint unsigned, u_int_col int unsigned, u_bigint_col bigint unsigned, tinyint_col tinyint, smallint_col smallint, int_col int, bigint_col bigint, float_col float, double_col double, bool_col bool, binary_16_col binary(16), binary_32_col binary(32), nchar_16_col nchar(16), nchar_32_col nchar(32)) SMA(u_tinyint_col)")

        tdLog.info(f"insert data into org tables.")
        tdSql.execute("insert into vtb_org_normal_0 values (1696000000000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
        tdSql.execute("insert into vtb_org_child_0 values (1696000000000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
        tdSql.execute("insert into vtb_org_normal_1 values (1696000000000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
        tdSql.execute("insert into vtb_org_child_1 values (1696000000000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
        tdSql.execute("insert into vtb_org_normal_2 values (1696000000000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
        tdSql.execute("insert into vtb_org_child_2 values (1696000000000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")

        tdSql.execute("insert into vtb_org_normal_0 values (1696000001000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
        tdSql.execute("insert into vtb_org_child_0 values (1696000001000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
        tdSql.execute("insert into vtb_org_normal_1 values (1696000001000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
        tdSql.execute("insert into vtb_org_child_1 values (1696000001000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
        tdSql.execute("insert into vtb_org_normal_2 values (1696000001000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
        tdSql.execute("insert into vtb_org_child_2 values (1696000001000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")

        tdSql.execute("insert into vtb_org_normal_0 values (1696000002000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
        tdSql.execute("insert into vtb_org_child_0 values (1696000002000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
        tdSql.execute("insert into vtb_org_normal_1 values (1696000002000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
        tdSql.execute("insert into vtb_org_child_1 values (1696000002000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
        tdSql.execute("insert into vtb_org_normal_2 values (1696000002000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
        tdSql.execute("insert into vtb_org_child_2 values (1696000002000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")

        tdSql.execute("insert into vtb_org_normal_0 values (1696000003000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
        tdSql.execute("insert into vtb_org_child_0 values (1696000003000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
        tdSql.execute("insert into vtb_org_normal_1 values (1696000003000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
        tdSql.execute("insert into vtb_org_child_1 values (1696000003000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
        tdSql.execute("insert into vtb_org_normal_2 values (1696000003000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
        tdSql.execute("insert into vtb_org_child_2 values (1696000003000, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")

        tdLog.info(f"prepare virtual super table.")
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

        tdLog.info(f"prepare virtual normal table.")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ntb_full` ("
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


        tdLog.info(f"prepare virtual child table.")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ctb_full` ("
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


    def test_select_virtualtable_with_null_value(self):
        """Query: virtual table with null value

        1. test vtable select normal table projection with null value
        2. test vtable select child table projection with null value
        3. test vtable select super table projection with null value


        Catalog:
            - VirtualTable

        Since: v3.3.8.0

        Labels: virtual

        Jira: None

        History:
            - 2025-10-17 Jing Sima Created

        """
        tdSql.query("select * from test_vtable_select_with_null.vtb_virtual_ntb_full");
        tdSql.checkData(0, 0, 1696000000000)
        tdSql.checkData(1, 0, 1696000001000)
        tdSql.checkData(2, 0, 1696000002000)
        tdSql.checkData(3, 0, 1696000003000)
        for i in range(0,3):
            for j in range(1, 16):
                tdSql.checkData(i, j, None)

        tdSql.query("select * from test_vtable_select_with_null.vtb_virtual_ctb_full");
        tdSql.checkData(0, 0, 1696000000000)
        tdSql.checkData(1, 0, 1696000001000)
        tdSql.checkData(2, 0, 1696000002000)
        tdSql.checkData(3, 0, 1696000003000)
        for i in range(0,3):
            for j in range(1, 16):
                tdSql.checkData(i, j, None)

        tdSql.query("select * from test_vtable_select_with_null.vtb_virtual_stb");
        tdSql.checkData(0, 0, 1696000000000)
        tdSql.checkData(1, 0, 1696000001000)
        tdSql.checkData(2, 0, 1696000002000)
        tdSql.checkData(3, 0, 1696000003000)
        for i in range(0,3):
            for j in range(1, 16):
                tdSql.checkData(i, j, None)
