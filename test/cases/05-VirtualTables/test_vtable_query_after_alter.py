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
from new_test_framework.utils import tdLog, tdSql, etool, tdCom, sleep
import os


class TestVtableQueryAfterAlter:

    def setup_class(cls):
        tdLog.info(f"prepare org tables.")

        tdSql.execute("drop database if exists test_vtable_select_after_alter;")
        tdSql.execute("create database test_vtable_select_after_alter;")
        tdSql.execute("use test_vtable_select_after_alter;")

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
        datafile = etool.getFilePath(__file__, "data", "data1.csv")
        tdSql.execute("insert into vtb_org_normal_0 file '%s';" % datafile)
        tdSql.execute("insert into vtb_org_child_0 file '%s';" % datafile)

        datafile = etool.getFilePath(__file__, "data", "data2.csv")
        tdSql.execute("insert into vtb_org_normal_1 file '%s';" % datafile)
        tdSql.execute("insert into vtb_org_child_1 file '%s';" % datafile)

        datafile = etool.getFilePath(__file__, "data", "data3.csv")
        tdSql.execute("insert into vtb_org_normal_2 file '%s';" % datafile)
        tdSql.execute("insert into vtb_org_child_2 file '%s';" % datafile)

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

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ntb_half_full` ("
                      "ts timestamp, "
                      "u_tinyint_col tinyint unsigned from vtb_org_normal_0.u_tinyint_col, "
                      "u_smallint_col smallint unsigned from vtb_org_normal_1.u_smallint_col, "
                      "u_int_col int unsigned from vtb_org_normal_2.u_int_col, "
                      "u_bigint_col bigint unsigned, "
                      "tinyint_col tinyint, "
                      "smallint_col smallint, "
                      "int_col int from vtb_org_normal_0.int_col, "
                      "bigint_col bigint from vtb_org_normal_1.bigint_col, "
                      "float_col float from vtb_org_normal_2.float_col, "
                      "double_col double, "
                      "bool_col bool, "
                      "binary_16_col binary(16),"
                      "binary_32_col binary(32) from vtb_org_normal_0.binary_32_col,"
                      "nchar_16_col nchar(16) from vtb_org_normal_1.nchar_16_col,"
                      "nchar_32_col nchar(32) from vtb_org_normal_2.nchar_32_col)")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ntb_empty` ("
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
                      "nchar_32_col nchar(32))")

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

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ctb_half_full` ("
                      "u_tinyint_col from vtb_org_normal_0.u_tinyint_col, "
                      "u_smallint_col from vtb_org_normal_1.u_smallint_col, "
                      "u_int_col from vtb_org_normal_2.u_int_col, "
                      "int_col from vtb_org_normal_0.int_col, "
                      "bigint_col from vtb_org_normal_1.bigint_col, "
                      "float_col from vtb_org_normal_2.float_col, "
                      "binary_32_col from vtb_org_normal_0.binary_32_col,"
                      "nchar_16_col from vtb_org_normal_1.nchar_16_col,"
                      "nchar_32_col from vtb_org_normal_2.nchar_32_col)"
                      "USING `vtb_virtual_stb` TAGS (1, false, 1, 1, 'child1', 'child1')")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ctb_empty` "
                      "USING `vtb_virtual_stb` TAGS (2, false, 2, 2, 'child2', 'child2')")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ctb_mix` ("
                      f"u_tinyint_col from vtb_org_child_0.u_tinyint_col, "
                      f"u_smallint_col from vtb_org_child_1.u_smallint_col, "
                      f"u_int_col from vtb_org_child_2.u_int_col, "
                      f"u_bigint_col from vtb_org_child_0.u_bigint_col, "
                      f"tinyint_col from vtb_org_child_1.tinyint_col, "
                      f"smallint_col from vtb_org_child_2.smallint_col, "
                      f"int_col from vtb_org_child_0.int_col, "
                      f"bigint_col from vtb_org_child_1.bigint_col, "
                      f"float_col from vtb_org_child_2.float_col, "
                      f"double_col from vtb_org_child_0.double_col, "
                      f"bool_col from vtb_org_child_1.bool_col, "
                      f"binary_16_col from vtb_org_child_2.binary_16_col,"
                      f"binary_32_col from vtb_org_child_0.binary_32_col,"
                      f"nchar_16_col from vtb_org_child_1.nchar_16_col,"
                      f"nchar_32_col from vtb_org_child_2.nchar_32_col)"
                      f"USING `vtb_virtual_stb` TAGS (3, false, 3, 3, 'child3', 'child3')")

    def check_row_and_col(self, table_name, rows, cols):
        tdSql.query(f"select * from {table_name};")
        tdSql.checkRows(rows)
        tdSql.checkCols(cols)

    def test_virtual_stable_and_child_table(self):
        """Query: after alter super/child table

        test virtual table select after alter stable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2025-3-15 Jing Sima Created
            - 2025-5-6 Huo Hong Migrated to new test framework

        """
        tdSql.execute("use test_vtable_select_after_alter;")
        self.check_row_and_col("vtb_virtual_ctb_full", 23333, 16)
        self.check_row_and_col("vtb_virtual_ctb_half_full", 23333, 16)
        self.check_row_and_col("vtb_virtual_ctb_empty", 0, 16)
        self.check_row_and_col("vtb_virtual_ctb_mix", 23333, 16)
        self.check_row_and_col("vtb_virtual_stb", 23333 * 3, 22)


        tdSql.execute("alter stable vtb_virtual_stb drop column u_smallint_col;")
        self.check_row_and_col("vtb_virtual_ctb_full", 23333, 15)
        self.check_row_and_col("vtb_virtual_ctb_half_full", 23333, 15)
        self.check_row_and_col("vtb_virtual_ctb_empty", 0, 15)
        self.check_row_and_col("vtb_virtual_ctb_mix", 23333, 15)
        self.check_row_and_col("vtb_virtual_stb", 23333 * 3, 21)

        tdSql.execute("alter stable vtb_virtual_stb add column u_smallint_col smallint unsigned;")
        self.check_row_and_col("vtb_virtual_ctb_full", 23333, 16)
        self.check_row_and_col("vtb_virtual_ctb_half_full", 23333, 16)
        self.check_row_and_col("vtb_virtual_ctb_empty", 0, 16)
        self.check_row_and_col("vtb_virtual_ctb_mix", 23333, 16)
        self.check_row_and_col("vtb_virtual_stb", 23333 * 3, 22)

        tdSql.execute("alter vtable vtb_virtual_ctb_full alter column u_smallint_col set vtb_org_normal_1.u_smallint_col;")
        tdSql.execute("alter vtable vtb_virtual_ctb_half_full alter column u_smallint_col set vtb_org_normal_1.u_smallint_col;")
        tdSql.execute("alter vtable vtb_virtual_ctb_mix alter column u_smallint_col set vtb_org_normal_1.u_smallint_col;")
        self.check_row_and_col("vtb_virtual_ctb_full", 23333, 16)
        self.check_row_and_col("vtb_virtual_ctb_half_full", 23333, 16)
        self.check_row_and_col("vtb_virtual_ctb_empty", 0, 16)
        self.check_row_and_col("vtb_virtual_ctb_mix", 23333, 16)
        self.check_row_and_col("vtb_virtual_stb", 23333 * 3, 22)

        tdSql.execute("alter stable vtb_virtual_stb add column u_smallint_col_new smallint unsigned;")
        self.check_row_and_col("vtb_virtual_ctb_full", 23333, 17)
        self.check_row_and_col("vtb_virtual_ctb_half_full", 23333, 17)
        self.check_row_and_col("vtb_virtual_ctb_empty", 0, 17)
        self.check_row_and_col("vtb_virtual_ctb_mix", 23333, 17)
        self.check_row_and_col("vtb_virtual_stb", 23333 * 3, 23)

        tdSql.execute("alter vtable vtb_virtual_ctb_full alter column u_smallint_col_new set vtb_org_normal_1.u_smallint_col;")
        tdSql.execute("alter vtable vtb_virtual_ctb_half_full alter column u_smallint_col_new set vtb_org_normal_1.u_smallint_col;")
        tdSql.execute("alter vtable vtb_virtual_ctb_mix alter column u_smallint_col_new set vtb_org_normal_1.u_smallint_col;")
        self.check_row_and_col("vtb_virtual_ctb_full", 23333, 17)
        self.check_row_and_col("vtb_virtual_ctb_half_full", 23333, 17)
        self.check_row_and_col("vtb_virtual_ctb_empty", 0, 17)
        self.check_row_and_col("vtb_virtual_ctb_mix", 23333, 17)
        self.check_row_and_col("vtb_virtual_stb", 23333 * 3, 23)

    def test_virtual_normal_table(self):
        """Query: after alter normal table

        test virtual table select after alter normal table

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2025-3-15 Jing Sima Created
            - 2025-5-6 Huo Hong Migrated to new test framework

        """
        tdSql.execute("use test_vtable_select_after_alter;")
        self.check_row_and_col("vtb_virtual_ntb_full", 23333, 16)
        self.check_row_and_col("vtb_virtual_ntb_half_full", 23333, 16)
        self.check_row_and_col("vtb_virtual_ntb_empty", 0, 16)

        tdSql.execute("alter vtable vtb_virtual_ntb_full drop column u_smallint_col;")
        tdSql.execute("alter vtable vtb_virtual_ntb_half_full drop column u_smallint_col;")
        tdSql.execute("alter vtable vtb_virtual_ntb_empty drop column u_smallint_col;")
        self.check_row_and_col("vtb_virtual_ntb_full", 23333, 15)
        self.check_row_and_col("vtb_virtual_ntb_half_full", 23333, 15)
        self.check_row_and_col("vtb_virtual_ntb_empty", 0, 15)

        tdSql.execute("alter vtable vtb_virtual_ntb_full add column u_smallint_col smallint unsigned;")
        tdSql.execute("alter vtable vtb_virtual_ntb_half_full add column u_smallint_col smallint unsigned;")
        tdSql.execute("alter vtable vtb_virtual_ntb_empty add column u_smallint_col smallint unsigned;")
        self.check_row_and_col("vtb_virtual_ntb_full", 23333, 16)
        self.check_row_and_col("vtb_virtual_ntb_half_full", 23333, 16)
        self.check_row_and_col("vtb_virtual_ntb_empty", 0, 16)

        tdSql.execute("alter vtable vtb_virtual_ntb_full alter column u_smallint_col set vtb_org_normal_1.u_smallint_col;")
        tdSql.execute("alter vtable vtb_virtual_ntb_half_full alter column u_smallint_col set vtb_org_normal_1.u_smallint_col;")
        tdSql.execute("alter vtable vtb_virtual_ntb_empty alter column u_smallint_col set vtb_org_normal_1.u_smallint_col;")
        self.check_row_and_col("vtb_virtual_ntb_full", 23333, 16)
        self.check_row_and_col("vtb_virtual_ntb_half_full", 23333, 16)
        self.check_row_and_col("vtb_virtual_ntb_empty", 10000, 16)

        tdSql.execute("alter vtable vtb_virtual_ntb_full add column u_smallint_col_new smallint unsigned;")
        tdSql.execute("alter vtable vtb_virtual_ntb_half_full add column u_smallint_col_new smallint unsigned;")
        tdSql.execute("alter vtable vtb_virtual_ntb_empty add column u_smallint_col_new smallint unsigned;")
        self.check_row_and_col("vtb_virtual_ntb_full", 23333, 17)
        self.check_row_and_col("vtb_virtual_ntb_half_full", 23333, 17)
        self.check_row_and_col("vtb_virtual_ntb_empty", 10000, 17)

        tdSql.execute("alter vtable vtb_virtual_ntb_full alter column u_smallint_col_new set vtb_org_normal_1.u_smallint_col;")
        tdSql.execute("alter vtable vtb_virtual_ntb_half_full alter column u_smallint_col_new set vtb_org_normal_1.u_smallint_col;")
        tdSql.execute("alter vtable vtb_virtual_ntb_empty alter column u_smallint_col_new set vtb_org_normal_1.u_smallint_col;")
        self.check_row_and_col("vtb_virtual_ntb_full", 23333, 17)
        self.check_row_and_col("vtb_virtual_ntb_half_full", 23333, 17)
        self.check_row_and_col("vtb_virtual_ntb_empty", 10000, 17)


