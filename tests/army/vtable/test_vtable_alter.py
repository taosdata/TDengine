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

    def prepare_tables(self):
        tdLog.info(f"prepare org tables.")
        tdSql.execute("drop database if exists test_vtable_alter;")
        tdSql.execute("create database test_vtable_alter;")
        tdSql.execute("use test_vtable_alter;")

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
        tdSql.execute("CREATE VTABLE `vtb_virtual_ctb0`("
                      "u_tinyint_col FROM vtb_org_child_0.u_tinyint_col, "
                      "u_smallint_col FROM vtb_org_child_1.u_smallint_col, "
                      "u_int_col FROM vtb_org_child_2.u_int_col, "
                      "u_bigint_col FROM vtb_org_child_3.u_bigint_col,"
                      "tinyint_col FROM vtb_org_child_4.tinyint_col, "
                      "smallint_col FROM vtb_org_child_5.smallint_col, "
                      "int_col FROM vtb_org_child_6.int_col, "
                      "bigint_col FROM vtb_org_child_7.bigint_col,"
                      "float_col FROM vtb_org_child_8.float_col, "
                      "double_col FROM vtb_org_child_9.double_col, "
                      "bool_col FROM vtb_org_child_10.bool_col, "
                      "binary_16_col FROM vtb_org_child_11.binary_16_col,"
                      "binary_32_col FROM vtb_org_child_12.binary_32_col, "
                      "nchar_16_col FROM vtb_org_child_13.nchar_16_col, "
                      "nchar_32_col FROM vtb_org_child_14.nchar_32_col,"
                      "varbinary_16_col FROM vtb_org_child_15.varbinary_16_col, "
                      "varbinary_32_col FROM vtb_org_child_16.varbinary_32_col, "
                      "geo_16_col FROM vtb_org_child_17.geo_16_col, "
                      "geo_32_col FROM vtb_org_child_18.geo_32_col) USING vtb_virtual_stb TAGS (0, false, 0, 0, 'vchild0', 'vchild0')")

        tdLog.info(f"prepare virtual normal tables.")
        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ntb0` ("
                      "ts timestamp, "
                      "u_tinyint_col tinyint unsigned from vtb_org_child_0.u_tinyint_col, "
                      "u_smallint_col smallint unsigned from vtb_org_normal_1.u_smallint_col, "
                      "u_int_col int unsigned from vtb_org_child_2.u_int_col, "
                      "u_bigint_col bigint unsigned from vtb_org_normal_3.u_bigint_col, "
                      "tinyint_col tinyint from vtb_org_child_4.tinyint_col, "
                      "smallint_col smallint from vtb_org_normal_5.smallint_col, "
                      "int_col int from vtb_org_child_6.int_col, "
                      "bigint_col bigint from vtb_org_normal_7.bigint_col, "
                      "float_col float from vtb_org_child_8.float_col, "
                      "double_col double from vtb_org_normal_9.double_col, "
                      "bool_col bool from vtb_org_child_10.bool_col, "
                      "binary_16_col binary(16) from vtb_org_normal_11.binary_16_col,"
                      "binary_32_col binary(32) from vtb_org_child_12.binary_32_col,"
                      "nchar_16_col nchar(16) from vtb_org_normal_13.nchar_16_col,"
                      "nchar_32_col nchar(32) from vtb_org_child_14.nchar_32_col,"
                      "varbinary_16_col varbinary(16) from vtb_org_normal_15.varbinary_16_col,"
                      "varbinary_32_col varbinary(32) from vtb_org_child_16.varbinary_32_col,"
                      "geo_16_col geometry(16) from vtb_org_normal_17.geo_16_col,"
                      "geo_32_col geometry(32) from vtb_org_child_18.geo_32_col)")

    def check_col_num(self, isnormal, col_num):
        if isnormal:
            tdSql.query("select * from information_schema.ins_columns where db_name='test_vtable_alter' and table_name='vtb_virtual_ntb0'")
        else:
            tdSql.query("select * from information_schema.ins_columns where db_name='test_vtable_alter' and table_name='vtb_virtual_ctb0'")

        tdSql.checkRows(col_num)

    def check_col_ref(self, isnormal, col_name, col_ref):
        if isnormal:
            tdSql.query(f"select col_source from information_schema.ins_columns where db_name='test_vtable_alter' and table_name='vtb_virtual_ntb0' and col_name='{col_name}'")
        else:
            tdSql.query(f"select col_source from information_schema.ins_columns where db_name='test_vtable_alter' and table_name='vtb_virtual_ctb0' and col_name='{col_name}'")

        tdSql.checkData(0, 0, col_ref)

    def check_col_type(self, isnormal, col_name, col_type):
        if isnormal:
            tdSql.query(f"select col_type from information_schema.ins_columns where db_name='test_vtable_alter' and table_name='vtb_virtual_ntb0' and col_name='{col_name}'")
        else:
            tdSql.query(f"select col_type from information_schema.ins_columns where db_name='test_vtable_alter' and table_name='vtb_virtual_ctb0' and col_name='{col_name}'")

        tdSql.checkData(0, 0, col_type)

    def check_col(self, isnormal, col_name, col_type, col_ref):
        self.check_col_type(isnormal, col_name, col_type)
        self.check_col_ref(isnormal, col_name, col_ref)

    def test_alter_virtual_normal_table(self):
        tdLog.info(f"test alter virtual normal tables.")

        tdSql.execute("use test_vtable_alter;")
        tdSql.execute("select database();")

        # 1. add column
        # 1.1. add column without column reference
        tdSql.execute("alter vtable vtb_virtual_ntb0 add column extra_boolcol bool")
        self.check_col_num(True, 21)
        self.check_col(True, "extra_boolcol", "BOOL", None)

        # 1.2. add column with column reference
        tdSql.execute("alter vtable vtb_virtual_ntb0 add column extra_intcol int from vtb_org_child_19.int_col")
        self.check_col_num(True, 22)
        self.check_col(True, "extra_intcol", "INT", "vtb_org_child_19.int_col")

        # 2. drop column
        tdSql.execute("alter vtable vtb_virtual_ntb0 drop column extra_intcol;")
        self.check_col_num(True, 21)

        # 3. change column reference
        # 3.1. change column reference to another column
        tdSql.execute("alter vtable vtb_virtual_ntb0 alter column extra_boolcol set vtb_org_child_19.bool_col;")
        self.check_col_num(True, 21)
        self.check_col(True, "extra_boolcol", "BOOL", "vtb_org_child_19.bool_col")

        # 3.2. change column reference to NULL
        tdSql.execute("alter vtable vtb_virtual_ntb0 alter column extra_boolcol set NULL;")
        self.check_col_num(True, 21)
        self.check_col(True, "extra_boolcol", "BOOL", None)

        # 4. change column type length
        self.check_col(True, "nchar_16_col", "NCHAR(16)", "vtb_org_normal_13.nchar_16_col")
        tdSql.execute("alter vtable vtb_virtual_ntb0 alter column nchar_16_col set NULL;")
        self.check_col_num(True, 21)
        self.check_col(True, "nchar_16_col", "NCHAR(16)", None)

        tdSql.execute("alter vtable vtb_virtual_ntb0 modify column nchar_16_col nchar(32);")
        self.check_col_num(True, 21)
        self.check_col(True, "nchar_16_col", "NCHAR(32)", None)

        tdSql.execute("alter vtable vtb_virtual_ntb0 alter column nchar_16_col set vtb_org_child_19.nchar_32_col;")
        self.check_col_num(True, 21)
        self.check_col(True, "nchar_16_col", "NCHAR(32)", "vtb_org_child_19.nchar_32_col")

        # 5. change column name
        self.check_col(True, "u_smallint_col", "SMALLINT UNSIGNED", "vtb_org_normal_1.u_smallint_col")
        tdSql.execute("alter vtable vtb_virtual_ntb0 rename column u_smallint_col u_smallint_col_rename;")
        self.check_col_num(True, 21)
        self.check_col(True, "u_smallint_col_rename", "SMALLINT UNSIGNED", "vtb_org_normal_1.u_smallint_col")

    def test_alter_virtual_child_table(self):
        tdLog.info(f"test alter virtual child tables.")

        tdSql.execute("use test_vtable_alter;")
        tdSql.execute("select database();")

        # 1. change column reference
        # 1.1. change column reference to another column
        tdSql.execute("alter vtable vtb_virtual_ctb0 alter column bool_col set vtb_org_child_19.bool_col;")
        self.check_col_num(False, 20)
        self.check_col(False, "bool_col", "BOOL", "vtb_org_child_19.bool_col")

        # 1.2. change column reference to NULL
        tdSql.execute("alter vtable vtb_virtual_ctb0 alter column bool_col set NULL;")
        self.check_col_num(False, 20)
        self.check_col(False, "bool_col", "BOOL", None)

        # 2. change tag value
        tdSql.execute("alter vtable vtb_virtual_ctb0 set tag int_tag = 10;")
        tdSql.query(f"select tag_value from information_schema.ins_tags where db_name='test_vtable_alter' and table_name='vtb_virtual_ctb0' and tag_name='int_tag'")
        tdSql.checkData(0, 0, 10)


    def test_alter_virtual_super_table(self):
        tdLog.info(f"test alter virtual super tables.")

        tdSql.execute("use test_vtable_alter;")
        tdSql.execute("select database();")


    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        self.prepare_tables()
        self.test_alter_virtual_normal_table()
        self.test_alter_virtual_child_table()
        self.test_alter_virtual_super_table()
        #self.test_error_cases()

        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
