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


class TestVtableCreate:

    def setup_class(cls):
        tdLog.info(f"prepare org tables.")

        tdSql.execute("create database test_vtable_create;")
        tdSql.execute("use test_vtable_create;")

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

        tdLog.info(f"prepare org normal table with compositive primary key.")
        tdSql.execute(f"CREATE TABLE `vtb_org_normal_pk` (ts timestamp, int_col int PRIMARY KEY, u_smallint_col int unsigned)")

    def test_create_virtual_super_table(self):
        """Create: virtual super table

        test create virtual super tables

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: create,virtual,integration,functional

        Jira: None

        History:
            - 2025-3-15 Jing Sima Created
            - 2025-5-6 Huo Hong Migrated to new test framework

        """
        tdLog.info(f"test create virtual super tables.")

        tdSql.execute("use test_vtable_create;")
        tdSql.execute("select database();")

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

    def check_virtual_table_create(self, vctable_num, vntable_num):
        tdSql.query("show test_vtable_create.vtables;")
        tdSql.checkRows(vctable_num + vntable_num)
        tdSql.query("show child test_vtable_create.vtables;")
        tdSql.checkRows(vctable_num)
        tdSql.query("show normal test_vtable_create.vtables;")
        tdSql.checkRows(vntable_num)

    def test_create_virtual_child_table(self):
        """Create: virtual child table

        1.create virtual child table and don't use 'FROM' to specify the origin table
        2.create virtual child table and use 'FROM' to specify the origin table

        Catalog:
            - VirtualTable
            
        Since: v3.3.6.0

        Labels: create,virtual,integration,functional

        Jira: None

        History:
            - 2025-3-15 Jing Sima Created
            - 2025-5-6 Huo Hong Migrated to new test framework

        """
        tdLog.info(f"test create virtual child tables.")

        tdSql.execute("use test_vtable_create;")
        tdSql.execute("select database();")

        # 1.create virtual child table and don't use 'FROM' to specify the origin table
        # 1.1 specify part of columns of vtable
        # 1.1.1 origin table is child table
        tdSql.execute("CREATE VTABLE `vtb_virtual_ctb0`("
                      "vtb_org_child_0.u_tinyint_col, "
                      "vtb_org_child_1.u_smallint_col, "
                      "vtb_org_child_2.u_int_col, "
                      "vtb_org_child_3.u_bigint_col,"
                      "vtb_org_child_4.tinyint_col) USING vtb_virtual_stb TAGS (0, false, 0, 0, 'vchild0', 'vchild0')")

        self.check_virtual_table_create(1, 0)

        # 1.1.2 origin table is normal table
        tdSql.execute("CREATE VTABLE `vtb_virtual_ctb1`("
                      "vtb_org_normal_0.u_tinyint_col, "
                      "vtb_org_normal_1.u_smallint_col, "
                      "vtb_org_normal_2.u_int_col, "
                      "vtb_org_normal_3.u_bigint_col,"
                      "vtb_org_normal_4.tinyint_col) USING vtb_virtual_stb TAGS (1, false, 1, 1, 'vchild1', 'vchild1')")

        self.check_virtual_table_create(2, 0)

        # 1.1.3 origin table is child table and normal table
        tdSql.execute("CREATE VTABLE `vtb_virtual_ctb2`("
                      "vtb_org_child_0.u_tinyint_col, "
                      "vtb_org_normal_1.u_smallint_col, "
                      "vtb_org_child_2.u_int_col, "
                      "vtb_org_normal_3.u_bigint_col,"
                      "vtb_org_child_4.tinyint_col) USING vtb_virtual_stb TAGS (2, false, 2, 2, 'vchild2', 'vchild2')")

        self.check_virtual_table_create(3, 0)

        # 1.2 specify all columns of vtable
        # 1.2.1 origin table is child table
        tdSql.execute("CREATE VTABLE `vtb_virtual_ctb3`("
                      "vtb_org_child_0.u_tinyint_col, "
                      "vtb_org_child_1.u_smallint_col, "
                      "vtb_org_child_2.u_int_col, "
                      "vtb_org_child_3.u_bigint_col,"
                      "vtb_org_child_4.tinyint_col, "
                      "vtb_org_child_5.smallint_col, "
                      "vtb_org_child_6.int_col, "
                      "vtb_org_child_7.bigint_col,"
                      "vtb_org_child_8.float_col, "
                      "vtb_org_child_9.double_col, "
                      "vtb_org_child_10.bool_col, "
                      "vtb_org_child_11.binary_16_col,"
                      "vtb_org_child_12.binary_32_col, "
                      "vtb_org_child_13.nchar_16_col, "
                      "vtb_org_child_14.nchar_32_col,"
                      "vtb_org_child_15.varbinary_16_col, "
                      "vtb_org_child_16.varbinary_32_col, "
                      "vtb_org_child_17.geo_16_col, "
                      "vtb_org_child_18.geo_32_col) USING vtb_virtual_stb TAGS (3, false, 3, 3, 'vchild3', 'vchild3')")

        self.check_virtual_table_create(4, 0)

        # 1.2.2 origin table is normal table
        tdSql.execute("CREATE VTABLE `vtb_virtual_ctb4`("
                      "vtb_org_normal_0.u_tinyint_col, "
                      "vtb_org_normal_1.u_smallint_col, "
                      "vtb_org_normal_2.u_int_col, "
                      "vtb_org_normal_3.u_bigint_col,"
                      "vtb_org_normal_4.tinyint_col, "
                      "vtb_org_normal_5.smallint_col, "
                      "vtb_org_normal_6.int_col, "
                      "vtb_org_normal_7.bigint_col,"
                      "vtb_org_normal_8.float_col, "
                      "vtb_org_normal_9.double_col, "
                      "vtb_org_normal_10.bool_col, "
                      "vtb_org_normal_11.binary_16_col,"
                      "vtb_org_normal_12.binary_32_col, "
                      "vtb_org_normal_13.nchar_16_col, "
                      "vtb_org_normal_14.nchar_32_col,"
                      "vtb_org_normal_15.varbinary_16_col, "
                      "vtb_org_normal_16.varbinary_32_col, "
                      "vtb_org_normal_17.geo_16_col, "
                      "vtb_org_normal_18.geo_32_col) USING vtb_virtual_stb TAGS (4, false, 4, 4, 'vchild4', 'vchild4')")

        self.check_virtual_table_create(5, 0)

        # 1.2.3 origin table is child table and normal table
        tdSql.execute("CREATE VTABLE `vtb_virtual_ctb5`("
                      "vtb_org_child_0.u_tinyint_col, "
                      "vtb_org_normal_1.u_smallint_col, "
                      "vtb_org_child_2.u_int_col, "
                      "vtb_org_normal_3.u_bigint_col,"
                      "vtb_org_child_4.tinyint_col, "
                      "vtb_org_normal_5.smallint_col, "
                      "vtb_org_child_6.int_col, "
                      "vtb_org_normal_7.bigint_col,"
                      "vtb_org_child_8.float_col, "
                      "vtb_org_normal_9.double_col, "
                      "vtb_org_child_10.bool_col, "
                      "vtb_org_normal_11.binary_16_col,"
                      "vtb_org_child_12.binary_32_col, "
                      "vtb_org_normal_13.nchar_16_col, "
                      "vtb_org_child_14.nchar_32_col,"
                      "vtb_org_normal_15.varbinary_16_col, "
                      "vtb_org_child_16.varbinary_32_col, "
                      "vtb_org_normal_17.geo_16_col, "
                      "vtb_org_child_18.geo_32_col) USING vtb_virtual_stb TAGS (5, false, 5, 5, 'vchild5', 'vchild5')")

        self.check_virtual_table_create(6, 0)

        # 2.create virtual child table and use 'FROM' to specify the origin table
        # 2.1 specify part of columns of vtable
        # 2.1.1 origin table is child table
        tdSql.execute("CREATE VTABLE `vtb_virtual_ctb6`("
                      "u_tinyint_col FROM vtb_org_child_0.u_tinyint_col, "
                      "u_bigint_col FROM vtb_org_child_3.u_bigint_col,"
                      "int_col FROM vtb_org_child_6.int_col,"
                      "float_col FROM vtb_org_child_8.float_col,"
                      "bool_col FROM vtb_org_child_10.bool_col,"
                      "binary_32_col FROM vtb_org_child_12.binary_32_col) USING vtb_virtual_stb  TAGS (6, false, 6, 6, 'vchild6', 'vchild6')")

        self.check_virtual_table_create(7, 0)

        # 2.1.2 origin table is normal table
        tdSql.execute("CREATE VTABLE `vtb_virtual_ctb7`("
                      "u_tinyint_col FROM vtb_org_normal_0.u_tinyint_col, "
                      "u_bigint_col FROM vtb_org_normal_3.u_bigint_col,"
                      "int_col FROM vtb_org_normal_6.int_col,"
                      "float_col FROM vtb_org_normal_8.float_col,"
                      "bool_col FROM vtb_org_normal_10.bool_col,"
                      "binary_32_col FROM vtb_org_normal_12.binary_32_col) USING vtb_virtual_stb TAGS (7, false, 7, 7, 'vchild7', 'vchild7')")

        self.check_virtual_table_create(8, 0)

        # 2.1.3 origin table is child table and normal table
        tdSql.execute("CREATE VTABLE `vtb_virtual_ctb8`("
                      "u_tinyint_col FROM vtb_org_child_0.u_tinyint_col, "
                      "u_bigint_col FROM vtb_org_normal_3.u_bigint_col,"
                      "int_col FROM vtb_org_child_6.int_col,"
                      "float_col FROM vtb_org_normal_8.float_col,"
                      "bool_col FROM vtb_org_child_10.bool_col,"
                      "binary_32_col FROM vtb_org_normal_12.binary_32_col) USING vtb_virtual_stb TAGS (8, false, 8, 8, 'vchild8', 'vchild8')")

        self.check_virtual_table_create(9, 0)

        # 2.2 specify all columns of vtable
        # 2.2.1 origin table is child table
        tdSql.execute("CREATE VTABLE `vtb_virtual_ctb9`("
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
                      "geo_32_col FROM vtb_org_child_18.geo_32_col) USING vtb_virtual_stb TAGS (9, false, 9, 9, 'vchild9', 'vchild9')")

        self.check_virtual_table_create(10, 0)

        # 2.2.2 origin table is normal table
        tdSql.execute("CREATE VTABLE `vtb_virtual_ctb10`("
                      "u_tinyint_col FROM vtb_org_normal_0.u_tinyint_col, "
                      "u_smallint_col FROM vtb_org_normal_1.u_smallint_col, "
                      "u_int_col FROM vtb_org_normal_2.u_int_col, "
                      "u_bigint_col FROM vtb_org_normal_3.u_bigint_col,"
                      "tinyint_col FROM vtb_org_normal_4.tinyint_col, "
                      "smallint_col FROM vtb_org_normal_5.smallint_col, "
                      "int_col FROM vtb_org_normal_6.int_col, "
                      "bigint_col FROM vtb_org_normal_7.bigint_col,"
                      "float_col FROM vtb_org_normal_8.float_col, "
                      "double_col FROM vtb_org_normal_9.double_col, "
                      "bool_col FROM vtb_org_normal_10.bool_col, "
                      "binary_16_col FROM vtb_org_normal_11.binary_16_col,"
                      "binary_32_col FROM vtb_org_normal_12.binary_32_col, "
                      "nchar_16_col FROM vtb_org_normal_13.nchar_16_col, "
                      "nchar_32_col FROM vtb_org_normal_14.nchar_32_col,"
                      "varbinary_16_col FROM vtb_org_normal_15.varbinary_16_col, "
                      "varbinary_32_col FROM vtb_org_normal_16.varbinary_32_col, "
                      "geo_16_col FROM vtb_org_normal_17.geo_16_col, "
                      "geo_32_col FROM vtb_org_normal_18.geo_32_col) USING vtb_virtual_stb TAGS (10, false, 10, 10, 'vchild10', 'vchild10')")

        self.check_virtual_table_create(11, 0)

        # 2.2.3 origin table is child table and normal table
        tdSql.execute("CREATE VTABLE `vtb_virtual_ctb11`("
                      "u_tinyint_col FROM vtb_org_child_0.u_tinyint_col, "
                      "u_smallint_col FROM vtb_org_normal_1.u_smallint_col, "
                      "u_int_col FROM vtb_org_child_2.u_int_col, "
                      "u_bigint_col FROM vtb_org_normal_3.u_bigint_col,"
                      "tinyint_col FROM vtb_org_child_4.tinyint_col, "
                      "smallint_col FROM vtb_org_normal_5.smallint_col, "
                      "int_col FROM vtb_org_child_6.int_col, "
                      "bigint_col FROM vtb_org_normal_7.bigint_col,"
                      "float_col FROM vtb_org_child_8.float_col, "
                      "double_col FROM vtb_org_normal_9.double_col, "
                      "bool_col FROM vtb_org_child_10.bool_col, "
                      "binary_16_col FROM vtb_org_normal_11.binary_16_col,"
                      "binary_32_col FROM vtb_org_child_12.binary_32_col, "
                      "nchar_16_col FROM vtb_org_normal_13.nchar_16_col, "
                      "nchar_32_col FROM vtb_org_child_14.nchar_32_col,"
                      "varbinary_16_col FROM vtb_org_normal_15.varbinary_16_col, "
                      "varbinary_32_col FROM vtb_org_child_16.varbinary_32_col, "
                      "geo_16_col FROM vtb_org_normal_17.geo_16_col, "
                      "geo_32_col FROM vtb_org_child_18.geo_32_col) USING vtb_virtual_stb TAGS (11, false, 11, 11, 'vchild11', 'vchild11')")

        self.check_virtual_table_create(12, 0)

        # 2.3 specify all columns in random order of vtable
        # 2.3.1 origin table is child table
        tdSql.execute("CREATE VTABLE `vtb_virtual_ctb12`("
                      "u_tinyint_col FROM vtb_org_child_0.u_tinyint_col, "
                      "u_bigint_col FROM vtb_org_child_3.u_bigint_col,"
                      "int_col FROM vtb_org_child_6.int_col,"
                      "float_col FROM vtb_org_child_8.float_col,"
                      "bool_col FROM vtb_org_child_10.bool_col,"
                      "binary_32_col FROM vtb_org_child_12.binary_32_col,"
                      "tinyint_col FROM vtb_org_child_4.tinyint_col, "
                      "smallint_col FROM vtb_org_child_5.smallint_col, "
                      "double_col FROM vtb_org_child_9.double_col, "
                      "binary_16_col FROM vtb_org_child_11.binary_16_col,"
                      "nchar_16_col FROM vtb_org_child_13.nchar_16_col, "
                      "nchar_32_col FROM vtb_org_child_14.nchar_32_col,"
                      "varbinary_16_col FROM vtb_org_child_15.varbinary_16_col, "
                      "varbinary_32_col FROM vtb_org_child_16.varbinary_32_col, "
                      "geo_16_col FROM vtb_org_child_17.geo_16_col, "
                      "geo_32_col FROM vtb_org_child_18.geo_32_col, "
                      "u_smallint_col FROM vtb_org_child_1.u_smallint_col, "
                      "bigint_col FROM vtb_org_child_7.bigint_col) USING vtb_virtual_stb TAGS (12, false, 12, 12, 'vchild12', 'vchild12')")

        self.check_virtual_table_create(13, 0)

        # 2.3.2 origin table is normal table
        tdSql.execute("CREATE VTABLE `vtb_virtual_ctb13`("
                      "u_tinyint_col FROM vtb_org_normal_0.u_tinyint_col, "
                      "u_bigint_col FROM vtb_org_normal_3.u_bigint_col,"
                      "int_col FROM vtb_org_normal_6.int_col,"
                      "float_col FROM vtb_org_normal_8.float_col,"
                      "bool_col FROM vtb_org_normal_10.bool_col,"
                      "binary_32_col FROM vtb_org_normal_12.binary_32_col,"
                      "tinyint_col FROM vtb_org_normal_4.tinyint_col, "
                      "smallint_col FROM vtb_org_normal_5.smallint_col, "
                      "double_col FROM vtb_org_normal_9.double_col, "
                      "binary_16_col FROM vtb_org_normal_11.binary_16_col,"
                      "nchar_16_col FROM vtb_org_normal_13.nchar_16_col, "
                      "nchar_32_col FROM vtb_org_normal_14.nchar_32_col,"
                      "varbinary_16_col FROM vtb_org_normal_15.varbinary_16_col, "
                      "varbinary_32_col FROM vtb_org_normal_16.varbinary_32_col, "
                      "geo_16_col FROM vtb_org_normal_17.geo_16_col, "
                      "geo_32_col FROM vtb_org_normal_18.geo_32_col, "
                      "u_smallint_col FROM vtb_org_normal_1.u_smallint_col, "
                      "bigint_col FROM vtb_org_normal_7.bigint_col) USING vtb_virtual_stb TAGS (13, false, 13, 13, 'vchild13', 'vchild13')")

        self.check_virtual_table_create(14, 0)

        # 2.3.3 origin table is child table and normal table
        tdSql.execute("CREATE VTABLE `vtb_virtual_ctb14`("
                      "u_tinyint_col FROM vtb_org_child_0.u_tinyint_col, "
                      "u_bigint_col FROM vtb_org_normal_3.u_bigint_col,"
                      "int_col FROM vtb_org_child_6.int_col,"
                      "float_col FROM vtb_org_normal_8.float_col,"
                      "bool_col FROM vtb_org_child_10.bool_col,"
                      "binary_32_col FROM vtb_org_normal_12.binary_32_col,"
                      "tinyint_col FROM vtb_org_child_4.tinyint_col, "
                      "smallint_col FROM vtb_org_normal_5.smallint_col, "
                      "double_col FROM vtb_org_child_9.double_col, "
                      "binary_16_col FROM vtb_org_normal_11.binary_16_col,"
                      "nchar_16_col FROM vtb_org_child_13.nchar_16_col, "
                      "nchar_32_col FROM vtb_org_normal_14.nchar_32_col,"
                      "varbinary_16_col FROM vtb_org_child_15.varbinary_16_col, "
                      "varbinary_32_col FROM vtb_org_normal_16.varbinary_32_col, "
                      "geo_16_col FROM vtb_org_child_17.geo_16_col, "
                      "geo_32_col FROM vtb_org_normal_18.geo_32_col, "
                      "u_smallint_col FROM vtb_org_normal_1.u_smallint_col, "
                      "bigint_col FROM vtb_org_child_7.bigint_col) USING vtb_virtual_stb TAGS (14, false, 14, 14, 'vchild14', 'vchild14')")

        self.check_virtual_table_create(15, 0)

    def test_create_virtual_normal_table(self):
        """Create: virtual normal table

        test create virtual normal tables

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: create,virtual,integration,functional

        Jira: None

        History:
            - 2025-3-15 Jing Sima Created
            - 2025-5-6 Huo Hong Migrated to new test framework

        """
        tdLog.info(f"test create virtual normal tables.")

        tdSql.execute("use test_vtable_create;")
        tdSql.execute("select database();")
        # 3. create virtual normal table
        # 3.1 specify part of columns of vtable
        # 3.1.1 origin table is child table
        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ntb0` ("
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

        self.check_virtual_table_create(15, 1)

        # 3.1.2 origin table is normal table
        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ntb1` ("
                      "ts timestamp, "
                      "u_tinyint_col tinyint unsigned from vtb_org_normal_0.u_tinyint_col, "
                      "u_smallint_col smallint unsigned from vtb_org_normal_1.u_smallint_col, "
                      "u_int_col int unsigned, "
                      "u_bigint_col bigint unsigned, "
                      "tinyint_col tinyint from vtb_org_normal_4.tinyint_col, "
                      "smallint_col smallint, "
                      "int_col int from vtb_org_normal_6.int_col, "
                      "bigint_col bigint from vtb_org_normal_7.bigint_col, "
                      "float_col float, "
                      "double_col double, "
                      "bool_col bool from vtb_org_normal_10.bool_col, "
                      "binary_16_col binary(16) from vtb_org_normal_11.binary_16_col,"
                      "binary_32_col binary(32) from vtb_org_normal_12.binary_32_col,"
                      "nchar_16_col nchar(16),"
                      "nchar_32_col nchar(32),"
                      "varbinary_16_col varbinary(16) from vtb_org_normal_15.varbinary_16_col,"
                      "varbinary_32_col varbinary(32),"
                      "geo_16_col geometry(16) from vtb_org_normal_17.geo_16_col,"
                      "geo_32_col geometry(32) from vtb_org_normal_18.geo_32_col)")

        self.check_virtual_table_create(15, 2)

        # 3.1.3 origin table is child table and normal table
        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ntb2` ("
                      "ts timestamp, "
                      "u_tinyint_col tinyint unsigned from vtb_org_child_0.u_tinyint_col, "
                      "u_smallint_col smallint unsigned from vtb_org_normal_1.u_smallint_col, "
                      "u_int_col int unsigned, "
                      "u_bigint_col bigint unsigned, "
                      "tinyint_col tinyint from vtb_org_child_4.tinyint_col, "
                      "smallint_col smallint, "
                      "int_col int, "
                      "bigint_col bigint from vtb_org_normal_7.bigint_col, "
                      "float_col float from vtb_org_child_8.float_col, "
                      "double_col double, "
                      "bool_col bool, "
                      "binary_16_col binary(16) from vtb_org_normal_11.binary_16_col,"
                      "binary_32_col binary(32) from vtb_org_child_12.binary_32_col,"
                      "nchar_16_col nchar(16),"
                      "nchar_32_col nchar(32),"
                      "varbinary_16_col varbinary(16) from vtb_org_normal_15.varbinary_16_col,"
                      "varbinary_32_col varbinary(32) from vtb_org_child_16.varbinary_32_col,"
                      "geo_16_col geometry(16),"
                      "geo_32_col geometry(32) from vtb_org_child_18.geo_32_col)")

        self.check_virtual_table_create(15, 3)

        # 3.2 specify all columns of vtable
        # 3.2.1 origin table is child table
        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ntb3` ("
                      "ts timestamp, "
                      "u_tinyint_col tinyint unsigned from vtb_org_child_0.u_tinyint_col, "
                      "u_smallint_col smallint unsigned from vtb_org_child_1.u_smallint_col, "
                      "u_int_col int unsigned from vtb_org_child_2.u_int_col, "
                      "u_bigint_col bigint unsigned from vtb_org_child_3.u_bigint_col, "
                      "tinyint_col tinyint from vtb_org_child_4.tinyint_col, "
                      "smallint_col smallint from vtb_org_child_5.smallint_col, "
                      "int_col int from vtb_org_child_6.int_col, "
                      "bigint_col bigint from vtb_org_child_7.bigint_col, "
                      "float_col float from vtb_org_child_8.float_col, "
                      "double_col double from vtb_org_child_9.double_col, "
                      "bool_col bool from vtb_org_child_10.bool_col, "
                      "binary_16_col binary(16) from vtb_org_child_11.binary_16_col,"
                      "binary_32_col binary(32) from vtb_org_child_12.binary_32_col,"
                      "nchar_16_col nchar(16) from vtb_org_child_13.nchar_16_col,"
                      "nchar_32_col nchar(32) from vtb_org_child_14.nchar_32_col,"
                      "varbinary_16_col varbinary(16) from vtb_org_child_15.varbinary_16_col,"
                      "varbinary_32_col varbinary(32) from vtb_org_child_16.varbinary_32_col,"
                      "geo_16_col geometry(16) from vtb_org_child_17.geo_16_col,"
                      "geo_32_col geometry(32) from vtb_org_child_18.geo_32_col)")

        self.check_virtual_table_create(15, 4)

        # 3.2.2 origin table is normal table
        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ntb4` ("
                      "ts timestamp, "
                      "u_tinyint_col tinyint unsigned from vtb_org_normal_0.u_tinyint_col, "
                      "u_smallint_col smallint unsigned from vtb_org_normal_1.u_smallint_col, "
                      "u_int_col int unsigned from vtb_org_normal_2.u_int_col, "
                      "u_bigint_col bigint unsigned from vtb_org_normal_3.u_bigint_col, "
                      "tinyint_col tinyint from vtb_org_normal_4.tinyint_col, "
                      "smallint_col smallint from vtb_org_normal_5.smallint_col, "
                      "int_col int from vtb_org_normal_6.int_col, "
                      "bigint_col bigint from vtb_org_normal_7.bigint_col, "
                      "float_col float from vtb_org_normal_8.float_col, "
                      "double_col double from vtb_org_normal_9.double_col, "
                      "bool_col bool from vtb_org_normal_10.bool_col, "
                      "binary_16_col binary(16) from vtb_org_normal_11.binary_16_col,"
                      "binary_32_col binary(32) from vtb_org_normal_12.binary_32_col,"
                      "nchar_16_col nchar(16) from vtb_org_normal_13.nchar_16_col,"
                      "nchar_32_col nchar(32) from vtb_org_normal_14.nchar_32_col,"
                      "varbinary_16_col varbinary(16) from vtb_org_normal_15.varbinary_16_col,"
                      "varbinary_32_col varbinary(32) from vtb_org_normal_16.varbinary_32_col,"
                      "geo_16_col geometry(16) from vtb_org_normal_17.geo_16_col,"
                      "geo_32_col geometry(32) from vtb_org_normal_18.geo_32_col)")

        self.check_virtual_table_create(15, 5)

        # 3.2.3 origin table is child table and normal table
        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ntb5` ("
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

        self.check_virtual_table_create(15, 6)

    def test_error_cases(self):
        """Create: virtual table errors

        1. create virtual child table using non-virtual super table
        2. create child table using virtual super table
        3. create virtual child table using non-exist super table
        4. column definition different from referenced column
        5. set data source for primary timestamp column
        6. data source column does not exist
        7. data source table does not exist
        8. data source table has composite primary key
        9. data source is tag
        10. create virtual child table using from to specify some columns and do not use from for other columns
        11. create virtual table using decimal

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: create,negative,virtual,integration,functional

        Jira: None

        History:
            - 2025-3-15 Jing Sima Created
            - 2025-5-6 Huo Hong Migrated to new test framework

        """
        # 1. create virtual child table using non-virtual super table
        tdSql.error("CREATE VTABLE `error_vtb_virtual_ctb0`("
                    "vtb_org_child_0.u_tinyint_col, "
                    "vtb_org_child_1.u_smallint_col, "
                    "vtb_org_child_2.u_int_col, "
                    "vtb_org_child_3.u_bigint_col,"
                    "vtb_org_child_4.tinyint_col) USING vtb_org_stb TAGS (0, false, 0, 0, 'vchild0', 'vchild0')")

        # 2. create child table using virtual super table
        tdSql.error("CREATE TABLE `error_vtb_virtual_ctb1` USING vtb_virtual_stb TAGS (1, false, 1, 1, 'vchild1', 'vchild1')")

        # 3. create virtual child table using non-exist super table
        tdSql.error("CREATE VTABLE `error_vtb_virtual_ctb2`("
                    "vtb_org_child_0.u_tinyint_col, "
                    "vtb_org_child_1.u_smallint_col, "
                    "vtb_org_child_2.u_int_col, "
                    "vtb_org_child_3.u_bigint_col,"
                    "vtb_org_child_4.tinyint_col) USING vtb_virtual_stb_not_exist TAGS (2, false, 2, 2, 'vchild2', 'vchild2')")

        # 4. column definition different from referenced column
        # 4.1 child table
        # 4.1.1 child table use from to specify the origin table
        tdSql.error("CREATE VTABLE `error_vtb_virtual_ctb3`("
                    "u_tinyint_col FROM vtb_org_child_0.tinyint_col"
                    ") USING vtb_virtual_stb TAGS (3, false, 3, 3, 'vchild3', 'vchild3')")

        # 4.1.2 child table do not use from to specify the origin table
        tdSql.error("CREATE VTABLE `error_vtb_virtual_ctb4`("
                    "vtb_org_child_0.tinyint_col"
                    ") USING vtb_virtual_stb TAGS (4, false, 4, 4, 'vchild4', 'vchild4')")

        # 4.2 normal table
        tdSql.error("CREATE VTABLE `error_vtb_virtual_ntb0` ("
                    "ts timestamp, "
                    "u_tinyint_col tinyint unsigned from vtb_org_child_0.tinyint_col)")

        # 5. set data source for primary timestamp column
        # 5.1 child table
        tdSql.error("CREATE VTABLE `error_vtb_virtual_ctb5`("
                    "ts timestamp FROM vtb_org_child_0.ts, "
                    "u_tinyint_col FROM vtb_org_child_0.u_tinyint_col, "
                    "u_bigint_col FROM vtb_org_child_3.u_bigint_col,"
                    "int_col FROM vtb_org_child_6.int_col,"
                    "float_col FROM vtb_org_child_8.float_col,"
                    "bool_col FROM vtb_org_child_10.bool_col,"
                    "binary_32_col FROM vtb_org_child_12.binary_32_col) USING vtb_virtual_stb TAGS (5, false, 5, 5, 'vchild5', 'vchild5')")

        # 5.2 normal table
        tdSql.error("CREATE VTABLE `error_vtb_virtual_ntb1` ("
                    "ts timestamp FROM vtb_org_normal_0.ts, "
                    "u_tinyint_col tinyint unsigned from vtb_org_normal_0.u_tinyint_col, "
                    "u_smallint_col smallint unsigned from vtb_org_normal_1.u_smallint_col, "
                    "u_int_col int unsigned)")

        # 6. data source column does not exist
        # 6.1 child table
        # 6.1.1 child table use from to specify the origin table
        tdSql.error("CREATE VTABLE `error_vtb_virtual_ctb6`("
                    "u_tinyint_col FROM vtb_org_child_0.not_exists_col"
                    ") USING vtb_virtual_stb TAGS (6, false, 6, 6, 'vchild6', 'vchild6')")

        # 6.1.2 child table do not use from to specify the origin table
        tdSql.error("CREATE VTABLE `error_vtb_virtual_ctb7`("
                    "vtb_org_child_0.not_exists_col"
                    ") USING vtb_virtual_stb TAGS (7, false, 7, 7, 'vchild7', 'vchild7')")

        # 6.2 normal table
        tdSql.error("CREATE VTABLE `error_vtb_virtual_ntb2` ("
                    "ts timestamp, "
                    "u_tinyint_col tinyint unsigned from vtb_org_child_0.not_exists_col)")

        # 7. data source table does not exist
        # 7.1 child table
        # 7.1.1 child table use from to specify the origin table
        tdSql.error("CREATE VTABLE `error_vtb_virtual_ctb8`("
                    "u_tinyint_col FROM not_exists_table.u_tinyint_col"
                    ") USING vtb_virtual_stb TAGS (8, false, 8, 8, 'vchild8', 'vchild8')")

        # 7.1.2 child table do not use from to specify the origin table
        tdSql.error("CREATE VTABLE `error_vtb_virtual_ctb9`("
                    "not_exists_table.u_tinyint_col"
                    ") USING vtb_virtual_stb TAGS (9, false, 9, 9, 'vchild9', 'vchild9')")

        # 7.2 normal table
        tdSql.error("CREATE VTABLE `error_vtb_virtual_ntb3` ("
                    "ts timestamp, "
                    "u_tinyint_col tinyint unsigned from not_exists_table.u_tinyint_col)")

        # 8. data source table has composite primary key
        # 8.1 child table
        # 8.1.1 child table use from to specify the origin table
        tdSql.error("CREATE VTABLE `error_vtb_virtual_ctb10`("
                    "u_tinyint_col FROM vtb_org_child_0.u_tinyint_col, "
                    "u_smallint_col FROM vtb_org_normal_pk.u_smallint_col"
                    ") USING vtb_virtual_stb TAGS (10, false, 10, 10, 'vchild10', 'vchild10')")

        # 8.1.2 child table do not use from to specify the origin table
        tdSql.error("CREATE VTABLE `error_vtb_virtual_ctb11`("
                    "vtb_org_child_0.u_tinyint_col, "
                    "vtb_org_normal_pk.u_smallint_col"
                    ") USING vtb_virtual_stb TAGS (11, false, 11, 11, 'vchild11', 'vchild11')")

        # 8.2 normal table
        tdSql.error("CREATE VTABLE `error_vtb_virtual_ntb4` ("
                    "ts timestamp, "
                    "u_tinyint_col tinyint unsigned from vtb_org_child_0.u_tinyint_col, "
                    "u_smallint_col smallint unsigned from vtb_org_normal_pk.u_smallint_col)")

        # 9. data source is tag
        # 9.1 child table
        tdSql.error("CREATE VTABLE `error_vtb_virtual_ctb12`("
                    "int_col FROM vtb_org_child_0.int_tag"
                    ") USING vtb_virtual_stb TAGS (12, false, 12, 12, 'vchild12', 'vchild12')")

        # 9.2 normal table
        tdSql.error("CREATE VTABLE `error_vtb_virtual_ntb5` ("
                    "ts timestamp, "
                    "int_col int from vtb_org_child_0.int_tag)")

        # 10. create virtual child table using from to specify some columns and do not use from for other columns
        tdSql.error("CREATE VTABLE `error_vtb_virtual_ctb13`("
                    "u_tinyint_col FROM vtb_org_child_0.u_tinyint_col, "
                    "u_smallint_col, "
                    "u_int_col FROM vtb_org_child_2.u_int_col, "
                    "u_bigint_col, "
                    "tinyint_col FROM vtb_org_child_4.tinyint_col, "
                    "smallint_col, "
                    "int_col FROM vtb_org_child_6.int_col, "
                    "bigint_col, "
                    "float_col FROM vtb_org_child_8.float_col, "
                    "double_col, "
                    "bool_col FROM vtb_org_child_10.bool_col, "
                    "binary_16_col FROM vtb_org_child_11.binary_16_col,"
                    "binary_32_col FROM vtb_org_child_12.binary_32_col,"
                    "nchar_16_col FROM vtb_org_child_13.nchar_16_col,"
                    "nchar_32_col, "
                    "varbinary_16_col FROM vtb_org_child_15.varbinary_16_col,"
                    "varbinary_32_col, "
                    "geo_16_col FROM vtb_org_child_17.geo_16_col,"
                    "geo_32_col FROM vtb_org_child_18.geo_32_col)"
                    "USING vtb_virtual_stb TAGS (13, false, 13, 13, 'vchild13', 'vchild13')")

    def test_drop_vtable_batch(self):
        """Drop: batch drop virtual tables

        1. batch drop multiple virtual normal tables in one statement
        2. batch drop a mix of virtual normal and virtual child tables
        3. batch drop with IF EXISTS skipping a missing table

        Catalog:
            - VirtualTable

        Since: v3.4.2.2

        Labels: drop,virtual,batch,integration,functional

        Jira: None

        History:
            - 2026-8-14 Yihao Deng Created

        """
        tdLog.info(f"test batch drop virtual tables.")
        tdSql.execute("use test_vtable_create;")
        tdSql.execute("select database();")

        # virtual super table for the virtual child tables in this test
        tdSql.execute("CREATE STABLE `vtb_batch_stb` ("
                      "ts timestamp, "
                      "u_tinyint_col tinyint unsigned, "
                      "u_smallint_col smallint unsigned, "
                      "u_int_col int unsigned) "
                      "TAGS ("
                      "int_tag int,"
                      "bool_tag bool,"
                      "float_tag float,"
                      "double_tag double,"
                      "nchar_32_tag nchar(32),"
                      "binary_32_tag binary(32))"
                      "VIRTUAL 1")

        # create a few virtual normal tables and virtual child tables
        for i in range(3):
            tdSql.execute(f"CREATE VTABLE `vtb_batch_n{i}` ("
                          "ts timestamp, "
                          f"u_tinyint_col tinyint unsigned from vtb_org_child_{i}.u_tinyint_col, "
                          f"u_smallint_col smallint unsigned from vtb_org_child_{i}.u_smallint_col, "
                          f"u_int_col int unsigned)")
        for i in range(3):
            tdSql.execute(f"CREATE VTABLE `vtb_batch_c{i}`("
                          f"vtb_org_child_{i}.u_tinyint_col, "
                          f"vtb_org_child_{i}.u_smallint_col, "
                          f"vtb_org_child_{i}.u_int_col) "
                          "USING vtb_batch_stb "
                          f"TAGS ({i}, false, {i}, {i}, 'batchc{i}', 'batchc{i}')")

        # 1. batch drop multiple virtual normal tables in one statement
        tdSql.execute("DROP VTABLE `vtb_batch_n0`, `vtb_batch_n1`")
        for n in ("vtb_batch_n0", "vtb_batch_n1"):
            tdSql.query("SELECT count(*) FROM information_schema.ins_tables "
                        f"WHERE db_name='test_vtable_create' AND table_name='{n}'")
            tdSql.checkData(0, 0, 0)

        # 2. batch drop a mix of virtual normal and virtual child tables
        tdSql.execute("DROP VTABLE `vtb_batch_n2`, `vtb_batch_c0`, `vtb_batch_c1`")
        for n in ("vtb_batch_n2", "vtb_batch_c0", "vtb_batch_c1"):
            tdSql.query("SELECT count(*) FROM information_schema.ins_tables "
                        f"WHERE db_name='test_vtable_create' AND table_name='{n}'")
            tdSql.checkData(0, 0, 0)

        # 3. IF EXISTS guards the first table of the list (same semantics as DROP TABLE):
        #    a missing first table is skipped, the rest are dropped.
        tdSql.execute("DROP VTABLE IF EXISTS `vtb_batch_no_such`, `vtb_batch_c2`")
        tdSql.query("SELECT count(*) FROM information_schema.ins_tables "
                    "WHERE db_name='test_vtable_create' AND table_name='vtb_batch_c2'")
        tdSql.checkData(0, 0, 0)

        # 4. dropping a non-virtual table via DROP VTABLE must fail
        tdSql.error("DROP VTABLE `vtb_org_normal_0`", expectedErrno=0x2694)

        tdSql.execute("DROP STABLE `vtb_batch_stb`")

    def test_drop_vtable_batch_boundary(self):
        """Drop: batch drop virtual tables boundary behaviors

        1. batch drop across databases in one statement
        2. duplicate table names in one statement (first wins, second fails)
        3. missing table anywhere in the list fails and nothing is dropped
        4. IF EXISTS only guards the first table (same as DROP TABLE)
        5. dropping a virtual super table via DROP VTABLE is rejected
        6. mixing a virtual normal table and a virtual super table fails atomically
        7. a large batch (50 tables) drops all of them
        8. recreate a table with the same name after a batch drop

        Catalog:
            - VirtualTable

        Since: v3.4.2.2

        Labels: drop,virtual,batch,boundary,integration,functional

        Jira: None

        History:
            - 2026-8-14 Yihao Deng Created

        """
        tdLog.info(f"test batch drop virtual tables boundary behaviors.")
        tdSql.execute("use test_vtable_create;")
        tdSql.execute("select database();")

        # second database for the cross-database scenario
        # (dropped first so the test re-runs cleanly after a mid-test failure)
        tdSql.execute("drop database if exists test_vtable_drop_b;")
        tdSql.execute("create database test_vtable_drop_b;")
        tdSql.execute("use test_vtable_drop_b;")
        tdSql.execute("CREATE TABLE `vtb_b_org_n0` (ts timestamp, c1 int)")
        tdSql.execute("CREATE TABLE `vtb_b_org_n1` (ts timestamp, c1 int)")

        # virtual super table in the main database for the virtual child tables
        tdSql.execute("use test_vtable_create;")
        tdSql.execute("CREATE STABLE `vtb_bdr_stb` ("
                      "ts timestamp, "
                      "c1 int) "
                      "TAGS (t1 int)"
                      "VIRTUAL 1")

        # 1. batch drop across databases in one statement
        tdSql.execute("CREATE VTABLE `vtb_bdr_n0` (ts timestamp, c1 int)")
        tdSql.execute("use test_vtable_drop_b;")
        tdSql.execute("CREATE VTABLE `vtb_bdr_n1` (ts timestamp, c1 int from vtb_b_org_n1.c1)")
        tdSql.execute("DROP VTABLE test_vtable_create.`vtb_bdr_n0`, test_vtable_drop_b.`vtb_bdr_n1`")
        tdSql.query("SELECT count(*) FROM information_schema.ins_tables "
                    "WHERE db_name='test_vtable_create' AND table_name='vtb_bdr_n0'")
        tdSql.checkData(0, 0, 0)
        tdSql.query("SELECT count(*) FROM information_schema.ins_tables "
                    "WHERE db_name='test_vtable_drop_b' AND table_name='vtb_bdr_n1'")
        tdSql.checkData(0, 0, 0)

        # 2. duplicate table names in one statement: the first occurrence is
        #    dropped, the second fails with "table does not exist"
        tdSql.execute("use test_vtable_create;")
        tdSql.execute("CREATE VTABLE `vtb_bdr_dup` (ts timestamp, c1 int)")
        tdSql.error("DROP VTABLE `vtb_bdr_dup`, `vtb_bdr_dup`",
                    expectedErrno=0x2603)
        tdSql.query("SELECT count(*) FROM information_schema.ins_tables "
                    "WHERE db_name='test_vtable_create' AND table_name='vtb_bdr_dup'")
        tdSql.checkData(0, 0, 0)

        # 3. a missing table anywhere in the list fails the whole statement
        #    and nothing is dropped (translate-time check, atomic)
        tdSql.execute("CREATE VTABLE `vtb_bdr_p0` (ts timestamp, c1 int)")
        tdSql.execute("CREATE VTABLE `vtb_bdr_p1` (ts timestamp, c1 int)")
        tdSql.error("DROP VTABLE `vtb_bdr_p0`, `vtb_bdr_no_such`, `vtb_bdr_p1`",
                    expectedErrno=0x2603)
        for n in ("vtb_bdr_p0", "vtb_bdr_p1"):
            tdSql.query("SELECT count(*) FROM information_schema.ins_tables "
                        f"WHERE db_name='test_vtable_create' AND table_name='{n}'")
            tdSql.checkData(0, 0, 1)

        # 4. IF EXISTS only guards the first table of the list: a missing
        #    table in the middle still fails
        tdSql.error("DROP VTABLE IF EXISTS `vtb_bdr_p0`, `vtb_bdr_no_such`, `vtb_bdr_p1`",
                    expectedErrno=0x2603)
        tdSql.query("SELECT count(*) FROM information_schema.ins_tables "
                    "WHERE db_name='test_vtable_create' AND table_name='vtb_bdr_p0'")
        tdSql.checkData(0, 0, 1)

        # 5. dropping a virtual super table via DROP VTABLE is rejected,
        #    with or without children, in single and batch form
        tdSql.execute("CREATE STABLE `vtb_bdr_vstb_empty` (ts timestamp, c1 int) TAGS (t1 int) VIRTUAL 1")
        tdSql.error("DROP VTABLE `vtb_bdr_vstb_empty`", expectedErrno=0x0118)
        tdSql.execute("CREATE VTABLE `vtb_bdr_vc0` (vtb_org_child_0.int_col) USING `vtb_bdr_stb` TAGS (0)")
        tdSql.error("DROP VTABLE `vtb_bdr_stb`", expectedErrno=0x0118)
        tdSql.query("SELECT count(*) FROM information_schema.ins_stables "
                    "WHERE db_name='test_vtable_create' AND stable_name='vtb_bdr_stb'")
        tdSql.checkData(0, 0, 1)

        # 6. mixing a virtual normal table and a virtual super table:
        #    rejected atomically at translate time — nothing is dropped
        tdSql.execute("CREATE VTABLE `vtb_bdr_mix` (ts timestamp, c1 int)")
        tdSql.error("DROP VTABLE `vtb_bdr_mix`, `vtb_bdr_stb`", expectedErrno=0x0118)
        tdSql.query("SELECT count(*) FROM information_schema.ins_tables "
                    "WHERE db_name='test_vtable_create' AND table_name='vtb_bdr_mix'")
        tdSql.checkData(0, 0, 1)

        # 7. a large batch drops all of its tables
        for i in range(50):
            tdSql.execute(f"CREATE VTABLE `vtb_bdr_big{i}` (ts timestamp, c1 int)")
        tdSql.execute("DROP VTABLE " + ", ".join(f"`vtb_bdr_big{i}`" for i in range(50)))
        tdSql.query("SELECT count(*) FROM information_schema.ins_tables "
                    "WHERE db_name='test_vtable_create' AND table_name LIKE 'vtb_bdr_big%'")
        tdSql.checkData(0, 0, 0)

        # 8. recreate a table with the same name after a batch drop, then drop it again
        tdSql.execute("DROP VTABLE `vtb_bdr_p0`, `vtb_bdr_p1`")
        tdSql.execute("CREATE VTABLE `vtb_bdr_p0` (ts timestamp, c1 int from vtb_org_normal_0.int_col)")
        tdSql.execute("DROP VTABLE `vtb_bdr_p0`")
        tdSql.query("SELECT count(*) FROM information_schema.ins_tables "
                    "WHERE db_name='test_vtable_create' AND table_name='vtb_bdr_p0'")
        tdSql.checkData(0, 0, 0)

        # cleanup
        tdSql.execute("DROP VTABLE `vtb_bdr_mix`")
        tdSql.execute("DROP VTABLE `vtb_bdr_vc0`")
        tdSql.execute("DROP STABLE `vtb_bdr_stb`")
        tdSql.execute("DROP STABLE `vtb_bdr_vstb_empty`")
        tdSql.execute("drop database if exists test_vtable_drop_b;")

    def test_drop_vtable_batch_syntax(self):
        """Drop: batch drop virtual tables syntax variants

        1. single-table DROP VTABLE still works (backwards compatibility)
        2. keywords are case-insensitive
        3. unquoted / backtick-quoted / database-qualified names in one batch
        4. trailing comma is a syntax error
        5. DROP VTABLE with no table list is a syntax error
        6. DROP TABLE also removes virtual tables (old syntax interoperates)
        7. dropping a non-virtual child table via DROP VTABLE is rejected

        Catalog:
            - VirtualTable

        Since: v3.4.2.2

        Labels: drop,virtual,batch,syntax,integration,functional

        Jira: None

        History:
            - 2026-8-14 Yihao Deng Created

        """
        tdLog.info(f"test batch drop virtual tables syntax variants.")
        tdSql.execute("use test_vtable_create;")
        tdSql.execute("select database();")

        tdSql.execute("CREATE STABLE `vtb_bsy_stb` (ts timestamp, c1 int) TAGS (t1 int) VIRTUAL 1")

        # 1. single-table DROP VTABLE (backwards compatibility)
        tdSql.execute("CREATE VTABLE `vtb_bsy_single` (ts timestamp, c1 int)")
        tdSql.execute("DROP VTABLE `vtb_bsy_single`")
        tdSql.query("SELECT count(*) FROM information_schema.ins_tables "
                    "WHERE db_name='test_vtable_create' AND table_name='vtb_bsy_single'")
        tdSql.checkData(0, 0, 0)

        # 2. the VTABLE keyword is case-insensitive (single keyword only;
        #    the two-word "VIRTUAL TABLE" spelling is not valid syntax)
        tdSql.execute("CREATE VTABLE `vtb_bsy_lower` (ts timestamp, c1 int)")
        tdSql.execute("drop vtable `vtb_bsy_lower`")
        tdSql.execute("CREATE VTABLE `vtb_bsy_lower` (ts timestamp, c1 int)")
        tdSql.execute("Drop VTABLE If Exists `vtb_bsy_lower`")

        # 3. unquoted / backtick-quoted / database-qualified names in one batch
        tdSql.execute("CREATE VTABLE vtb_bsy_a (ts timestamp, c1 int)")
        tdSql.execute("CREATE VTABLE `vtb_bsy_b` (ts timestamp, c1 int)")
        tdSql.execute("CREATE VTABLE `vtb_bsy_c` (ts timestamp, c1 int)")
        tdSql.execute("DROP VTABLE vtb_bsy_a, `vtb_bsy_b`, test_vtable_create.vtb_bsy_c")
        for n in ("vtb_bsy_a", "vtb_bsy_b", "vtb_bsy_c"):
            tdSql.query("SELECT count(*) FROM information_schema.ins_tables "
                        f"WHERE db_name='test_vtable_create' AND table_name='{n}'")
            tdSql.checkData(0, 0, 0)

        # 4. trailing comma is a syntax error
        tdSql.execute("CREATE VTABLE `vtb_bsy_t` (ts timestamp, c1 int)")
        tdSql.error("DROP VTABLE `vtb_bsy_t`,", expectedErrno=0x2601)
        tdSql.query("SELECT count(*) FROM information_schema.ins_tables "
                    "WHERE db_name='test_vtable_create' AND table_name='vtb_bsy_t'")
        tdSql.checkData(0, 0, 1)

        # 5. DROP VTABLE with no table list is a syntax error
        tdSql.error("DROP VTABLE", expectedErrno=0x2601)

        # 6. DROP TABLE also removes virtual tables (old syntax interoperates)
        tdSql.execute("CREATE VTABLE `vtb_bsy_via_tb` (ts timestamp, c1 int)")
        tdSql.execute("CREATE VTABLE `vtb_bsy_via_tb2` (ts timestamp, c1 int)")
        tdSql.execute("DROP TABLE `vtb_bsy_via_tb`, `vtb_bsy_via_tb2`")
        for n in ("vtb_bsy_via_tb", "vtb_bsy_via_tb2"):
            tdSql.query("SELECT count(*) FROM information_schema.ins_tables "
                        f"WHERE db_name='test_vtable_create' AND table_name='{n}'")
            tdSql.checkData(0, 0, 0)

        # 7. dropping a non-virtual child table via DROP VTABLE is rejected
        tdSql.error("DROP VTABLE `vtb_org_child_0`",
                    expectedErrno=0x2694)

        # cleanup (`vtb_bsy_lower` was already dropped in step 2)
        tdSql.execute("DROP STABLE `vtb_bsy_stb`")
        tdSql.execute("DROP VTABLE `vtb_bsy_t`")

    def test_drop_vtable_batch_with(self):
        """Drop: batch drop virtual tables via the WITH (uid replay) form

        1. batch DROP VTABLE WITH uid1, uid2 resolves both uids and drops both tables
        2. single WITH uid keeps working
        3. mixing a uid with a non-digit name fails and nothing is dropped
        4. IF EXISTS guards only the first entry of a WITH list

        Catalog:
            - VirtualTable

        Since: v3.4.2.2

        Labels: drop,virtual,batch,with,uid,integration,functional

        Jira: None

        History:
            - 2026-8-14 Yihao Deng Created

        """
        tdLog.info(f"test batch drop virtual tables via WITH uid.")
        tdSql.execute("use test_vtable_create;")
        tdSql.execute("select database();")

        names = [f"vtb_bwith_{i}" for i in range(3)]
        for n in names:
            tdSql.execute(f"CREATE VTABLE `{n}` (ts timestamp, c1 int)")

        tdSql.query("SELECT table_name, uid FROM information_schema.ins_tables "
                    "WHERE db_name='test_vtable_create' AND table_name IN "
                    f"('{names[0]}', '{names[1]}', '{names[2]}') ORDER BY table_name")
        assert tdSql.queryRows == 3, "expected 3 vtables in ins_tables"
        uids = {tdSql.getData(i, 0): str(tdSql.getData(i, 1)) for i in range(tdSql.queryRows)}
        for n in names:
            assert uids[n].isdigit(), f"uid of {n} is not numeric: {uids[n]}"

        # 1. batch WITH drop resolves both uids to names and drops both tables
        tdSql.execute(f"DROP VTABLE WITH `{uids[names[0]]}`, `{uids[names[1]]}`")
        for n in names[:2]:
            tdSql.query("SELECT count(*) FROM information_schema.ins_tables "
                        f"WHERE db_name='test_vtable_create' AND table_name='{n}'")
            tdSql.checkData(0, 0, 0)

        # 2. single WITH uid keeps working
        tdSql.execute(f"DROP VTABLE WITH `{uids[names[2]]}`")
        tdSql.query("SELECT count(*) FROM information_schema.ins_tables "
                    f"WHERE db_name='test_vtable_create' AND table_name='{names[2]}'")
        tdSql.checkData(0, 0, 0)

        # 3. a non-digit entry in a WITH list fails the whole statement; the
        #    uid that does exist must NOT be dropped (translate-time, atomic)
        tdSql.execute("CREATE VTABLE `vtb_bwith_keep` (ts timestamp, c1 int)")
        tdSql.query("SELECT uid FROM information_schema.ins_tables "
                    "WHERE db_name='test_vtable_create' AND table_name='vtb_bwith_keep'")
        keep_uid = str(tdSql.getData(0, 0))
        tdSql.error(f"DROP VTABLE WITH `{keep_uid}`, `not_a_uid`", expectedErrno=0x2603)
        tdSql.query("SELECT count(*) FROM information_schema.ins_tables "
                    "WHERE db_name='test_vtable_create' AND table_name='vtb_bwith_keep'")
        tdSql.checkData(0, 0, 1)

        # 4. IF EXISTS guards only the first entry of a WITH list: a later
        #    non-digit entry still fails (IF EXISTS does not extend to it)
        tdSql.error(f"DROP VTABLE IF EXISTS `{keep_uid}`, `also_not_a_uid`", expectedErrno=0x2603)

        # cleanup
        tdSql.execute("DROP VTABLE `vtb_bwith_keep`")

    def test_drop_vtable_batch_txn(self):
        """Drop: batch drop virtual tables inside a transaction

        1. batch DROP VTABLE inside BEGIN/ROLLBACK is undone by ROLLBACK
        2. batch DROP VTABLE inside BEGIN/COMMIT takes effect after COMMIT

        Note: transactions are an enterprise-only feature (translateTransStmt
        is compiled under TD_ENTERPRISE); on a community build BEGIN fails
        with 0x0100 and this test skips itself — same as the 21-MetaData
        test_meta_batch_txn_* suite, which also only passes on enterprise CI.

        Virtual tables are read-only projections ("Virtual table can not be
        written", 0x80000200), so the row each vtable is expected to expose is
        inserted into a backing normal table and mapped in via FROM; the first
        column (primary ts) must NOT carry a FROM ref (0x6202).

        Catalog:
            - VirtualTable

        Since: v3.4.2.2

        Labels: drop,virtual,batch,txn,integration,functional

        Jira: None

        History:
            - 2026-8-14 Yihao Deng Created
            - 2026-8-17 Yihao Deng Fixed setup inserting into read-only
              vtables (0x80000200); data now mapped from backing tables

        """
        tdLog.info(f"test batch drop virtual tables inside a transaction.")

        # probe transaction support: community builds reject BEGIN (0x0100)
        # (affectedRows of BEGIN/ROLLBACK may legitimately be None, so probe
        # via the raw cursor instead of execute_ignore_error's return value)
        tdSql.execute_ignore_error("ROLLBACK")  # no-op if no active txn
        try:
            tdSql.cursor.execute("BEGIN")
        except Exception:
            pytest.skip("transactions are enterprise-only (BEGIN -> 0x0100)")
        tdSql.execute_ignore_error("ROLLBACK")

        # own database so the test never depends on (or pollutes) other state
        tdSql.execute("drop database if exists test_vdrop_txn;")
        tdSql.execute("create database test_vdrop_txn;")
        tdSql.execute("use test_vdrop_txn;")
        for i in range(3):
            # vtables are read-only: seed the row in a backing normal table,
            # then map it into the vtable (first column ts has no FROM ref)
            tdSql.execute(f"CREATE TABLE `vtxn_org{i}` (ts timestamp, c1 int)")
            tdSql.execute(f"insert into `vtxn_org{i}` values (now, {i})")
            tdSql.execute(f"CREATE VTABLE `vtxn_t{i}`"
                          f" (ts timestamp, c1 int from `vtxn_org{i}`.c1)")

        # 1. ROLLBACK undoes a batch drop: tables and data come back
        tdSql.execute("BEGIN")
        tdSql.execute("DROP VTABLE `vtxn_t0`, `vtxn_t1`, `vtxn_t2`")
        tdSql.execute("ROLLBACK")
        for i in range(3):
            tdSql.query(f"SELECT count(*), max(c1) FROM `vtxn_t{i}`")
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(0, 1, i)

        # 2. COMMIT makes a batch drop permanent (backing tables survive;
        #    the dropped vtables are gone from ins_tables)
        tdSql.execute("BEGIN")
        tdSql.execute("DROP VTABLE `vtxn_t0`, `vtxn_t1`, `vtxn_t2`")
        tdSql.execute("COMMIT")
        tdSql.query("SELECT count(*) FROM information_schema.ins_tables "
                    "WHERE db_name='test_vdrop_txn' AND table_name LIKE 'vtxn_t%'")
        tdSql.checkData(0, 0, 0)

        # cleanup
        tdSql.execute("use test_vtable_create;")
        tdSql.execute("drop database if exists test_vdrop_txn;")

