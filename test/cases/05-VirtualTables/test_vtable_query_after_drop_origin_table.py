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

class TestVtableQueryAfterDropOriginTable:

    def setup_class(cls):
        tdLog.info(f"prepare org tables.")

        tdSql.execute("create database test_vtable_query_after_drop_origin_table;")
        tdSql.execute("use test_vtable_query_after_drop_origin_table;")

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

        tdLog.info(f"test create virtual super tables.")

        tdSql.execute("use test_vtable_query_after_drop_origin_table;")
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
        tdSql.query("show test_vtable_query_after_drop_origin_table.vtables;")
        tdSql.checkRows(vctable_num + vntable_num)
        tdSql.query("show child test_vtable_query_after_drop_origin_table.vtables;")
        tdSql.checkRows(vctable_num)
        tdSql.query("show normal test_vtable_query_after_drop_origin_table.vtables;")
        tdSql.checkRows(vntable_num)

        """test create virtual child tables.

        test virtual table select after alter normal table.

        Since: v3.3.6.0

        Labels: virtual

        Jira: None

        History:
            - 2025-3-15 Jing Sima Created
            - 2025-5-6 Huo Hong Migrated to new test framework

        """
        tdLog.info(f"test create virtual child tables.")

        tdSql.execute("use test_vtable_query_after_drop_origin_table;")
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

    def test_query_after_drop(self):
        """test query after drop tables.

        test query after drop tables.

        Since: v3.3.6.0

        Labels: virtual, drop

        Jira: None

        History:
            - 2025-3-15 Jing Sima Created
            - 2025-5-6 Huo Hong Migrated to new test framework

        """
        tdLog.info(f"test create virtual normal tables.")

        tdSql.execute("use test_vtable_query_after_drop_origin_table;")
        tdSql.execute("select database();")

        tdSql.execute(f"CREATE VTABLE `vtb_virtual_ntb` ("
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

        oldTdSql = tdCom.newTdSql()
        newTdSql = tdCom.newTdSql()

        oldTdSql.execute("use test_vtable_query_after_drop_origin_table;")
        oldTdSql.execute("select database();")

        oldTdSql.query("select * from vtb_virtual_ntb;")
        oldTdSql.checkCols(20)
        oldTdSql.checkRows(0)

        newTdSql.execute("use test_vtable_query_after_drop_origin_table;")
        newTdSql.execute("select database();")
        newTdSql.execute("drop stable vtb_org_stb;")

        newTdSql.execute(f"CREATE STABLE `vtb_org_stb` ("
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

        for i in range(30):
            newTdSql.execute(f"CREATE TABLE `vtb_org_child_{i}` USING `vtb_org_stb` TAGS ({i}, false, {i}, {i}, 'child{i}', 'child{i}');")

        oldTdSql.query("select * from vtb_virtual_ntb;")
        oldTdSql.checkCols(20)
        oldTdSql.checkRows(0)


