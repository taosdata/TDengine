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


class TestVtableCreate:
    def setup_class(cls):
        tdLog.info(f"prepare org tables.")

        tdSql.execute("create database test_vtable_max_column;")
        tdSql.execute("use test_vtable_max_column;")
        tdSql.execute("alter local 'maxSQLLength' '4194304'")

        for i in range(32767):
            tdSql.execute(f"create table d_{i}(ts timestamp, bool_col bool)")
            if i % 2:
                tdSql.execute(f"insert into d_{i} values(1696000000000, true)")
                tdSql.execute(f"insert into d_{i} values(1696000001000, false)")
            else:
                tdSql.execute(f"insert into d_{i} values(1696000000000, false)")
                tdSql.execute(f"insert into d_{i} values(1696000001000, true)")


    def test_virtual_super_child_table_max_column(self):
        """Create: virtual super/child table with max column num

        test create and query virtual super/child tables with max column num

        Catalog:
            - VirtualTable

        Since: v3.3.8.0

        Labels: virtual, create

        Jira: None

        History:
            - 2025-12-3 Jing Sima Created

        """
        tdLog.info(f"test create virtual super tables.")

        tdSql.execute("use test_vtable_max_column;")
        tdSql.execute("select database();")

        sql = "CREATE STABLE `vtb_virtual_stb_max_col` (ts timestamp"
        for i in range(32763):
            sql += f", col_{i} bool"
        sql += ") TAGS (id int) VIRTUAL 1;"
        tdSql.execute(sql)

        tdLog.info(f"test create virtual child tables.")
        sql = "CREATE VTABLE vtb_virtual_child_max_col_1 ("
        for i in range(32763):
            sql += f"col_{i} from d_{i}.bool_col"
            if i != 32762:
                sql += ", "
        sql += ") USING vtb_virtual_stb_max_col TAGS (1);"
        tdSql.execute(sql)


        tdLog.info(f"test create virtual child tables.")
        sql = "CREATE VTABLE vtb_virtual_child_max_col_2 ("
        for i in range(32763):
            sql += f"col_{i} from d_{i}.bool_col"
            if i != 32762:
                sql += ", "
        sql += ") USING vtb_virtual_stb_max_col TAGS (1);"
        tdSql.execute(sql, queryTimes = 1)

        tdLog.info(f"test query virtual child tables.")
        tdSql.query("SELECT ts, col_1, col_4097, col_32762 FROM vtb_virtual_child_max_col_1;")
        tdSql.checkRows(2)
        tdSql.checkCols(4)
        tdSql.checkData(0, 0, 1696000000000)
        tdSql.checkData(0, 1, True)
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, False)
        tdSql.checkData(1, 0, 1696000001000)
        tdSql.checkData(1, 1, False)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, True)

        tdLog.info(f"test query virtual super tables.")
        tdSql.query("SELECT ts, col_1, col_4097, col_32762 FROM vtb_virtual_stb_max_col order by ts;")
        tdSql.checkRows(4)
        tdSql.checkCols(4)
        tdSql.checkData(0, 0, 1696000000000)
        tdSql.checkData(0, 1, True)
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, False)
        tdSql.checkData(1, 0, 1696000000000)
        tdSql.checkData(1, 1, True)
        tdSql.checkData(1, 2, True)
        tdSql.checkData(1, 3, False)
        tdSql.checkData(2, 0, 1696000001000)
        tdSql.checkData(2, 1, False)
        tdSql.checkData(2, 2, False)
        tdSql.checkData(2, 3, True)
        tdSql.checkData(3, 0, 1696000001000)
        tdSql.checkData(3, 1, False)
        tdSql.checkData(3, 2, False)
        tdSql.checkData(3, 3, True)

    def test_virtual_normal_table_max_column(self):
        """Create: virtual normal table with max column num

        test create and query virtual normal tables with max column num

        Catalog:
            - VirtualTable

        Since: v3.3.8.0

        Labels: virtual, create

        Jira: None

        History:
            - 2025-12-3 Jing Sima Created

        """
        tdLog.info(f"test create virtual normal tables.")

        tdSql.execute("use test_vtable_max_column;")
        tdSql.execute("select database();")

        sql = "CREATE VTABLE `vtb_virtual_ntb_max_col` (ts timestamp"
        for i in range(32766):
            sql += f", col_{i} bool from d_{i}.bool_col"
        sql += ")"
        tdSql.execute(sql)

        tdLog.info(f"test query virtual child tables.")
        tdSql.query("SELECT ts, col_1, col_4097, col_32762 FROM vtb_virtual_ntb_max_col;")
        tdSql.checkRows(2)
        tdSql.checkCols(4)  # ts + 32766 bool cols
        tdSql.checkData(0, 0, 1696000000000)
        tdSql.checkData(0, 1, True)
        tdSql.checkData(0, 2, True)
        tdSql.checkData(0, 3, False)
        tdSql.checkData(1, 0, 1696000001000)
        tdSql.checkData(1, 1, False)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(1, 3, True)

    def test_virtual_table_exceed_max_column(self):
        """Create: virtual table exceed max column num

        test create virtual tables exceed max column num

        Catalog:
            - VirtualTable

        Since: v3.3.8.0

        Labels: virtual, create, negative

        Jira: None

        History:
            - 2025-12-3 Jing Sima Created

        """
        tdLog.info(f"test create virtual tables exceed max column num.")

        tdSql.execute("use test_vtable_max_column;")
        tdSql.execute("select database();")

        sql = "CREATE STABLE `vtb_virtual_stb_exceed_max_col` (ts timestamp"
        for i in range(32764):
            sql += f", col_{i} bool"
        sql += ") TAGS (id int) VIRTUAL 1;"
        tdSql.error(sql)

        sql = "CREATE VTABLE vtb_virtual_ntb_exceed_max_col (ts timestamp"
        for i in range(32767):
            sql += f", col_{i} bool from d_{i}.bool_col"
        sql += ")"
        tdSql.error(sql)

        tdLog.info(f"test alter virtual tables exceed max column num.")

        sql = "CREATE STABLE `vtb_virtual_stb_alter_exceed_max_col` (ts timestamp"
        for i in range(32763):
            sql += f", col_{i} bool"
        sql += ") TAGS (id int) VIRTUAL 1;"
        tdSql.execute(sql)

        sql = "CREATE VTABLE vtb_virtual_ntb_alter_exceed_max_col (ts timestamp"
        for i in range(32766):
            sql += f", col_{i} bool from d_{i}.bool_col"
        sql += ")"
        tdSql.execute(sql)

        tdSql.error("ALTER TABLE vtb_virtual_stb_alter_exceed_max_col ADD COLUMN col_32763 bool;")
        tdSql.error("ALTER TABLE vtb_virtual_stb_alter_exceed_max_col ADD TAG id2 int;")
        tdSql.error("ALTER TABLE vtb_virtual_ntb_alter_exceed_max_col ADD COLUMN col_32766 bool;")

        tdLog.info(f"test alter after drop virtual tables, colId exceed INT16_MAX.")
        tdSql.execute("ALTER TABLE vtb_virtual_stb_alter_exceed_max_col DROP COLUMN col_32762;")
        tdSql.error("ALTER TABLE vtb_virtual_stb_alter_exceed_max_col ADD COLUMN col_32763 bool;")
        tdSql.error("ALTER TABLE vtb_virtual_stb_alter_exceed_max_col ADD TAG id2 int;")
        tdSql.execute("ALTER TABLE vtb_virtual_ntb_alter_exceed_max_col DROP COLUMN col_32765;")
        tdSql.error("ALTER TABLE vtb_virtual_ntb_alter_exceed_max_col ADD COLUMN col_32766 bool;")