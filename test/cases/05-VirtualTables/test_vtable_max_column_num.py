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

        sql = "insert into "
        for i in range(32767):
            tdSql.execute(f"create table d_{i}(ts timestamp, double_col double)")
            sql += f" d_{i} values(1696000000000, {i})"
            sql += f" (1696000001000, {i+1})"
        # execute
        tdSql.execute(sql)

    def test_virtual_super_child_table_max_column(self):
        """Virtual super/child table max column

        test create and query virtual super/child tables with max column num


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
            sql += f", col_{i} double"
        sql += ") TAGS (id int) VIRTUAL 1;"
        tdSql.execute(sql)

        tdLog.info(f"test create virtual child tables.")
        sql = "CREATE VTABLE vtb_virtual_child_max_col_1 ("
        for i in range(32763):
            sql += f"col_{i} from d_{i}.double_col"
            if i != 32762:
                sql += ", "
        sql += ") USING vtb_virtual_stb_max_col TAGS (1);"
        tdSql.execute(sql)


        tdLog.info(f"test create virtual child tables.")
        sql = "CREATE VTABLE vtb_virtual_child_max_col_2 ("
        for i in range(32763):
            sql += f"col_{i} from d_{i}.double_col"
            if i != 32762:
                sql += ", "
        sql += ") USING vtb_virtual_stb_max_col TAGS (1);"
        tdSql.execute(sql, queryTimes = 1)

        tdLog.info(f"test query virtual child tables.")
        tdSql.query("SELECT ts, col_1, col_4097, col_32762 FROM vtb_virtual_child_max_col_1;")
        tdSql.checkRows(2)
        tdSql.checkCols(4)
        tdSql.checkData(0, 0, 1696000000000)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 4097)
        tdSql.checkData(0, 3, 32762)
        tdSql.checkData(1, 0, 1696000001000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 4098)
        tdSql.checkData(1, 3, 32763)

        tdLog.info(f"test query virtual super tables.")
        tdSql.query("SELECT ts, col_1, col_4097, col_32762 FROM vtb_virtual_stb_max_col order by ts;")
        tdSql.checkRows(4)
        tdSql.checkCols(4)
        tdSql.checkData(0, 0, 1696000000000)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 4097)
        tdSql.checkData(0, 3, 32762)
        tdSql.checkData(1, 0, 1696000000000)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, 4097)
        tdSql.checkData(1, 3, 32762)
        tdSql.checkData(2, 0, 1696000001000)
        tdSql.checkData(2, 1, 2)
        tdSql.checkData(2, 2, 4098)
        tdSql.checkData(2, 3, 32763)
        tdSql.checkData(3, 0, 1696000001000)
        tdSql.checkData(3, 1, 2)
        tdSql.checkData(3, 2, 4098)
        tdSql.checkData(3, 3, 32763)

    def test_virtual_normal_table_max_column(self):
        """Virtual normal table max column

        test create and query virtual normal tables with max column num

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
            sql += f", col_{i} double from d_{i}.double_col"
        sql += ")"
        tdSql.execute(sql)

        tdLog.info(f"test query virtual child tables.")
        tdSql.query("SELECT ts, col_1, col_4097, col_32762 FROM vtb_virtual_ntb_max_col;")
        tdSql.checkRows(2)
        tdSql.checkCols(4)  # ts + 32766 double cols
        tdSql.checkData(0, 0, 1696000000000)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 4097)
        tdSql.checkData(0, 3, 32762)
        tdSql.checkData(1, 0, 1696000001000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 4098)
        tdSql.checkData(1, 3, 32763)

    def test_virtual_table_exceed_max_column(self):
        """Virtual table exceed max column

        test create virtual tables exceed max column num

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
            sql += f", col_{i} double"
        sql += ") TAGS (id int) VIRTUAL 1;"
        tdSql.error(sql)

        sql = "CREATE VTABLE vtb_virtual_ntb_exceed_max_col (ts timestamp"
        for i in range(32767):
            sql += f", col_{i} double from d_{i}.double_col"
        sql += ")"
        tdSql.error(sql)

        tdLog.info(f"test alter virtual tables exceed max column num.")

        sql = "CREATE STABLE `vtb_virtual_stb_alter_exceed_max_col` (ts timestamp"
        for i in range(32763):
            sql += f", col_{i} double"
        sql += ") TAGS (id int) VIRTUAL 1;"
        tdSql.execute(sql)

        sql = "CREATE VTABLE vtb_virtual_ntb_alter_exceed_max_col (ts timestamp"
        for i in range(32766):
            sql += f", col_{i} double from d_{i}.double_col"
        sql += ")"
        tdSql.execute(sql)

        tdSql.error("ALTER TABLE vtb_virtual_stb_alter_exceed_max_col ADD COLUMN col_32763 double;")
        tdSql.error("ALTER TABLE vtb_virtual_stb_alter_exceed_max_col ADD TAG id2 int;")
        tdSql.error("ALTER TABLE vtb_virtual_ntb_alter_exceed_max_col ADD COLUMN col_32766 double;")

        tdLog.info(f"test alter after drop virtual tables, colId exceed INT16_MAX.")
        tdSql.execute("ALTER TABLE vtb_virtual_stb_alter_exceed_max_col DROP COLUMN col_32762;")
        tdSql.error("ALTER TABLE vtb_virtual_stb_alter_exceed_max_col ADD COLUMN col_32763 double;")
        tdSql.error("ALTER TABLE vtb_virtual_stb_alter_exceed_max_col ADD TAG id2 int;")
        tdSql.execute("ALTER TABLE vtb_virtual_ntb_alter_exceed_max_col DROP COLUMN col_32765;")
        tdSql.error("ALTER TABLE vtb_virtual_ntb_alter_exceed_max_col ADD COLUMN col_32766 double;")

    def test_virtual_table_wide_org_tables_32767_cols(self):
        """Virtual normal table wide origin table
        
        test create and query virtual normal tables when org tables have 4096 columns
        (1 ts + 4095 double) and total mapped columns reach 32767


        Since: v3.3.8.0

        Labels: virtual, create

        Jira: None

        History:
            - 2025-12-8 Jing Sima Created

        """
        tdLog.info("test create virtual table from wide org tables with 32767 columns.")

        tdSql.execute("use test_vtable_max_column;")
        tdSql.execute("select database();")

        total_double_cols = 32766
        double_per_table = 4095

        full_table_cnt = total_double_cols // double_per_table   # 8 个 4096 列的表
        remain_cols = total_double_cols % double_per_table       # 剩余 6 列，由最后一个表承载

        tdLog.info(f"full_table_cnt={full_table_cnt}, remain_cols={remain_cols}")

        insert_sql = "insert into "
        for t in range(full_table_cnt):
            base_idx = t * double_per_table

            sql = f"create table org_{t}(ts timestamp"
            for j in range(double_per_table):
                col_idx = base_idx + j
                sql += f", double_col_{col_idx} double"
            sql += ");"
            tdSql.execute(sql)

            insert_sql += f" org_{t} values(1696000000000"
            for j in range(double_per_table):
                col_idx = base_idx + j
                insert_sql += f", {col_idx}"
            insert_sql += ") "
        tdSql.execute(insert_sql)

        if remain_cols > 0:
            t = full_table_cnt
            base_idx = t * double_per_table

            sql = f"create table org_{t}(ts timestamp"
            for j in range(remain_cols):
                col_idx = base_idx + j
                sql += f", double_col_{col_idx} double"
            sql += ");"
            tdSql.execute(sql)

            sql = f"insert into org_{t} values(1696000000000"
            for j in range(remain_cols):
                col_idx = base_idx + j
                sql += f", {col_idx}"
            sql += ");"
            tdSql.execute(sql)

        tdLog.info("create virtual normal table with 32767 mapped columns from wide org tables.")
        sql = "CREATE VTABLE vtb_virtual_ntb_wide_org_32767_cols (ts timestamp"
        for i in range(total_double_cols):
            table_idx = i // double_per_table
            sql += f", double_col_{i} double from org_{table_idx}.double_col_{i}"
        sql += ");"
        tdSql.execute(sql)

        tdLog.info("query virtual table and check every column.")
        tdSql.query("SELECT * FROM vtb_virtual_ntb_wide_org_32767_cols;")

        tdSql.checkRows(1)
        tdSql.checkCols(total_double_cols + 1)

        # ts
        tdSql.checkData(0, 0, 1696000000000)

        for i in range(total_double_cols):
            tdSql.checkData(0, 1 + i, i)

    def test_virtual_table_error_case(self):
        """Virtual table max column except

        1. create virtual super table with max columns (exceed max)
        2. create virtual super table with max bytes per row
        3. create virtual normal table with max columns (exceed max)
        4. create virtual normal table with max bytes per row

        Since: v3.3.8.0

        Labels: virtual, create, negative

        Jira: None

        History:
            - 2025-12-8 Jing Sima Created

        """
        tdLog.info("test create virtual table error cases.")

        tdSql.execute("use test_vtable_max_column;")
        tdSql.execute("select database();")

        # Case 1: create virtual super table with max columns (exceed max)
        sql = "CREATE STABLE `vtb_virtual_stb_max_col_error` (ts timestamp"
        for i in range(32764):
            sql += f", col_{i} double"
        sql += ") TAGS (id int) VIRTUAL 1;"
        tdSql.error(sql)

        # Case 2: create virtual super table with max bytes per row
        sql = "CREATE STABLE `vtb_virtual_stb_max_col_no_error` (ts timestamp"
        for i in range(32750):
            sql += f", col_{i} double"
        for i in range(4):
            sql += f", var_col_{i} varchar(60000)"
        sql += ", var_col_last varchar(22265)) TAGS (id int) VIRTUAL 1;"
        tdSql.execute(sql)

        sql = "CREATE STABLE `vtb_virtual_stb_max_col_error` (ts timestamp"
        for i in range(32750):
            sql += f", col_{i} double"
        for i in range(4):
            sql += f", var_col_{i} varchar(60000)"
        sql += ", var_col_last varchar(22266)) TAGS (id int) VIRTUAL 1;"
        tdSql.error(sql)

        tdSql.error("ALTER STABLE vtb_virtual_stb_max_col_no_error ADD COLUMN var_len_exceed varchar(1);")
        tdSql.error("ALTER STABLE vtb_virtual_stb_max_col_no_error MODIFY COLUMN var_col_last varchar(22266);")
        tdSql.execute("ALTER STABLE vtb_virtual_stb_max_col_no_error DROP COLUMN var_col_last;")
        tdSql.execute("ALTER STABLE vtb_virtual_stb_max_col_no_error ADD COLUMN var_col_last varchar(1);")
        tdSql.execute("ALTER STABLE vtb_virtual_stb_max_col_no_error MODIFY COLUMN var_col_last varchar(22265);")
        tdSql.error("ALTER STABLE vtb_virtual_stb_max_col_no_error MODIFY COLUMN var_col_last varchar(22266);")

        # Case 3: create virtual normal table with max columns (exceed max)
        sql = "CREATE VTABLE `vtb_virtual_ntb_max_col_error` (ts timestamp"
        for i in range(32767):
            sql += f", col_{i} double"
        sql += ");"
        tdSql.error(sql)

        # Case 4: create virtual super table with max bytes per row
        sql = "CREATE VTABLE `vtb_virtual_ntb_max_col_no_error` (ts timestamp"
        for i in range(32750):
            sql += f", col_{i} double"
        for i in range(4):
            sql += f", var_col_{i} varchar(60000)"
        sql += ", var_col_last varchar(22265));"
        tdSql.execute(sql)

        sql = "CREATE VTABLE `vtb_virtual_ntb_max_col_error` (ts timestamp"
        for i in range(32750):
            sql += f", col_{i} double"
        for i in range(4):
            sql += f", var_col_{i} varchar(60000)"
        sql += ", var_col_last varchar(22266));"
        tdSql.error(sql)

        tdSql.error("ALTER VTABLE vtb_virtual_ntb_max_col_no_error ADD COLUMN var_len_exceed varchar(1);")
        tdSql.error("ALTER VTABLE vtb_virtual_ntb_max_col_no_error MODIFY COLUMN var_col_last varchar(22266);")
        tdSql.execute("ALTER VTABLE vtb_virtual_ntb_max_col_no_error DROP COLUMN var_col_last;")
        tdSql.execute("ALTER VTABLE vtb_virtual_ntb_max_col_no_error ADD COLUMN var_col_last varchar(1);")
        tdSql.execute("ALTER VTABLE vtb_virtual_ntb_max_col_no_error MODIFY COLUMN var_col_last varchar(22265);")
        tdSql.error("ALTER VTABLE vtb_virtual_ntb_max_col_no_error MODIFY COLUMN var_col_last varchar(22266);")
