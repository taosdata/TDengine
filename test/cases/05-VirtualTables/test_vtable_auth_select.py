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
from new_test_framework.utils import tdLog, tdSql, etool, tdCom, TDSql, sleep
import taos


class TestVtableAuthSelect:

    def setup_class(cls):
        tdLog.info(f"prepare databases.")
        tdSql.execute("drop database if exists test_vtable_auth_select;")
        tdSql.execute("create database test_vtable_auth_select;")
        tdSql.execute("drop database if exists test_vctable_auth_select;")
        tdSql.execute("create database test_vctable_auth_select;")

    def test_select_virtual_normal_table(self):
        """Auth: select virtual normal table

        test "write", "read", "none", "all" each auth user select opration

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, select, auth

        Jira: None

        History:
            - 2025-3-15 Jing Sima Created
            - 2025-5-6 Huo Hong Migrated to new test framework

        """
        tdSql.execute("use test_vtable_auth_select;")
        tdSql.execute("create table test_vtable_auth_org_table_1(ts timestamp, int_col int);")
        tdSql.execute("create table test_vtable_auth_org_table_2(ts timestamp, int_col int);")
        tdSql.execute("create user test_vtable_user_select PASS 'test12@#*';")
        tdSql.execute("create vtable test_vtable_auth_vtb_0("
                      "ts timestamp, "
                      "int_col_1 int from test_vtable_auth_org_table_1.int_col, "
                      "int_col_2 int from test_vtable_auth_org_table_2.int_col);")

        priv_list = ["write", "read", "none", "all"]

        testconn = taos.connect(user='test_vtable_user_select', password='test12@#*')
        cursor = testconn.cursor()
        testSql = TDSql()
        testSql.init(cursor)

        i = 0
        for priv_db in priv_list:
            if (priv_db == "none"):
                continue # meaningless to test db has no privilege
            for priv_vtb in priv_list:
                tdSql.execute("use test_vtable_auth_select;")

                tdSql.execute(f"grant {priv_db} on test_vtable_auth_select to test_vtable_user_select;")
                if (priv_vtb != "none"):
                    tdSql.execute(f"grant {priv_vtb} on test_vtable_auth_select.test_vtable_auth_vtb_0 to test_vtable_user_select;")

                tdSql.execute(f"reset query cache")

                tdLog.info(f"priv_db: {priv_db}, priv_vtb: {priv_vtb}")
                testSql.execute("use test_vtable_auth_select;")
                if (priv_db == "read" or priv_db == "all"):
                    testSql.query("select * from test_vtable_auth_vtb_0;")
                    testSql.checkRows(0)
                    testSql.query("select int_col_1 from test_vtable_auth_vtb_0;")
                    testSql.checkRows(0)
                    testSql.query("select int_col_2 from test_vtable_auth_vtb_0;")
                    testSql.checkRows(0)
                else:
                    if (priv_vtb == "read" or priv_vtb == "all"):
                        testSql.query("select * from test_vtable_auth_vtb_0;")
                        testSql.checkRows(0)
                        testSql.query("select int_col_1 from test_vtable_auth_vtb_0;")
                        testSql.checkRows(0)
                        testSql.query("select int_col_2 from test_vtable_auth_vtb_0;")
                        testSql.checkRows(0)
                    else:
                        testSql.error("select * from test_vtable_auth_vtb_0;", expectErrInfo="Permission denied or target object not exist")
                        testSql.error("select int_col_1 from test_vtable_auth_vtb_0;", expectErrInfo="Permission denied or target object not exist")
                        testSql.error("select int_col_2 from test_vtable_auth_vtb_0;", expectErrInfo="Permission denied or target object not exist")

                tdSql.execute(f"revoke {priv_db} on test_vtable_auth_select from test_vtable_user_select;")
                if (priv_vtb != "none"):
                    tdSql.execute(f"revoke {priv_vtb} on test_vtable_auth_select.test_vtable_auth_vtb_0 from test_vtable_user_select;")
                i+=1

        tdSql.execute("drop database test_vtable_auth_select;")

    def test_select_virtual_child_table(self):
        """Auth: select virtual child table

        test "write", "read", "none", "all" each auth user select opration

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, select, auth

        Jira: None

        History:
            - 2025-3-15 Jing Sima Created
            - 2025-5-6 Huo Hong Migrated to new test framework

        """
        tdSql.execute("use test_vctable_auth_select;")
        tdSql.execute("create table test_vtable_auth_org_table_1(ts timestamp, int_col int);")
        tdSql.execute("create table test_vtable_auth_org_table_2(ts timestamp, int_col int);")
        tdSql.execute("create user test_vct_user_select PASS 'test12@#*';")
        tdSql.execute("create stable test_vtable_auth_stb_1(ts timestamp, int_col_1 int, int_col_2 int) TAGS (int_tag int) virtual 1;")
        tdSql.execute(f"create vtable test_vctable_auth_vtb_0(test_vtable_auth_org_table_1.int_col, test_vtable_auth_org_table_2.int_col) USING test_vtable_auth_stb_1 TAGS (1);")

        priv_list = ["write", "read", "none", "all"]

        testconn = taos.connect(user='test_vct_user_select', password='test12@#*')
        cursor = testconn.cursor()
        testSql = TDSql()
        testSql.init(cursor)

        i = 0
        for priv_db in priv_list:
            if (priv_db == "none"):
                continue # meaningless to test db has no privilege
            for priv_vtb in priv_list:
                tdSql.execute("use test_vctable_auth_select;")

                tdSql.execute(f"grant {priv_db} on test_vctable_auth_select to test_vct_user_select;")
                if (priv_vtb != "none"):
                    tdSql.execute(f"grant {priv_vtb} on test_vctable_auth_select.test_vtable_auth_stb_1 with int_tag = 1 to test_vct_user_select;")

                tdSql.execute(f"reset query cache")

                tdLog.info(f"priv_db: {priv_db}, priv_vtb: {priv_vtb}")
                testSql.execute("use test_vctable_auth_select;")
                if (priv_db == "read" or priv_db == "all"):
                    testSql.query("select * from test_vctable_auth_vtb_0;")
                    testSql.checkRows(0)
                    testSql.query("select int_col_1 from test_vctable_auth_vtb_0;")
                    testSql.checkRows(0)
                    testSql.query("select int_col_2 from test_vctable_auth_vtb_0;")
                    testSql.checkRows(0)
                else:
                    if (priv_vtb == "read" or priv_vtb == "all"):
                        testSql.query("select * from test_vctable_auth_vtb_0;")
                        testSql.checkRows(0)
                        testSql.query("select int_col_1 from test_vctable_auth_vtb_0;")
                        testSql.checkRows(0)
                        testSql.query("select int_col_2 from test_vctable_auth_vtb_0;")
                        testSql.checkRows(0)
                    else:
                        testSql.error("select * from test_vctable_auth_vtb_0;", expectErrInfo="Permission denied or target object not exist")
                        testSql.error("select int_col_1 from test_vctable_auth_vtb_0;", expectErrInfo="Permission denied or target object not exist")
                        testSql.error("select int_col_2 from test_vctable_auth_vtb_0;", expectErrInfo="Permission denied or target object not exist")

                tdSql.execute(f"revoke {priv_db} on test_vctable_auth_select from test_vct_user_select;")
                if (priv_vtb != "none"):
                    tdSql.execute(f"revoke {priv_vtb} on test_vctable_auth_select.test_vtable_auth_stb_1 with int_tag = 1 from test_vct_user_select;")
                i+=1

        tdSql.execute("drop database if exists test_vctable_auth_select;")


