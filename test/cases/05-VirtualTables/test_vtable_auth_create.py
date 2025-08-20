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


class TestVtableAuthCreate:

    def setup_class(cls):
        tdLog.info(f"prepare databases.")
        tdSql.execute("drop database if exists test_vtable_auth_create;")
        tdSql.execute("drop database if exists test_vctable_auth_create;")
        tdSql.execute("create database test_vtable_auth_create;")
        tdSql.execute("create database test_vctable_auth_create;")


    def test_create_virtual_normal_table(self):
        """Auth: create virtual normal table

        test "write", "read", "none", "all" each auth user create opration

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: virtual, create, auth

        Jira: None

        History:
            - 2025-3-15 Jing Sima Created
            - 2025-5-6 Huo Hong Migrated to new test framework

        """
        tdSql.execute("use test_vtable_auth_create;")
        tdSql.execute("create table test_vtable_auth_org_table_1(ts timestamp, int_col int);")
        tdSql.execute("create table test_vtable_auth_org_table_2(ts timestamp, int_col int);")
        tdSql.execute("create user test_vtable_user_create PASS 'test12@#*';")

        priv_list = ["write", "read", "none", "all"]

        testconn = taos.connect(user='test_vtable_user_create', password='test12@#*')
        cursor = testconn.cursor()
        testSql = TDSql()
        testSql.init(cursor)

        i = 0
        for priv_db in priv_list:
            if (priv_db == "none"):
                continue # meaningless to test db has no privilege
            for priv_tb1 in priv_list:
                for priv_tb2 in priv_list:
                    tdSql.execute("use test_vtable_auth_create;")
                    tdSql.execute(f"grant {priv_db} on test_vtable_auth_create to test_vtable_user_create;")
                    if (priv_tb1 != "none"):
                        tdSql.execute(f"grant {priv_tb1} on test_vtable_auth_create.test_vtable_auth_org_table_1 to test_vtable_user_create;")
                    if (priv_tb2 != "none"):
                        tdSql.execute(f"grant {priv_tb2} on test_vtable_auth_create.test_vtable_auth_org_table_2 to test_vtable_user_create;")

                    tdSql.execute(f"reset query cache")

                    tdLog.info(f"priv_db: {priv_db}, priv_tb1: {priv_tb1}, priv_tb2: {priv_tb2}")
                    testSql.execute("use test_vtable_auth_create;")
                    if (priv_db == "read"):
                        testSql.error(f"create vtable test_vtable_auth_vtb_{i}("
                                      "ts timestamp, "
                                      "int_col_1 int from test_vtable_auth_org_table_1.int_col, "
                                      "int_col_2 int from test_vtable_auth_org_table_2.int_col);", expectErrInfo="Permission denied or target object not exist")
                    elif (priv_db == "all"):
                        testSql.execute(f"create vtable test_vtable_auth_vtb_{i}("
                                        "ts timestamp, "
                                        "int_col_1 int from test_vtable_auth_org_table_1.int_col, "
                                        "int_col_2 int from test_vtable_auth_org_table_2.int_col);")
                    elif (priv_tb1 == "write" or priv_tb2 == "write" or priv_tb1 == "none" or priv_tb2 == "none"):
                        testSql.error(f"create vtable test_vtable_auth_vtb_{i}("
                                      "ts timestamp, "
                                      "int_col_1 int from test_vtable_auth_org_table_1.int_col, "
                                      "int_col_2 int from test_vtable_auth_org_table_2.int_col);", expectErrInfo="Permission denied or target object not exist")
                    else:
                        testSql.execute(f"create vtable test_vtable_auth_vtb_{i}("
                                        "ts timestamp, "
                                        "int_col_1 int from test_vtable_auth_org_table_1.int_col, "
                                        "int_col_2 int from test_vtable_auth_org_table_2.int_col);")



                    tdSql.execute(f"revoke {priv_db} on test_vtable_auth_create from test_vtable_user_create;")
                    if (priv_tb1 != "none"):
                        tdSql.execute(f"revoke {priv_tb1} on test_vtable_auth_create.test_vtable_auth_org_table_1 from test_vtable_user_create;")
                    if (priv_tb2 != "none"):
                        tdSql.execute(f"revoke {priv_tb2} on test_vtable_auth_create.test_vtable_auth_org_table_2 from test_vtable_user_create;")
                    i+=1

        tdSql.execute("drop database test_vtable_auth_create;")

    def test_create_virtual_child_table(self):
        """Auth: create virtual child table

        test "write", "read", "none", "all" each auth user create opration

        Catalog:
            - VirtualTable
            
        Since: v3.3.6.0

        Labels: virtual, create, auth

        Jira: None

        History:
            - 2025-3-15 Jing Sima Created
            - 2025-5-6 Huo Hong Migrated to new test framework

        """
        tdSql.execute("use test_vctable_auth_create;")
        tdSql.execute("create table test_vtable_auth_org_table_1(ts timestamp, int_col int);")
        tdSql.execute("create table test_vtable_auth_org_table_2(ts timestamp, int_col int);")
        tdSql.execute("create user test_vct_user_create PASS 'test12@#*';")
        tdSql.execute("create stable test_vtable_auth_stb_1(ts timestamp, int_col_1 int, int_col_2 int) TAGS (int_tag int) virtual 1;")
        priv_list = ["write", "read", "none", "all"]

        testconn = taos.connect(user='test_vct_user_create', password='test12@#*')
        cursor = testconn.cursor()
        testSql = TDSql()
        testSql.init(cursor)

        i = 0
        for priv_db in priv_list:
            if (priv_db == "none"):
                continue # meaningless to test db has no privilege
            for priv_tb1 in priv_list:
                for priv_tb2 in priv_list:
                    tdSql.execute("use test_vctable_auth_create;")
                    tdSql.execute(f"grant {priv_db} on test_vctable_auth_create to test_vct_user_create;")
                    if (priv_tb1 != "none"):
                        tdSql.execute(f"grant {priv_tb1} on test_vctable_auth_create.test_vtable_auth_org_table_1 to test_vct_user_create;")
                    if (priv_tb2 != "none"):
                        tdSql.execute(f"grant {priv_tb2} on test_vctable_auth_create.test_vtable_auth_org_table_2 to test_vct_user_create;")

                    tdSql.execute(f"reset query cache")

                    tdLog.info(f"priv_db: {priv_db}, priv_tb1: {priv_tb1}, priv_tb2: {priv_tb2}")
                    testSql.execute("use test_vctable_auth_create;")
                    if (priv_db == "read"):
                        testSql.error(f"create vtable test_vctable_auth_vtb_{i}("
                                      "test_vtable_auth_org_table_1.int_col, "
                                      "test_vtable_auth_org_table_2.int_col) "
                                      "USING test_vtable_auth_stb_1 "
                                      "TAGS (1);", expectErrInfo="Permission denied or target object not exist")
                    elif (priv_db == "all"):
                        testSql.execute(f"create vtable test_vctable_auth_vtb_{i}("
                                        "test_vtable_auth_org_table_1.int_col, "
                                        "test_vtable_auth_org_table_2.int_col)"
                                        "USING test_vtable_auth_stb_1 "
                                        "TAGS (1);")
                    else:
                        if (priv_tb1 == "write" or priv_tb1 == "none" or priv_tb2 == "write" or priv_tb2 == "none"):
                            testSql.error(f"create vtable test_vctable_auth_vtb_{i}("
                                          "test_vtable_auth_org_table_1.int_col, "
                                          "test_vtable_auth_org_table_2.int_col) "
                                          "USING test_vtable_auth_stb_1 "
                                          "TAGS (1);", expectErrInfo="Permission denied or target object not exist")
                        else:
                            testSql.execute(f"create vtable test_vctable_auth_vtb_{i}("
                                            "test_vtable_auth_org_table_1.int_col, "
                                            "test_vtable_auth_org_table_2.int_col)"
                                            "USING test_vtable_auth_stb_1 "
                                            "TAGS (1);")



                    tdSql.execute(f"revoke {priv_db} on test_vctable_auth_create from test_vct_user_create;")
                    if (priv_tb1 != "none"):
                        tdSql.execute(f"revoke {priv_tb1} on test_vctable_auth_create.test_vtable_auth_org_table_1 from test_vct_user_create;")
                    if (priv_tb2 != "none"):
                        tdSql.execute(f"revoke {priv_tb2} on test_vctable_auth_create.test_vtable_auth_org_table_2 from test_vct_user_create;")
                    i+=1

        tdSql.execute("drop database test_vctable_auth_create;")


