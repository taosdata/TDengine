# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

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

        test "insert", "select", "none", "all" each auth user create opration

        Catalog:
            - VirtualTable

        Since: v3.3.6.0

        Labels: auth,create,virtual,integration,functional

        Jira: None

        History:
            - 2025-3-15 Jing Sima Created
            - 2025-5-6 Huo Hong Migrated to new test framework

        """
        tdSql.execute("use test_vtable_auth_create;")
        tdSql.execute("create table test_vtable_auth_org_table_1(ts timestamp, int_col int);")
        tdSql.execute("create table test_vtable_auth_org_table_2(ts timestamp, int_col int);")
        tdSql.execute("create user test_vtable_user_create PASS 'test12@#*';")

        priv_list = ["insert", "select", "none", "all"]

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
                    tdSql.execute(f"grant {priv_db} on test_vtable_auth_create.* to test_vtable_user_create;")
                    tdSql.execute(f"grant use on database test_vtable_auth_create to test_vtable_user_create;")
                    if(priv_db == "insert" or priv_db == "all"):
                        tdSql.execute(f"grant create table on database test_vtable_auth_create to test_vtable_user_create;")
                    if (priv_tb1 != "none"):
                        tdSql.execute(f"grant {priv_tb1} on test_vtable_auth_create.test_vtable_auth_org_table_1 to test_vtable_user_create;")
                    if (priv_tb2 != "none"):
                        tdSql.execute(f"grant {priv_tb2} on test_vtable_auth_create.test_vtable_auth_org_table_2 to test_vtable_user_create;")

                    tdSql.execute(f"reset query cache")
                    tdLog.info(f"start to check: priv_db: {priv_db}, priv_tb1: {priv_tb1}, priv_tb2: {priv_tb2}")
                    testSql.execute("use test_vtable_auth_create;")
                    if (priv_db == "select"):
                        testSql.error(f"create vtable test_vtable_auth_vtb_{i}("
                                      "ts timestamp, "
                                      "int_col_1 int from test_vtable_auth_org_table_1.int_col, "
                                      "int_col_2 int from test_vtable_auth_org_table_2.int_col);", expectErrInfo="Permission denied or the target object does not exist")
                    elif (priv_db == "all"):
                        testSql.execute(f"create vtable test_vtable_auth_vtb_{i}("
                                        "ts timestamp, "
                                        "int_col_1 int from test_vtable_auth_org_table_1.int_col, "
                                        "int_col_2 int from test_vtable_auth_org_table_2.int_col);")
                    elif (priv_tb1 == "insert" or priv_tb2 == "insert" or priv_tb1 == "none" or priv_tb2 == "none"):
                        testSql.error(f"create vtable test_vtable_auth_vtb_{i}("
                                      "ts timestamp, "
                                      "int_col_1 int from test_vtable_auth_org_table_1.int_col, "
                                      "int_col_2 int from test_vtable_auth_org_table_2.int_col);", expectErrInfo="Permission denied to select from table or view")
                    else:
                        testSql.execute(f"create vtable test_vtable_auth_vtb_{i}("
                                        "ts timestamp, "
                                        "int_col_1 int from test_vtable_auth_org_table_1.int_col, "
                                        "int_col_2 int from test_vtable_auth_org_table_2.int_col);")



                    tdSql.execute(f"revoke {priv_db} on test_vtable_auth_create.* from test_vtable_user_create;")
                    if(priv_db == "insert" or priv_db == "all"):
                        tdSql.execute(f"revoke create table on database test_vtable_auth_create from test_vtable_user_create;")
                    if (priv_tb1 != "none"):
                        tdSql.execute(f"revoke {priv_tb1} on test_vtable_auth_create.test_vtable_auth_org_table_1 from test_vtable_user_create;")
                    if (priv_tb2 != "none"):
                        tdSql.execute(f"revoke {priv_tb2} on test_vtable_auth_create.test_vtable_auth_org_table_2 from test_vtable_user_create;")
                    tdLog.info(f"finish to check: priv_db: {priv_db}, priv_tb1: {priv_tb1}, priv_tb2: {priv_tb2}")
                    i+=1

        tdSql.execute("drop database test_vtable_auth_create;")

    def test_create_virtual_child_table(self):
        """Auth: create virtual child table

        test "insert", "select", "none", "all" each auth user create opration

        Catalog:
            - VirtualTable
            
        Since: v3.3.6.0

        Labels: auth,create,virtual,integration,functional

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
        priv_list = ["insert", "select", "none", "all"]

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
                    tdSql.execute(f"grant {priv_db} on test_vctable_auth_create.* to test_vct_user_create;")
                    tdSql.execute(f"grant use on database test_vctable_auth_create to test_vct_user_create;")
                    if(priv_db == "insert" or priv_db == "all"):
                        tdSql.execute(f"grant create table on database test_vctable_auth_create to test_vct_user_create;")
                    if (priv_tb1 != "none"):
                        tdSql.execute(f"grant {priv_tb1} on test_vctable_auth_create.test_vtable_auth_org_table_1 to test_vct_user_create;")
                    if (priv_tb2 != "none"):
                        tdSql.execute(f"grant {priv_tb2} on test_vctable_auth_create.test_vtable_auth_org_table_2 to test_vct_user_create;")

                    tdSql.execute(f"reset query cache")
                    tdLog.info(f"start to check: priv_db: {priv_db}, priv_tb1: {priv_tb1}, priv_tb2: {priv_tb2}")
                    testSql.execute("use test_vctable_auth_create;")
                    if (priv_db == "select"):
                        testSql.error(f"create vtable test_vctable_auth_vtb_{i}("
                                      "test_vtable_auth_org_table_1.int_col, "
                                      "test_vtable_auth_org_table_2.int_col) "
                                      "USING test_vtable_auth_stb_1 "
                                      "TAGS (1);", expectErrInfo="Permission denied or the target object does not exist")
                    elif (priv_db == "all"):
                        testSql.execute(f"create vtable test_vctable_auth_vtb_{i}("
                                        "test_vtable_auth_org_table_1.int_col, "
                                        "test_vtable_auth_org_table_2.int_col)"
                                        "USING test_vtable_auth_stb_1 "
                                        "TAGS (1);")
                    else:
                        if (priv_tb1 == "insert" or priv_tb1 == "none" or priv_tb2 == "insert" or priv_tb2 == "none"):
                            testSql.error(f"create vtable test_vctable_auth_vtb_{i}("
                                          "test_vtable_auth_org_table_1.int_col, "
                                          "test_vtable_auth_org_table_2.int_col) "
                                          "USING test_vtable_auth_stb_1 "
                                          "TAGS (1);", expectErrInfo="Permission denied to select from table or view")
                        else:
                            testSql.execute(f"create vtable test_vctable_auth_vtb_{i}("
                                            "test_vtable_auth_org_table_1.int_col, "
                                            "test_vtable_auth_org_table_2.int_col)"
                                            "USING test_vtable_auth_stb_1 "
                                            "TAGS (1);")



                    tdSql.execute(f"revoke {priv_db} on test_vctable_auth_create.* from test_vct_user_create;")
                    if(priv_db == "insert" or priv_db == "all"):
                        tdSql.execute(f"revoke create table on database test_vctable_auth_create from test_vct_user_create;")
                    if (priv_tb1 != "none"):
                        tdSql.execute(f"revoke {priv_tb1} on test_vctable_auth_create.test_vtable_auth_org_table_1 from test_vct_user_create;")
                    if (priv_tb2 != "none"):
                        tdSql.execute(f"revoke {priv_tb2} on test_vctable_auth_create.test_vtable_auth_org_table_2 from test_vct_user_create;")
                    tdLog.info(f"finish to check: priv_db: {priv_db}, priv_tb1: {priv_tb1}, priv_tb2: {priv_tb2}")
                    i+=1

        tdSql.execute("drop database test_vctable_auth_create;")
    def test_create_vtable_own_column_source_auth(self):
        """Auth: bound own columns need source SELECT, bare ones do not

        A user granted only database usage and table creation must not be able
        to bind an own column (nor a cascaded one) to a source table it cannot
        read: create time is the only gate, queries through the virtual table
        never re-check the source. A bare own-column declaration touches no
        source table and must go through.

        Catalog:
            - VirtualTable

        Since: v3.4.3.0

        Labels: auth,create,virtual,integration,functional

        Jira: None

        History:
            - 2026-11-05 Yihao Deng Added for the review round of defect 7155574571

        """
        tdSql.execute("create database if not exists test_vct_auth_own;")
        tdSql.execute("use test_vct_auth_own;")
        tdSql.execute("create table org_src(ts timestamp, secret nchar(32));")
        tdSql.execute("insert into org_src values(now, 'classified');")
        tdSql.execute("create stable vstb(ts timestamp, c1 int) tags(t int) virtual 1;")
        tdSql.execute("create user test_vct_own_user PASS 'test12@#*';")
        # database usage and table creation only: no SELECT on the source table
        tdSql.execute("grant use on database test_vct_auth_own to test_vct_own_user;")
        tdSql.execute("grant create table on database test_vct_auth_own to test_vct_own_user;")
        tdSql.execute("reset query cache")

        testconn = taos.connect(user='test_vct_own_user', password='test12@#*')
        cursor = testconn.cursor()
        testSql = TDSql()
        testSql.init(cursor)
        testSql.execute("use test_vct_auth_own;")

        # 1. a bound own column reads a table the user cannot select from: denied
        testSql.error("create vtable vt_bound(note nchar(32) from org_src.secret) using vstb tags (1);",
                      expectErrInfo="Permission denied to select from table or view")
        # 2. a bound cascaded column is denied by the same gate
        testSql.error("create vtable vt_bound_cas(c1 from org_src.ts) using vstb tags (2);",
                      expectErrInfo="Permission denied to select from table or view")
        # 3. a bare own-column declaration involves no source table: allowed
        testSql.execute("create vtable vt_bare(note nchar(32)) using vstb tags (3);")

        cursor.close()
        testconn.close()

        # the bare column exists with its declared type and no source
        tdSql.query(
            "select col_type, col_source from information_schema.ins_columns "
            "where db_name='test_vct_auth_own' and table_name='vt_bare' and col_name='note';"
        )
        tdSql.checkData(0, 0, "NCHAR(32)")
        tdSql.checkData(0, 1, None)

        tdSql.execute("revoke use on database test_vct_auth_own from test_vct_own_user;")
        tdSql.execute("revoke create table on database test_vct_auth_own from test_vct_own_user;")
        tdSql.execute("drop user test_vct_own_user;")
        tdSql.execute("drop database test_vct_auth_own;")
