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

import taos
import time

class TDTestCase(TBase):

    def prepare_data(self):
        tdLog.info(f"prepare databases.")
        tdSql.execute("drop database if exists test_vctable_auth_alter;")
        tdSql.execute("create database test_vctable_auth_alter;")

    def test_alter_drop_virtual_child_table(self):
        tdSql.execute("use test_vctable_auth_alter;")
        tdSql.execute("create table test_vtable_auth_org_table_1(ts timestamp, int_col int);")
        tdSql.execute("create table test_vtable_auth_org_table_2(ts timestamp, int_col int);")
        tdSql.execute("create user test_vct_user_alter PASS 'test12@#*';")
        tdSql.execute("create stable test_vtable_auth_stb_1(ts timestamp, int_col_1 int, int_col_2 int) TAGS (int_tag int) virtual 1;")
        priv_list = ["write", "read", "none", "all"]

        testconn = taos.connect(user='test_vct_user_alter', password='test12@#*')
        cursor = testconn.cursor()
        testSql = TDSql()
        testSql.init(cursor)

        i = 0
        for priv_db in priv_list:
            if (priv_db == "none"):
                continue # meaningless to test db has no privilege
            for priv_vtb in priv_list:
                for priv_orgtb in priv_list:
                    tdSql.execute("use test_vctable_auth_alter;")
                    tdSql.execute(f"create vtable test_vctable_auth_vtb_{i}("
                                  "test_vtable_auth_org_table_1.int_col)"
                                  "USING test_vtable_auth_stb_1 "
                                  "TAGS (1);")
                    tdSql.execute(f"grant {priv_db} on test_vctable_auth_alter to test_vct_user_alter;")
                    if (priv_vtb != "none"):
                        tdSql.execute(f"grant {priv_vtb} on test_vctable_auth_alter.test_vtable_auth_stb_1 to test_vct_user_alter;")
                    if (priv_orgtb != "none"):
                        tdSql.execute(f"grant {priv_orgtb} on test_vctable_auth_alter.test_vtable_auth_org_table_2 to test_vct_user_alter;")

                    sleep(1)

                    tdLog.info(f"priv_db: {priv_db}, priv_tb1: {priv_vtb}, priv_tb2: {priv_orgtb}")
                    testSql.execute("use test_vctable_auth_alter;")
                    if (priv_db == "read"):
                        if (priv_vtb == "write" or priv_vtb == "all"):
                            testSql.execute(f"alter vtable test_vctable_auth_vtb_{i} alter column int_col_2 set test_vtable_auth_org_table_2.int_col;")
                            testSql.execute(f"alter vtable test_vctable_auth_vtb_{i} alter column int_col_2 SET NULL;")
                            testSql.execute(f"alter vtable test_vctable_auth_vtb_{i} set tag int_tag = 2;")
                        else:
                            testSql.error(f"alter vtable test_vctable_auth_vtb_{i} alter column int_col_2 set test_vtable_auth_org_table_2.int_col;", expectErrInfo="Permission denied or target object not exist")
                            testSql.error(f"alter vtable test_vctable_auth_vtb_{i} alter column int_col_2 SET NULL;", expectErrInfo="Permission denied or target object not exist")
                            testSql.error(f"alter vtable test_vctable_auth_vtb_{i} set tag int_tag = 2;", expectErrInfo="Permission denied or target object not exist")
                    elif (priv_db == "all"):
                        testSql.execute(f"alter vtable test_vctable_auth_vtb_{i} alter column int_col_2 set test_vtable_auth_org_table_2.int_col;")
                        testSql.execute(f"alter vtable test_vctable_auth_vtb_{i} alter column int_col_2 SET NULL;")
                        testSql.execute(f"alter vtable test_vctable_auth_vtb_{i} set tag int_tag = 2;")
                    else:
                        if (priv_orgtb == "none" or priv_orgtb == "write"):
                            testSql.error(f"alter vtable test_vctable_auth_vtb_{i} alter column int_col_2 set test_vtable_auth_org_table_2.int_col;")
                            testSql.execute(f"alter vtable test_vctable_auth_vtb_{i} alter column int_col_2 SET NULL;")
                            testSql.execute(f"alter vtable test_vctable_auth_vtb_{i} set tag int_tag = 2;")
                        else:
                            testSql.execute(f"alter vtable test_vctable_auth_vtb_{i} alter column int_col_2 set test_vtable_auth_org_table_2.int_col;")
                            testSql.execute(f"alter vtable test_vctable_auth_vtb_{i} alter column int_col_2 SET NULL;")
                            testSql.execute(f"alter vtable test_vctable_auth_vtb_{i} set tag int_tag = 2;")


                    if (priv_db == "read"):
                        if (priv_vtb == "write" or priv_vtb == "all"):
                            testSql.execute(f"drop vtable test_vctable_auth_vtb_{i};")
                        else:
                            testSql.error(f"drop vtable test_vctable_auth_vtb_{i};", expectErrInfo="Permission denied or target object not exist")
                    elif (priv_db == "all"):
                        testSql.execute(f"drop vtable test_vctable_auth_vtb_{i};")
                    else:
                        testSql.execute(f"drop vtable test_vctable_auth_vtb_{i};")

                    tdSql.execute(f"revoke {priv_db} on test_vctable_auth_alter from test_vct_user_alter;")
                    if (priv_vtb != "none"):
                        tdSql.execute(f"revoke {priv_vtb} on test_vctable_auth_alter.test_vtable_auth_stb_1 from test_vct_user_alter;")
                    if (priv_orgtb != "none"):
                        tdSql.execute(f"revoke {priv_orgtb} on test_vctable_auth_alter.test_vtable_auth_org_table_2 from test_vct_user_alter;")
                    i+=1

        tdSql.execute("drop database test_vctable_auth_alter;")

    def run(self):
        tdLog.debug(f"start to excute {__file__}")

        self.prepare_data()
        self.test_alter_drop_virtual_child_table()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
