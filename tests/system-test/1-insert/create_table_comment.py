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

import random
import string
from util.log import *
from util.cases import *
from util.sql import *

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def get_long_name(self, length, mode="mixed"):
        """
        generate long name
        mode could be numbers/letters/letters_mixed/mixed
        """
        if mode == "numbers":
            population = string.digits
        elif mode == "letters":
            population = string.ascii_letters.lower()
        elif mode == "letters_mixed":
            population = string.ascii_letters.upper() + string.ascii_letters.lower()
        else:
            population = string.ascii_letters.lower() + string.digits
        return "".join(random.choices(population, k=length))

    def __create_tb(self,dbname,stbname,tbname,comment):
        tdSql.execute(f'create database if not exists {dbname}')
        tdSql.execute(f'use {dbname}')
        tdSql.execute(
            f'create table {stbname} (ts timestamp,c0 int) tags(t0 int) ')
        tdSql.execute(
            f'create table {tbname} using {stbname} tags(1) comment "{comment}"')
    def __create_normaltb(self,dbname,tbname,comment):
        tdSql.execute(f'create database if not exists {dbname}')
        tdSql.execute(f'use {dbname}')
        tdSql.execute(
            f'create table {tbname} (ts timestamp,c0 int) comment "{comment}"')

    def check_comment(self):
        dbname = self.get_long_name(length=10, mode="letters")
        ntbname = self.get_long_name(length=5, mode="letters")

        # create normal table with comment
        comment = self.get_long_name(length=10, mode="letters")
        self.__create_normaltb(dbname,ntbname,comment)
        ntb_kv_list = tdSql.getResult("show tables")
        print(ntb_kv_list)
        tdSql.checkEqual(ntb_kv_list[0][8], comment)
        tdSql.error('alter table {ntbname} comment "test1"')
        tdSql.execute(f'drop database {dbname}')

        # max length(1024)
        comment = self.get_long_name(length=1024, mode="letters")
        self.__create_normaltb(dbname,ntbname,comment)
        ntb_kv_list = tdSql.getResult("show tables")
        tdSql.checkEqual(ntb_kv_list[0][8], comment)
        tdSql.execute(f'drop database {dbname}')

        # error overlength
        comment = self.get_long_name(length=1025, mode="letters")
        tdSql.execute(f'create database if not exists {dbname}')
        tdSql.execute(f'use {dbname}')
        tdSql.error(f"create table ntb (ts timestamp,c0 int) comment '{comment}'")
        tdSql.execute(f'drop database {dbname}')

        # create child table with comment
        comment = self.get_long_name(length=10, mode="letters")
        stbname = self.get_long_name(length=5, mode="letters")
        tbname = self.get_long_name(length=3, mode="letters")
        self.__create_tb(dbname,stbname,tbname,comment)
        ntb_kv_list = tdSql.getResult("show tables")
        tdSql.checkEqual(ntb_kv_list[0][8], comment)
        tdSql.error(f'alter table {tbname} comment "test1"')
        tdSql.execute(f'drop database {dbname}')

        # max length 1024
        comment = self.get_long_name(length=1024, mode="letters")
        self.__create_tb(dbname,ntbname,comment)
        ntb_kv_list = tdSql.getResult("show tables")
        tdSql.checkEqual(ntb_kv_list[0][8], comment)
        tdSql.execute(f'drop database {dbname}')

        # error overlength
        comment = self.get_long_name(length=1025, mode="letters")
        tdSql.execute(f'create database if not exists {dbname}')
        tdSql.execute(f'use {dbname}')
        tdSql.execute(f"create table stb (ts timestamp,c0 int) tags(t0 int)")
        tdSql.error(f'create table stb_1 us stb tags(1) comment "{comment}"')
        tdSql.execute(f'drop database {dbname}')

    def run(self):
        self.check_comment()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())