###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
from taostest import TDCase, T
from taostest.util.common import TDCom

class TestTableComment(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def __create_tb(self,dbname,stbname,tbname,comment):
        self.tdCom.createDb(dbname)
        self.tdSql.execute(f'use {dbname}')
        self.tdSql.execute(
            f'create table {stbname} (ts timestamp,c0 int) tags(t0 int) ')
        self.tdSql.execute(
            f'create table {tbname} using {stbname} tags(1) comment "{comment}"')
    def __create_normaltb(self,dbname,tbname,comment):
        self.tdCom.createDb(dbname)
        self.tdSql.execute(f'use {dbname}')
        self.tdSql.execute(
            f'create table {tbname} (ts timestamp,c0 int) comment "{comment}"')
        
    def check_comment(self):
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        ntbname = self.tdCom.get_long_name(length=5, mode="letters")

        # create normal table with comment
        comment = self.tdCom.get_long_name(length=10, mode="letters")
        self.__create_normaltb(dbname,ntbname,comment)
        self.tdSql.query("show tables")
        ntb_kv_list = self.tdSql.getOneRow(0, ntbname)
        self.tdSql.checkEqual(ntb_kv_list[0][8], comment)
        self.tdSql.error('alter table {ntbname} comment "test1"')
        self.tdSql.execute(f'drop database {dbname}')

        # max length(1024)
        comment = self.tdCom.get_long_name(length=1024, mode="letters")
        self.__create_normaltb(dbname,ntbname,comment)
        self.tdSql.query("show tables")
        ntb_kv_list = self.tdSql.getOneRow(0, ntbname)
        self.tdSql.checkEqual(ntb_kv_list[0][8], comment)
        self.tdSql.execute(f'drop database {dbname}')

        # error overlength
        comment = self.tdCom.get_long_name(length=1025, mode="letters")
        self.tdCom.createDb(dbname)
        self.tdSql.execute(f'use {dbname}')
        self.tdSql.error(f"create table ntb (ts timestamp,c0 int) comment '{comment}'")
        self.tdSql.execute(f'drop database {dbname}')

        # create child table with comment
        comment = self.tdCom.get_long_name(length=10, mode="letters")
        stbname = self.tdCom.get_long_name(length=5, mode="letters")
        tbname = self.tdCom.get_long_name(length=3, mode="letters")
        self.__create_tb(dbname,stbname,tbname,comment)
        self.tdSql.query("show tables")
        ntb_kv_list = self.tdSql.getOneRow(0, ntbname)
        self.tdSql.checkEqual(ntb_kv_list[0][8], comment)
        self.tdSql.error(f'alter table {tbname} comment "test1"')
        self.tdSql.execute(f'drop database {dbname}')

        # max length 1024
        comment = self.tdCom.get_long_name(length=1024, mode="letters")
        self.__create_tb(dbname,ntbname,comment)
        self.tdSql.query("show tables")
        ntb_kv_list = self.tdSql.getOneRow(0, ntbname)
        self.tdSql.checkEqual(ntb_kv_list[0][8], comment)
        self.tdSql.execute(f'drop database {dbname}')
    def run(self) -> bool:
        self.check_comment()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            
            comment check <jiacy>:  [TD-16138] : table comment check;
            """
        return case_description

    def author(self) -> str:
        return "Jiacy"

    def tags(self):
        return T.Write.TaoscSql.Stable.Create, T.Write.TaoscSql.Stable.Alter