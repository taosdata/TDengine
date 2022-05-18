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


class TestComp(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def check_comment(self):

        comment = self.tdCom.get_long_name(length=10, mode="letters")
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        stbname = self.tdCom.get_long_name(length=5, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.execute(f'use {dbname}')
        self.tdSql.execute(
            f'create table {stbname} (ts timestamp,c0 int) tags(t0 int) comment "{comment}"')
        self.tdSql.query("show stables")
        stb_kv_list = self.tdSql.getOneRow(0, stbname)
        # print(stb_kv_list)
        self.tdSql.checkEqual(stb_kv_list[0][6], comment)
        self.tdSql.execute(f'drop database {dbname}')

        # max length
        comment = self.tdCom.get_long_name(length=1023, mode="letters")
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        stbname = self.tdCom.get_long_name(length=5, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.execute(f'use {dbname}')
        self.tdSql.execute(
            f'create table {stbname} (ts timestamp,c0 int) tags(t0 int) comment "{comment}"')
        self.tdSql.query("show stables")
        stb_kv_list = self.tdSql.getOneRow(0, stbname)
        # print(stb_kv_list)
        self.tdSql.checkEqual(stb_kv_list[0][6], comment)
        self.tdSql.execute(f'drop database {dbname}')

        # error
        comment = self.tdCom.get_long_name(length=1025, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        self.tdSql.execute(f'use {dbname}')
        self.tdSql.execute(
            f'create table {stbname} (ts timestamp,c0 int) tags(t0 int) comment "{comment}"')
    def run(self) -> bool:
        self.check_comment()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            
            comment check <jiacy>:  [TD-15381] : comment check;
            """
        return case_description

    def author(self) -> str:
        return "Jiacy"

    def tags(self):
        return T.Write.TaoscSql.Stable.Create, T.Write.TaoscSql.Stable.Alter
