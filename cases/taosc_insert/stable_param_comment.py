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
        self.dbname = 'db'
        self.stbname = 'stb'
        self.comment_length = [0,1024]
    def create_comment_check(self,comment):
        self.tdCom.createDb(self.dbname)
        self.tdSql.execute(f'use {self.dbname}')
        self.tdSql.execute(
            f'create table {self.stbname} (ts timestamp,c0 int) tags(t0 int) comment "{comment}"')
        self.tdSql.query("show stables")
        stb_kv_list = self.tdSql.getOneRow(0, self.stbname)
        self.tdSql.checkEqual(stb_kv_list[0][6], comment)
        self.tdSql.execute(f'drop database {self.dbname}')
        
    def create_comment_error(self):
        comment = self.tdCom.get_long_name(length=max(self.comment_length)+1)
        self.tdCom.createDb(self.dbname)
        self.tdSql.execute(f'use {self.dbname}')
        self.tdSql.error(
            f'create table {self.stbname} (ts timestamp,c0 int) tags(t0 int) comment "{comment}"')
        self.tdSql.execute(f'drop database {self.dbname}')

    def alter_comment_check(self,comment):
        comment_init = self.tdCom.get_long_name(length=min(self.comment_length))
        self.tdSql.execute(f'drop database  if exists {self.dbname}')
        self.tdCom.createDb(self.dbname)
        self.tdSql.execute(f'use {self.dbname}')
        self.tdSql.execute(
            f'create table {self.stbname} (ts timestamp,c0 int) tags(t0 int) comment "{comment_init}"')
        self.tdSql.execute(f'alter table {self.stbname} comment "{comment}"')
        self.tdSql.query("show stables")
        stb_kv_list = self.tdSql.getOneRow(0, self.stbname)
        self.tdSql.checkEqual(stb_kv_list[0][6], comment)
        self.tdSql.execute(f'drop database {self.dbname}')
    def alter_comment_error(self):
        comment_init = self.tdCom.get_long_name(length=min(self.comment_length))
        self.tdSql.execute(f'drop database  if exists {self.dbname}')
        self.tdCom.createDb(self.dbname)
        self.tdSql.execute(f'use {self.dbname}')
        self.tdSql.execute(
            f'create table {self.stbname} (ts timestamp,c0 int) tags(t0 int) comment "{comment_init}"')
        comment = self.tdCom.get_long_name(length=max(self.comment_length)+1)
        self.tdSql.error(f'alter table {self.stbname} comment "{comment}"')
    def create_comment_null(self):
        self.tdSql.execute(f'drop database  if exists {self.dbname}')
        self.tdCom.createDb(self.dbname)
        self.tdSql.execute(f'use {self.dbname}')
        self.tdSql.execute(
            f'create table {self.stbname} (ts timestamp,c0 int) tags(t0 int)')
        self.tdSql.query("show stables")
        stb_kv_list = self.tdSql.getOneRow(0, self.stbname)
        self.tdSql.checkEqual(stb_kv_list[0][6], None)
        self.tdSql.execute(f'drop database {self.dbname}')
    def check_comment(self):
        self.create_comment_null()
        for i in self.comment_length:
            comment = self.tdCom.get_long_name(i)
            self.create_comment_check(comment)
        for i in self.comment_length:
            comment = self.tdCom.get_long_name(i)
            self.alter_comment_check(comment)
        self.alter_comment_error()
        self.create_comment_error()
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
