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
import copy
from taostest.util.rest import TDRest

class TestTb(TDCase):
    def init(self):
        
        self.tdCom = TDCom(self.tdSql)
        self.tdRest = TDRest(env_setting=self.env_setting)
        self.dbname = self.tdCom.get_long_name()
    def tbname_length_check(self):
        """
        max length: 192
        """
        tbname = self.tdCom.get_long_name(length=self.tdCom.Boundary.TBNAME_MAX_LENGTH, mode="letters")
        self.tdRest.request(f'create table if not exists {self.dbname}.{tbname} (ts timestamp, c1 int)')
        self.tdRest.error(f'create table {self.dbname}.{tbname} (ts timestamp, c1 int)')
        self.tdRest.request(f'show {self.dbname}.tables')
        self.tdSql.checkEqual(self.tdRest.resp['data'][0][0], tbname)
        dbname_exceed = self.tdCom.get_long_name(length=self.tdCom.Boundary.TBNAME_MAX_LENGTH+1, mode="letters")
        self.tdSql.error(f'create table if not exists {dbname_exceed} (ts timestamp, c1 int)')

    def tbname_with_backquote(self):
        """
        backquote supported
        """
        self.tdCom.cleanTb()
        tbname = '1' + self.tdCom.get_long_name(5)
        self.tdRest.request(f'create table if not exists {self.dbname}.`{tbname}` (ts timestamp, c1 int)')
        self.tdRest.request(f'show {self.dbname}.tables')
        self.tdSql.checkEqual(self.tdRest.resp['data'][0][0], tbname)
        self.tdRest.request(f'drop table if exists {self.dbname}.`{tbname}`')
        tbname = self.tdCom.get_long_name(3)
        symbol_list = self.tdCom.gen_symbol_list()
        symbol_list.remove('`')
        symbol_list.remove('.')
        for insert_str in symbol_list:
            d_list = list(tbname)
            for i in range(len(d_list)+1):
                d_list_new = copy.deepcopy(d_list)
                d_list_new.insert(i, insert_str)
                tbname_new = ''.join(d_list_new)
                self.tdRest.request(f'create table if not exists {self.dbname}.`{tbname_new}` (ts timestamp, c1 int)')
                self.tdRest.request(f'show {self.dbname}.tables')
                self.tdSql.checkEqual(self.tdRest.resp['data'][0][0], tbname_new)
                self.tdRest.request(f'drop table if exists {self.dbname}.`{tbname_new}`')

    def tbname_without_backquote(self):
        """
        error occured when illegal tbname without backquote
        """
        self.tdCom.cleanTb()
        tbname = '1' + self.tdCom.get_long_name(5)
        self.tdSql.error(f'create table if not exists {tbname} (ts timestamp, c1 int)')
        tbname = self.tdCom.get_long_name(3)
        symbol_list = self.tdCom.gen_symbol_list()
        symbol_list.remove(' ')
        for insert_str in symbol_list:
            d_list = list(tbname)
            for i in range(len(d_list)+1):
                d_list_new = copy.deepcopy(d_list)
                d_list_new.insert(i, insert_str)
                tbname_new = ''.join(d_list_new)
                self.tdSql.error(f'create table if not exists {tbname_new} (ts timestamp, c1 int)')

    def upper_lower_tbname_check(self):
        """
        without backquote: case insensitive
        with backquote: keep upper or mixed
        """
        for tbname in [self.tdCom.get_long_name(length=5, mode="letters_mixed"), self.tdCom.get_long_name(length=5, mode="letters_mixed").upper()]:
            self.tdRest.request(f'create table if not exists {self.dbname}.{tbname} (ts timestamp, c1 int)')
            self.tdRest.request(f'show {self.dbname}.tables')
            self.tdSql.checkEqual(self.tdRest.resp['data'][0][0], tbname.lower())
            self.tdRest.request(f'drop table if exists {self.dbname}.`{tbname.lower()}`')

        for tbname in [self.tdCom.get_long_name(length=5, mode="letters_mixed"), self.tdCom.get_long_name(length=5, mode="letters_mixed").upper()]:
            self.tdRest.request(f'create table if not exists {self.dbname}.`{tbname}` (ts timestamp, c1 int)')
            self.tdRest.request(f'show {self.dbname}.tables')
            self.tdSql.checkEqual(self.tdRest.resp['data'][0][0], tbname)
            self.tdRest.request(f'drop table if exists {self.dbname}.`{tbname}`')

    def illegal_tbsql_check(self):
        """
        mixed invalid symbol
        """
        dbname = self.tdCom.get_long_name(5)
        self.tdCom.createDb(dbname)
        stbname = self.tdCom.get_long_name(3)
        tbname = self.tdCom.get_long_name(2)
        self.tdRest.request(f'create stable if not exists {dbname}.{stbname} (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned, c6 smallint unsigned, \
                c7 int unsigned, c8 bigint unsigned, c9 float, c10 double, c13 bool) tags (tag_ts timestamp, t1 tinyint, t2 smallint, t3 int, \
                t4 bigint, t5 tinyint unsigned, t6 smallint unsigned, t7 int unsigned, t8 bigint unsigned, t9 float, t10 double, t13 bool)')
        base_sql1 = f'create table if not exists {dbname}.{tbname} using {dbname}.stb tags (now, 1, 2, 3, 4, 5, 6, 7, 8, 9.9, 10.1, True)'
        base_sql2 = f'create table if not exists {dbname}.{tbname} (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned, c6 smallint unsigned, c7 int unsigned, c8 bigint unsigned, c9 float, c10 double, c13 bool)'

        symbol_list = self.tdCom.gen_symbol_list()
        symbol_list.remove(' ')
        symbol_list.remove('+')
        symbol_list.remove(';')
        for insert_str in symbol_list:
            for base_sql in [base_sql1, base_sql2]:
                d_list = list(base_sql)
                for i in range(len(d_list)+1):
                    d_list_new = copy.deepcopy(d_list)
                    d_list_new.insert(i, insert_str)
                    sql_new = ''.join(d_list_new)
                    self.tdSql.error(sql_new)
        self.tdRest.request(f'drop table if exists {dbname}.`{stbname}`')

    def comment_check(self):
        """
        tb comment check
        """
        test_param = 'comment'
        tbname = self.tdCom.get_long_name()
        comment = "tb_param_test"
        self.tdRest.request(f'create table if not exists {self.dbname}.{tbname} (ts timestamp, c1 int) {test_param} "{comment}"')
        self.tdRest.request(f'show {self.dbname}.tables')
        self.tdSql.checkEqual(self.tdRest.resp['data'][0][8], comment)
        self.tdRest.request(f'drop table {self.dbname}.{tbname}')
        

    def ttl_check(self):
        """
        check ttl
        """
        tbname = self.tdCom.get_long_name()
        test_ttl = 2
        self.tdRest.request(f'create table if not exists {self.dbname}.{tbname} (ts timestamp, c1 int) ttl {test_ttl}')
        self.tdRest.request(f'show {self.dbname}.tables')
        self.tdSql.checkEqual(self.tdRest.resp['data'][0][7], test_ttl)
        self.tdRest.request(f'drop table {self.dbname}.{tbname}')
    def run(self):
        self.tdCom.createDb(self.dbname)
        self.tbname_length_check()
        self.tbname_with_backquote()
        self.tbname_without_backquote()
        self.upper_lower_tbname_check()
        self.illegal_tbsql_check()
        self.comment_check()
        self.ttl_check()
        self.tdSql.execute(f'drop database {self.dbname}')

    def desc(self):
        case_description = """
            tbname_length_check <jayden>: [TD-13419] : tbname length check (max 192);\n
            tbname_with_backquote <jayden>: [TD-13419] : backquote supported;\n
            tbname_without_backquote <jayden>: [TD-13419] : error occured when illegal tbname without backquote;\n
            upper_lower_tbname_check <jayden>: [TD-13419] : upper lower tbname check;\n
            illegal_tbsql_check <jayden>: [TD-13419] : illegal tbsql check;n
            ttl_check <jayden>: [TD-14994] : ttl check;\n
            comment_check <jayden>: [TD-14994] : comment check;
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Table.Create, T.Write.TaoscSql.Table.Drop, T.Write.TaoscSql.Table.Alter