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
from taostest.util.rest import TDRest
import copy

class TestTb(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdRest = TDRest()
        self.dbname = self.get_default_database()
        self.tdRest.request(f'create database if not exists {self.dbname}')

    def tbname_length_check(self):
        '''
            max length: 192
        '''
        tbname = self.tdCom.get_long_name(len=192, mode="letters")
        self.tdRest.request(f'create table if not exists {self.dbname}.{tbname} (ts timestamp, c1 int)')
        self.tdRest.error(f'create table {self.dbname}.{tbname} (ts timestamp, c1 int)')
        self.tdRest.request(f'show {self.dbname}.tables')
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], tbname)
        dbname_exceed = self.tdCom.get_long_name(len=193, mode="letters")
        self.tdRest.error(f'create table if not exists {self.dbname}.{dbname_exceed} (ts timestamp, c1 int)')

    def tbname_with_backquote(self):
        '''
            backquote supported
        '''
        self.tdCom.cleanTb(type="restful", dbname=self.dbname)
        tbname = '1' + self.tdCom.get_long_name(len=5, mode="letters")
        self.tdRest.request(f'create table if not exists {self.dbname}.`{tbname}` (ts timestamp, c1 int)')
        self.tdRest.request(f'show {self.dbname}.tables')
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], tbname)
        self.tdRest.request(f'drop table if exists {self.dbname}.`{tbname}`')
        tbname = self.tdCom.get_long_name(len=3, mode="letters")
        symbol_list = self.tdCom.gen_symbol_list()
        symbol_list.remove('`')
        for insert_str in symbol_list:
            d_list = list(tbname)
            for i in range(len(d_list)+1):
                d_list_new = copy.deepcopy(d_list)
                d_list_new.insert(i, insert_str)
                tbname_new = ''.join(d_list_new)
                self.tdRest.request(f'create table if not exists {self.dbname}.`{tbname_new}` (ts timestamp, c1 int)')
                self.tdRest.request(f'show {self.dbname}.tables')
                self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], tbname_new)
                self.tdRest.request(f'drop table if exists {self.dbname}.`{tbname_new}`')

    def tbname_without_backquote(self):
        '''
            error occured when illegal tbname without backquote
        '''
        self.tdCom.cleanTb(type="restful", dbname=self.dbname)
        tbname = '1' + self.tdCom.get_long_name(len=5, mode="letters")
        self.tdRest.error(f'create table if not exists {self.dbname}.{tbname} (ts timestamp, c1 int)')
        tbname = self.tdCom.get_long_name(len=3, mode="letters")
        symbol_list = self.tdCom.gen_symbol_list()
        symbol_list.remove(' ')
        for insert_str in symbol_list:
            d_list = list(tbname)
            for i in range(len(d_list)+1):
                d_list_new = copy.deepcopy(d_list)
                d_list_new.insert(i, insert_str)
                tbname_new = ''.join(d_list_new)
                self.tdRest.error(f'create table if not exists {self.dbname}.{tbname_new} (ts timestamp, c1 int)')

    def upper_lower_tbname_check(self):
        '''
            without backquote: case insensitive
            with backquote: keep upper or mixed
        '''
        for tbname in [self.tdCom.get_long_name(len=5, mode="letters_mixed"), self.tdCom.get_long_name(len=5, mode="letters_mixed").upper()]:
            self.tdRest.request(f'create table if not exists {self.dbname}.{tbname} (ts timestamp, c1 int)')
            self.tdRest.request(f'show {self.dbname}.tables')
            self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], tbname.lower())
            self.tdRest.request(f'drop table if exists {self.dbname}.`{tbname.lower()}`')
        
        for tbname in [self.tdCom.get_long_name(len=5, mode="letters_mixed"), self.tdCom.get_long_name(len=5, mode="letters_mixed").upper()]:
            self.tdRest.request(f'create table if not exists {self.dbname}.`{tbname}` (ts timestamp, c1 int)')
            self.tdRest.request(f'show {self.dbname}.tables')
            self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], tbname)
            self.tdRest.request(f'drop table if exists {self.dbname}.`{tbname}`')

    def desc_check(self):
        '''
        describe table
        '''
        self.tdCom.cleanTb(type="restful", dbname=self.dbname)
        tbname = self.tdCom.get_long_name(len=192, mode="letters")
        self.tdRest.request(f'create table if not exists {self.dbname}.{tbname} (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned, c6 smallint unsigned, \
                c7 int unsigned, c8 bigint unsigned, c9 float, c10 double, c11 binary(16), c12 nchar(16), c13 bool)')
        self.tdRest.request(f'describe {self.dbname}.{tbname}')
        col_name_list, col_type_list, length_list, note_list = self.tdRest.getColNameList(True)
        self.tdSql.checkEqual(col_name_list, ['col_ts', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7', 'c8', 'c9', 'c10', 'c11', 'c12', 'c13'])
        self.tdSql.checkEqual(col_type_list, ['TIMESTAMP', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'TINYINT UNSIGNED', 'SMALLINT UNSIGNED', 'INT UNSIGNED', 'BIGINT UNSIGNED', 'FLOAT', 'DOUBLE', 'BINARY', 'NCHAR', 'BOOL'])
        self.tdSql.checkEqual(length_list, [8, 1, 2, 4, 8, 1, 2, 4, 8, 4, 8, 16, 16, 1])
        self.tdSql.checkEqual(note_list, ['', '', '', '', '', '', '', '', '', '', '', '', '', ''])

    def alter_tb(self):
        """
        alter table
        """
        self.tdCom.cleanTb(type="restful", dbname=self.dbname)
        tbname = self.tdCom.get_long_name(len=192, mode="letters")
        self.tdRest.request(f'create table if not exists {self.dbname}.{tbname} (col_ts timestamp, c1 binary(16), c2 nchar(16))')
        self.tdRest.request(f'alter table {self.dbname}.{tbname} modify column c1 binary(32) ')
        self.tdRest.request(f'alter table {self.dbname}.{tbname} modify column c2 nchar(32) ')
        self.tdRest.error(f'alter table {self.dbname}.{tbname} modify column c1 binary(16) ')
        self.tdRest.error(f'alter table {self.dbname}.{tbname} modify column c2 nchar(16) ')
        self.tdRest.request(f'describe {self.dbname}.{tbname}')
        length_list = self.tdRest.getColNameList(True)[2]
        self.tdSql.checkEqual(length_list, [8, 32, 32])

    def add_drop_column(self):
        """
        add/drop column
        """
        self.tdCom.cleanTb(type="restful", dbname=self.dbname)
        tbname = self.tdCom.get_long_name(len=192, mode="letters")
        self.tdRest.request(f'create table if not exists {self.dbname}.{tbname} (col_ts timestamp, c1 int, c2 tinyint)')
        # drop column
        self.tdRest.request(f'alter table {self.dbname}.{tbname} drop column c2')

        self.tdRest.request(f'describe {self.dbname}.{tbname}')
        col_name_list = self.tdRest.getColNameList(True)[0]
        col_type_list = self.tdRest.getColNameList(True)[1]
        self.tdSql.checkEqual(col_name_list, ['col_ts', 'c1'])
        self.tdSql.checkEqual(col_type_list, ['TIMESTAMP', 'INT'])

        # add column
        self.tdRest.request(f'alter table {self.dbname}.{tbname} add column c2 tinyint')

        self.tdRest.request(f'describe {self.dbname}.{tbname}')
        col_name_list = self.tdRest.getColNameList(True)[0]
        col_type_list = self.tdRest.getColNameList(True)[1]
        self.tdSql.checkEqual(col_name_list, ['col_ts', 'c1', 'c2'])
        self.tdSql.checkEqual(col_type_list, ['TIMESTAMP', 'INT', 'TINYINT'])

    def illegal_tbsql_check(self):
        '''
            mixed invalid symbol
        '''
        dbname = self.tdCom.get_long_name(len=5, mode="letters")
        self.tdRest.request(f'create database if not exists {dbname}')
        tbname = self.tdCom.get_long_name(len=2, mode="letters")
        base_sql = f'create table if not exists {dbname}.{tbname} (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned, c6 smallint unsigned, \
                c7 int unsigned, c8 bigint unsigned, c9 float, c10 double, c13 bool)'

        symbol_list = self.tdCom.gen_symbol_list()
        symbol_list.remove(' ')
        symbol_list.remove('+')
        symbol_list.remove(';')
        for insert_str in symbol_list:
            d_list = list(base_sql)
            for i in range(len(d_list)+1):
                d_list_new = copy.deepcopy(d_list)
                d_list_new.insert(i, insert_str)
                sql_new = ''.join(d_list_new)
                self.tdRest.error(sql_new)
        self.tdRest.request(f'drop table if exists {self.dbname}.`{dbname}`')

    def run(self):
        self.tbname_length_check()
        self.tbname_with_backquote()
        self.tbname_without_backquote()
        self.upper_lower_tbname_check()
        self.desc_check()
        self.alter_tb()
        self.add_drop_column()
        self.illegal_tbsql_check()

    def desc(self):
        case_description = '''
            tbname_length_check <jayden>: [TD-12748] : tbname length check (max 192);\n
            tbname_with_backquote <jayden>: [TD-12748] : backquote supported;\n
            tbname_without_backquote <jayden>: [TD-12748] : error occured when illegal tbname without backquote;\n
            upper_lower_tbname_check <jayden>: [TD-12748] : upper lower tbname check;\n
            desc_check <jayden>: [TD-12748] : describe table;\n
            alter_stb <jayden>: [TD-12748] : alter table modify (binary/nchar) length;\n
            add_drop_column <jayden>: [TD-12748] : add/drop column/tag;\n
            illegal_tbsql_check <jayden>: [TD-12748] : illegal tbsql check;
        '''
        return case_description
    
    def author(self) -> str:
        return "Jayden"
    
    def tags(self):
        return T.Write.RestfulSql.Table.Create, T.Write.RestfulSql.Table.Drop, T.Write.RestfulSql.Table.Alter