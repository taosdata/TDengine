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

class TestStb(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.tdRest = TDRest()
        self.dbname = self.get_default_database()
        self.tdRest.request(f'create database if not exists {self.dbname}')
    
    def child_tbname_length_check(self):
        '''
            max length: 192
        '''
        stbname = self.tdCom.get_long_name(len=10, mode="letters")
        tbname = self.tdCom.get_long_name(len=192, mode="letters")
        self.tdRest.request(f'create stable if not exists {self.dbname}.{stbname} (ts timestamp, c1 int) tags (t1 int)')
        self.tdRest.request(f'create table if not exists {self.dbname}.{tbname} using {self.dbname}.{stbname} tags (127)')
        self.tdRest.error(f'create table {self.dbname}.{tbname} using {self.dbname}.{stbname} tags (127)')
        self.tdRest.request(f'show {self.dbname}.tables')
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], tbname)
        tbname_exceed = self.tdCom.get_long_name(len=193, mode="letters")
        self.tdRest.request(f'create table if not exists {self.dbname}.{tbname} using {self.dbname}.{tbname_exceed} tags (127)')

    def child_tbname_with_backquote(self):
        '''
            backquote supported
        '''
        self.tdCom.cleanTb(type="restful", dbname=self.dbname)
        stbname = self.tdCom.get_long_name(len=10, mode="letters")
        tbname = '1' + self.tdCom.get_long_name(len=10, mode="letters")
        self.tdRest.request(f'create stable if not exists {self.dbname}.{stbname} (ts timestamp, c1 int) tags (t1 int)')
        self.tdRest.request(f'create table if not exists {self.dbname}.`{tbname}` using {self.dbname}.{stbname} tags (127)')
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
                self.tdRest.request(f'create table if not exists {self.dbname}.`{tbname_new}` using {self.dbname}.{stbname} tags (127)')
                self.tdRest.request(f'show {self.dbname}.tables')
                self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], tbname_new)
                self.tdRest.request(f'drop table if exists {self.dbname}.`{tbname_new}`')

    def child_tbname_without_backquote(self):
        '''
            error occured when illegal child tbname without backquote
        '''
        self.tdCom.cleanTb(type="restful", dbname=self.dbname)
        stbname = self.tdCom.get_long_name(len=10, mode="letters")
        tbname = '1' + self.tdCom.get_long_name(len=10, mode="letters")
        self.tdRest.request(f'create stable if not exists {self.dbname}.{stbname} (ts timestamp, c1 int) tags (t1 int)')
        self.tdRest.error(f'create table if not exists {self.dbname}.`{tbname}` using {self.dbname}.{stbname} tags (127)')
        tbname = self.tdCom.get_long_name(len=3, mode="letters")
        symbol_list = self.tdCom.gen_symbol_list()
        symbol_list.remove(' ')
        for insert_str in symbol_list:
            d_list = list(tbname)
            for i in range(len(d_list)+1):
                d_list_new = copy.deepcopy(d_list)
                d_list_new.insert(i, insert_str)
                tbname = ''.join(d_list_new)
                self.tdRest.error(f'create table if not exists {self.dbname}.{tbname} using {self.dbname}.{stbname} tags (127)')

    def upper_lower_child_tbname_check(self):
        '''
            without backquote: case insensitive
            with backquote: keep upper or mixed
        '''
        self.tdCom.cleanTb(type="restful", dbname=self.dbname)
        stbname = self.tdCom.get_long_name(len=10, mode="letters")
        self.tdRest.request(f'create stable if not exists {self.dbname}.{stbname} (ts timestamp, c1 int) tags (t1 int)')
        for tbname in [self.tdCom.get_long_name(len=10, mode="letters_mixed"), self.tdCom.get_long_name(len=10, mode="letters_mixed").upper()]:
            self.tdRest.request(f'create table if not exists {self.dbname}.{tbname} using {self.dbname}.{stbname} tags (127)')
            self.tdRest.request(f'show {self.dbname}.tables')
            self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], tbname.lower())
            self.tdRest.request(f'drop table if exists {self.dbname}.`{tbname.lower()}`')
        
        for tbname in [self.tdCom.get_long_name(len=10, mode="letters_mixed"), self.tdCom.get_long_name(len=10, mode="letters_mixed").upper()]:
            self.tdRest.request(f'create table if not exists {self.dbname}.`{tbname}` using {self.dbname}.{stbname} tags (127)')
            self.tdRest.request(f'show {self.dbname}.tables')
            self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], tbname)
            self.tdRest.request(f'drop table if exists {self.dbname}.`{tbname}`')

    def desc_check(self):
        '''
        describe table
        '''
        self.tdCom.cleanTb(type="restful", dbname=self.dbname)
        stbname = self.tdCom.get_long_name(len=192, mode="letters")
        self.tdRest.request(f'create stable if not exists {self.dbname}.{stbname} (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned, c6 smallint unsigned, \
                c7 int unsigned, c8 bigint unsigned, c9 float, c10 double, c11 binary(16), c12 nchar(16), c13 bool) tags (tag_ts timestamp, t1 tinyint, t2 smallint, t3 int, \
                t4 bigint, t5 tinyint unsigned, t6 smallint unsigned, t7 int unsigned, t8 bigint unsigned, t9 float, t10 double, t11 binary(16), t12 nchar(16), t13 bool)')
        self.tdRest.request(f'create table if not exists {self.dbname}.tb using {self.dbname}.{stbname} tags (now, 1, 2, 3, 4, 5, 6, 7, 8, 9.9, 10.1, "binary", "nchar", True)')
        self.tdRest.request(f'describe {self.dbname}.tb')
        col_name_list, col_type_list, length_list, note_list = self.tdRest.getColNameList(True)
        self.tdSql.checkEqual(col_name_list, ['col_ts', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7', 'c8', 'c9', 'c10', 'c11', 'c12', 'c13', 'tag_ts', 't1', 't2', 't3', 't4', 't5', 't6', 't7', 't8', 't9', 't10', 't11', 't12', 't13'])
        self.tdSql.checkEqual(col_type_list, ['TIMESTAMP', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'TINYINT UNSIGNED', 'SMALLINT UNSIGNED', 'INT UNSIGNED', 'BIGINT UNSIGNED', 'FLOAT', 'DOUBLE', 'BINARY', 'NCHAR', 'BOOL', 'TIMESTAMP', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'TINYINT UNSIGNED', 'SMALLINT UNSIGNED', 'INT UNSIGNED', 'BIGINT UNSIGNED', 'FLOAT', 'DOUBLE', 'BINARY', 'NCHAR', 'BOOL'])
        self.tdSql.checkEqual(length_list, [8, 1, 2, 4, 8, 1, 2, 4, 8, 4, 8, 16, 16, 1, 8, 1, 2, 4, 8, 1, 2, 4, 8, 4, 8, 16, 16, 1])
        self.tdSql.checkEqual(note_list, ['', '', '', '', '', '', '', '', '', '', '', '', '', '', 'TAG', 'TAG', 'TAG', 'TAG', 'TAG', 'TAG', 'TAG', 'TAG', 'TAG', 'TAG', 'TAG', 'TAG', 'TAG', 'TAG'])

    def alter_child_tb(self):
        """
        alter child table
        """
        self.tdCom.cleanTb(type="restful", dbname=self.dbname)
        stbname = self.tdCom.get_long_name(len=192, mode="letters")
        tbname = self.tdCom.get_long_name(len=10, mode="letters")
        self.tdRest.request(f'create stable if not exists {self.dbname}.{stbname} (col_ts timestamp, c1 binary(16), c2 nchar(16)) tags (t1 binary(16), t2 nchar(16))')
        self.tdRest.request(f'create table if not exists {self.dbname}.{tbname} using {self.dbname}.{stbname} tags ("1234567891234567", "1234")')
        self.tdRest.error(f'alter stable {self.dbname}.{tbname} modify column c1 binary(32) ')
        self.tdRest.error(f'alter stable {self.dbname}.{tbname} modify column c2 nchar(32) ')
        self.tdRest.error(f'alter stable {self.dbname}.{tbname} modify tag t1 binary(32) ')
        self.tdRest.error(f'alter stable {self.dbname}.{tbname} modify tag t2 nchar(32) ')
        self.tdRest.request(f'describe {self.dbname}.{tbname}')
        length_list = self.tdRest.getColNameList(True)[2]
        self.tdSql.checkEqual(length_list, [8, 16, 16, 16, 16])

    def add_drop_column(self):
        """
        add/drop column
        """
        self.tdCom.cleanTb(type="restful", dbname=self.dbname)
        tbname = self.tdCom.get_long_name(len=10, mode="letters")
        stbname = self.tdCom.get_long_name(len=192, mode="letters")
        self.tdRest.request(f'create stable if not exists {self.dbname}.{stbname} (col_ts timestamp, c1 int, c2 tinyint) tags (t1 int, t2 tinyint)')
        self.tdRest.request(f'create table if not exists {self.dbname}.{tbname} using {self.dbname}.{stbname} tags ("1", "1")')
        # drop column
        self.tdRest.error(f'alter table {self.dbname}.{tbname} drop column c2')
        # drop tag
        self.tdRest.error(f'alter table {self.dbname}.{tbname} drop tag t2')
        # add column
        self.tdRest.error(f'alter stable {self.dbname}.{tbname} add column c2 tinyint')
        # add tag
        self.tdRest.error(f'alter stable {self.dbname}.{tbname} add tag t2 tinyint')   

        self.tdRest.request(f'describe {self.dbname}.{tbname}')
        col_name_list = self.tdRest.getColNameList(True)[0]
        col_type_list = self.tdRest.getColNameList(True)[1]
        self.tdSql.checkEqual(col_name_list, ['col_ts', 'c1', 'c2', 't1', 't2'])
        self.tdSql.checkEqual(col_type_list, ['TIMESTAMP', 'INT', 'TINYINT', 'INT', 'TINYINT'])

    def illegal_child_tbsql_check(self):
        '''
            mixed invalid symbol
            mixed space
        '''
        dbname = self.tdCom.get_long_name(len=10, mode="letters")
        self.tdRest.request(f'create database if not exists {dbname}')
        stbname = self.tdCom.get_long_name(len=3, mode="letters")
        self.tdRest.request(f'create stable if not exists {dbname}.{stbname} (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned, c6 smallint unsigned, \
                c7 int unsigned, c8 bigint unsigned, c9 float, c10 double, c11 binary(16), c12 nchar(16), c13 bool) tags (tag_ts timestamp, t1 tinyint, t2 smallint, t3 int, \
                t4 bigint, t5 tinyint unsigned, t6 smallint unsigned, t7 int unsigned, t8 bigint unsigned, t9 float, t10 double, t13 bool)')
        base_sql = f'create table if not exists {self.dbname}.tb using {self.dbname}.{stbname} tags (now, 1, 2, 3, 4, 5, 6, 7, 8, 9.9, 10.1, True)'

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
        self.tdRest.request(f'drop table if exists {dbname}.`{dbname}`')
        self.tdRest.request(f'drop database if exists {self.dbname}')

    def run(self):
        self.child_tbname_length_check()
        self.child_tbname_with_backquote()
        self.child_tbname_without_backquote()
        self.upper_lower_child_tbname_check()
        self.desc_check()
        self.alter_child_tb()
        self.add_drop_column()
        self.illegal_child_tbsql_check()

    def desc(self) -> str:
        case_description = '''
            child_tbname_length_check <jayden>: [TD-12748] : child tb name length check (max 192);\n
            child_tbname_with_backquote <jayden>: [TD-12748] : backquote supported;\n
            child_tbname_without_backquote <jayden>: [TD-12748] : error occured when illegal child tbname without backquote;\n
            upper_lower_child_tbname_check <jayden>: [TD-12748] : upper lower child tbname check;\n
            desc_check <jayden>: [TD-12748] : describe child table;\n
            alter_child_tb <jayden>: [TD-12748] : alter child table modify (binary/nchar) length;\n
            add_drop_column <jayden>: [TD-12748] : add/drop column/tag;\n
            illegal_child_tbsql_check <jayden>: [TD-12748] : illegal child tbsql check;
        '''
        return case_description
    
    def author(self) -> str:
        return "Jayden"
    
    def tags(self):
        return T.Write.RestfulSql.Table.Create, T.Write.RestfulSql.Table.Drop, T.Write.RestfulSql.Table.Alter
