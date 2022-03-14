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
    
    def stbname_length_check(self):
        '''
            max length: 192
        '''
        stbname = self.tdCom.get_long_name(len=192, mode="letters")
        self.tdRest.request(f'create stable if not exists {self.dbname}.{stbname} (ts timestamp, c1 int) tags (t1 int)')
        self.tdRest.error(f'create stable {self.dbname}.{stbname} (ts timestamp, c1 int) tags (t1 int)')
        self.tdRest.request(f'show {self.dbname}.stables')
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], stbname)
        stbname_exceed = self.tdCom.get_long_name(len=193, mode="letters")
        self.tdRest.error(f'create stable if not exists {self.dbname}.{stbname_exceed} (ts timestamp, c1 int) tags (t1 int)')

    def stbname_with_backquote(self):
        '''
            backquote supported
        '''
        self.tdCom.cleanTb(type="restful", dbname=self.dbname)
        stbname = '1' + self.tdCom.get_long_name(len=10, mode="letters")
        self.tdRest.request(f'create stable if not exists {self.dbname}.`{stbname}` (ts timestamp, c1 int) tags (t1 int)')
        self.tdRest.request(f'show {self.dbname}.stables')
        self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], stbname)
        self.tdRest.request(f'drop table if exists {self.dbname}.`{stbname}`')
        stbname = self.tdCom.get_long_name(len=3, mode="letters")
        symbol_list = self.tdCom.gen_symbol_list()
        symbol_list.remove('`')
        for insert_str in symbol_list:
            d_list = list(stbname)
            for i in range(len(d_list)+1):
                d_list_new = copy.deepcopy(d_list)
                d_list_new.insert(i, insert_str)
                stbname_new = ''.join(d_list_new)
                self.tdRest.request(f'create stable if not exists {self.dbname}.`{stbname_new}` (ts timestamp, c1 int) tags (t1 int)')
                self.tdRest.request(f'show {self.dbname}.stables')
                self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], stbname_new)
                self.tdRest.request(f'drop table if exists {self.dbname}.`{stbname_new}`')

    def stbname_without_backquote(self):
        '''
            error occured when illegal stbname without backquote
        '''
        self.tdCom.cleanTb(type="restful", dbname=self.dbname)
        stbname = '1' + self.tdCom.get_long_name(len=10, mode="letters")
        self.tdRest.error(f'create stable if not exists  {self.dbname}.{stbname} (ts timestamp, c1 int) tags (t1 int)')
        stbname = self.tdCom.get_long_name(len=3, mode="letters")
        symbol_list = self.tdCom.gen_symbol_list()
        symbol_list.remove(' ')
        for insert_str in symbol_list:
            d_list = list(stbname)
            for i in range(len(d_list)+1):
                d_list_new = copy.deepcopy(d_list)
                d_list_new.insert(i, insert_str)
                stbname_new = ''.join(d_list_new)
                self.tdRest.error(f'create stable if not exists  {self.dbname}.{stbname_new} (ts timestamp, c1 int) tags (t1 int)')

    def upper_lower_stbname_check(self):
        '''
            without backquote: case insensitive
            with backquote: keep upper or mixed
        '''
        for stbname in [self.tdCom.get_long_name(len=10, mode="letters_mixed"), self.tdCom.get_long_name(len=10, mode="letters_mixed").upper()]:
            self.tdRest.request(f'create stable if not exists {self.dbname}.{stbname} (ts timestamp, c1 int) tags (t1 int)')
            self.tdRest.request(f'show {self.dbname}.stables')
            self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], stbname.lower())
            self.tdRest.request(f'drop stable if exists {self.dbname}.`{stbname.lower()}`')
        
        for stbname in [self.tdCom.get_long_name(len=10, mode="letters_mixed"), self.tdCom.get_long_name(len=10, mode="letters_mixed").upper()]:
            self.tdRest.request(f'create stable if not exists {self.dbname}.`{stbname}` (ts timestamp, c1 int) tags (t1 int)')
            self.tdRest.request(f'show {self.dbname}.stables')
            self.tdSql.checkEqual(self.tdRest.resp["data"][0][0], stbname)
            self.tdRest.request(f'drop stable if exists {self.dbname}.`{stbname}`')

    def desc_check(self):
        '''
        describe stable
        '''
        self.tdCom.cleanTb(type="restful", dbname=self.dbname)
        stbname = self.tdCom.get_long_name(len=192, mode="letters")
        self.tdRest.request(f'create stable if not exists {self.dbname}.{stbname} (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned, c6 smallint unsigned, \
                c7 int unsigned, c8 bigint unsigned, c9 float, c10 double, c11 binary(16), c12 nchar(16), c13 bool) tags (tag_ts timestamp, t1 tinyint, t2 smallint, t3 int, \
                t4 bigint, t5 tinyint unsigned, t6 smallint unsigned, t7 int unsigned, t8 bigint unsigned, t9 float, t10 double, t11 binary(16), t12 nchar(16), t13 bool)')
        self.tdRest.request(f'describe {self.dbname}.{stbname}')
        col_name_list, col_type_list, length_list, note_list = self.tdRest.getColNameList(True)
        self.tdSql.checkEqual(col_name_list, ['col_ts', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7', 'c8', 'c9', 'c10', 'c11', 'c12', 'c13', 'tag_ts', 't1', 't2', 't3', 't4', 't5', 't6', 't7', 't8', 't9', 't10', 't11', 't12', 't13'])
        self.tdSql.checkEqual(col_type_list, ['TIMESTAMP', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'TINYINT UNSIGNED', 'SMALLINT UNSIGNED', 'INT UNSIGNED', 'BIGINT UNSIGNED', 'FLOAT', 'DOUBLE', 'BINARY', 'NCHAR', 'BOOL', 'TIMESTAMP', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'TINYINT UNSIGNED', 'SMALLINT UNSIGNED', 'INT UNSIGNED', 'BIGINT UNSIGNED', 'FLOAT', 'DOUBLE', 'BINARY', 'NCHAR', 'BOOL'])
        self.tdSql.checkEqual(length_list, [8, 1, 2, 4, 8, 1, 2, 4, 8, 4, 8, 16, 16, 1, 8, 1, 2, 4, 8, 1, 2, 4, 8, 4, 8, 16, 16, 1])
        self.tdSql.checkEqual(note_list, ['', '', '', '', '', '', '', '', '', '', '', '', '', '', 'TAG', 'TAG', 'TAG', 'TAG', 'TAG', 'TAG', 'TAG', 'TAG', 'TAG', 'TAG', 'TAG', 'TAG', 'TAG', 'TAG'])

    def alter_stb(self):
        """
        alter stable
        """
        self.tdCom.cleanTb(type="restful", dbname=self.dbname)
        stbname = self.tdCom.get_long_name(len=192, mode="letters")
        self.tdRest.request(f'create stable if not exists {self.dbname}.{stbname} (col_ts timestamp, c1 binary(16), c2 nchar(16)) tags (t1 binary(16), t2 nchar(16))')
        self.tdRest.request(f'alter stable {self.dbname}.{stbname} modify column c1 binary(32) ')
        self.tdRest.request(f'alter stable {self.dbname}.{stbname} modify column c2 nchar(32) ')
        self.tdRest.request(f'alter stable {self.dbname}.{stbname} modify tag t1 binary(32) ')
        self.tdRest.request(f'alter stable {self.dbname}.{stbname} modify tag t2 nchar(32) ')
        self.tdRest.error(f'alter stable {self.dbname}.{stbname} modify column c1 binary(16) ')
        self.tdRest.error(f'alter stable {self.dbname}.{stbname} modify column c2 nchar(16) ')
        self.tdRest.error(f'alter stable {self.dbname}.{stbname} modify tag t1 binary(16) ')
        self.tdRest.error(f'alter stable {self.dbname}.{stbname} modify tag t2 nchar(16) ')
        self.tdRest.request(f'describe {self.dbname}.{stbname}')
        length_list = self.tdRest.getColNameList(True)[2]
        self.tdSql.checkEqual(length_list, [8, 32, 32, 32, 32])

    def add_drop_column(self):
        """
        add/drop column
        """
        self.tdCom.cleanTb(type="restful", dbname=self.dbname)
        stbname = self.tdCom.get_long_name(len=192, mode="letters")
        self.tdRest.request(f'create stable if not exists {self.dbname}.{stbname} (col_ts timestamp, c1 int, c2 tinyint) tags (t1 int, t2 tinyint)')
        # drop column
        self.tdRest.request(f'alter stable {self.dbname}.{stbname} drop column c2')
        # drop tag
        self.tdRest.request(f'alter stable {self.dbname}.{stbname} drop tag t2')

        self.tdRest.request(f'describe {self.dbname}.{stbname}')
        col_name_list = self.tdRest.getColNameList(True)[0]
        col_type_list = self.tdRest.getColNameList(True)[1]
        self.tdSql.checkEqual(col_name_list, ['col_ts', 'c1', 't1'])
        self.tdSql.checkEqual(col_type_list, ['TIMESTAMP', 'INT', 'INT'])

        # add column
        self.tdRest.request(f'alter stable {self.dbname}.{stbname} add column c2 tinyint')
        # add tag
        self.tdRest.request(f'alter stable {self.dbname}.{stbname} add tag t2 tinyint')

        self.tdRest.request(f'describe {self.dbname}.{stbname}')
        col_name_list = self.tdRest.getColNameList(True)[0]
        col_type_list = self.tdRest.getColNameList(True)[1]
        self.tdSql.checkEqual(col_name_list, ['col_ts', 'c1', 'c2', 't1', 't2'])
        self.tdSql.checkEqual(col_type_list, ['TIMESTAMP', 'INT', 'TINYINT', 'INT', 'TINYINT'])

    def illegal_stbsql_check(self):
        '''
            mixed invalid symbol
            mixed space
        '''
        dbname = self.tdCom.get_long_name(len=10, mode="letters")
        self.tdRest.request(f'create database if not exists {dbname}')
        stbname = self.tdCom.get_long_name(len=3, mode="letters")
        base_sql = f'create stable if not exists {dbname}.{stbname} (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned, c6 smallint unsigned, \
                c7 int unsigned, c8 bigint unsigned, c9 float, c10 double, c11 binary(16), c12 nchar(16), c13 bool) tags (tag_ts timestamp, t1 tinyint, t2 smallint, t3 int, \
                t4 bigint, t5 tinyint unsigned, t6 smallint unsigned, t7 int unsigned, t8 bigint unsigned, t9 float, t10 double, t11 binary(16), t12 nchar(16), t13 bool)'

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
        self.tdRest.request(f'drop stable if exists {dbname}.`{dbname}`')
        self.tdRest.request(f'drop database if exists {self.dbname}')

    def run(self):
        self.stbname_length_check()
        self.stbname_with_backquote()
        self.stbname_without_backquote()
        self.upper_lower_stbname_check()
        self.desc_check()
        self.alter_stb()
        self.add_drop_column()
        self.illegal_stbsql_check()

    def desc(self) -> str:
        case_description = '''
            stbname_length_check <jayden>: [TD-12748] : stb name length check (max 192);\n
            stbname_with_backquote <jayden>: [TD-12748] : backquote supported;\n
            stbname_without_backquote <jayden>: [TD-12748] : error occured when illegal stbname without backquote;\n
            upper_lower_stbname_check <jayden>: [TD-12748] : upper lower stbname check;\n
            desc_check <jayden>: [TD-12748] : describe stable;\n
            alter_stb <jayden>: [TD-12748] : alter stable modify (binary/nchar) length;\n
            add_drop_column <jayden>: [TD-12748] : add/drop column/tag;\n
            illegal_stbsql_check <jayden>: [TD-12748] : illegal stbsql check;
        '''
        return case_description
    
    def author(self) -> str:
        return "Jayden"
    
    def tags(self):
        return T.Write.RestfulSql.Stable.Create, T.Write.RestfulSql.Stable.Drop, T.Write.RestfulSql.Stable.Alter
