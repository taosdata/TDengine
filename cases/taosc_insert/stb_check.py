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
class TestStb(TDCase):
    def init(self):
        super().init()
        self.tdCom = TDCom(self.tdSql)

    def stbname_length_check(self):
        """
        max length: 192
        """
        stbname = self.tdCom.get_long_name(length=self.tdCom.boundary_config["STBNAME_MAX_LENGTH"], mode="letters")
        self.tdSql.execute(f'create stable if not exists {stbname} (ts timestamp, c1 int) tags (t1 int)')
        self.tdSql.error(f'create stable {stbname} (ts timestamp, c1 int) tags (t1 int)')
        self.tdSql.query('show stables')
        self.tdSql.checkEqual(self.tdSql.query_data[0][0], stbname)
        dbname_exceed = self.tdCom.get_long_name(length=self.tdCom.boundary_config["STBNAME_MAX_LENGTH"]+1, mode="letters")
        self.tdSql.error(f'create stable if not exists {dbname_exceed} (ts timestamp, c1 int) tags (t1 int)')

    def stb_params_check(self):
        """
        stb params check
        """
        # comment
        stbname = self.tdCom.get_long_name(length=10, mode="letters")
        comment = "stb_param_test"
        self.tdSql.execute(f'create stable if not exists {stbname} (ts timestamp, c1 int) tags (t1 int) comment "{comment}"')
        self.tdSql.query('show stables')
        res = self.tdSql.get_db_field_kv(0, stbname)
        self.tdSql.checkEqual(res["table_comment"], comment)

    def stbname_with_backquote(self):
        """
        backquote supported
        """
        self.tdCom.cleanTb()
        stbname = '1' + self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create stable if not exists `{stbname}` (ts timestamp, c1 int) tags (t1 int)')
        self.tdSql.query('show stables')
        self.tdSql.checkEqual(self.tdSql.query_data[0][0], stbname)
        self.tdSql.execute(f'drop table if exists `{stbname}`')
        stbname = self.tdCom.get_long_name(length=3, mode="letters")
        symbol_list = self.tdCom.gen_symbol_list()
        symbol_list.remove('`')
        # !bug and please remove \\ after TD-15208 is fixed
        symbol_list.remove('\\')
        for insert_str in symbol_list:
            d_list = list(stbname)
            for i in range(len(d_list)+1):
                d_list_new = copy.deepcopy(d_list)
                d_list_new.insert(i, insert_str)
                stbname_new = ''.join(d_list_new)
                self.tdSql.execute(f'create stable if not exists `{stbname_new}` (ts timestamp, c1 int) tags (t1 int)')
                self.tdSql.query('show stables')
                self.tdSql.checkEqual(self.tdSql.query_data[0][0], stbname_new)
                self.tdSql.execute(f'drop table if exists `{stbname_new}`')

    def stbname_without_backquote(self):
        """
        error occured when illegal stbname without backquote
        """
        self.tdCom.cleanTb()
        stbname = '1' + self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.error(f'create stable if not exists {stbname} (ts timestamp, c1 int) tags (t1 int)')
        stbname = self.tdCom.get_long_name(length=3, mode="letters")
        symbol_list = self.tdCom.gen_symbol_list()
        symbol_list.remove(' ')
        for insert_str in symbol_list:
            d_list = list(stbname)
            for i in range(len(d_list)+1):
                d_list_new = copy.deepcopy(d_list)
                d_list_new.insert(i, insert_str)
                stbname_new = ''.join(d_list_new)
                self.tdSql.error(f'create stable if not exists {stbname_new} (ts timestamp, c1 int) tags (t1 int)')

    def upper_lower_stbname_check(self):
        """
        without backquote: case insensitive
        with backquote: keep upper or mixed
        """
        for stbname in [self.tdCom.get_long_name(length=10, mode="letters_mixed"), self.tdCom.get_long_name(length=10, mode="letters_mixed").upper()]:
            self.tdSql.execute(f'create stable if not exists {stbname} (ts timestamp, c1 int) tags (t1 int)')
            self.tdSql.query('show stables')
            self.tdSql.checkEqual(self.tdSql.query_data[0][0], stbname.lower())
            self.tdSql.execute(f'drop stable if exists `{stbname.lower()}`')

        for stbname in [self.tdCom.get_long_name(length=10, mode="letters_mixed"), self.tdCom.get_long_name(length=10, mode="letters_mixed").upper()]:
            self.tdSql.execute(f'create stable if not exists `{stbname}` (ts timestamp, c1 int) tags (t1 int)')
            self.tdSql.query('show stables')
            self.tdSql.checkEqual(self.tdSql.query_data[0][0], stbname)
            self.tdSql.execute(f'drop stable if exists `{stbname}`')

    def illegal_stbsql_check(self):
        """
        mixed invalid symbol
        mixed space
        """
        dbname = self.tdCom.get_long_name(length=10, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        stbname = self.tdCom.get_long_name(length=3, mode="letters")
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
                self.tdSql.error(sql_new)
        self.tdSql.execute(f'drop stable if exists `{dbname}`')

    def run(self):
        self.stbname_length_check()
        # self.stb_params_check()
        self.stbname_with_backquote()
        self.stbname_without_backquote()
        self.upper_lower_stbname_check()
        self.illegal_stbsql_check()

    def desc(self) -> str:
        case_description = """
            stbname_length_check <jayden>: [TD-13419] : stb name length check (max 192);\n
            stbname_with_backquote <jayden>: [TD-13419] : backquote supported;\n
            stbname_without_backquote <jayden>: [TD-13419] : error occured when illegal stbname without backquote;\n
            upper_lower_stbname_check <jayden>: [TD-13419] : upper lower stbname check;\n
            illegal_stbsql_check <jayden>: [TD-13419] : illegal stbsql check;
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Stable.Create, T.Write.TaoscSql.Stable.Drop, T.Write.TaoscSql.Stable.Alter
