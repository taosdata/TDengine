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

class TestTb(TDCase):
    def init(self):
        super().init()
        self.tdCom = TDCom(self.tdSql)

    def tbname_length_check(self):
        """
        max length: 192
        """
        tbname = self.tdCom.get_long_name(length=self.tdCom.boundary_config["TBNAME_MAX_LENGTH"], mode="letters")
        self.tdSql.execute(f'create table if not exists {tbname} (ts timestamp, c1 int)')
        self.tdSql.execute(f'create table {tbname} (ts timestamp, c1 int)')
        self.tdSql.query('show tables')
        self.tdSql.checkEqual(self.tdSql.query_data[0][0], tbname)
        dbname_exceed = self.tdCom.get_long_name(length=self.tdCom.boundary_config["TBNAME_MAX_LENGTH"]+1, mode="letters")
        self.tdSql.error(f'create table if not exists {dbname_exceed} (ts timestamp, c1 int)')

    def tbname_with_backquote(self):
        """
        backquote supported
        """
        self.tdCom.cleanTb()
        tbname = '1' + self.tdCom.get_long_name(length=5, mode="letters")
        self.tdSql.execute(f'create table if not exists `{tbname}` (ts timestamp, c1 int)')
        self.tdSql.query('show tables')
        self.tdSql.checkEqual(self.tdSql.query_data[0][0], tbname)
        self.tdSql.execute(f'drop table if exists `{tbname}`')
        tbname = self.tdCom.get_long_name(length=3, mode="letters")
        symbol_list = self.tdCom.gen_symbol_list()
        symbol_list.remove('`')
        for insert_str in symbol_list:
            d_list = list(tbname)
            for i in range(len(d_list)+1):
                d_list_new = copy.deepcopy(d_list)
                d_list_new.insert(i, insert_str)
                tbname_new = ''.join(d_list_new)
                self.tdSql.execute(f'create table if not exists `{tbname_new}` (ts timestamp, c1 int)')
                self.tdSql.query('show tables')
                self.tdSql.checkEqual(self.tdSql.query_data[0][0], tbname_new)
                self.tdSql.execute(f'drop table if exists `{tbname_new}`')

    def tbname_without_backquote(self):
        """
        error occured when illegal tbname without backquote
        """
        self.tdCom.cleanTb()
        tbname = '1' + self.tdCom.get_long_name(length=5, mode="letters")
        self.tdSql.error(f'create table if not exists {tbname} (ts timestamp, c1 int)')
        tbname = self.tdCom.get_long_name(length=3, mode="letters")
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
            self.tdSql.execute(f'create table if not exists {tbname} (ts timestamp, c1 int)')
            self.tdSql.query('show tables')
            self.tdSql.checkEqual(self.tdSql.query_data[0][0], tbname.lower())
            self.tdSql.execute(f'drop table if exists `{tbname.lower()}`')

        for tbname in [self.tdCom.get_long_name(length=5, mode="letters_mixed"), self.tdCom.get_long_name(length=5, mode="letters_mixed").upper()]:
            self.tdSql.execute(f'create table if not exists `{tbname}` (ts timestamp, c1 int)')
            self.tdSql.query('show tables')
            self.tdSql.checkEqual(self.tdSql.query_data[0][0], tbname)
            self.tdSql.execute(f'drop table if exists `{tbname}`')

    def illegal_tbsql_check(self):
        """
        mixed invalid symbol
        """
        dbname = self.tdCom.get_long_name(length=5, mode="letters")
        self.tdSql.execute(f'create database if not exists {dbname}')
        stbname = self.tdCom.get_long_name(length=3, mode="letters")
        tbname = self.tdCom.get_long_name(length=2, mode="letters")
        self.tdSql.execute(f'create stable if not exists {dbname}.{stbname} (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned, c6 smallint unsigned, \
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
        self.tdSql.execute(f'drop stable if exists `{dbname}`')

    def comment_check(self):
        """
        tb comment check
        """
        tbname = self.tdCom.get_long_name(length=10, mode="letters")
        comment = "stb_param_test"
        self.tdSql.execute(f'create table if not exists {tbname} (ts timestamp, c1 int) comment "{comment}"')
        self.tdSql.query('show tables')
        res = self.tdSql.get_db_field_kv(0, "comment")
        self.tdSql.checkEqual(res["table_comment"], comment)

    def ttl_check(self):
        """
        check ttl
        """
        tbname = self.tdCom.get_long_name(length=10, mode="letters")
        test_ttl = 2
        self.tdSql.execute(f'create table if not exists {tbname} (ts timestamp, c1 int) ttl {test_ttl}')
        self.tdSql.query(f'show tables')
        res = self.tdSql.get_db_field_kv(0, tbname)
        self.tdSql.checkEqual(int(res["ttl"]), test_ttl)

    def run(self):
        # self.tbname_length_check()
        # self.tbname_with_backquote()
        # self.tbname_without_backquote()
        # self.upper_lower_tbname_check()
        self.illegal_tbsql_check()
        # self.comment_check()
        # self.ttl_check()

    def desc(self):
        case_description = """
            tbname_length_check <jayden>: [TD-13419] : tbname length check (max 192);\n
            tbname_with_backquote <jayden>: [TD-13419] : backquote supported;\n
            tbname_without_backquote <jayden>: [TD-13419] : error occured when illegal tbname without backquote;\n
            upper_lower_tbname_check <jayden>: [TD-13419] : upper lower tbname check;\n
            illegal_tbsql_check <jayden>: [TD-13419] : illegal tbsql check;
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Table.Create, T.Write.TaoscSql.Table.Drop, T.Write.TaoscSql.Table.Alter