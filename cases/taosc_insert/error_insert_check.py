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

class TestErrorInsert(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)

    def illegal_insertsql_check(self):
        """
        mixed invalid symbol
        """
        dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname)
        stbname = self.tdCom.get_long_name(3)
        tbname = self.tdCom.get_long_name(3)
        self.tdSql.execute(f'create stable if not exists {dbname}.{stbname} (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned, c6 smallint unsigned, \
                c7 int unsigned, c8 bigint unsigned, c9 float, c10 double, c13 bool) tags (tag_ts timestamp, t1 tinyint, t2 smallint, t3 int, \
                t4 bigint, t5 tinyint unsigned, t6 smallint unsigned, t7 int unsigned, t8 bigint unsigned, t9 float, t10 double, t13 bool)')
        self.tdSql.execute(f'create table if not exists {dbname}.tb (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned, c6 smallint unsigned, \
                c7 int unsigned, c8 bigint unsigned, c9 float, c10 double, c13 bool)')
        self.tdSql.execute(f'create table if not exists {dbname}.{tbname} using {dbname}.{stbname} tags (now, 1, 2, 3, 4, 5, 6, 7, 8, 9.9, 10.1, True)')
        symbol_list = self.tdCom.gen_symbol_list()
        symbol_list.remove(' ')
        symbol_list.remove('+')
        symbol_list.remove(';')
        symbol_list.remove('-')
        # ! TD-13248
        symbol_list.remove(',')
        for base_sql in [f'insert into {dbname}.{tbname} values (now, 1, 2, 3, 4, 5, 6, 7, 8, 9.9, 10.1, True)', f'insert into {dbname}.tb values (now, 1, 2, 3, 4, 5, 6, 7, 8, 9.9, 10.1, True)']:
            for insert_str in symbol_list:
                d_list = list(base_sql)
                for i in range(len(d_list)+1):
                    d_list_new = copy.deepcopy(d_list)
                    d_list_new.insert(i, insert_str)
                    sql_new = ''.join(d_list_new)
                    self.tdSql.error(sql_new)
            base_sql = base_sql.replace("values", "")
            self.tdSql.error(base_sql)

        self.tdSql.execute(f'drop database if exists {dbname}')

    def type_mismatch_check(self):
        """
        type mismatch check
        """
        dbname = self.tdCom.get_long_name()
        self.tdCom.createDb(dbname)
        self.tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned, c6 smallint unsigned, \
                c7 int unsigned, c8 bigint unsigned, c9 float, c10 double, c11 binary(16), c12 nchar(16), c13 bool) tags (tag_ts timestamp, t1 tinyint, t2 smallint, t3 int, \
                t4 bigint, t5 tinyint unsigned, t6 smallint unsigned, t7 int unsigned, t8 bigint unsigned, t9 float, t10 double, t11 binary(16), t12 nchar(16), t13 bool)')
        self.tdSql.execute(f'create table if not exists {dbname}.ctb using {dbname}.stb tags (now, 1, 2, 3, 4, 5, 6, 7, 8, 9.9, 10.1, "binary", "nchar", True)')
        base_create_ctb_sql = f'create table if not exists {dbname}.ctb_error using {dbname}.stb tags (now+1s, 11, 22, 33, 44, 55, 66, 77, 88, 9.9, 10.1, "binary", "nchar", True)'
        base_insert_ctb_sql = f'insert into {dbname}.ctb values (now, 11, 22, 33, 44, 55, 66, 77, 88, 9.9, 10.1, "binary", "nchar", True)'
        base_specified_column_insert_ctb_sql = f'insert into {dbname}.ctb (ts, c1, c5) values (now, 11, 55)'
        self.tdSql.execute(f'create table if not exists {dbname}.tb (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned, c6 smallint unsigned, \
                c7 int unsigned, c8 bigint unsigned, c9 float, c10 double, c11 binary(16), c12 nchar(16), c13 bool)')
        base_create_tb_sql = f'create table if not exists {dbname}.tb_error'
        base_insert_tb_sql = f'insert into {dbname}.tb values (now, 11, 22, 33, 44, 55, 66, 77, 88, 9.9, 10.1, "binary", "nchar", True)'
        base_specified_column_insert_tb_sql = f'insert into {dbname}.tb (ts, c1, c5) values (now, 11, 55)'

        symbol_list = self.tdCom.gen_symbol_list()
        symbol_list.append("a")
        for i in [11, 22, 33, 44, 55, 66, 77, 88, 9.9, 10.1, "True"]:
            for replace_str in symbol_list:
                new_sql = base_create_ctb_sql.replace(str(i), replace_str)
                self.tdSql.error(new_sql)
                new_insert_sql = base_insert_ctb_sql.replace(str(i), replace_str)
                self.tdSql.error(new_insert_sql)
                new_sql = base_create_tb_sql.replace(str(i), replace_str)
                self.tdSql.error(new_sql)
                new_insert_sql = base_insert_tb_sql.replace(str(i), replace_str)
                self.tdSql.error(new_insert_sql)
        for i in [11, 55]:
            for replace_str in symbol_list:
                new_specified_column_insert_sql = base_specified_column_insert_ctb_sql.replace(str(i), replace_str)
                self.tdSql.error(new_specified_column_insert_sql)
                new_specified_column_insert_sql = base_specified_column_insert_tb_sql.replace(str(i), replace_str)
                self.tdSql.error(new_specified_column_insert_sql)

    def run(self):
        self.illegal_insertsql_check()
        self.type_mismatch_check()

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            illegal_insertsql_check <jayden>: [TD-13419] : illegal insertsql check;\n
            type_mismatch_check <jayden>: [TD-13419] : type mismatch check;
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Abnormal