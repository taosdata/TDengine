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
import random
import os

class CreateTablesByCSV(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.remove_symbol_list = [" ", "+", ";", "&", "*", "%", "/", "-", "|"]
        self.dbname = 'test'
        self.stbname = 'stb'
        self.common_type_list = ['timestamp', 'tinyint', 'smallint', 'int', 'bigint', 'tinyint unsigned', 'smallint unsigned', 'int unsigned', 'bigint unsigned', 'float', 'double', 'varchar', 'varbinary', 'nchar', 'geometry', 'bool']
        # self.common_type_list = ['timestamp', 'tinyint', 'smallint']
        self.str_type_list = ['varchar', 'varbinary', 'nchar', 'geometry']
        self.common_tag_type_str = ",".join(self.common_type_list)
        self.common_tag_name_list = list()
        for _ in range(len(self.common_type_list)):
            self.common_tag_name_list.append(f'{self.tdCom.default_tagname_prefix}{self.tdCom.default_tag_index_start_num}')
            self.tdCom.default_tag_index_start_num += 1
        self.tdCom.default_tag_index_start_num = 1
        self.common_tag_name_str = ",".join(self.common_tag_name_list)
        self.batch_create_table_str = self.common_tag_name_str + ",tbname"
        
        print(self.common_tag_type_str)
        self.tdCom.full_type_list = self.common_type_list
        self.tdCom.default_varchar_length = 64
        self.tdCom.default_nchar_length = 64
        self.csv_file = f'{os.path.dirname(os.path.abspath(__file__))}/{self.stbname}.csv'
        
        
    def create_db(self):
        self.tdCom.createDb(self.dbname)
    
    def create_stb(self):
        tag_type_str = self.tdCom.gen_default_tag_str()
        column_type_str = self.tdCom.gen_default_column_str()
        self.tdSql.execute(f'create stable if not exists {self.dbname}.{self.stbname} ({column_type_str}) tags ({tag_type_str})')
    
    def insert_data(self, tbname):
        self.tdSql.execute(f'insert into {tbname} values ({",".join(self.tdCom.get_col_value_list())})')
    
    def get_tag_value_list(self):
        random_type_value_list = list(map(lambda i: f'"{self.tdCom.gen_random_type_value(i, self.tdCom.default_varchar_length, self.tdCom.default_varchar_datatype, self.tdCom.default_nchar_length, self.tdCom.default_nchar_datatype)}"' if i in self.str_type_list else self.tdCom.gen_random_type_value(i, self.tdCom.default_varchar_length, self.tdCom.default_varchar_datatype, self.tdCom.default_nchar_length, self.tdCom.default_nchar_datatype), self.common_type_list))
        if self.common_type_list[0].lower() == 'timestamp':
            random_type_value_list[0] = f'"{self.tdCom.genTs()[1]}"'
            # random_type_value_list[0] = f'"{random.choice(self.tdCom.genTs())}"'
        return random_type_value_list
    
    def gen_csv(self, ctable_count=10, row_count=10, ctable_field_exists=True):
        with open(self.csv_file, 'w') as f:
            for row_num in range(row_count):
                idx = str(row_num % ctable_count + 1)
                f.write(f'{",".join(map(str, self.get_tag_value_list()))},"ctb{idx}"\n') if ctable_field_exists else f.write(f'ctb{idx}\n')
    
    def create_tables_by_csv(self, if_not_exists=False, tag_fields="", csv="ctbs.csv"):
        if_not_exists_field = "if not exists" if if_not_exists else ""
        self.tdSql.execute(f'create table {if_not_exists_field} using {self.dbname}.{self.stbname} ({tag_fields}) file "{self.csv_file}"')
    
    def init_env(self):
        self.create_db()
        self.create_stb()

    def create_ctables_by_tag_and_tbname(self):
        self.init_env()
        self.gen_csv()
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file)
        self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb1")
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, if_not_exists=True, csv=self.csv_file)
    
    def create_ctables_by_notag_and_tbname(self):
        self.init_env()
        self.gen_csv(ctable_field_exists=False)
        self.create_tables_by_csv(tag_fields="tbname", csv=self.csv_file)
        self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb1")
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, if_not_exists=True, csv=self.csv_file)
    
    
    def run(self):
        # self.create_ctables_by_tag_and_tbname()
        self.create_ctables_by_notag_and_tbname()
    def desc(self) -> str:
        case_description = """
            child_tbname_length_check <jayden>: [TD-12748] : child tb name length check (max 192);\n
            child_tbname_with_backquote <jayden>: [TD-12748] : backquote supported;\n
            child_tbname_without_backquote <jayden>: [TD-12748] : error occured when illegal child tbname without backquote;\n
            upper_lower_child_tbname_check <jayden>: [TD-12748] : upper lower child tbname check;\n
            ttl_check <jayden>: [TD-14993] : ttl check;\n
            comment_check <jayden>: [TD-14993] : comment check;\n
            desc_check <jayden>: [TD-12748] : describe child table;\n
            illegal_child_tbsql_check <jayden>: [TD-12748] : illegal child tbsql check;
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Table.Create, T.Write.TaoscSql.Table.Drop, T.Write.TaoscSql.Table.Alter