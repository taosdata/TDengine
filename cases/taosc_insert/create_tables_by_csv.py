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
    
    def create_stb(self, tag_type_str=None):
        tag_type_str = self.tdCom.gen_default_tag_str() if tag_type_str is None else tag_type_str
        column_type_str = self.tdCom.gen_default_column_str()
        self.tdSql.execute(f'create stable if not exists {self.dbname}.{self.stbname} ({column_type_str}) tags ({tag_type_str})')
    
    def get_tag_value_list(self):
        random_type_value_list = list(map(lambda i: f'"{self.tdCom.gen_random_type_value(i, self.tdCom.default_varchar_length, self.tdCom.default_varchar_datatype, self.tdCom.default_nchar_length, self.tdCom.default_nchar_datatype)}"' if i in self.str_type_list else self.tdCom.gen_random_type_value(i, self.tdCom.default_varchar_length, self.tdCom.default_varchar_datatype, self.tdCom.default_nchar_length, self.tdCom.default_nchar_datatype), self.common_type_list))
        if self.common_type_list[0].lower() == 'timestamp':
            random_type_value_list[0] = f'"{self.tdCom.genTs()[1]}"'
            # random_type_value_list[0] = f'"{random.choice(self.tdCom.genTs())}"'
        return random_type_value_list
    
    def gen_csv(self, ctable_count=10, row_count=10, ctable_field_exists=True, custom_tag_count=0, note=False):
        if custom_tag_count == 128 or custom_tag_count > 128:
            tag_str_exceed = self.tdCom.gen_tag_col_str("t", "int", self.tdCom.Boundary.MAX_TAG_COUNT+1)
            tag_str = self.tdCom.gen_tag_col_str("t", "int", self.tdCom.Boundary.MAX_TAG_COUNT)
            with open(self.csv_file, 'w') as f:
                for row_num in range(row_count):
                    idx = str(row_num % ctable_count + 1)
                    tag_fields_value = "1," * custom_tag_count
                    print(tag_fields_value)
                    f.write(f'{tag_fields_value}"ctb{idx}"\n')
            return tag_str_exceed, tag_str
        else:
            with open(self.csv_file, 'w') as f:
                for row_num in range(row_count):
                    idx = str(row_num % ctable_count + 1)
                    f.write(f'{",".join(map(str, self.get_tag_value_list()))},"ctb{idx}"\n') if ctable_field_exists else f.write(f'"ctb{idx}"\n')
                if note:
                    f.write(f'#{",".join(map(str, self.get_tag_value_list()))},"ctb{idx}"\n') if ctable_field_exists else f.write(f'#"ctb{idx}"\n')
                    
    
    def create_tables_by_csv(self, if_not_exists=False, tag_fields="", csv="ctbs.csv", use_except=False):
        if_not_exists_field = "if not exists" if if_not_exists else ""
        if use_except:
            self.tdSql.error(f'create table {if_not_exists_field} using {self.dbname}.{self.stbname} ({tag_fields}) file "{self.csv_file}"')
        else:
            self.tdSql.execute(f'create table {if_not_exists_field} using {self.dbname}.{self.stbname} ({tag_fields}) file "{self.csv_file}"')
    
    def init_env(self, tag_type_str=None):
        self.create_db()
        self.create_stb(tag_type_str=tag_type_str)

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
        self.create_tables_by_csv(tag_fields="tbname", if_not_exists=True, csv=self.csv_file)
        
    def create_ctables_by_128tag_and_tbname(self, custom_tag_count=128, use_except=False):
        tag_str_exceed, tag_str = self.gen_csv(custom_tag_count=custom_tag_count)
        self.init_env(tag_type_str=tag_str)
        tag128_str = ",".join([f't{i}' for i in range(custom_tag_count)]) + ",tbname"
        self.create_tables_by_csv(tag_fields=tag128_str, csv=self.csv_file)
        self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb1")
        self.create_tables_by_csv(tag_fields=tag128_str, if_not_exists=True, csv=self.csv_file)
    
        tag_str_exceed, tag_str = self.gen_csv(custom_tag_count=custom_tag_count+1)
        tag129_str = ",".join([f't{i}' for i in range(custom_tag_count+1)]) + ",tbname"
        self.create_tables_by_csv(tag_fields=tag129_str, csv=self.csv_file, use_except=use_except)
    
    def create_ctables_by_tag_and_tbname_with_note(self, note=True):
        self.init_env()
        self.gen_csv(note=note)
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file)
        self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb1")
        
    def create_exists_ctables_without_if_not_exists(self):
        self.init_env()
        self.gen_csv()
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file, if_not_exists=False)
        self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb1")
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file, if_not_exists=False, use_except=True)
    
    def create_ctables_with_disorder_tagtype_legal(self):
        varchar_idx = self.common_type_list.index('varchar')
        nchar_idx = self.common_type_list.index('nchar')
        disorder_tagtype_str = self.change_char_order(self.batch_create_table_str, varchar_idx+1, nchar_idx+1)
        self.init_env()
        self.gen_csv()
        self.create_tables_by_csv(tag_fields=disorder_tagtype_str, csv=self.csv_file)
        self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb1")
    
    def create_ctables_with_disorder_tagtype_illegal(self):
        varchar_idx = self.common_type_list.index('tinyint')
        nchar_idx = self.common_type_list.index('bigint')
        disorder_tagtype_str = self.change_char_order(self.batch_create_table_str, varchar_idx+1, nchar_idx+1)
        self.init_env()
        self.gen_csv()
        self.create_tables_by_csv(tag_fields=disorder_tagtype_str, csv=self.csv_file)
        self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb1")
    
    def change_char_order(self, tag_str, idx1, idx2):
        tag_list = copy.deepcopy(tag_str).split(',')
        tag_list[idx1], tag_list[idx2] = tag_list[idx2], tag_list[idx1]
        return ",".join(tag_list)
    
    def run(self):
        # self.gen_csv(custom_tag_count=128)
        # print(self.tdCom.gen_default_tag_str())
        # return
        # self.create_ctables_by_tag_and_tbname()
        # self.create_ctables_by_notag_and_tbname()
        # self.create_ctables_by_128tag_and_tbname(use_except=True)
        # self.create_ctables_by_tag_and_tbname_with_note()
        # self.create_exists_ctables_without_if_not_exists()
        self.create_ctables_with_disorder_tagtype_legal()
        
    def desc(self) -> str:
        case_description = """
            child_tbname_length_check <jayden>: [TD-12748] : child tb name length check (max 192);\n
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Table.Create