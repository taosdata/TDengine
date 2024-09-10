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
import threading
import time
import csv
from shapely.geometry import Point
from shapely import wkb
from shapely.geometry.base import BaseGeometry
import re
import datetime
import json

class CreateTablesByCSV(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.symbol_list = [" ", "+", ";", "&", "*", "%", "/", "-", "|"]
        self.symbol_list = ["&", "*", "%", "/", "-", "|"]
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
        self.tablename_startid = 1
        self.csv_file = f'{os.path.dirname(os.path.abspath(__file__))}/{self.stbname}.csv'
        self.perf_varchar_len = 4
        self.other_files = [f'{os.path.dirname(os.path.abspath(__file__))}/{self.stbname}.txt', f'{os.path.dirname(os.path.abspath(__file__))}/{self.stbname}.xlsx']
        self.custom_tag_offset = 0
        self.perf_type_count = 3
        self.tdCom.Boundary.GEOMETRY_BOUNDARY = self.GEOMETRY_BOUNDARY = ["point(1.0 1.0)"]
        
        
    def create_db(self):
        self.tdCom.createDb(self.dbname, vgroups=10)
    
    def create_stb(self, tag_type_str=None):
        tag_type_str = self.tdCom.gen_default_tag_str() if tag_type_str is None else tag_type_str
        column_type_str = self.tdCom.gen_default_column_str()
        self.tdSql.execute(f'create stable if not exists {self.dbname}.{self.stbname} ({column_type_str}) tags ({tag_type_str})')
    
    def get_tag_value_list(self, exchange_type_list=None, exchange_ids=None, tag_kv_dict=None):
        random_type_value_list = list(map(lambda i: f'"{self.tdCom.gen_random_type_value(i, self.tdCom.default_varchar_length, self.tdCom.default_varchar_datatype, self.tdCom.default_nchar_length, self.tdCom.default_nchar_datatype)}"' if i in self.str_type_list else self.tdCom.gen_random_type_value(i, self.tdCom.default_varchar_length, self.tdCom.default_varchar_datatype, self.tdCom.default_nchar_length, self.tdCom.default_nchar_datatype), self.common_type_list))
        if self.common_type_list[0].lower() == 'timestamp':
            random_type_value_list[0] = f'"{self.tdCom.genTs()[1]}"'
            # random_type_value_list[0] = f'"{random.choice(self.tdCom.genTs())}"'
        if exchange_type_list is not None and exchange_ids is not None:
            random_type_value_list[exchange_ids[0]], random_type_value_list[exchange_ids[1]] = self.tdCom.Boundary.TINYINT_BOUNDARY[1], self.tdCom.Boundary.BIGINT_BOUNDARY[1]
        if tag_kv_dict is not None:
            for key, value in tag_kv_dict.items():
                idx = self.common_type_list.index(key)
                random_type_value_list[idx] = value
        return random_type_value_list
    
    def replace_quotes_in_csv(self, input_filename, output_filename):
        with open(input_filename, 'r', encoding='utf-8') as file:
            content = file.read()
        modified_content = content.replace('"', "'")
        with open(output_filename, 'w', encoding='utf-8') as file:
            file.write(modified_content)
    
    def gen_csv(self, ctable_count=10, row_count=10, ctable_field_exists=True, tbname_exists=True, custom_tag_count=0, note=False, exchange_type_list=None, exchange_ids=None, part_error=False, symbol=None, illegal_tbname=False, tag_kv_dict=None, len193_tbname=False, custom_file=None, exceed_tags=False, custom_type=None):
        import_file = custom_file if custom_file is not None else self.csv_file
        if custom_type == "json":
            with open(import_file, 'w') as f:
                for idx in range(ctable_count):
                    f.write((f'\'{{\"loc+\":\"fff\",\"id\":{idx}}}\', "ctb{self.tablename_startid}"\n'))
                    # f.write(f'{",".join(map(str, self.get_tag_value_list(exchange_type_list=exchange_type_list, exchange_ids=exchange_ids, tag_kv_dict=tag_kv_dict)))},"ctb{self.tablename_startid}"\n') if ctable_field_exists else f.write(f'"ctb{self.tablename_startid}"\n')
                    self.tablename_startid += 1
        else:
            if custom_tag_count == 128 or custom_tag_count > 128:
                tag_str_exceed = self.tdCom.gen_tag_col_str("t", "int", self.tdCom.Boundary.MAX_TAG_COUNT+1)
                tag_str = self.tdCom.gen_tag_col_str("t", "int", self.tdCom.Boundary.MAX_TAG_COUNT)
                with open(import_file, 'w') as f:
                    for row_num in range(row_count):
                        idx = str(row_num % ctable_count + 1)
                        tag_fields_value = "1," * custom_tag_count
                        f.write(f'{tag_fields_value}"ctb{idx}"\n')
                return tag_str_exceed, tag_str
            elif custom_tag_count > 0 and custom_tag_count < 128:
                # custom_tag_offset = custom_tag_count
                int_tag_str = ','.join(map(lambda i: f't{i} int', range(custom_tag_count)))
                double_tag_str = ','.join(map(lambda i: f't{i} double', range(custom_tag_count, custom_tag_count*2)))
                varchar_tag_str = ','.join(map(lambda i: f't{i} varchar({self.perf_varchar_len})', range(custom_tag_count*2, custom_tag_count*3)))
                tag_str = int_tag_str + "," + double_tag_str + "," + varchar_tag_str
                with open(import_file, 'w') as f:
                    for idx in range(ctable_count):
                        int_tag_fields_value = f"{idx}," * custom_tag_count
                        float_tag_fields_value = f"{idx}.{idx}," * custom_tag_count
                        varchar_tag_fields_value = f'"{self.tdCom.get_long_name(self.perf_varchar_len)}",' * custom_tag_count
                        tag_fields_value = int_tag_fields_value + float_tag_fields_value + varchar_tag_fields_value
                        f.write(f'{tag_fields_value}"ctb{idx}"\n')
                return tag_str
            else:
                with open(import_file, 'w') as f:
                    # for row_num in range(row_count):
                    #     idx = str(row_num % ctable_count + 1)
                    for idx in range(ctable_count):
                        # if tbname_exists:
                        # f.write(f'{",".join(map(str, self.get_tag_value_list(exchange_type_list=exchange_type_list, exchange_ids=exchange_ids)))},"ctb{idx}"\n') if ctable_field_exists else f.write(f'"ctb{idx}"\n')
                        # else:
                        #     f.write(f'{",".join(map(str, self.get_tag_value_list(exchange_type_list=exchange_type_list, exchange_ids=exchange_ids)))}\n') if ctable_field_exists else f.write(f'\n')
                        if symbol is not None:
                            f.write(f'{symbol.join(map(str, self.get_tag_value_list(exchange_type_list=exchange_type_list, exchange_ids=exchange_ids, tag_kv_dict=tag_kv_dict)))},"ctb{self.tablename_startid}"\n') if ctable_field_exists else f.write(f'"ctb{self.tablename_startid}"\n')
                        else:
                            f.write(f'{",".join(map(str, self.get_tag_value_list(exchange_type_list=exchange_type_list, exchange_ids=exchange_ids, tag_kv_dict=tag_kv_dict)))},"ctb{self.tablename_startid}"\n') if ctable_field_exists else f.write(f'"ctb{self.tablename_startid}"\n')
                        self.tablename_startid += 1
                    
                    if note:
                        f.write(f'#{",".join(map(str, self.get_tag_value_list(exchange_type_list=exchange_type_list, exchange_ids=exchange_ids, tag_kv_dict=tag_kv_dict)))},"ctb{self.tablename_startid}"\n') if ctable_field_exists else f.write(f'#"ctb{self.tablename_startid}"\n')
                    if part_error:
                        f.write(f'{",".join(map(str, self.get_tag_value_list(exchange_type_list=exchange_type_list, exchange_ids=exchange_ids, tag_kv_dict=tag_kv_dict)))},t100,"ctb{self.tablename_startid}"\n') if ctable_field_exists else f.write(f'#"ctb{self.tablename_startid}"\n')
                        f.write(f'{",".join(map(str, self.get_tag_value_list(exchange_type_list=exchange_type_list, exchange_ids=exchange_ids, tag_kv_dict=tag_kv_dict)))},"ctb{self.tablename_startid}"\n') if ctable_field_exists else f.write(f'#"ctb{self.tablename_startid}"\n')
                    if illegal_tbname:
                        f.write(f'{",".join(map(str, self.get_tag_value_list(exchange_type_list=exchange_type_list, exchange_ids=exchange_ids, tag_kv_dict=tag_kv_dict)))},"ctb.{self.tablename_startid}"\n') if ctable_field_exists else f.write(f'#"ctb{self.tablename_startid}"\n')
                    if len193_tbname:
                        f.write(f'{",".join(map(str, self.get_tag_value_list(exchange_type_list=exchange_type_list, exchange_ids=exchange_ids, tag_kv_dict=tag_kv_dict)))},"{self.tdCom.get_long_name(193)}"\n') if ctable_field_exists else f.write(f'#"ctb{self.tablename_startid}"\n')
                    if exceed_tags:
                        f.write(f'{",".join(map(str, self.get_tag_value_list(exchange_type_list=exchange_type_list, exchange_ids=exchange_ids, tag_kv_dict=tag_kv_dict)))},"ctb{self.tablename_startid}","exceed_tags"\n') if ctable_field_exists else f.write(f'#"ctb{self.tablename_startid}"\n')
                    
    def change_csv_col_order(self, csv_file, idx1, idx2):
        with open(csv_file, 'r') as infile:
            reader = csv.reader(infile)
            data = [row for row in reader]

        for row in data:
            row[idx1], row[idx2] = row[idx2], row[idx1]

        with open(csv_file, 'w', newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerows(data)
    
    def create_tables_by_csv(self, if_not_exists=False, tag_fields="", csv="stb.csv", use_except=False):
        if_not_exists_field = "if not exists" if if_not_exists else ""
        if use_except:
            self.tdSql.error(f'create table {if_not_exists_field} using {self.dbname}.{self.stbname} ({tag_fields}) file "{csv}"')
        else:
            self.tdSql.execute(f'create table {if_not_exists_field} using {self.dbname}.{self.stbname} ({tag_fields}) file "{csv}"')
    
    def init_env(self, tag_type_str=None, create_stb=True, custom_type=None):
        self.tdCom.default_tag_index_start_num = 1
        self.tablename_startid = 1
        self.create_db()
        if create_stb:
            if custom_type is None:
                self.create_stb(tag_type_str=tag_type_str)
            elif custom_type == "json":
                self.tdSql.execute(f'create stable if not exists {self.dbname}.{self.stbname} (ts timestamp, c1 int) tags (t1 json)')
            elif custom_type == "varchar":
                self.tdSql.execute(f'create stable if not exists {self.dbname}.{self.stbname} (ts timestamp, c1 int) tags (t1 varchar(16382))')

    def check_res(self, tbname, tag_str, csv_file, check_rows=2):
        with open(csv_file, mode='r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            data_list = [row for row in csv_reader if not row[0].startswith('#')]
            data_list = [[item.strip("'") for item in row if item != "exceed_tags"] for row in data_list]
        print("------", data_list)
        csv_check_list = list()
        for i in range(check_rows):
            # if i < 2:
            # csv_check_list.append(data_list[i][:-1])
            csv_check_list.append(data_list[i][::])
        self.tdSql.query(f'select {tag_str} from {self.dbname}.{tbname} order by tbname')
        query_data = self.tdSql.query_data
        
        new_data_list = list()
        for res in query_data:
            tmp_data_list = list()
            for row in res:
                if isinstance(row, datetime.datetime):
                    item_str = row.strftime('%Y-%m-%d %H:%M:%S.%f')
                elif isinstance(row, BaseGeometry):
                    item_str = wkb.dumps(row)
                elif isinstance(row, bytes):
                    if res.index(row) == self.common_type_list.index('geometry'):
                        point = wkb.loads(row)
                        item_str = "point({:.1f} {:.1f})".format(point.x, point.y)

                    else:
                        item_str = row.decode('utf-8')
                else:
                    item_str = str(row)
                tmp_data_list.append(str(item_str))
            new_data_list.append(tmp_data_list)
        sorted_new_data_list = sorted(new_data_list, key=lambda x: (x[1], x[2]))
        sorted_csv_check_list = sorted(csv_check_list, key=lambda x: (x[1], x[2]))
        float_index = self.common_type_list.index('float')
        for data_list, checklist in zip(sorted_new_data_list,sorted_csv_check_list):
            if 0.9 < float(data_list[float_index])/float(checklist[float_index]) < 1.1:
                data_list[float_index] = str(checklist[float_index])
        print("-----sorted_new_data_list", sorted_new_data_list)
        print("-----sorted_csv_check_list", sorted_csv_check_list)
        self.tdSql.checkEqual(sorted_new_data_list, sorted_csv_check_list)

    def create_ctables_by_tag_and_tbname(self, table_count=10):
        self.init_env()
        self.gen_csv(ctable_count=table_count, row_count=table_count)
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file)
        self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb1")
        self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb2")
        
        self.check_res(self.stbname, self.batch_create_table_str, self.csv_file, 2)
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, if_not_exists=True, csv=self.csv_file)
        self.check_res(self.stbname, self.batch_create_table_str, self.csv_file, 2)
        
    def create_ctables_by_notag_and_tbname(self):
        self.init_env()
        self.gen_csv(ctable_field_exists=False)
        self.create_tables_by_csv(tag_fields="tbname", csv=self.csv_file)
        self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb1")
        self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb2")
        self.tdSql.query(f'select {self.common_tag_name_str},tbname from {self.dbname}.{self.stbname} order by tbname')
        expected_res = [(None,) * len(self.common_type_list) + (f'ctb{i}',) for i in range(1, 3)]
        self.tdSql.checkEqual(self.tdSql.query_data, expected_res)
        self.create_tables_by_csv(tag_fields="tbname", if_not_exists=True, csv=self.csv_file)
        self.tdSql.query(f'select {self.common_tag_name_str},tbname from {self.dbname}.{self.stbname} order by tbname')
        self.tdSql.checkEqual(self.tdSql.query_data, expected_res)
        
    def create_ctables_by_128tag_and_tbname(self, custom_tag_count=128, use_except=False):
        _, tag_str = self.gen_csv(custom_tag_count=custom_tag_count)
        self.init_env(tag_type_str=tag_str)
        tag128_str = ",".join([f't{i}' for i in range(custom_tag_count)]) + ",tbname"
        self.create_tables_by_csv(tag_fields=tag128_str, csv=self.csv_file)
        self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb1")
        self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb2")
        expected_res = [(1,) * 128 + (f'ctb{i}',) for i in range(1, 3)]
        self.tdSql.query(f'select {tag128_str} from {self.dbname}.{self.stbname} order by tbname')
        self.tdSql.checkEqual(self.tdSql.query_data, expected_res)
        self.create_tables_by_csv(tag_fields=tag128_str, if_not_exists=True, csv=self.csv_file)
        _, tag_str = self.gen_csv(custom_tag_count=custom_tag_count+1)
        tag129_str = ",".join([f't{i}' for i in range(custom_tag_count+1)]) + ",tbname"
        self.create_tables_by_csv(tag_fields=tag129_str, csv=self.csv_file, use_except=use_except)
        self.tdSql.checkIn("Invalid tag name: t128", str(self.tdSql.error_msg))
        self.tdSql.query(f'select {tag128_str} from {self.dbname}.{self.stbname} order by tbname')
        self.tdSql.checkEqual(self.tdSql.query_data, expected_res)
    
    def create_ctables_by_tag_and_tbname_with_note(self, note=True):
        self.init_env()
        self.gen_csv(note=note)
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file)
        self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb1")
        self.check_res(self.stbname, self.batch_create_table_str, self.csv_file, 1)
        self.tdSql.query(f'show {self.dbname}.tables')
        ctables = list(map(lambda x: x[0], self.tdSql.query_data))
        self.tdSql.checkNotIn("ctb11", ctables)
        
    def create_exists_ctables_without_if_not_exists(self):
        self.init_env()
        self.gen_csv()
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file, if_not_exists=False)
        self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb1")
        self.check_res(self.stbname, self.batch_create_table_str, self.csv_file, 1)
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file, if_not_exists=False, use_except=True)
        self.check_res(self.stbname, self.batch_create_table_str, self.csv_file, 1)
        self.tdSql.checkIn("Table already exists", str(self.tdSql.error_msg))
    
    def create_ctables_with_disorder_tagtype_legal(self):
        varchar_idx = self.common_type_list.index('varchar')
        nchar_idx = self.common_type_list.index('nchar')
        disorder_tagtype_str = self.change_char_order(self.batch_create_table_str, varchar_idx, nchar_idx)
        self.init_env()
        self.gen_csv()
        self.create_tables_by_csv(tag_fields=f'{disorder_tagtype_str}', csv=self.csv_file)
        self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb1")
        self.check_res(self.stbname, disorder_tagtype_str, self.csv_file, 1)
    
    def create_ctables_with_disorder_tagtype_illegal(self, exchange_type_list=["tinyint", "bigint"]):
        exchange_idx1 = self.common_type_list.index(exchange_type_list[0])
        exchange_idx2 = self.common_type_list.index(exchange_type_list[1])
        exchange_ids = [exchange_idx1, exchange_idx2]
        disorder_tagtype_str = self.change_char_order(self.batch_create_table_str, exchange_idx1, exchange_idx2)
        self.init_env()
        self.gen_csv(exchange_type_list=exchange_type_list, exchange_ids=exchange_ids)
        self.create_tables_by_csv(tag_fields=disorder_tagtype_str, csv=self.csv_file, use_except=True)
        self.tdSql.checkIn("tinyint data overflow", str(self.tdSql.error_msg))
        self.tdSql.query(f'show {self.dbname}.tables')
        self.tdSql.checkEqual(len(self.tdSql.query_data), 0)
    
    def change_char_order(self, tag_str, idx1, idx2):
        tag_list = copy.deepcopy(tag_str).split(',')
        tag_list[idx1], tag_list[idx2] = tag_list[idx2], tag_list[idx1]
        return ",".join(tag_list)
    
    def create_ctables_with_no_stables(self):
        self.init_env(create_stb=False)
        self.gen_csv()
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file, use_except=True)
        self.tdSql.checkIn("Table does not exist", str(self.tdSql.error_msg))
        self.tdSql.query(f'show {self.dbname}.tables')
        self.tdSql.checkEqual(len(self.tdSql.query_data), 0)
    
    def create_ctables_with_dup_tagname(self):
        self.init_env()
        self.gen_csv()
        tmp_tag_str = copy.deepcopy(self.batch_create_table_str)
        dup_tag_str = tmp_tag_str.replace("t1", "t2")
        self.create_tables_by_csv(tag_fields=dup_tag_str, csv=self.csv_file, use_except=True)
        self.tdSql.checkIn("Tag name:t2 duplicated", str(self.tdSql.error_msg))
        self.tdSql.query(f'show {self.dbname}.tables')
        self.tdSql.checkEqual(len(self.tdSql.query_data), 0)
    
    def create_ctables_by_tag_and_notbname(self):
        self.init_env()
        self.gen_csv()
        tmp_tag_str = copy.deepcopy(self.batch_create_table_str)
        notbname_str = tmp_tag_str.replace(",tbname", "")
        self.create_tables_by_csv(tag_fields=notbname_str, csv=self.csv_file, use_except=True)
        self.tdSql.checkIn("Pseudo tag tbname not set", str(self.tdSql.error_msg))
        self.tdSql.query(f'show {self.dbname}.tables')
        self.tdSql.checkEqual(len(self.tdSql.query_data), 0)
        
    def create_ctables_by_no_contained_tag(self):
        self.init_env()
        self.gen_csv()
        tmp_tag_str = copy.deepcopy(self.batch_create_table_str)
        nocontain_tag_str = tmp_tag_str.replace("t10", "t100")
        self.create_tables_by_csv(tag_fields=nocontain_tag_str, csv=self.csv_file, use_except=True)
        self.tdSql.checkIn("Invalid tag name: t100", str(self.tdSql.error_msg))
        self.tdSql.query(f'show {self.dbname}.tables')
        self.tdSql.checkEqual(len(self.tdSql.query_data), 0)
    
    def create_ctables_by_not_existed_csv(self):
        self.init_env()
        self.gen_csv()
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=f'{self.csv_file}_1', use_except=True)
        self.tdSql.checkIn("No such file or directory", str(self.tdSql.error_msg))
        self.tdSql.query(f'show {self.dbname}.tables')
        self.tdSql.checkEqual(len(self.tdSql.query_data), 0)
    
    def create_ctables_with_part_error_rows(self):
        self.init_env()
        self.gen_csv(part_error=True)
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file, use_except=True)
        self.tdSql.checkIn("invalid data or symbol", str(self.tdSql.error_msg))
        self.tdSql.query(f'show {self.dbname}.tables')
        ctables = list(map(lambda x: x[0], self.tdSql.query_data))
        self.tdSql.checkNotIn("ctb11", ctables)
        
    def create_ctables_with_exceed_tags(self):
        self.init_env()
        self.gen_csv(exceed_tags=True)
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file)
        for tbname in [f'ctb{i}' for i in range(1, 12)]:
            self.tdCom.insert_rows(dbname=self.dbname, tbname=tbname)
        self.check_res(self.stbname, self.batch_create_table_str, self.csv_file, 11)
    
    def create_ctables_csv_split_without_comma(self):
        self.init_env()
        for symbol in self.symbol_list:
            error_msg = "syntax error near"
            # TODO confirm
            # if symbol == "-":
            #     error_msg = "invalid timestamp"
            self.gen_csv(symbol=symbol)
            self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file, use_except=True)
            self.tdSql.checkIn(error_msg, str(self.tdSql.error_msg))
            self.tdSql.query(f'show {self.dbname}.tables')
            self.tdSql.checkEqual(len(self.tdSql.query_data), 0)
    
    def create_ctables_by_illegal_tbname(self):
        self.init_env()
        self.gen_csv(illegal_tbname=True)
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file, use_except=True)
        self.tdSql.checkIn("tbname can not contain \'.\'", str(self.tdSql.error_msg))
        self.tdSql.query(f'show {self.dbname}.tables')
        self.tdSql.checkEqual(len(self.tdSql.query_data), 0)
        
    def create_ctables_by_193len_tbname(self):
        self.init_env()
        self.gen_csv(len193_tbname=True)
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file, use_except=True)
        self.tdSql.checkIn("tbname is too long", str(self.tdSql.error_msg))
        self.tdSql.query(f'show {self.dbname}.tables')
        self.tdSql.checkEqual(len(self.tdSql.query_data), 0)
    
    def create_ctables_str_type_check(self):
        for str_type in ["varchar", "nchar", "varbinary"]:
        # for str_type in ["varbinary"]:
            for value in ["'abc'", "'a,bc'", '"a,bc"', '\'\"a\\\'b\\"c\"\'', '"a\\"b\\"c\"']:
                self.tdCom.default_tag_index_start_num = 1
                self.init_env()
                self.gen_csv(tag_kv_dict={str_type: value})
                self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file)
                self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb1")
                self.tdSql.query(f'select t{self.common_type_list.index(str_type)+1} from {self.dbname}.{self.stbname}')
                query_data = self.tdSql.query_data[0][0] if str_type != "varbinary" else self.tdSql.query_data[0][0].decode('utf-8')
                if value in ["'abc'", "'a,bc'", '"a,bc"']:
                    self.tdSql.checkEqual(query_data, value.strip("'").strip('"').replace("\\", ""))
                elif value in ['\'\"a\\\'b\\"c\"\'']:
                    self.tdSql.checkEqual(query_data, value.strip("'").replace("\\", ""))
                else:
                    self.tdSql.checkEqual(query_data, value.strip('"').replace("\\", ""))

    def create_ctables_bool_type_check(self):
        for value in [True, False, 0, 1, "True", "False"]:
            self.tdCom.default_tag_index_start_num = 1
            self.init_env()
            self.gen_csv(tag_kv_dict={"bool": value})
            self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file)
            self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb1")
            self.tdSql.query(f'select t16 from {self.dbname}.{self.stbname}')
            if value == True or value == "True" or value == 1:
                self.tdSql.checkEqual(self.tdSql.query_data[0][0], True)
            else:
                self.tdSql.checkEqual(self.tdSql.query_data[0][0], False)

    def create_ctables_str_cross_border(self):
        for str_type in ["varchar", "nchar", "varbinary"]:
            self.init_env()
            self.gen_csv(tag_kv_dict={str_type: f'"{self.tdCom.get_long_name(self.tdCom.default_varchar_length+1)}"'})
            self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file, use_except=True)
            self.tdSql.checkIn("Value too long for column/tag", str(self.tdSql.error_msg))
            self.tdSql.query(f'show {self.dbname}.tables')
            self.tdSql.checkEqual(len(self.tdSql.query_data), 0)
    
    def create_ctables_numeric_cross_border(self):
        for numeric_type in ['tinyint', 'smallint', 'int', 'bigint', 'tinyint unsigned', 'smallint unsigned', 'int unsigned', 'bigint unsigned']:
            self.init_env()
            self.gen_csv(tag_kv_dict={numeric_type: self.tdCom.Boundary.UBIGINT_BOUNDARY[1]+1})
            self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file, use_except=True)
            match = re.search("invalid.*data", str(self.tdSql.error_msg))
            res = True if match else False
            self.tdSql.checkEqual(res, True)
            self.tdSql.query(f'show {self.dbname}.tables')
            self.tdSql.checkEqual(len(self.tdSql.query_data), 0)
            
    def create_ctables_float_cross_border(self):
        for float_type in ['float', 'double']:
            exceed_value = 1.797693134862316e308 if float_type == "double" else 3.4028234664e38
            self.init_env()
            self.gen_csv(tag_kv_dict={float_type: exceed_value})
            self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file, use_except=True)
            if float_type == "float":
                self.tdSql.checkIn("illegal float data", str(self.tdSql.error_msg))
            else:
                self.tdSql.checkIn("invalid data or symbol", str(self.tdSql.error_msg))
            self.tdSql.query(f'show {self.dbname}.tables')
            self.tdSql.checkEqual(len(self.tdSql.query_data), 0)
    
    def create_ctables_by_txt_or_xlsx(self):
        for import_file in self.other_files:
            self.init_env()
            self.gen_csv(custom_file=import_file)
            self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=import_file)
            self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb1")
            self.create_tables_by_csv(tag_fields=self.batch_create_table_str, if_not_exists=True, csv=import_file)
            self.check_res(self.stbname, self.batch_create_table_str, import_file, 1)
    
    def creating_but_killed(self):
        self.init_env()
        self.gen_csv(ctable_count=500000, row_count=500000)
        self.logger.info(f'generate csv finished, start creating tables by csv...')
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file)
        # self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb1")
        # self.create_tables_by_csv(tag_fields=self.batch_create_table_str, if_not_exists=True, csv=self.csv_file)
    
    def threading_create_ctables(self, part_except=False, dup_tbname=False):
        self.init_env()
        csv_file_list = [f'{os.path.dirname(os.path.abspath(__file__))}/{self.stbname}_{i+1}.csv' for i in range(5)]
        tlist = list()
        for i in range(5):
            if part_except and i == 2:
                self.gen_csv(ctable_count=100, row_count=100, custom_file=csv_file_list[i], part_error=True)
            else:
                if dup_tbname:
                    self.tablename_startid = 1
                self.gen_csv(ctable_count=100, row_count=100, custom_file=csv_file_list[i])
            t = threading.Thread(target=self.create_tables_by_csv, args=(False, self.batch_create_table_str, csv_file_list[i]))
            tlist.append(t)
        self.tdCom.multi_thread_run(tlist)
        # self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file)
        self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb1")
        self.check_res(self.stbname, self.batch_create_table_str, csv_file_list[0], 1)
        # self.create_tables_by_csv(tag_fields=self.batch_create_table_str, if_not_exists=True, csv=self.csv_file)    

    def create_ctables_by_tag_and_tbname_perf(self, table_count=10):
        self.init_env()
        self.gen_csv(ctable_count=table_count, row_count=table_count)
        start = time.time()
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file)
        end = time.time()
        perf = int(table_count/(end - start))
        self.logger.info(f'create {table_count} tables by csv cost {end-start:.2f}s, and QPS is {perf}tables/s')
        
    def create_ctables_by_diff_tag_and_tbname_perf(self, table_count=10, custom_tag_count=1):
        tag_str = self.gen_csv(ctable_count=table_count, custom_tag_count=custom_tag_count)
        self.init_env(tag_type_str=tag_str)
        tag_cnt_str = ",".join([f't{i}' for i in range(custom_tag_count*self.perf_type_count)]) + ",tbname"
        start = time.time()
        self.create_tables_by_csv(tag_fields=tag_cnt_str, csv=self.csv_file)
        end = time.time()
        perf = int(table_count/(end - start))
        self.logger.info(f'create {table_count} tables with {custom_tag_count*self.perf_type_count} tags by csv cost {end-start:.2f}s, and QPS is {perf}tables/s')

    def create_ctables_by_exchange_tag_and_tbname(self, table_count=10, idx1=-1, idx2=-2):
        self.init_env()
        self.gen_csv(ctable_count=table_count, row_count=table_count)
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file)
        tmp_tag_str = copy.deepcopy(self.batch_create_table_str)
        tag_list = tmp_tag_str.split(',') 
        tag_list[idx1], tag_list[idx2] = tag_list[idx2], tag_list[idx1]
        tag_str = ",".join(tag_list)
        self.change_csv_col_order(self.csv_file, idx1, idx2)
        self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb1")
        self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb2")
        self.check_res(self.stbname, tag_str, self.csv_file, 2)
        # self.create_tables_by_csv(tag_fields=self.batch_create_table_str, if_not_exists=True, csv=self.csv_file)
        # self.check_res(self.stbname, self.common_tag_name_str, self.csv_file, 2)

    def create_ctables_by_json_tag_and_tbname(self, table_count=10, custom_type="json"):
        self.init_env(custom_type=custom_type)
        self.gen_csv(ctable_count=table_count, row_count=table_count, custom_type=custom_type)
        self.tdSql.execute(f'create table using {self.dbname}.{self.stbname} (t1,tbname) file "{self.csv_file}"')
        for i in range(1, table_count+1):
            self.tdSql.execute(f'insert into {self.dbname}.ctb{i} values (now+{i}s, {i})')
        self.tdSql.query(f'select * from {self.dbname}.{self.stbname} order by ts')
        res = self.tdSql.query_data
        c1_expected_res = [x for x in range(1, table_count+1)]
        t1_expected_list = [{'id':x,'loc+':'fff'} for x in range(0, table_count)]
        t1_expected_res = [json.dumps(item) for item in t1_expected_list]
        t1_expected_res = [json.dumps(json.loads(item), separators=(',', ':')) for item in t1_expected_res]
        self.tdSql.checkEqual(list(map(lambda x:x[1], res)), c1_expected_res)
        t1_query_res = [json.dumps(json.loads(item), separators=(',', ':')) for item in list(map(lambda x:x[2], res))]
        self.tdSql.checkEqual(t1_query_res, t1_expected_res)

    def run(self):
        # self.gen_csv(custom_tag_count=128)
        # print(self.tdCom.gen_default_tag_str())
        # return
        self.create_ctables_by_tag_and_tbname()
        self.create_ctables_by_notag_and_tbname()
        self.create_ctables_by_128tag_and_tbname(use_except=True)
        self.create_ctables_by_tag_and_tbname_with_note()
        # # return
        self.create_exists_ctables_without_if_not_exists()
        self.create_ctables_with_disorder_tagtype_legal()
        self.create_ctables_with_disorder_tagtype_illegal()
        self.create_ctables_with_no_stables()
        # TODO confirm TD-30865
        self.create_ctables_with_dup_tagname()
        self.create_ctables_by_tag_and_notbname()
        self.create_ctables_by_no_contained_tag()
        self.create_ctables_by_not_existed_csv()
        self.create_ctables_with_part_error_rows()
        self.create_ctables_with_exceed_tags()
        self.create_ctables_csv_split_without_comma()
        self.create_ctables_by_illegal_tbname()
        self.create_ctables_by_193len_tbname()
        self.create_ctables_str_type_check()
        self.create_ctables_str_cross_border()
        self.create_ctables_bool_type_check()
        self.create_ctables_numeric_cross_border()
        self.create_ctables_float_cross_border()
        self.create_ctables_by_txt_or_xlsx()
        # #  TODO
        # self.creating_but_killed()
        self.threading_create_ctables()
        self.threading_create_ctables(part_except=True)
        self.threading_create_ctables(dup_tbname=True)
        self.create_ctables_by_exchange_tag_and_tbname(idx1=-1, idx2=-2)
        self.create_ctables_by_exchange_tag_and_tbname(idx1=-1, idx2=0)
        self.create_ctables_by_json_tag_and_tbname()
        # perf test
        
        self.create_ctables_by_diff_tag_and_tbname_perf(table_count=100000, custom_tag_count=1)
        self.create_ctables_by_diff_tag_and_tbname_perf(table_count=100000, custom_tag_count=2)
        self.create_ctables_by_diff_tag_and_tbname_perf(table_count=100000, custom_tag_count=4)
        self.create_ctables_by_diff_tag_and_tbname_perf(table_count=100000, custom_tag_count=8)
        self.create_ctables_by_diff_tag_and_tbname_perf(table_count=100000, custom_tag_count=16)
        self.create_ctables_by_diff_tag_and_tbname_perf(table_count=100000, custom_tag_count=32)
        # ! TD-30856
        # self.create_ctables_by_diff_tag_and_tbname_perf(table_count=1000000, custom_tag_count=32)
        # self.create_ctables_by_diff_tag_and_tbname_perf(table_count=10000000, custom_tag_count=32)
        
        # stability
        self.create_ctables_by_tag_and_tbname_perf(100000000)
        
        
        

    def desc(self) -> str:
        case_description = """
            child_tbname_length_check <jayden>: [TD-12748] : child tb name length check (max 192);\n
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Table.Create