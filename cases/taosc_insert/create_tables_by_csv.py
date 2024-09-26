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

# -*- taostest --setup=cluster/compact_test_rep3.yaml --case=taosc_insert/create_tables_by_csv.py --keep -*-

from taostest import TDCase, T
from taostest.util.common import TDCom
import copy
import os
import threading
import time
import csv
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
        """
        Returns a list of random type values based on the provided parameters.

        Args:
            exchange_type_list (list): A list of exchange types.
            exchange_ids (list): A list of exchange IDs.
            tag_kv_dict (dict): A dictionary containing tag key-value pairs.

        Returns:
            list: A list of random type values.

        """
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
        """
        Replaces double quotes with single quotes in a CSV file.

        Args:
            input_filename (str): The path to the input CSV file.
            output_filename (str): The path to the output CSV file.

        Returns:
            None
        """
        with open(input_filename, 'r', encoding='utf-8') as file:
            content = file.read()
        modified_content = content.replace('"', "'")
        with open(output_filename, 'w', encoding='utf-8') as file:
            file.write(modified_content)
    
    def gen_csv(self, ctable_count=10, row_count=10, ctable_field_exists=True, tbname_exists=True, custom_tag_count=0, note=False, exchange_type_list=None, exchange_ids=None, part_error=False, symbol=None, illegal_tbname=False, tag_kv_dict=None, len193_tbname=False, custom_file=None, exceed_tags=False, custom_type=None):
        """
        Generate a CSV file with specified parameters.

        Args:
            ctable_count (int): Number of ctables to generate.
            row_count (int): Number of rows per ctable.
            ctable_field_exists (bool): Whether ctable fields exist.
            tbname_exists (bool): Whether tbname exists.
            custom_tag_count (int): Number of custom tags.
            note (bool): Whether to include a note.
            exchange_type_list (list): List of exchange types.
            exchange_ids (list): List of exchange IDs.
            part_error (bool): Whether to include a partial error.
            symbol (str): Symbol to join tag values.
            illegal_tbname (bool): Whether to include an illegal tbname.
            tag_kv_dict (dict): Dictionary of tag key-value pairs.
            len193_tbname (bool): Whether to include a tbname with length 193.
            custom_file (str): Custom file path.
            exceed_tags (bool): Whether to exceed the maximum number of tags.
            custom_type (str): Custom file type.

        Returns:
            tuple or str: If exceed_tags is True, returns a tuple of tag strings (tag_str_exceed, tag_str).
                          If exceed_tags is False, returns a single tag string (tag_str).
        """
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
        """
        Change the order of columns in a CSV file.

        Args:
            csv_file (str): The path to the CSV file.
            idx1 (int): The index of the first column to swap.
            idx2 (int): The index of the second column to swap.

        Returns:
            None
        """
        with open(csv_file, 'r') as infile:
            reader = csv.reader(infile)
            data = [row for row in reader]

        for row in data:
            row[idx1], row[idx2] = row[idx2], row[idx1]

        with open(csv_file, 'w', newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerows(data)
    
    def create_tables_by_csv(self, if_not_exists=False, tag_fields="", csv="stb.csv", use_except=False):
        """
        Create tables by importing data from a CSV file.

        Args:
            if_not_exists (bool, optional): If True, the table will only be created if it doesn't already exist. Defaults to False.
            tag_fields (str, optional): The fields to be included in the table. Defaults to an empty string.
            csv (str, optional): The path to the CSV file. Defaults to "stb.csv".
            use_except (bool, optional): If True, an error message will be logged instead of executing the SQL statement. Defaults to False.
        """
        if_not_exists_field = "if not exists" if if_not_exists else ""
        if use_except:
            self.tdSql.error(f'create table {if_not_exists_field} using {self.dbname}.{self.stbname} ({tag_fields}) file "{csv}"')
        else:
            self.tdSql.execute(f'create table {if_not_exists_field} using {self.dbname}.{self.stbname} ({tag_fields}) file "{csv}"')
    
    def init_env(self, tag_type_str=None, create_stb=True, custom_type=None):
        """
        Initializes the environment for table creation.

        Args:
            tag_type_str (str, optional): The tag type string. Defaults to None.
            create_stb (bool, optional): Whether to create the stable. Defaults to True.
            custom_type (str, optional): The custom type for the stable. Defaults to None.

        Returns:
            None
        """
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
        """
        Check the results of a query against a CSV file.

        Args:
            tbname (str): The name of the table.
            tag_str (str): The tag string.
            csv_file (str): The path to the CSV file.
            check_rows (int, optional): The number of rows to check. Defaults to 2.

        Returns:
            None
        """
        with open(csv_file, mode='r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            data_list = [row for row in csv_reader if not row[0].startswith('#')]
            data_list = [[item.strip("'") for item in row if item != "exceed_tags"] for row in data_list]
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
        self.tdSql.checkEqual(sorted_new_data_list, sorted_csv_check_list)

    def create_ctables_by_tag_and_tbname(self, table_count=10):
        """
        Creates multiple tables by tag and table name.

        Args:
            table_count (int): The number of tables to create. Defaults to 10.
        """
        self.init_env()
        self.gen_csv(ctable_count=table_count, row_count=table_count)
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file)
        self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb1")
        self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb2")

        self.check_res(self.stbname, self.batch_create_table_str, self.csv_file, 2)
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, if_not_exists=True, csv=self.csv_file)
        self.check_res(self.stbname, self.batch_create_table_str, self.csv_file, 2)
        
    def create_ctables_by_notag_and_tbname(self):
         """
         This method creates tables in a database using a CSV file and inserts rows into the tables.
         It performs the following steps:
         1. Initializes the environment.
         2. Generates a CSV file with table field information.
         3. Creates tables in the database using the CSV file, with tag fields specified as 'tbname'.
         4. Inserts rows into the tables 'ctb1' and 'ctb2' in the specified database.
         5. Executes a SQL query to retrieve the tag fields and table names from the created tables.
         6. Compares the query result with the expected result.
         7. Creates tables in the database using the CSV file again, with tag fields specified as 'tbname',
             and with the 'if_not_exists' flag set to True.
         8. Executes a SQL query again to retrieve the tag fields and table names from the created tables.
         9. Compares the query result with the expected result.

         Note: The expected result is a list of tuples, where each tuple represents a row of data.
         """
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
        """
        Creates tables with 128 tags and a table name using CSV data.

        Args:
            custom_tag_count (int, optional): The number of custom tags to generate. Defaults to 128.
            use_except (bool, optional): Whether to use exception handling. Defaults to False.
        """
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
        """
        Creates tables by tag and table name using CSV file.

        Args:
            note (bool, optional): Whether to include a note. Defaults to True.
        """
        self.init_env()
        self.gen_csv(note=note)
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file)
        self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb1")
        self.check_res(self.stbname, self.batch_create_table_str, self.csv_file, 1)
        self.tdSql.query(f'show {self.dbname}.tables')
        ctables = list(map(lambda x: x[0], self.tdSql.query_data))
        self.tdSql.checkNotIn("ctb11", ctables)
        
    def create_exists_ctables_without_if_not_exists(self):
        """
        Creates tables without using the 'IF NOT EXISTS' clause and checks if the tables already exist.
        
        This method initializes the environment, generates a CSV file, creates tables using the CSV file,
        inserts rows into the 'ctb1' table, and checks the results. It then creates tables using the CSV file
        again, but this time with the 'use_except' parameter set to True. Finally, it checks if the error message
        contains the string 'Table already exists'.
        """
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
        """
        Creates tables with disorder tag types and illegal values.

        Args:
            exchange_type_list (list, optional): List of exchange types. Defaults to ["tinyint", "bigint"].

        Returns:
            None
        """
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
        """
        Change the order of characters in a string based on the given indices.

        Args:
            tag_str (str): The input string.
            idx1 (int): The index of the first character to swap.
            idx2 (int): The index of the second character to swap.

        Returns:
            str: The modified string with the characters swapped.

        Example:
            >>> change_char_order("abcde", 1, 3)
            'adcbe'
        """
        tag_list = copy.deepcopy(tag_str).split(',')
        tag_list[idx1], tag_list[idx2] = tag_list[idx2], tag_list[idx1]
        return ",".join(tag_list)
    
    def create_ctables_with_no_stables(self):
        """
        Creates tables using CSV file without any stable tables.

        This method initializes the environment, generates a CSV file, and creates tables using the CSV file.
        It checks if the table does not exist and verifies that the number of tables is 0.

        Args:
            None

        Returns:
            None
        """
        self.init_env(create_stb=False)
        self.gen_csv()
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file, use_except=True)
        self.tdSql.checkIn("Table does not exist", str(self.tdSql.error_msg))
        self.tdSql.query(f'show {self.dbname}.tables')
        self.tdSql.checkEqual(len(self.tdSql.query_data), 0)
    
    def create_ctables_with_dup_tagname(self):
        """
        This method creates tables with duplicate tag names.
        It initializes the environment, generates a CSV file, and creates tables using the CSV file.
        It checks for the presence of an error message indicating that the tag name is duplicated.
        It also checks that no tables are created after the operation.
        """
        self.init_env()
        self.gen_csv()
        tmp_tag_str = copy.deepcopy(self.batch_create_table_str)
        dup_tag_str = tmp_tag_str.replace("t1", "t2")
        self.create_tables_by_csv(tag_fields=dup_tag_str, csv=self.csv_file, use_except=True)
        self.tdSql.checkIn("Tag name:t2 duplicated", str(self.tdSql.error_msg))
        self.tdSql.query(f'show {self.dbname}.tables')
        self.tdSql.checkEqual(len(self.tdSql.query_data), 0)
    
    def create_ctables_by_tag_and_notbname(self):
        """
        Creates tables by tag and not by bname.

        This method initializes the environment, generates a CSV file, and creates tables based on the tag fields
        excluding the tbname field. It then checks for any error messages and verifies that no tables are created.

        Args:
            self: The object instance.

        Returns:
            None
        """
        self.init_env()
        self.gen_csv()
        tmp_tag_str = copy.deepcopy(self.batch_create_table_str)
        notbname_str = tmp_tag_str.replace(",tbname", "")
        self.create_tables_by_csv(tag_fields=notbname_str, csv=self.csv_file, use_except=True)
        self.tdSql.checkIn("Pseudo tag tbname not set", str(self.tdSql.error_msg))
        self.tdSql.query(f'show {self.dbname}.tables')
        self.tdSql.checkEqual(len(self.tdSql.query_data), 0)
        
    def create_ctables_by_no_contained_tag(self):
        """
        Creates tables by CSV file, replacing the tag name 't10' with 't100' and performing validation checks.

        Args:
            None

        Returns:
            None
        """
        self.init_env()
        self.gen_csv()
        tmp_tag_str = copy.deepcopy(self.batch_create_table_str)
        nocontain_tag_str = tmp_tag_str.replace("t10", "t100")
        self.create_tables_by_csv(tag_fields=nocontain_tag_str, csv=self.csv_file, use_except=True)
        self.tdSql.checkIn("Invalid tag name: t100", str(self.tdSql.error_msg))
        self.tdSql.query(f'show {self.dbname}.tables')
        self.tdSql.checkEqual(len(self.tdSql.query_data), 0)
    
    def create_ctables_by_not_existed_csv(self):
        """
        This method creates tables by using a CSV file that does not exist.
        It initializes the environment, generates a CSV file, and attempts to create tables using the non-existent CSV file.
        It then checks for an error message indicating that the file does not exist, and verifies that no tables were created.
        """
        self.init_env()
        self.gen_csv()
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=f'{self.csv_file}_1', use_except=True)
        self.tdSql.checkIn("No such file or directory", str(self.tdSql.error_msg))
        self.tdSql.query(f'show {self.dbname}.tables')
        self.tdSql.checkEqual(len(self.tdSql.query_data), 0)
    
    def create_ctables_with_part_error_rows(self):
        """
        Creates tables with partially erroneous rows.

        This method initializes the environment, generates a CSV file with partially erroneous rows,
        and creates tables using the CSV file. It then checks for any error messages and queries the
        tables in the database. Finally, it checks that the table 'ctb11' is not present in the list
        of tables.

        Args:
            self: The object instance.

        Returns:
            None
        """
        self.init_env()
        self.gen_csv(part_error=True)
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file, use_except=True)
        self.tdSql.checkIn("invalid data or symbol", str(self.tdSql.error_msg))
        self.tdSql.query(f'show {self.dbname}.tables')
        ctables = list(map(lambda x: x[0], self.tdSql.query_data))
        self.tdSql.checkNotIn("ctb11", ctables)
        
    def create_ctables_with_exceed_tags(self):
        """
        Creates multiple tables with exceeded tags.

        This method initializes the environment, generates a CSV file with exceeded tags,
        creates tables based on the CSV file, inserts rows into each table, and checks the results.

        Args:
            None

        Returns:
            None
        """
        self.init_env()
        self.gen_csv(exceed_tags=True)
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file)
        for tbname in [f'ctb{i}' for i in range(1, 12)]:
            self.tdCom.insert_rows(dbname=self.dbname, tbname=tbname)
        self.check_res(self.stbname, self.batch_create_table_str, self.csv_file, 11)
    
    def create_ctables_csv_split_without_comma(self):
        """
        This method is used to create tables by CSV file without splitting on commas.
        It initializes the environment, generates a CSV file for each symbol in the symbol_list,
        creates tables using the generated CSV file, checks for any error messages,
        queries the tables in the database, and checks that the number of tables is 0.
        """
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
        """
        This method creates tables using an illegal table name.
        It initializes the environment, generates a CSV file with an illegal table name,
        creates tables using the CSV file, checks for an error message indicating that the table name is illegal,
        and verifies that no tables are created in the database.

        Args:
            self: The object instance.

        Returns:
            None
        """
        self.init_env()
        self.gen_csv(illegal_tbname=True)
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file, use_except=True)
        self.tdSql.checkIn("tbname can not contain \'.\'", str(self.tdSql.error_msg))
        self.tdSql.query(f'show {self.dbname}.tables')
        self.tdSql.checkEqual(len(self.tdSql.query_data), 0)
        
    def create_ctables_by_193len_tbname(self):
        """
        Creates tables using a CSV file with a table name of length 193 characters.
        
        This method initializes the environment, generates a CSV file with a table name of length 193 characters,
        creates tables using the CSV file, checks for any error messages related to the table name being too long,
        queries the tables in the database, and checks that no tables are created.
        """
        self.init_env()
        self.gen_csv(len193_tbname=True)
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file, use_except=True)
        self.tdSql.checkIn("tbname is too long", str(self.tdSql.error_msg))
        self.tdSql.query(f'show {self.dbname}.tables')
        self.tdSql.checkEqual(len(self.tdSql.query_data), 0)
    
    def create_ctables_str_type_check(self):
        """
        This method is used to create tables with string data types and perform data insertion and verification.

        It iterates over different string types (varchar, nchar, varbinary) and values to create tables, generate CSV files,
        create tables using the CSV files, insert rows into the tables, and perform data verification.

        Args:
            None

        Returns:
            None
        """
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
        """
        This method performs a boolean type check for creating tables using CSV files.
        It iterates through a list of values including True, False, 0, 1, "True", and "False".
        For each value, it sets the default tag index start number, initializes the environment,
        generates a CSV file with the specified tag key-value dictionary, creates tables using the CSV file,
        inserts rows into the specified database and table, and performs a query to check the value of t16 column.
        If the value is True, "True", or 1, it checks if the query result is True. Otherwise, it checks if the result is False.
        """
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
        """
        This method creates tables with string data types using CSV files.
        It iterates over different string types such as varchar, nchar, and varbinary.
        For each string type, it initializes the environment, generates a CSV file with a specific string length,
        creates tables using the generated CSV file, and checks for any errors or exceptions.
        Finally, it queries the tables and checks if the number of tables is zero.
        """
        for str_type in ["varchar", "nchar", "varbinary"]:
            self.init_env()
            self.gen_csv(tag_kv_dict={str_type: f'"{self.tdCom.get_long_name(self.tdCom.default_varchar_length+1)}"'})
            self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file, use_except=True)
            self.tdSql.checkIn("Value too long for column/tag", str(self.tdSql.error_msg))
            self.tdSql.query(f'show {self.dbname}.tables')
            self.tdSql.checkEqual(len(self.tdSql.query_data), 0)
    
    def create_ctables_numeric_cross_border(self):
        """
        This method creates tables with numeric data types that cross the border of their maximum value.
        It performs the following steps:
        1. Initializes the environment.
        2. Generates a CSV file with a tag key-value dictionary containing the specified numeric type and a value that exceeds the maximum boundary.
        3. Creates tables using the generated CSV file, with the option to handle exceptions.
        4. Checks if the error message contains the word "invalid" followed by "data".
        5. Verifies that no tables are created in the specified database.
        """
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
        """
        Creates tables with float and double data types, inserting values that exceed the maximum limit.
        Checks for errors and verifies that no tables are created.

        Returns:
            None
        """
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
        """
        Creates tables by importing data from text or Excel files.

        This method iterates over the `other_files` list and performs the following steps for each file:
        1. Initializes the environment.
        2. Generates a CSV file from the input file (`import_file`).
        3. Creates tables in the database by importing data from the CSV file.
        4. Inserts rows into the "ctb1" table in the database.
        5. Creates tables in the database by importing data from the CSV file, if they don't already exist.
        6. Checks the result by comparing the created table name, batch create table string, and the input file.

        Note: The specific details of the `init_env()`, `gen_csv()`, `create_tables_by_csv()`, `tdCom.insert_rows()`,
        and `check_res()` methods are not provided in this code snippet.

        Args:
            self: The instance of the class.

        Returns:
            None
        """
        for import_file in self.other_files:
            self.init_env()
            self.gen_csv(custom_file=import_file)
            self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=import_file)
            self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb1")
            self.create_tables_by_csv(tag_fields=self.batch_create_table_str, if_not_exists=True, csv=import_file)
            self.check_res(self.stbname, self.batch_create_table_str, import_file, 1)
    
    def creating_but_killed(self):
        """
        This method initializes the environment, generates a CSV file, and creates tables using the CSV file.
        It logs the progress and information during the process.
        """
        self.init_env()
        self.gen_csv(ctable_count=500000, row_count=500000)
        self.logger.info(f'generate csv finished, start creating tables by csv...')
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file)
        # self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb1")
        # self.create_tables_by_csv(tag_fields=self.batch_create_table_str, if_not_exists=True, csv=self.csv_file)
    
    def threading_create_ctables(self, part_except=False, dup_tbname=False):
        """
        Threaded method to create tables by CSV files.

        Args:
            part_except (bool): Flag to indicate whether to generate a CSV file with a partition error.
            dup_tbname (bool): Flag to indicate whether to reset the tablename_startid to 1.

        Returns:
            None
        """
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
        self.tdCom.insert_rows(dbname=self.dbname, tbname="ctb1")
        self.check_res(self.stbname, self.batch_create_table_str, csv_file_list[0], 1)

    def create_ctables_by_tag_and_tbname_perf(self, table_count=10):
        """
        Creates multiple tables by using CSV file as input.

        Args:
            table_count (int): The number of tables to create. Default is 10.

        Returns:
            None

        """
        self.init_env()
        self.gen_csv(ctable_count=table_count, row_count=table_count)
        start = time.time()
        self.create_tables_by_csv(tag_fields=self.batch_create_table_str, csv=self.csv_file)
        end = time.time()
        perf = int(table_count/(end - start))
        self.logger.info(f'create {table_count} tables by csv cost {end-start:.2f}s, and QPS is {perf}tables/s')
        
    def create_ctables_by_diff_tag_and_tbname_perf(self, table_count=10, custom_tag_count=1):
        """
        Create tables with different tags and table names using a CSV file.

        Args:
            table_count (int): The number of tables to create. Default is 10.
            custom_tag_count (int): The number of custom tags to generate. Default is 1.

        Returns:
            None

        """
        tag_str = self.gen_csv(ctable_count=table_count, custom_tag_count=custom_tag_count)
        self.init_env(tag_type_str=tag_str)
        tag_cnt_str = ",".join([f't{i}' for i in range(custom_tag_count*self.perf_type_count)]) + ",tbname"
        start = time.time()
        self.create_tables_by_csv(tag_fields=tag_cnt_str, csv=self.csv_file)
        end = time.time()
        perf = int(table_count/(end - start))
        self.logger.info(f'create {table_count} tables with {custom_tag_count*self.perf_type_count} tags by csv cost {end-start:.2f}s, and QPS is {perf} tables/s')

    def create_ctables_by_exchange_tag_and_tbname(self, table_count=10, idx1=-1, idx2=-2):
        """
        Create ctables by exchange tag and table name.

        Args:
            table_count (int): Number of tables to create. Default is 10.
            idx1 (int): Index of the first tag to swap. Default is -1.
            idx2 (int): Index of the second tag to swap. Default is -2.
        """
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
        """
        Creates tables using JSON tag and table name.

        Args:
            table_count (int): The number of tables to create. Default is 10.
            custom_type (str): The custom type to use. Default is "json".

        Returns:
            None
        """
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
        self.tdSql.execute(f'alter local "maxInsertBatchRows 10000"')
        # return
        self.create_ctables_by_tag_and_tbname()
        self.create_ctables_by_notag_and_tbname()
        self.create_ctables_by_128tag_and_tbname(use_except=True)
        self.create_ctables_by_tag_and_tbname_with_note()
        self.create_exists_ctables_without_if_not_exists()
        self.create_ctables_with_disorder_tagtype_legal()
        self.create_ctables_with_disorder_tagtype_illegal()
        self.create_ctables_with_no_stables()
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
        # # #  TODO
        # # self.creating_but_killed()
        self.threading_create_ctables()
        self.threading_create_ctables(part_except=True)
        self.threading_create_ctables(dup_tbname=True)
        self.create_ctables_by_exchange_tag_and_tbname(idx1=-1, idx2=-2)
        self.create_ctables_by_exchange_tag_and_tbname(idx1=-1, idx2=0)
        self.create_ctables_by_json_tag_and_tbname()
        # perf test
        
        self.create_ctables_by_diff_tag_and_tbname_perf(table_count=100000, custom_tag_count=1)
        # self.create_ctables_by_diff_tag_and_tbname_perf(table_count=100000, custom_tag_count=2)
        # self.create_ctables_by_diff_tag_and_tbname_perf(table_count=100000, custom_tag_count=4)
        # self.create_ctables_by_diff_tag_and_tbname_perf(table_count=100000, custom_tag_count=8)
        # self.create_ctables_by_diff_tag_and_tbname_perf(table_count=100000, custom_tag_count=16)
        # self.create_ctables_by_diff_tag_and_tbname_perf(table_count=100000, custom_tag_count=32)
        # self.create_ctables_by_diff_tag_and_tbname_perf(table_count=1000000, custom_tag_count=32)
        # self.create_ctables_by_diff_tag_and_tbname_perf(table_count=10000000, custom_tag_count=32)
        
        # # stability
        # self.create_ctables_by_tag_and_tbname_perf(10000000)
        
        
        

    def desc(self) -> str:
        case_description = """
            csv_create_table <jayden>: [TS-4917];\n
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write.TaoscSql.Table.Create