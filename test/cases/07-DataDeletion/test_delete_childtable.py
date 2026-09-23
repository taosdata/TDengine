
###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql, constant, tdDnodes
from new_test_framework.utils.common import tdCom
from new_test_framework.utils.sqlset import TDSetSql
import random
from random import randint
import os
import time

class TestDeleteChildtable:
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        #tdSql.init(conn.cursor(), logSql), True)
        cls.dbname = 'db_test'
        cls.setsql = TDSetSql()
        cls.stbname = 'stb'
        cls.ntbname = 'ntb'
        cls.rowNum = 5
        cls.tbnum = 2
        cls.ts = 1537146000000
        cls.binary_str = 'taosdata'
        cls.nchar_str = '涛思数据'
        cls.str_length = 20
        cls.column_dict = {
            'col1': 'tinyint',
            'col2': 'smallint',
            'col3': 'int',
            'col4': 'bigint',
            'col5': 'tinyint unsigned',
            'col6': 'smallint unsigned',
            'col7': 'int unsigned',
            'col8': 'bigint unsigned',
            'col9': 'float',
            'col10': 'double',
            'col11': 'bool',
            'col12': f'binary({cls.str_length})',
            'col13': f'nchar({cls.str_length})',

        }

        cls.tinyint_val = random.randint(constant.TINYINT_MIN,constant.TINYINT_MAX)
        cls.smallint_val = random.randint(constant.SMALLINT_MIN,constant.SMALLINT_MAX)
        cls.int_val = random.randint(constant.INT_MIN,constant.INT_MAX)
        cls.bigint_val = random.randint(constant.BIGINT_MIN,constant.BIGINT_MAX)
        cls.untingint_val = random.randint(constant.TINYINT_UN_MIN,constant.TINYINT_UN_MAX)
        cls.unsmallint_val = random.randint(constant.SMALLINT_UN_MIN,constant.SMALLINT_UN_MAX)
        cls.unint_val = random.randint(constant.INT_UN_MIN,constant.INT_MAX)
        cls.unbigint_val = random.randint(constant.BIGINT_UN_MIN,constant.BIGINT_UN_MAX)
        cls.float_val = random.uniform(constant.FLOAT_MIN,constant.FLOAT_MAX)
        cls.double_val = random.uniform(constant.DOUBLE_MIN*(1E-300),constant.DOUBLE_MAX*(1E-300))
        cls.bool_val = random.randint(0,100)%2
        cls.binary_val = tdCom.getLongName(random.randint(0,cls.str_length))
        cls.nchar_val = tdCom.getLongName(random.randint(0,cls.str_length))
        cls.base_data = {
            'tinyint':cls.tinyint_val,
            'smallint':cls.smallint_val,
            'int':cls.int_val,
            'bigint':cls.bigint_val,
            'tinyint unsigned':cls.untingint_val,
            'smallint unsigned':cls.unsmallint_val,
            'int unsigned':cls.unint_val,
            'bigint unsigned':cls.unbigint_val,
            'bool':cls.bool_val,
            'float':cls.float_val,
            'double':cls.double_val,
            'binary':cls.binary_val,
            'nchar':cls.nchar_val
                    }
        
    def insert_base_data(self,col_type,tbname,rows,base_data):
        for i in range(rows):
            if col_type.lower() == 'tinyint':
                tdSql.execute(f'insert into {tbname} values({self.ts+i},{base_data["tinyint"]})')
            elif col_type.lower() == 'smallint':
                tdSql.execute(f'insert into {tbname} values({self.ts+i},{base_data["smallint"]})')
            elif col_type.lower() == 'int':
                tdSql.execute(f'insert into {tbname} values({self.ts+i},{base_data["int"]})')
            elif col_type.lower() == 'bigint':
                tdSql.execute(f'insert into {tbname} values({self.ts+i},{base_data["bigint"]})')
            elif col_type.lower() == 'tinyint unsigned':
                tdSql.execute(f'insert into {tbname} values({self.ts+i},{base_data["tinyint unsigned"]})')
            elif col_type.lower() == 'smallint unsigned':
                tdSql.execute(f'insert into {tbname} values({self.ts+i},{base_data["smallint unsigned"]})')
            elif col_type.lower() == 'int unsigned':
                tdSql.execute(f'insert into {tbname} values({self.ts+i},{base_data["int unsigned"]})')
            elif col_type.lower() == 'bigint unsigned':
                tdSql.execute(f'insert into {tbname} values({self.ts+i},{base_data["bigint unsigned"]})')
            elif col_type.lower() == 'bool':
                tdSql.execute(f'insert into {tbname} values({self.ts+i},{base_data["bool"]})')
            elif col_type.lower() == 'float':
                tdSql.execute(f'insert into {tbname} values({self.ts+i},{base_data["float"]})')
            elif col_type.lower() == 'double':
                tdSql.execute(f'insert into {tbname} values({self.ts+i},{base_data["double"]})')
            elif 'binary' in col_type.lower():
                tdSql.execute(f'''insert into {tbname} values({self.ts+i},"{base_data['binary']}")''')
            elif 'nchar' in col_type.lower():
                tdSql.execute(f'''insert into {tbname} values({self.ts+i},"{base_data['nchar']}")''')
    def delete_all_data(self,tbname,col_type,row_num,base_data,dbname,tb_num=1,stbname=''):
        tdSql.query(f'select count(*) from {tbname}')
        tdSql.execute(f'delete from {tbname}')
        tdSql.execute(f'flush database {dbname}')
        tdSql.execute('reset query cache')
        tdSql.query(f'select * from {tbname}')
        tdSql.checkRows(0)
        tdSql.query(f'select count(*) from {stbname}')
        if tb_num <= 1:
            if len(tdSql.queryResult) != 1 and tdSql.queryResult[0][0] != 0:
                tdLog.exit('delete case failure!')
        else:
            tdSql.checkEqual(tdSql.queryResult[0][0],(tb_num-1)*row_num)
        self.insert_base_data(col_type,tbname,row_num,base_data)
        tdSql.execute(f'flush database {dbname}')
        tdSql.execute('reset query cache')
        tdSql.query(f'select * from {tbname}')
        tdSql.checkRows(row_num)
    def delete_one_row(self,tbname,column_type,column_name,base_data,row_num,dbname,tb_num=1):
        tdSql.execute(f'delete from {tbname} where ts={self.ts}')
        tdSql.execute(f'flush database {dbname}')
        tdSql.execute('reset query cache')
        tdSql.query(f'select {column_name} from {tbname}')
        tdSql.checkRows(row_num-1)
        tdSql.query(f'select {column_name} from {tbname} where ts={self.ts}')
        tdSql.checkRows(0)
        if 'binary' in column_type.lower():
            tdSql.execute(f'''insert into {tbname} values({self.ts},"{base_data['binary']}")''')
        elif 'nchar' in column_type.lower():
            tdSql.execute(f'''insert into {tbname} values({self.ts},"{base_data['nchar']}")''')
        else:
            tdSql.execute(f'insert into {tbname} values({self.ts},{base_data[column_type]})')
        tdSql.query(f'select {column_name} from {tbname} where ts={self.ts}')
        if column_type.lower() == 'float' or column_type.lower() == 'double':
            if abs(tdSql.queryResult[0][0] - base_data[column_type]) / base_data[column_type] <= 0.0001:
                tdSql.checkEqual(tdSql.queryResult[0][0],tdSql.queryResult[0][0])
            else:
                tdLog.exit(f'{column_type} data check failure')
        elif 'binary' in column_type.lower():
            tdSql.checkEqual(tdSql.queryResult[0][0],base_data['binary'])
        elif 'nchar' in column_type.lower():
            tdSql.checkEqual(tdSql.queryResult[0][0],base_data['nchar'])
        else:
            tdSql.checkEqual(tdSql.queryResult[0][0],base_data[column_type])
    def delete_rows(self,dbname,tbname,col_name,col_type,base_data,row_num,tb_num=1):
        for i in range(row_num):
            tdSql.execute(f'delete from {tbname} where ts>{self.ts+i}')
            tdSql.execute(f'flush database {dbname}')
            tdSql.execute('reset query cache')
            tdSql.query(f'select {col_name} from {tbname}')
            tdSql.checkRows(i+1)
            self.insert_base_data(col_type,tbname,row_num,base_data)
        for i in range(row_num):
            tdSql.execute(f'delete from {tbname} where ts>={self.ts+i}')
            tdSql.execute(f'flush database {dbname}')
            tdSql.execute('reset query cache')
            tdSql.query(f'select {col_name} from {tbname}')
            tdSql.checkRows(i)
            self.insert_base_data(col_type,tbname,row_num,base_data)
        for i in range(row_num):
            tdSql.execute(f'delete from {tbname} where ts<={self.ts+i}')
            tdSql.execute(f'flush database {dbname}')
            tdSql.execute('reset query cache')
            tdSql.query(f'select {col_name} from {tbname}')
            tdSql.checkRows(row_num-i-1)
            self.insert_base_data(col_type,tbname,row_num,base_data)
        for i in range(row_num):
            tdSql.execute(f'delete from {tbname} where ts<{self.ts+i}')
            tdSql.execute(f'flush database {dbname}')
            tdSql.execute('reset query cache')
            tdSql.query(f'select {col_name} from {tbname}')
            tdSql.checkRows(row_num-i)
            self.insert_base_data(col_type,tbname,row_num,base_data)
        for i in range(row_num):
            tdSql.execute(f'delete from {tbname} where ts between {self.ts} and {self.ts+i}')
            tdSql.execute(f'flush database {dbname}')
            tdSql.execute('reset query cache')
            tdSql.query(f'select {col_name} from {tbname}')
            tdSql.checkRows(row_num - i-1)
            self.insert_base_data(col_type,tbname,row_num,base_data)
            tdSql.execute(f'delete from {tbname} where ts between {self.ts+i+1} and {self.ts}')
            tdSql.query(f'select {col_name} from {tbname}')
            tdSql.checkRows(row_num)
    def column_filter_predicate(self,column_name,column_type,base_data):
        if 'binary' in column_type.lower():
            return f'''{column_name} ="{base_data['binary']}"'''
        elif 'nchar' in column_type.lower():
            return f'''{column_name} ="{base_data['nchar']}"'''
        elif column_type.lower() in ('float','double'):
            # Exact equality on a float/double column is unreliable: the value is round-tripped
            # through decimal text (both when inserted and when embedded in this predicate), and
            # base_data's magnitude can be large enough (see FLOAT_MIN/MAX, DOUBLE_MIN/MAX in
            # setup_class) that the parsed value differs from the original by more than machine
            # epsilon, so `= value` matches nothing. Use a tolerant BETWEEN instead, consistent
            # with the approximate comparison delete_one_row already uses for these types.
            v = base_data[column_type]
            eps = max(abs(v) * 1e-6, 1e-6)
            lo, hi = v - eps, v + eps
            return f'{column_name} BETWEEN {lo} AND {hi}'
        else:
            return f'{column_name} = {base_data[column_type]}'

    def non_matching_value(self,column_type,base_data):
        # A value guaranteed to differ from base_data[column_type], used to plant a probe row that
        # a column-filter predicate built from base_data must never match.
        ct = column_type.lower()
        bounds = {
            'tinyint': (constant.TINYINT_MIN, constant.TINYINT_MAX),
            'smallint': (constant.SMALLINT_MIN, constant.SMALLINT_MAX),
            'int': (constant.INT_MIN, constant.INT_MAX),
            'bigint': (constant.BIGINT_MIN, constant.BIGINT_MAX),
            'tinyint unsigned': (constant.TINYINT_UN_MIN, constant.TINYINT_UN_MAX),
            'smallint unsigned': (constant.SMALLINT_UN_MIN, constant.SMALLINT_UN_MAX),
            'int unsigned': (constant.INT_UN_MIN, constant.INT_MAX),
            'bigint unsigned': (constant.BIGINT_UN_MIN, constant.BIGINT_UN_MAX),
        }
        if ct in bounds:
            lo, hi = bounds[ct]
            return lo if base_data[ct] != lo else hi
        if ct == 'bool':
            return 1 - base_data['bool']
        if ct in ('float', 'double'):
            return 0.0 if base_data[ct] != 0.0 else 1.0
        if 'binary' in ct or 'nchar' in ct:
            return 'non_matching_probe'
        raise ValueError(f'unsupported column_type: {column_type}')

    def insert_non_matching_row(self,column_type,tbname,ts,base_data):
        val = self.non_matching_value(column_type,base_data)
        if 'binary' in column_type.lower() or 'nchar' in column_type.lower():
            tdSql.execute(f'''insert into {tbname} values({ts},"{val}")''')
        else:
            tdSql.execute(f'insert into {tbname} values({ts},{val})')

    def delete_error(self,tbname,column_name,column_type,base_data):
        predicate = self.column_filter_predicate(column_name,column_type,base_data)
        # An ordinary column filter is supported on its own and combined with the time range using
        # AND, but OR across the two categories is still rejected.
        tdSql.error(f'delete from {tbname} where ts = {self.ts} or {predicate}')
        # Only simple comparisons against a constant are accepted.
        tdSql.error(f'delete from {tbname} where {column_name} = {column_name}')

    def delete_by_column(self,dbname,tbname,column_name,column_type,base_data,row_num):
        # insert_base_data gives every row the same column value, so on its own it cannot tell
        # "deleted only the matching rows" apart from "deleted everything". Add one non-matching
        # probe row so matched < total and the assertions below exercise column-filter precision.
        probe_ts = self.ts + row_num
        self.insert_non_matching_row(column_type,tbname,probe_ts,base_data)

        predicate = self.column_filter_predicate(column_name,column_type,base_data)
        tdSql.query(f'select {column_name} from {tbname} where {predicate}')
        matched = tdSql.getRows()
        tdSql.query(f'select {column_name} from {tbname}')
        total = tdSql.getRows()
        tdSql.checkNotEqual(matched, total)

        tdSql.execute(f'delete from {tbname} where {predicate}')
        tdSql.query(f'select {column_name} from {tbname}')
        tdSql.checkRows(total - matched)
        tdSql.query(f'select {column_name} from {tbname} where {predicate}')
        tdSql.checkRows(0)
        tdSql.query(f'select {column_name} from {tbname} where ts={probe_ts}')
        tdSql.checkRows(1)

        tdSql.execute(f'flush database {dbname}')
        tdSql.execute('reset query cache')
        tdSql.query(f'select {column_name} from {tbname}')
        tdSql.checkRows(total - matched)

        # remove the probe row and restore the uniform base rows so downstream steps in
        # delete_data_ctb see the same row count they would without this probe.
        tdSql.execute(f'delete from {tbname} where ts={probe_ts}')
        self.insert_base_data(column_type,tbname,row_num,base_data)

    def delete_data_ctb(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        for col_name,col_type in self.column_dict.items():
            tdSql.execute(f'create table {self.stbname} (ts timestamp,{col_name} {col_type}) tags(t1 int)')
            for i in range(self.tbnum):
                tdSql.execute(f'create table {self.stbname}_{i} using {self.stbname} tags(1)')
                self.insert_base_data(col_type,f'{self.stbname}_{i}',self.rowNum,self.base_data)
                self.delete_one_row(f'{self.stbname}_{i}',col_type,col_name,self.base_data,self.rowNum,self.dbname,)
                self.delete_all_data(f'{self.stbname}_{i}',col_type,self.rowNum,self.base_data,self.dbname,i+1,self.stbname)
                self.delete_by_column(self.dbname,f'{self.stbname}_{i}',col_name,col_type,self.base_data,self.rowNum)
                self.delete_error(f'{self.stbname}_{i}',col_name,col_type,self.base_data)
                self.delete_rows(self.dbname,f'{self.stbname}_{i}',col_name,col_type,self.base_data,self.rowNum)
                for func in ['first','last']:
                    tdSql.query(f'select {func}(*) from {self.stbname}_{i}')
            tdSql.execute(f'drop table {self.stbname}')
    def test_delete_childtable(self):
        """Delete child table

        1. Create child table
        2. Insert data into child table
        3. Delete data from child table with one row, all rows, multiple rows 
        4. Delete data from child table with error sql
        5. Check  data in child table

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-9-16 Alex Duan Migrated from uncatalog/system-test/1-insert/test_delete_childtable.py

        """
        self.delete_data_ctb()
        tdDnodes.stoptaosd(1)
        tdDnodes.starttaosd(1)
        self.delete_data_ctb()


