
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

class TestDeleteStable:
    updatecfgDict = {'tsdbdebugFlag': 131}
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        #tdSql.init(conn.cursor(), logSql), True)
        cls.dbname = 'db_test'
        cls.ns_dbname = 'ns_test'
        cls.us_dbname = 'us_test'
        cls.ms_dbname = 'ms_test'
        cls.setsql = TDSetSql()
        cls.stbname = 'stb'
        cls.ntbname = 'ntb'
        cls.rowNum = 3
        cls.tbnum = 3
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
    def delete_all_data(self,tbname,col_type,row_num,base_data,dbname,tb_num=1):
        tdSql.query(f'select count(*) from {tbname}')
        tdSql.execute(f'delete from {tbname}')
        tdSql.execute(f'flush database {dbname}')
        tdSql.execute('reset query cache')
        tdSql.query(f'select * from {tbname}')
        tdSql.checkRows(0)
        for i in range(tb_num):
            self.insert_base_data(col_type,f'{tbname}_{i}',row_num,base_data)
        tdSql.execute(f'flush database {dbname}')
        tdSql.execute('reset query cache')
        tdSql.query(f'select * from {tbname}')
        tdSql.checkRows(row_num*tb_num)
    def delete_one_row(self,tbname,column_type,column_name,base_data,row_num,dbname,tb_num=1):
        tdSql.execute(f'delete from {tbname} where ts={self.ts}')
        tdSql.execute(f'flush database {dbname}')
        tdSql.execute('reset query cache')
        tdSql.query(f'select {column_name} from {tbname}')
        tdSql.checkRows((row_num-1)*tb_num)
        tdSql.query(f'select {column_name} from {tbname} where ts={self.ts}')
        tdSql.checkRows(0)
        for i in range(tb_num):
            if 'binary' in column_type.lower():
                tdSql.execute(f'''insert into {tbname}_{i} values({self.ts},"{base_data['binary']}")''')
            elif 'nchar' in column_type.lower():
                tdSql.execute(f'''insert into {tbname}_{i} values({self.ts},"{base_data['nchar']}")''')
            else:
                tdSql.execute(f'insert into {tbname}_{i} values({self.ts},{base_data[column_type]})')
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
            tdSql.checkRows((i+1)*tb_num)
            for j in range(tb_num):
                self.insert_base_data(col_type,f'{tbname}_{j}',row_num,base_data)
        for i in range(row_num):
            tdSql.execute(f'delete from {tbname} where ts>={self.ts+i}')
            tdSql.execute(f'flush database {dbname}')
            tdSql.execute('reset query cache')
            tdSql.query(f'select {col_name} from {tbname}')
            tdSql.checkRows(i*tb_num)
            for j in range(tb_num):
                self.insert_base_data(col_type,f'{tbname}_{j}',row_num,base_data)
        for i in range(row_num):
            tdSql.execute(f'delete from {tbname} where ts<={self.ts+i}')
            tdSql.execute(f'flush database {dbname}')
            tdSql.execute('reset query cache')
            tdSql.query(f'select {col_name} from {tbname}')
            tdSql.checkRows((row_num-i-1)*tb_num)
            for j in range(tb_num):
                self.insert_base_data(col_type,f'{tbname}_{j}',row_num,base_data)
        for i in range(row_num):
            tdSql.execute(f'delete from {tbname} where ts<{self.ts+i}')
            tdSql.execute(f'flush database {dbname}')
            tdSql.execute('reset query cache')
            tdSql.query(f'select {col_name} from {tbname}')
            tdSql.checkRows((row_num-i)*tb_num)
            for j in range(tb_num):
                self.insert_base_data(col_type,f'{tbname}_{j}',row_num,base_data)
        for i in range(row_num):
            tdSql.execute(f'delete from {tbname} where ts between {self.ts} and {self.ts+i}')
            tdSql.execute(f'flush database {dbname}')
            tdSql.execute('reset query cache')
            tdSql.query(f'select {col_name} from {tbname}')
            tdSql.checkRows(tb_num*(row_num - i-1))
            for j in range(tb_num):
                self.insert_base_data(col_type,f'{tbname}_{j}',row_num,base_data)
            tdSql.execute(f'delete from {tbname} where ts between {self.ts+i+1} and {self.ts}')
            tdSql.query(f'select {col_name} from {tbname}')
            tdSql.checkRows(tb_num*row_num)
        for i in range(row_num):
            tdSql.execute(f'delete from {tbname} where t1 = 1')
            tdSql.execute(f'flush database {dbname}')
            tdSql.execute('reset query cache')
            tdSql.query(f'select {col_name} from {tbname}')
            tdSql.checkRows(0)
            for j in range(tb_num):
                self.insert_base_data(col_type,f'{tbname}_{j}',row_num,base_data)
            tdSql.execute(f'delete from {tbname} where t1 = 0')
            tdSql.query(f'select {col_name} from {tbname}')
            tdSql.checkRows(tb_num*row_num)
    def delete_error(self,tbname,column_name,column_type,base_data):
        for error_list in ['',f'ts = {self.ts} and',f'ts = {self.ts} or']:
            if 'binary' in column_type.lower():
                tdSql.error(f'''delete from {tbname} where {error_list} {column_name} ="{base_data['binary']}"''')
            elif 'nchar' in column_type.lower():
                tdSql.error(f'''delete from {tbname} where {error_list} {column_name} ="{base_data['nchar']}"''')
            else:
                tdSql.error(f'delete from {tbname} where {error_list} {column_name} = {base_data[column_type]}')
    def delete_data_stb(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        for col_name,col_type in self.column_dict.items():
            tdSql.execute(f'create table {self.stbname} (ts timestamp,{col_name} {col_type}) tags(t1 int)')
            for i in range(self.tbnum):
                tdSql.execute(f'create table {self.stbname}_{i} using {self.stbname} tags(1)')
                self.insert_base_data(col_type,f'{self.stbname}_{i}',self.rowNum,self.base_data)
            self.delete_error(self.stbname,col_name,col_type,self.base_data)
            self.delete_one_row(self.stbname,col_type,col_name,self.base_data,self.rowNum,self.dbname,self.tbnum)
            self.delete_all_data(self.stbname,col_type,self.rowNum,self.base_data,self.dbname,self.tbnum)
            self.delete_rows(self.dbname,self.stbname,col_name,col_type,self.base_data,self.rowNum,self.tbnum)
            for func in ['first','last']:
                tdSql.query(f'select {func}(*) from {self.stbname}')
            tdSql.execute(f'drop table {self.stbname}')
        tdSql.execute(f'drop database {self.dbname}')

    def precision_now_check(self):
        for dbname in [self.ms_dbname, self.us_dbname, self.ns_dbname]:
            self.ts = 1537146000000
            if dbname == self.us_dbname:
                self.ts = int(self.ts*1000)
                precision = "us"
            elif dbname == self.ns_dbname:
                precision = "ns"
                self.ts = int(self.ts*1000000)
            else:
                precision = "ms"
                self.ts = int(self.ts)
            tdSql.execute(f'drop database if exists {dbname}')
            tdSql.execute(f'create database if not exists {dbname} precision "{precision}"')
            tdSql.execute(f'use {dbname}')
            self.base_data = {
                'tinyint':self.tinyint_val
                        }
            self.column_dict = {
                'col1': 'tinyint'
            }
            for col_name,col_type in self.column_dict.items():
                tdSql.execute(f'create table if not exists {self.stbname} (ts timestamp,{col_name} {col_type}) tags(t1 int)')
                for i in range(self.tbnum):
                    tdSql.execute(f'create table if not exists {self.stbname}_{i} using {self.stbname} tags(1)')
                    self.insert_base_data(col_type,f'{self.stbname}_{i}',self.rowNum,self.base_data)
                tdSql.query(f'select * from {self.stbname}')
                tdSql.checkEqual(tdSql.queryRows, self.tbnum*self.rowNum)
                tdSql.execute(f'delete from {self.stbname} where ts < now()')
                tdSql.query(f'select * from {self.stbname}')
                tdSql.checkEqual(tdSql.queryRows, 0)

    def test_delete_stable(self):
        """Delete super table

        1. Create super table
        2. Insert data into super table
        3. Delete data from super table with one row, all rows, multiple rows
        4. Delete data from super table with error sql
        5. Check  data in super table

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-16 Alex Duan Migrated from uncatalog/system-test/1-insert/test_delete_stable.py

        """
        self.delete_data_stb()
        tdDnodes.stoptaosd(1)
        tdDnodes.starttaosd(1)
        self.delete_data_stb()
        self.precision_now_check()
        
        tdLog.success(f"{__file__} successfully executed")
