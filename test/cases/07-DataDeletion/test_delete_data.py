
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

class TestDeleteData:
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        #tdSql.init(conn.cursor(), logSql))
        cls.dbname = 'db_test'
        cls.setsql = TDSetSql()
        cls.stbname = 'stb'
        cls.ntbname = 'ntb'
        cls.rowNum = 10
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
    def delete_all_data(self,tbname,col_type,row_num,base_data,dbname,tb_type,tb_num=1,stbname=''):
        tdSql.query(f'select count(*) from {tbname}')
        tdSql.execute(f'delete from {tbname}')
        tdSql.execute(f'flush database {dbname}')
        tdSql.execute('reset query cache')
        tdSql.query(f'select * from {tbname}')
        tdSql.checkRows(0)
        if tb_type == 'ntb' or tb_type == 'ctb':
            if tb_type == 'ctb':
                tdSql.query(f'select count(*) from {stbname}')
                if tb_num <= 1:
                    if len(tdSql.queryResult) != 0:
                        tdLog.exit('delete case failure!')
                else:
                    tdSql.checkEqual(tdSql.queryResult[0][0],(tb_num-1)*row_num)

            self.insert_base_data(col_type,tbname,row_num,base_data)
        elif tb_type == 'stb':
            for i in range(tb_num):
                self.insert_base_data(col_type,f'{tbname}_{i}',row_num,base_data)
        tdSql.execute(f'flush database {dbname}')
        tdSql.execute('reset query cache')
        tdSql.query(f'select * from {tbname}')
        if tb_type == 'ntb' or tb_type == 'ctb':
            tdSql.checkRows(row_num)
        elif tb_type =='stb':
            tdSql.checkRows(row_num*tb_num)
    def delete_one_row(self,tbname,column_type,column_name,base_data,row_num,dbname,tb_type,tb_num=1):
        tdSql.execute(f'delete from {tbname} where ts={self.ts}')
        tdSql.execute(f'flush database {dbname}')
        tdSql.execute('reset query cache')
        tdSql.query(f'select {column_name} from {tbname}')
        if tb_type == 'ntb' or tb_type == 'ctb':
            tdSql.checkRows(row_num-1)
        elif tb_type == 'stb':
            tdSql.checkRows((row_num-1)*tb_num)
        tdSql.query(f'select {column_name} from {tbname} where ts={self.ts}')
        tdSql.checkRows(0)
        if tb_type == 'ntb' or tb_type == 'ctb':
            if 'binary' in column_type.lower():
                tdSql.execute(f'''insert into {tbname} values({self.ts},"{base_data['binary']}")''')
            elif 'nchar' in column_type.lower():
                tdSql.execute(f'''insert into {tbname} values({self.ts},"{base_data['nchar']}")''')
            else:
                tdSql.execute(f'insert into {tbname} values({self.ts},{base_data[column_type]})')
        elif tb_type == 'stb':
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
    def delete_rows(self,dbname,tbname,col_name,col_type,base_data,row_num,tb_type,tb_num=1):
        for i in range(row_num):
            tdSql.execute(f'delete from {tbname} where ts>{self.ts+i}')
            tdSql.execute(f'flush database {dbname}')
            tdSql.execute('reset query cache')
            tdSql.query(f'select {col_name} from {tbname}')
            if tb_type == 'ntb' or tb_type == 'ctb':
                tdSql.checkRows(i+1)
                self.insert_base_data(col_type,tbname,row_num,base_data)
            elif tb_type == 'stb':
                tdSql.checkRows((i+1)*tb_num)
                for j in range(tb_num):
                    self.insert_base_data(col_type,f'{tbname}_{j}',row_num,base_data)
        for i in range(row_num):
            tdSql.execute(f'delete from {tbname} where ts>={self.ts+i}')
            tdSql.execute(f'flush database {dbname}')
            tdSql.execute('reset query cache')
            tdSql.query(f'select {col_name} from {tbname}')
            if tb_type == 'ntb' or tb_type == 'ctb':
                tdSql.checkRows(i)
                self.insert_base_data(col_type,tbname,row_num,base_data)
            elif tb_type == 'stb':
                tdSql.checkRows(i*tb_num)
                for j in range(tb_num):
                    self.insert_base_data(col_type,f'{tbname}_{j}',row_num,base_data)
        for i in range(row_num):
            tdSql.execute(f'delete from {tbname} where ts<={self.ts+i}')
            tdSql.execute(f'flush database {dbname}')
            tdSql.execute('reset query cache')
            tdSql.query(f'select {col_name} from {tbname}')
            if tb_type == 'ntb' or tb_type == 'ctb':
                tdSql.checkRows(row_num-i-1)
                self.insert_base_data(col_type,tbname,row_num,base_data)
            elif tb_type == 'stb':
                tdSql.checkRows((row_num-i-1)*tb_num)
                for j in range(tb_num):
                    self.insert_base_data(col_type,f'{tbname}_{j}',row_num,base_data)
        for i in range(row_num):
            tdSql.execute(f'delete from {tbname} where ts<{self.ts+i}')
            tdSql.execute(f'flush database {dbname}')
            tdSql.execute('reset query cache')
            tdSql.query(f'select {col_name} from {tbname}')
            if tb_type == 'ntb' or tb_type == 'ctb':
                tdSql.checkRows(row_num-i)
                self.insert_base_data(col_type,tbname,row_num,base_data)
            elif tb_type == 'stb':
                tdSql.checkRows((row_num-i)*tb_num)
                for j in range(tb_num):
                    self.insert_base_data(col_type,f'{tbname}_{j}',row_num,base_data)
        for i in range(row_num):
            tdSql.execute(f'delete from {tbname} where ts between {self.ts} and {self.ts+i}')
            tdSql.execute(f'flush database {dbname}')
            tdSql.execute('reset query cache')
            tdSql.query(f'select {col_name} from {tbname}')
            if tb_type == 'ntb' or tb_type == 'ctb':
                tdSql.checkRows(row_num - i-1)
                self.insert_base_data(col_type,tbname,row_num,base_data)
            elif tb_type == 'stb':
                tdSql.checkRows(tb_num*(row_num - i-1))
                for j in range(tb_num):
                    self.insert_base_data(col_type,f'{tbname}_{j}',row_num,base_data)
            tdSql.execute(f'delete from {tbname} where ts between {self.ts+i+1} and {self.ts}')
            tdSql.query(f'select {col_name} from {tbname}')
            if tb_type == 'ntb' or tb_type == 'ctb':
                tdSql.checkRows(row_num)
            elif tb_type == 'stb':
                tdSql.checkRows(tb_num*row_num)
    def delete_error(self,tbname,column_name,column_type,base_data):
        for error_list in ['',f'ts = {self.ts} and',f'ts = {self.ts} or']:
            if 'binary' in column_type.lower():
                tdSql.error(f'''delete from {tbname} where {error_list} {column_name} ="{base_data['binary']}"''')
            elif 'nchar' in column_type.lower():
                tdSql.error(f'''delete from {tbname} where {error_list} {column_name} ="{base_data['nchar']}"''')
            else:
                tdSql.error(f'delete from {tbname} where {error_list} {column_name} = {base_data[column_type]}')
        
        tdSql.error(f'delete from {tbname} where _c0 > forecast(_c0)')

    def delete_data_ntb(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        for col_name,col_type in self.column_dict.items():
            tdSql.execute(f'create table {self.ntbname} (ts timestamp,{col_name} {col_type})')
            self.insert_base_data(col_type,self.ntbname,self.rowNum,self.base_data)
            self.delete_one_row(self.ntbname,col_type,col_name,self.base_data,self.rowNum,self.dbname,'ntb')
            self.delete_all_data(self.ntbname,col_type,self.rowNum,self.base_data,self.dbname,'ntb')
            self.delete_error(self.ntbname,col_name,col_type,self.base_data)
            self.delete_rows(self.dbname,self.ntbname,col_name,col_type,self.base_data,self.rowNum,'ntb')
            for func in ['first','last']:
                tdSql.query(f'select {func}(*) from {self.ntbname}')
            tdSql.execute(f'drop table {self.ntbname}')
        tdSql.execute(f'drop database {self.dbname}')
    def delete_data_ctb(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        for col_name,col_type in self.column_dict.items():
            tdSql.execute(f'create table {self.stbname} (ts timestamp,{col_name} {col_type}) tags(t1 int)')
            for i in range(self.tbnum):
                tdSql.execute(f'create table {self.stbname}_{i} using {self.stbname} tags(1)')
                self.insert_base_data(col_type,f'{self.stbname}_{i}',self.rowNum,self.base_data)
                self.delete_one_row(f'{self.stbname}_{i}',col_type,col_name,self.base_data,self.rowNum,self.dbname,'ctb')
                self.delete_all_data(f'{self.stbname}_{i}',col_type,self.rowNum,self.base_data,self.dbname,'ctb',i+1,self.stbname)
                self.delete_error(f'{self.stbname}_{i}',col_name,col_type,self.base_data)
                self.delete_rows(self.dbname,f'{self.stbname}_{i}',col_name,col_type,self.base_data,self.rowNum,'ctb')
                for func in ['first','last']:
                    tdSql.query(f'select {func}(*) from {self.stbname}_{i}')
            tdSql.execute(f'drop table {self.stbname}')
    def delete_data_stb(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        for col_name,col_type in self.column_dict.items():
            tdSql.execute(f'create table {self.stbname} (ts timestamp,{col_name} {col_type}) tags(t1 int)')
            for i in range(self.tbnum):
                tdSql.execute(f'create table {self.stbname}_{i} using {self.stbname} tags(1)')
                self.insert_base_data(col_type,f'{self.stbname}_{i}',self.rowNum,self.base_data)
            self.delete_error(self.stbname,col_name,col_type,self.base_data)
            self.delete_one_row(self.stbname,col_type,col_name,self.base_data,self.rowNum,self.dbname,'stb',self.tbnum)
            self.delete_all_data(self.stbname,col_type,self.rowNum,self.base_data,self.dbname,'stb',self.tbnum)
            self.delete_rows(self.dbname,self.stbname,col_name,col_type,self.base_data,self.rowNum,'stb',self.tbnum)
            for func in ['first','last']:
                tdSql.query(f'select {func}(*) from {self.stbname}')
            tdSql.execute(f'drop table {self.stbname}')
        tdSql.execute(f'drop database {self.dbname}')
    
    def FIX_TS_3987(self):
        tdSql.execute("create database db duration 1d vgroups 1;")
        tdSql.execute("use db;")
        tdSql.execute("create table t (ts timestamp, a int);")
        tdSql.execute("insert into t values (1694681045000, 1);")
        tdSql.execute("select * from t;")
        tdSql.execute("flush database db;")
        tdSql.execute("select * from t;")
        tdSql.execute("delete from t where ts = 1694681045000;")
        tdSql.execute("select * from t;")
        tdSql.execute("insert into t values (1694581045000, 2);")
        tdSql.execute("select * from t;")
        tdSql.execute("flush database db;")
        tdSql.query("select * from t;")
        time.sleep(5)
        tdSql.query("select * from t;")

        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1694581045000)
        tdSql.checkData(0, 1, 2)        
    
    def test_delete_data(self):
        """Delete data test (obsolete)

        1. Delete data from normal table
        2. Insert data into child table
        3. Delete data from super table
        4. Restart taosd service
        5. Delete data from normal table again
        6. JIRA TS-3987

        Since: v3.0.0.0

        Labels: common,ci,skip

        Jira: None

        History:
            - 2025-9-16 Alex Duan Migrated from uncatalog/system-test/1-insert/test_delete_data.py

        """
        self.FIX_TS_3987()
        self.delete_data_ntb()
        self.delete_data_ctb()
        self.delete_data_stb()
        tdDnodes.stoptaosd(1)
        tdDnodes.starttaosd(1)
        self.delete_data_ntb()
        
        tdLog.success("%s successfully executed" % __file__)
        