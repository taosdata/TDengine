
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

import random
import string

from numpy import logspace
from util import constant
from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import TDSetSql

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)
        self.dbname = 'db_test'
        self.setsql = TDSetSql()
        self.stbname = 'stb'
        self.ntbname = 'ntb'
        self.rowNum = 5
        self.tbnum = 2
        self.ts = 1537146000000
        self.binary_str = 'taosdata'
        self.nchar_str = '涛思数据'
        self.str_length = 20
        self.column_dict = {
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
            'col12': f'binary({self.str_length})',
            'col13': f'nchar({self.str_length})',

        }

        self.tinyint_val = random.randint(constant.TINYINT_MIN,constant.TINYINT_MAX)
        self.smallint_val = random.randint(constant.SMALLINT_MIN,constant.SMALLINT_MAX)
        self.int_val = random.randint(constant.INT_MIN,constant.INT_MAX)
        self.bigint_val = random.randint(constant.BIGINT_MIN,constant.BIGINT_MAX)
        self.untingint_val = random.randint(constant.TINYINT_UN_MIN,constant.TINYINT_UN_MAX)
        self.unsmallint_val = random.randint(constant.SMALLINT_UN_MIN,constant.SMALLINT_UN_MAX)
        self.unint_val = random.randint(constant.INT_UN_MIN,constant.INT_MAX)
        self.unbigint_val = random.randint(constant.BIGINT_UN_MIN,constant.BIGINT_UN_MAX)
        self.float_val = random.uniform(constant.FLOAT_MIN,constant.FLOAT_MAX)
        self.double_val = random.uniform(constant.DOUBLE_MIN*(1E-300),constant.DOUBLE_MAX*(1E-300))
        self.bool_val = random.randint(0,100)%2
        self.binary_val = tdCom.getLongName(random.randint(0,self.str_length))
        self.nchar_val = tdCom.getLongName(random.randint(0,self.str_length))
        self.base_data = {
            'tinyint':self.tinyint_val,
            'smallint':self.smallint_val,
            'int':self.int_val,
            'bigint':self.bigint_val,
            'tinyint unsigned':self.untingint_val,
            'smallint unsigned':self.unsmallint_val,
            'int unsigned':self.unint_val,
            'bigint unsigned':self.unbigint_val,
            'bool':self.bool_val,
            'float':self.float_val,
            'double':self.double_val,
            'binary':self.binary_val,
            'nchar':self.nchar_val
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
    def delete_error(self,tbname,column_name,column_type,base_data):
        for error_list in ['',f'ts = {self.ts} and',f'ts = {self.ts} or']:
            if 'binary' in column_type.lower():
                tdSql.error(f'''delete from {tbname} where {error_list} {column_name} ="{base_data['binary']}"''')
            elif 'nchar' in column_type.lower():
                tdSql.error(f'''delete from {tbname} where {error_list} {column_name} ="{base_data['nchar']}"''')
            else:
                tdSql.error(f'delete from {tbname} where {error_list} {column_name} = {base_data[column_type]}')

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
                self.delete_error(f'{self.stbname}_{i}',col_name,col_type,self.base_data)
                self.delete_rows(self.dbname,f'{self.stbname}_{i}',col_name,col_type,self.base_data,self.rowNum)
                for func in ['first','last']:
                    tdSql.query(f'select {func}(*) from {self.stbname}_{i}')
            tdSql.execute(f'drop table {self.stbname}')
    def run(self):
        self.delete_data_ctb()
        tdDnodes.stoptaosd(1)
        tdDnodes.starttaosd(1)
        self.delete_data_ctb()
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
