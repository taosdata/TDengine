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


import random
import string
import sys

from numpy import logspace


from taostest import TDCase, T
from taostest.util.common import TDCom


class DeleteData(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.dbname = 'db_test'
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
        
        self.tinyint_val = random.randint(-128,127)
        self.smallint_val = random.randint(-32768,32767)
        self.int_val = random.randint(-2147483648,2147483647)
        self.bigint_val = random.randint(-9223372036854775808,9223372036854775807)
        self.untingint_val = random.randint(0,255)
        self.unsmallint_val = random.randint(0,65535)
        self.unint_val = random.randint(0,4294967295)
        self.unbigint_val = random.randint(0,18446744073709551615)
        self.float_val = random.uniform(-3.40E+38,3.40E+38)
        self.double_val = random.uniform(-1.7E+308*(1E-300),1.7E+308*(1E-300))
        self.bool_val = random.randint(0,100)%2
        self.binary_val = self.tdCom.get_long_name(random.randint(0,self.str_length))
        self.nchar_val = self.tdCom.get_long_name(random.randint(0,self.str_length))
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
    def insert_base_data(self,col_type,tbname,rows,base_data,dbname):
        for i in range(rows):
            if col_type.lower() == 'tinyint':
                self.tdSql.execute(f'insert into {dbname}.{tbname} values({self.ts+i},{base_data["tinyint"]})')
            elif col_type.lower() == 'smallint':
                self.tdSql.execute(f'insert into {dbname}.{tbname} values({self.ts+i},{base_data["smallint"]})')
            elif col_type.lower() == 'int':
                self.tdSql.execute(f'insert into {dbname}.{tbname} values({self.ts+i},{base_data["int"]})')
            elif col_type.lower() == 'bigint':
                self.tdSql.execute(f'insert into {dbname}.{tbname} values({self.ts+i},{base_data["bigint"]})')
            elif col_type.lower() == 'tinyint unsigned':
                self.tdSql.execute(f'insert into {dbname}.{tbname} values({self.ts+i},{base_data["tinyint unsigned"]})')
            elif col_type.lower() == 'smallint unsigned':
                self.tdSql.execute(f'insert into {dbname}.{tbname} values({self.ts+i},{base_data["smallint unsigned"]})')
            elif col_type.lower() == 'int unsigned':
                self.tdSql.execute(f'insert into {dbname}.{tbname} values({self.ts+i},{base_data["int unsigned"]})')
            elif col_type.lower() == 'bigint unsigned':
                self.tdSql.execute(f'insert into {dbname}.{tbname} values({self.ts+i},{base_data["bigint unsigned"]})')
            elif col_type.lower() == 'bool':
                self.tdSql.execute(f'insert into {dbname}.{tbname} values({self.ts+i},{base_data["bool"]})')    
            elif col_type.lower() == 'float':
                self.tdSql.execute(f'insert into {dbname}.{tbname} values({self.ts+i},{base_data["float"]})')      
            elif col_type.lower() == 'double':
                self.tdSql.execute(f'insert into {dbname}.{tbname} values({self.ts+i},{base_data["double"]})')
            elif 'binary' in col_type.lower():
                self.tdSql.execute(f'''insert into {dbname}.{tbname} values({self.ts+i},"{base_data['binary']}")''')
            elif 'nchar' in col_type.lower():
                self.tdSql.execute(f'''insert into {dbname}.{tbname} values({self.ts+i},"{base_data['nchar']}")''')        
    def delete_all_data(self,tbname,col_type,row_num,base_data,dbname,tb_type,tb_num=1):
        self.tdSql.execute(f'delete from {dbname}.{tbname}')
        self.tdSql.execute(f'flush database {dbname}')
        self.tdSql.execute('reset query cache')
        self.tdSql.query(f'select * from {dbname}.{tbname}')
        self.tdSql.checkRow(0)
        if tb_type == 'ntb' or tb_type == 'ctb':
            self.insert_base_data(col_type,tbname,row_num,base_data,dbname)
        elif tb_type == 'stb':
            for i in range(tb_num):
                self.insert_base_data(col_type,f'{tbname}_{i}',row_num,base_data,dbname)
        self.tdSql.execute(f'flush database {dbname}')
        self.tdSql.execute('reset query cache')
        self.tdSql.query(f'select * from {dbname}.{tbname}')
        if tb_type == 'ntb' or tb_type == 'ctb':
            self.tdSql.checkRow(row_num)
        elif tb_type =='stb':
            self.tdSql.checkRow(row_num*tb_num)
    def delete_one_row(self,tbname,column_type,column_name,base_data,row_num,dbname,tb_type,tb_num=1):
        self.tdSql.execute(f'delete from {dbname}.{tbname} where ts={self.ts}')
        self.tdSql.execute(f'flush database {dbname}')
        self.tdSql.execute('reset query cache')
        self.tdSql.query(f'select {column_name} from {dbname}.{tbname}')
        if tb_type == 'ntb' or tb_type == 'ctb':
            self.tdSql.checkRow(row_num-1)
        elif tb_type == 'stb':
            self.tdSql.checkRow((row_num-1)*tb_num)
        self.tdSql.query(f'select {column_name} from {dbname}.{tbname} where ts={self.ts}')
        self.tdSql.checkRow(0)
        if tb_type == 'ntb' or tb_type == 'ctb':
            if 'binary' in column_type.lower():
                self.tdSql.execute(f'''insert into {dbname}.{tbname} values({self.ts},"{base_data['binary']}")''')
            elif 'nchar' in column_type.lower():
                self.tdSql.execute(f'''insert into {dbname}.{tbname} values({self.ts},"{base_data['nchar']}")''')
            else:
                self.tdSql.execute(f'insert into {dbname}.{tbname} values({self.ts},{base_data[column_type]})')
        elif tb_type == 'stb':
            for i in range(tb_num):
                if 'binary' in column_type.lower():
                    self.tdSql.execute(f'''insert into {dbname}.{tbname}_{i} values({self.ts},"{base_data['binary']}")''')
                elif 'nchar' in column_type.lower():
                    self.tdSql.execute(f'''insert into {dbname}.{tbname}_{i} values({self.ts},"{base_data['nchar']}")''')
                else:
                    self.tdSql.execute(f'insert into {dbname}.{tbname}_{i} values({self.ts},{base_data[column_type]})')
        self.tdSql.query(f'select {column_name} from {dbname}.{tbname} where ts={self.ts}')
        if column_type.lower() == 'float' or column_type.lower() == 'double':
            if abs(self.tdSql.query_data[0][0] - base_data[column_type]) / base_data[column_type] <= 0.0001:
                self.tdSql.checkEqual(self.tdSql.query_data[0][0],self.tdSql.query_data[0][0])
            else:
                sys.exit(f'{column_type} data check failure')
        elif 'binary' in column_type.lower():
            self.tdSql.checkEqual(self.tdSql.query_data[0][0],base_data['binary'])
        elif 'nchar' in column_type.lower():
            self.tdSql.checkEqual(self.tdSql.query_data[0][0],base_data['nchar'])
        else:
            self.tdSql.checkEqual(self.tdSql.query_data[0][0],base_data[column_type])  
    def delete_rows(self,dbname,tbname,col_name,col_type,base_data,row_num,tb_type,tb_num=1):
        for i in range(row_num):
            self.tdSql.execute(f'delete from {dbname}.{tbname} where ts>{self.ts+i}')
            self.tdSql.execute(f'flush database {dbname}')
            self.tdSql.execute('reset query cache')
            self.tdSql.query(f'select {col_name} from {dbname}.{tbname}')
            if tb_type == 'ntb' or tb_type == 'ctb':
                self.tdSql.checkRow(i+1)
                self.insert_base_data(col_type,tbname,row_num,base_data,dbname)
            elif tb_type == 'stb':
                self.tdSql.checkRow((i+1)*tb_num)
                for j in range(tb_num):
                    self.insert_base_data(col_type,f'{tbname}_{j}',row_num,base_data,dbname)
        for i in range(row_num):
            self.tdSql.execute(f'delete from {dbname}.{tbname} where ts>={self.ts+i}')
            self.tdSql.execute(f'flush database {dbname}')
            self.tdSql.execute('reset query cache')
            self.tdSql.query(f'select {col_name} from {dbname}.{tbname}')
            if tb_type == 'ntb' or tb_type == 'ctb':
                self.tdSql.checkRow(i)
                self.insert_base_data(col_type,tbname,row_num,base_data,dbname)
            elif tb_type == 'stb':
                self.tdSql.checkRow(i*tb_num)
                for j in range(tb_num):
                    self.insert_base_data(col_type,f'{tbname}_{j}',row_num,base_data,dbname)    
        for i in range(row_num):
            self.tdSql.execute(f'delete from {dbname}.{tbname} where ts<={self.ts+i}')
            self.tdSql.execute(f'flush database {dbname}')
            self.tdSql.execute('reset query cache')
            self.tdSql.query(f'select {col_name} from {dbname}.{tbname}')
            if tb_type == 'ntb' or tb_type == 'ctb':
                self.tdSql.checkRow(row_num-i-1)
                self.insert_base_data(col_type,tbname,row_num,base_data,dbname)
            elif tb_type == 'stb':
                self.tdSql.checkRow((row_num-i-1)*tb_num)
                for j in range(tb_num):
                    self.insert_base_data(col_type,f'{tbname}_{j}',row_num,base_data,dbname)
        for i in range(row_num):
            self.tdSql.execute(f'delete from {dbname}.{tbname} where ts<{self.ts+i}')
            self.tdSql.execute(f'flush database {dbname}')
            self.tdSql.execute('reset query cache')
            self.tdSql.query(f'select {col_name} from {dbname}.{tbname}')
            if tb_type == 'ntb' or tb_type == 'ctb':
                self.tdSql.checkRow(row_num-i)
                self.insert_base_data(col_type,tbname,row_num,base_data,dbname)
            elif tb_type == 'stb':
                self.tdSql.checkRow((row_num-i)*tb_num)
                for j in range(tb_num):
                    self.insert_base_data(col_type,f'{tbname}_{j}',row_num,base_data,dbname)
        for i in range(row_num):
            self.tdSql.execute(f'delete from {dbname}.{tbname} where ts between {self.ts} and {self.ts+i}')
            self.tdSql.execute(f'flush database {dbname}')
            self.tdSql.execute('reset query cache')
            self.tdSql.query(f'select {col_name} from {dbname}.{tbname}')
            if tb_type == 'ntb' or tb_type == 'ctb':
                self.tdSql.checkRow(row_num - i-1)
                self.insert_base_data(col_type,tbname,row_num,base_data,dbname)
            elif tb_type == 'stb':
                self.tdSql.checkRow(tb_num*(row_num - i-1))
                for j in range(tb_num):
                    self.insert_base_data(col_type,f'{tbname}_{j}',row_num,base_data,dbname)
            self.tdSql.execute(f'delete from {dbname}.{tbname} where ts between {self.ts+i+1} and {self.ts}')
            self.tdSql.query(f'select {col_name} from {dbname}.{tbname}')
            if tb_type == 'ntb' or tb_type == 'ctb':
                self.tdSql.checkRow(row_num)
            elif tb_type == 'stb':
                self.tdSql.checkRow(tb_num*row_num)
    def delete_error(self,tbname,column_name,column_type,base_data,dbname):
        for error_list in ['',f'ts = {self.ts} and',f'ts = {self.ts} or']:
            if 'binary' in column_type.lower():
                self.tdSql.error(f'''delete from {dbname}.{tbname} where {error_list} {column_name} ="{base_data['binary']}"''')
            elif 'nchar' in column_type.lower():
                self.tdSql.error(f'''delete from {dbname}.{tbname} where {error_list} {column_name} ="{base_data['nchar']}"''')
            else:
                self.tdSql.error(f'delete from {dbname}.{tbname} where {error_list} {column_name} = {base_data[column_type]}')
           
    def delete_data_ntb(self):
        self.tdSql.execute(f'drop database if exists {self.dbname}')
        self.tdCom.createDb(self.dbname)
        self.tdSql.execute(f'use {self.dbname}')
        for col_name,col_type in self.column_dict.items():
            self.tdSql.execute(f'create table {self.dbname}.{self.ntbname} (ts timestamp,{col_name} {col_type})')
            self.insert_base_data(col_type,self.ntbname,self.rowNum,self.base_data,self.dbname)
            self.delete_one_row(self.ntbname,col_type,col_name,self.base_data,self.rowNum,self.dbname,'ntb')
            self.delete_all_data(self.ntbname,col_type,self.rowNum,self.base_data,self.dbname,'ntb')
            self.delete_error(self.ntbname,col_name,col_type,self.base_data,self.dbname)
            self.delete_rows(self.dbname,self.ntbname,col_name,col_type,self.base_data,self.rowNum,'ntb')
            for func in ['first','last']:
                self.tdSql.query(f'select {func}(*) from {self.dbname}.{self.ntbname}')
            self.tdSql.execute(f'drop table {self.dbname}.{self.ntbname}')
        self.tdSql.execute(f'drop database {self.dbname}')
    def delete_data_ctb(self):
        self.tdSql.execute(f'drop database if exists {self.dbname}')
        self.tdCom.createDb(self.dbname)
        self.tdSql.execute(f'use {self.dbname}')
        for col_name,col_type in self.column_dict.items():
            self.tdSql.execute(f'create table {self.dbname}.{self.stbname} (ts timestamp,{col_name} {col_type}) tags(t1 int)')
            for i in range(self.tbnum):
                self.tdSql.execute(f'create table {self.dbname}.{self.stbname}_{i} using {self.stbname} tags(1)')
                self.insert_base_data(col_type,f'{self.stbname}_{i}',self.rowNum,self.base_data,self.dbname)
                self.delete_one_row(f'{self.stbname}_{i}',col_type,col_name,self.base_data,self.rowNum,self.dbname,'ctb')
                self.delete_all_data(f'{self.stbname}_{i}',col_type,self.rowNum,self.base_data,self.dbname,'ctb')
                self.delete_error(f'{self.stbname}_{i}',col_name,col_type,self.base_data,self.dbname)
                self.delete_rows(self.dbname,f'{self.stbname}_{i}',col_name,col_type,self.base_data,self.rowNum,'ctb')
                for func in ['first','last']:
                    self.tdSql.query(f'select {func}(*) from {self.stbname}_{i}')
            self.tdSql.execute(f'drop table {self.stbname}')
    def delete_data_stb(self):
        self.tdSql.execute(f'drop database if exists {self.dbname}')
        self.tdCom.createDb(self.dbname)
        self.tdSql.execute(f'use {self.dbname}')
        for col_name,col_type in self.column_dict.items():
            self.tdSql.execute(f'create table {self.stbname} (ts timestamp,{col_name} {col_type}) tags(t1 int)')
            for i in range(self.tbnum):
                self.tdSql.execute(f'create table {self.stbname}_{i} using {self.stbname} tags(1)')
                self.insert_base_data(col_type,f'{self.stbname}_{i}',self.rowNum,self.base_data,self.dbname)
            self.delete_error(self.stbname,col_name,col_type,self.base_data,self.dbname)
            self.delete_one_row(self.stbname,col_type,col_name,self.base_data,self.rowNum,self.dbname,'stb',self.tbnum)
            self.delete_all_data(self.stbname,col_type,self.rowNum,self.base_data,self.dbname,'stb',self.tbnum)
            self.delete_rows(self.dbname,self.stbname,col_name,col_type,self.base_data,self.rowNum,'stb',self.tbnum)
            for func in ['first','last']:
                self.tdSql.query(f'select {func}(*) from {self.dbname}.{self.stbname}')
            self.tdSql.execute(f'drop table {self.dbname}.{self.stbname}')
        self.tdSql.execute(f'drop database {self.dbname}')
    def run(self):
        self.delete_data_ntb()
        self.delete_data_ctb()
        self.delete_data_stb()
    
    def cleanup(self):
        pass

    def desc(self):
        case_description = """
            delete_data check <jiacy>
            """
        return case_description

    def author(self):
        return "Jiacy"

    def tags(self):
        return T.Write.TaoscSql.Insert