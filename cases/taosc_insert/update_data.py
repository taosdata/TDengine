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
from datetime import datetime
from taostest import TDCase, T
from taostest.util.common import TDCom
class UpdateData(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.dbname = 'db_test'
        self.ntbname = 'ntb'
        self.stbname = 'stb'
        self.ctbname = 'ctb'
        self.ts = 1537146000000
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
            'col_ts'  : 'timestamp'
        }

    def data_check(self,tbname,col_name,col_type,value,dbname):
        self.tdSql.query(f'select {col_name} from {dbname}.{tbname}')
        if col_type.lower() == 'float' or col_type.lower() == 'double':
            if abs(self.tdSql.query_data[0][0] - value) / value <= 0.0001:
                self.tdSql.checkEqual(self.tdSql.query_data[0][0],self.tdSql.query_data[0][0])
            else:
                sys.exit(f'{col_type} data check failure')
        elif col_type.lower() == 'timestamp':
            self.tdSql.checkEqual(str(self.tdSql.query_data[0][0]),str(datetime.fromtimestamp(value/1000).strftime("%Y-%m-%d %H:%M:%S.%f")))
        else:
            self.tdSql.checkEqual(self.tdSql.query_data[0][0],value)
    def update_and_check_data(self,tbname,col_name,col_type,value,dbname):
        if 'binary' in col_type.lower() or 'nchar' in col_type.lower():
            self.tdSql.execute(f'insert into {dbname}.{tbname} values({self.ts},"{value}")')
        else:
            self.tdSql.execute(f'insert into {dbname}.{tbname} values({self.ts},{value})')
        self.data_check(tbname,col_name,col_type,value,dbname)
        self.tdSql.execute(f'flush database {dbname}')
        self.tdSql.execute('reset query cache')
        self.data_check(tbname,col_name,col_type,value,dbname)
        for func in ['first','last']:
            self.tdSql.execute(f'select {func}({col_name}) from {dbname}.{tbname}')
    def error_check(self,tbname,column_dict,dbname,tb_type=None,stbname=None):
        str_length = self.str_length+1
        for col_name,col_type in column_dict.items():
            if tb_type == 'ntb':
                self.tdSql.execute(f'create table {dbname}.{tbname} (ts timestamp,{col_name} {col_type})')
            elif tb_type == 'ctb':
                self.tdSql.execute(f'create table {dbname}.{stbname} (ts timestamp,{col_name} {col_type}) tags(t0 int)')
                self.tdSql.execute(f'create table {dbname}.{tbname} using {stbname} tags(1)')
            self.tdSql.execute(f'insert into {dbname}.{tbname} values({self.ts},null)')
            if col_type.lower() == 'double':
                for error_value in [self.tdCom.get_long_name(self.str_length),True,False,1.1*(-1.7E+308),1.1*(1.7E+308)]:
                    self.tdSql.error(f'insert into {dbname}.{tbname} values({self.ts},{error_value})')
                    if tb_type == 'ctb':
                        self.tdSql.error(f'insert into {dbname}.{stbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'float':
                for error_value in [self.tdCom.get_long_name(self.str_length),True,False,1.1*(-3.40E+38),1.1*(3.40E+38)]:
                    self.tdSql.error(f'insert into {dbname}.{tbname} values({self.ts},{error_value})')
                    if tb_type == 'ctb':
                        self.tdSql.error(f'insert into {dbname}.{stbname} values({self.ts},{error_value})')
            elif 'binary' in col_type.lower() or 'nchar' in col_type.lower():
                for error_value in [self.tdCom.get_long_name(str_length)]:
                    self.tdSql.error(f'insert into {dbname}.{tbname} values({self.ts},"{error_value}")')
                    if tb_type == 'ctb':
                        self.tdSql.error(f'insert into {dbname}.{stbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'bool':
                for error_value in [self.tdCom.get_long_name(self.str_length)]:
                    self.tdSql.error(f'insert into {dbname}.{tbname} values({self.ts},{error_value})')
                    if tb_type == 'ctb':
                        self.tdSql.error(f'insert into {dbname}.{stbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'tinyint':
                for error_value in [-128-1,127+1,random.uniform(-3.40E+38,3.40E+38),self.tdCom.get_long_name(self.str_length),True,False]:
                    self.tdSql.error(f'insert into {dbname}.{tbname} values({self.ts},{error_value})')
                    if tb_type == 'ctb':
                        self.tdSql.error(f'insert into {dbname}.{stbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'smallint':
                for error_value in [-32768-1,32767+1,random.uniform(-3.40E+38,3.40E+38),self.tdCom.get_long_name(self.str_length),True,False]:
                    self.tdSql.error(f'insert into {dbname}.{tbname} values({self.ts},{error_value})')
                    if tb_type == 'ctb':
                        self.tdSql.error(f'insert into {dbname}.{stbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'int':
                for error_value in [-2147483648-1,2147483647+1,random.uniform(-3.40E+38,3.40E+38),self.tdCom.get_long_name(self.str_length),True,False]:
                    self.tdSql.error(f'insert into {dbname}.{tbname} values({self.ts},{error_value})')
                    if tb_type == 'ctb':
                        self.tdSql.error(f'insert into {dbname}.{stbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'bigint':
                for error_value in [-9223372036854775808-1,9223372036854775807+1,random.uniform(-3.40E+38,3.40E+38),self.tdCom.get_long_name(self.str_length),True,False]:
                    self.tdSql.error(f'insert into {dbname}.{tbname} values({self.ts},{error_value})')
                    if tb_type == 'ctb':
                        self.tdSql.error(f'insert into {dbname}.{stbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'tinyint unsigned':
                for error_value in [-1,256,random.uniform(-3.40E+38,3.40E+38),self.tdCom.get_long_name(self.str_length),True,False]:
                    self.tdSql.error(f'insert into {dbname}.{tbname} values({self.ts},{error_value})')
                    if tb_type == 'ctb':
                        self.tdSql.error(f'insert into {dbname}.{stbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'smallint unsigned':
                for error_value in [-1,65535+1,random.uniform(-3.40E+38,3.40E+38),self.tdCom.get_long_name(self.str_length),True,False]:
                    self.tdSql.error(f'insert into {dbname}.{tbname} values({self.ts},{error_value})')
                    if tb_type == 'ctb':
                        self.tdSql.error(f'insert into {stbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'int unsigned':
                for error_value in [-1,4294967295+1,random.uniform(-3.40E+38,3.40E+38),self.tdCom.get_long_name(self.str_length),True,False]:
                    self.tdSql.error(f'insert into {dbname}.{tbname} values({self.ts},{error_value})')
                    if tb_type == 'ctb':
                        self.tdSql.error(f'insert into {dbname}.{stbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'bigint unsigned':
                for error_value in [-1,18446744073709551615+1,random.uniform(-3.40E+38,3.40E+38),self.tdCom.get_long_name(self.str_length),True,False]:
                    self.tdSql.error(f'insert into {dbname}.{tbname} values({self.ts},{error_value})')
                    if tb_type == 'ctb':
                        self.tdSql.error(f'insert into {dbname}.{stbname} values({self.ts},{error_value})')
            self.tdSql.execute(f'drop table {dbname}.{tbname}')
            if tb_type == 'ctb':
                self.tdSql.execute(f'drop table {dbname}.{stbname}')
    def update_data_check(self,tbname,column_dict,dbname,tb_type=None,stbname=None):
        up_tinyint = random.randint(-128,127)
        up_smallint = random.randint(-32768,32767)
        up_int = random.randint(-2147483648,2147483647)
        up_bigint = random.randint(-9223372036854775808,9223372036854775807)
        up_untinyint = random.randint(0,255)
        up_unsmallint = random.randint(0,65535)
        up_unint = random.randint(0,4294967295)
        up_unbigint = random.randint(0,18446744073709551615)
        up_bool = random.randint(0,100)%2
        up_float = random.uniform(-3.40E+38,3.40E+38)
        up_double = random.uniform(-1.7E+308*(1E-300),1.7E+308*(1E-300))
        binary_length = random.randint(0,self.str_length)
        nchar_length = random.randint(0,self.str_length)
        up_binary = self.tdCom.get_long_name(binary_length)
        up_nchar = self.tdCom.get_long_name(nchar_length)
        for col_name,col_type in column_dict.items():
            if tb_type == 'ntb':
                self.tdSql.execute(f'create table {dbname}.{tbname} (ts timestamp,{col_name} {col_type})')
            elif tb_type == 'ctb':
                self.tdSql.execute(f'create table {dbname}.{stbname} (ts timestamp,{col_name} {col_type}) tags(t0 int)')
                self.tdSql.execute(f'create table {dbname}.{tbname} using {stbname} tags(1)')
            self.tdSql.execute(f'insert into {dbname}.{tbname} values({self.ts},null)')
            if col_type.lower() == 'tinyint':
                self.update_and_check_data(tbname,col_name,col_type,up_tinyint,dbname)
            elif col_type.lower() == 'smallint':
                self.update_and_check_data(tbname,col_name,col_type,up_smallint,dbname)
            elif col_type.lower() == 'int':
                self.update_and_check_data(tbname,col_name,col_type,up_int,dbname)
            elif col_type.lower() == 'bigint':
                self.update_and_check_data(tbname,col_name,col_type,up_bigint,dbname)
            elif col_type.lower() == 'tinyint unsigned':
                self.update_and_check_data(tbname,col_name,col_type,up_untinyint,dbname)
            elif col_type.lower() == 'smallint unsigned':
                self.update_and_check_data(tbname,col_name,col_type,up_unsmallint,dbname)
            elif col_type.lower() == 'int unsigned':
                self.update_and_check_data(tbname,col_name,col_type,up_unint,dbname)
            elif col_type.lower() == 'bigint unsigned':
                self.update_and_check_data(tbname,col_name,col_type,up_unbigint,dbname)
            elif col_type.lower() == 'bool':
                self.update_and_check_data(tbname,col_name,col_type,up_bool,dbname)
            elif col_type.lower() == 'float':
                self.update_and_check_data(tbname,col_name,col_type,up_float,dbname)
            elif col_type.lower() == 'double':
                self.update_and_check_data(tbname,col_name,col_type,up_double,dbname)
            elif 'binary' in col_type.lower():
                self.update_and_check_data(tbname,col_name,col_type,up_binary,dbname)
            elif 'nchar' in col_type.lower():
                self.update_and_check_data(tbname,col_name,col_type,up_nchar,dbname)
            elif col_type.lower() == 'timestamp':
                self.update_and_check_data(tbname,col_name,col_type,self.ts+1,dbname)
            self.tdSql.execute(f'insert into {dbname}.{tbname} values({self.ts},null)')
            self.tdSql.query(f'select {col_name} from {dbname}.{tbname}')
            self.tdSql.checkEqual(self.tdSql.query_data[0][0],None)
            self.tdSql.execute(f'flush database {self.dbname}')
            self.tdSql.execute('reset query cache')
            self.tdSql.query(f'select {col_name} from {dbname}.{tbname}')
            self.tdSql.checkEqual(self.tdSql.query_data[0][0],None)
            self.tdSql.execute(f'drop table {dbname}.{tbname}')
            if tb_type == 'ctb':
                self.tdSql.execute(f'drop table {dbname}.{stbname}')
    def update_check(self):
        self.tdSql.execute(f'drop database if exists {self.dbname}')
        self.tdSql.execute(f'create database {self.dbname}')
        self.tdSql.execute(f'use {self.dbname}')
        self.update_data_check(self.ntbname,self.column_dict,self.dbname,'ntb')
        for col_name,col_type in self.column_dict.items():
            self.tdSql.execute(f'create table {self.ntbname} (ts timestamp,{col_name} {col_type})')
            self.tdSql.execute(f'insert into {self.ntbname} values({self.ts},null)')
            if 'binary' in col_type.lower():
                up_binary = self.tdCom.get_long_name(self.str_length+1)
                self.tdSql.execute(f'alter table {self.ntbname} modify column {col_name} binary({self.str_length+1})')
                self.update_and_check_data(self.ntbname,col_name,col_type,up_binary,self.dbname)
            elif 'nchar' in col_type.lower():
                up_nchar = self.tdCom.get_long_name(self.str_length+1)
                self.tdSql.execute(f'alter table {self.ntbname} modify column {col_name} nchar({self.str_length+1})')
                self.update_and_check_data(self.ntbname,col_name,col_type,up_nchar,self.dbname)
            self.tdSql.execute(f'drop table {self.ntbname}')
        self.update_data_check(self.ctbname,self.column_dict,self.dbname,'ctb',self.stbname)
        for col_name,col_type in self.column_dict.items():
            self.tdSql.execute(f'create table {self.dbname}.{self.stbname} (ts timestamp,{col_name} {col_type}) tags(t0 int)')
            self.tdSql.execute(f'create table {self.dbname}.{self.ctbname} using {self.stbname} tags(1)')
            self.tdSql.execute(f'insert into {self.dbname}.{self.ctbname} values({self.ts},null)')
            if 'binary' in col_type.lower():
                up_binary = self.tdCom.get_long_name(self.str_length+1)
                self.tdSql.execute(f'alter table {self.dbname}.{self.stbname} modify column {col_name} binary({self.str_length+1})')
                self.update_and_check_data(self.ctbname,col_name,col_type,up_binary,self.dbname)
            elif 'nchar' in col_type.lower():
                up_nchar = self.tdCom.get_long_name(self.str_length+1)
                self.tdSql.execute(f'alter table {self.dbname}.{self.stbname} modify column {col_name} nchar({self.str_length+1})')
                self.update_and_check_data(self.ctbname,col_name,col_type,up_nchar,self.dbname)
            self.tdSql.execute(f'drop table {self.dbname}.{self.stbname}')

    def update_check_error(self):
        self.tdSql.execute(f'drop database if exists {self.dbname}')
        self.tdCom.createDb(self.dbname)
        self.tdSql.execute(f'use {self.dbname}')
        self.error_check(self.ntbname,self.column_dict,self.dbname,'ntb')
        self.error_check(self.ctbname,self.column_dict,self.dbname,'ctb',self.stbname)

    def run(self):
        #!bug TD-17708 and TD-17709
        # for i in range(10):
            self.update_check()
            self.update_check_error()
            # i+=1

    def cleanup(self):
        pass

    def desc(self):
        case_description = """
            update_data check <jiacy>
            """
        return case_description

    def author(self):
        return "Jiacy"

    def tags(self):
        return T.Write.TaoscSql.Insert