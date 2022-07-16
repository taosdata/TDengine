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
from util import constant
from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.dbname = 'db_test'
        self.ntbname = 'ntb'
        self.stbname = 'stb'
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
        self.tinyint = random.randint(constant.TINYINT_MIN,constant.TINYINT_MAX)
        self.smallint = random.randint(constant.SMALLINT_MIN,constant.SMALLINT_MAX)
        self.int = random.randint(constant.INT_MIN,constant.INT_MAX)
        self.bigint = random.randint(constant.BIGINT_MIN,constant.BIGINT_MAX)
        self.untinyint = random.randint(constant.TINYINT_UN_MIN,constant.TINYINT_UN_MAX)
        self.unsmallint = random.randint(constant.SMALLINT_UN_MIN,constant.SMALLINT_UN_MAX)
        self.unint = random.randint(constant.INT_UN_MIN,constant.INT_MAX)
        self.unbigint = random.randint(constant.BIGINT_UN_MIN,constant.BIGINT_UN_MAX)
        self.bool = random.randint(0,100)%2
        self.float = random.uniform(constant.FLOAT_MIN,constant.FLOAT_MAX)
        self.double = random.uniform(constant.DOUBLE_MIN*(1E-300),constant.DOUBLE_MAX*(1E-300))
        self.binary = tdCom.getLongName(self.str_length)
        self.tnchar = tdCom.getLongName(self.str_length)
    def insert_base_data(self,col_type,tbname,value=None):
        if value == None:
            if col_type.lower() == 'tinyint':
                tdSql.execute(f'insert into {tbname} values({self.ts},{self.tinyint})')
            elif col_type.lower() == 'smallint':
                tdSql.execute(f'insert into {tbname} values({self.ts},{self.smallint})')
            elif col_type.lower() == 'int':
                tdSql.execute(f'insert into {tbname} values({self.ts},{self.int})')
            elif col_type.lower() == 'bigint':
                tdSql.execute(f'insert into {tbname} values({self.ts},{self.bigint})')
            elif col_type.lower() == 'tinyint unsigned':
                tdSql.execute(f'insert into {tbname} values({self.ts},{self.untinyint})')
            elif col_type.lower() == 'smallint unsigned':
                tdSql.execute(f'insert into {tbname} values({self.ts},{self.unsmallint})')
            elif col_type.lower() == 'int unsigned':
                tdSql.execute(f'insert into {tbname} values({self.ts},{self.unint})')
            elif col_type.lower() == 'bigint unsigned':
                tdSql.execute(f'insert into {tbname} values({self.ts},{self.unbigint})')
            elif col_type.lower() == 'bool':
                tdSql.execute(f'insert into {tbname} values({self.ts},{self.bool})')    
            elif col_type.lower() == 'float':
                tdSql.execute(f'insert into {tbname} values({self.ts},{self.float})')      
            elif col_type.lower() == 'double':
                tdSql.execute(f'insert into {tbname} values({self.ts},{self.double})')
            elif 'binary' in col_type.lower():
                tdSql.execute(f'insert into {tbname} values({self.ts},"{self.binary}")')
            elif 'nchar' in col_type.lower():
                tdSql.execute(f'insert into {tbname} values({self.ts},"{self.nchar}")')   
                   
    def update_and_check_data(self,tbname,col_name,col_type,value):
        if 'binary' in col_type.lower() or 'nchar' in col_type.lower():
            tdSql.execute(f'insert into {tbname} values({self.ts},"{value}")')
        else:
            tdSql.execute(f'insert into {tbname} values({self.ts},{value})')
        tdSql.query(f'select {col_name} from {tbname}')
        if col_type.lower() == 'float' or col_type.lower() == 'double':
            if abs(tdSql.queryResult[0][0] - value) / value <= 0.0001:
                tdSql.checkEqual(tdSql.queryResult[0][0],tdSql.queryResult[0][0])
            else:
                tdLog.exit(f'{col_name} data check failure')
        else:
            tdSql.checkEqual(tdSql.queryResult[0][0],value)
    
    def update_check_ntb(self):
        up_tinyint = random.randint(constant.TINYINT_MIN,constant.TINYINT_MAX)
        up_smallint = random.randint(constant.SMALLINT_MIN,constant.SMALLINT_MAX)
        up_int = random.randint(constant.INT_MIN,constant.INT_MAX)
        up_bigint = random.randint(constant.BIGINT_MIN,constant.BIGINT_MAX)
        up_untinyint = random.randint(constant.TINYINT_UN_MIN,constant.TINYINT_UN_MAX)
        up_unsmallint = random.randint(constant.SMALLINT_UN_MIN,constant.SMALLINT_UN_MAX)
        up_unint = random.randint(constant.INT_UN_MIN,constant.INT_MAX)
        up_unbigint = random.randint(constant.BIGINT_UN_MIN,constant.BIGINT_UN_MAX)
        up_bool = random.randint(0,100)%2
        up_float = random.uniform(constant.FLOAT_MIN,constant.FLOAT_MAX)
        up_double = random.uniform(constant.DOUBLE_MIN*(1E-300),constant.DOUBLE_MAX*(1E-300))
        binary_length = random.randint(0,self.str_length)
        nchar_length = random.randint(0,self.str_length)
        up_binary = tdCom.getLongName(binary_length)
        up_nchar = tdCom.getLongName(nchar_length)
        tdSql.execute(f'drop database if exists {self.dbname}')
        tdSql.execute(f'create database {self.dbname}')
        tdSql.execute(f'use {self.dbname}')

        for col_name,col_type in self.column_dict.items():
            tdSql.execute(f'create table {self.ntbname} (ts timestamp,{col_name} {col_type})')
            self.insert_base_data(col_name,self.ntbname)
            if col_type.lower() == 'tinyint':
                self.update_and_check_data(self.ntbname,col_name,col_type,up_tinyint)
            elif col_type.lower() == 'smallint':
                self.update_and_check_data(self.ntbname,col_name,col_type,up_smallint)
            elif col_type.lower() == 'int':
                self.update_and_check_data(self.ntbname,col_name,col_type,up_int)
            elif col_type.lower() == 'bigint':
                self.update_and_check_data(self.ntbname,col_name,col_type,up_bigint)
            elif col_type.lower() == 'tinyint unsigned':
                self.update_and_check_data(self.ntbname,col_name,col_type,up_untinyint)
            elif col_type.lower() == 'smallint unsigned':
                self.update_and_check_data(self.ntbname,col_name,col_type,up_unsmallint)
            elif col_type.lower() == 'int unsigned':
                self.update_and_check_data(self.ntbname,col_name,col_type,up_unint)
            elif col_type.lower() == 'bigint unsigned':
                self.update_and_check_data(self.ntbname,col_name,col_type,up_unbigint)
            elif col_type.lower() == 'bool':
                self.update_and_check_data(self.ntbname,col_name,col_type,up_bool)    
            elif col_type.lower() == 'float':
                self.update_and_check_data(self.ntbname,col_name,col_type,up_float)      
            elif col_type.lower() == 'double':
                self.update_and_check_data(self.ntbname,col_name,col_type,up_double)
            elif 'binary' in col_type.lower():
                self.update_and_check_data(self.ntbname,col_name,col_type,up_binary)
            elif 'nchar' in col_type.lower():
                self.update_and_check_data(self.ntbname,col_name,col_type,up_nchar)
            tdSql.execute(f'drop table {self.ntbname}')
        for col_name,col_type in self.column_dict.items():
            tdSql.execute(f'create table {self.ntbname} (ts timestamp,{col_name} {col_type})')
            self.insert_base_data(col_name,self.ntbname)
            tdSql.execute(f'insert into {self.ntbname} values({self.ts},null)')
            tdSql.query(f'select {col_name} from {self.ntbname}')
            tdSql.checkEqual(tdSql.queryResult[0][0],None)
            tdSql.execute(f'drop table {self.ntbname}')


    def update_check_error_ntb(self):
        tdSql.execute(f'drop database if exists {self.dbname}')
        tdSql.execute(f'create database {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        str_length = self.str_length+1
        for col_name,col_type in self.column_dict.items():
            tdSql.execute(f'create table {self.ntbname} (ts timestamp,{col_name} {col_type})')
            self.insert_base_data(col_name,self.ntbname)
            if col_type.lower() == 'double':
                for error_value in [tdCom.getLongName(self.str_length),True,False,1.1*constant.DOUBLE_MIN,1.1*constant.DOUBLE_MAX]:
                    tdSql.error(f'insert into {self.ntbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'float':
                for error_value in [tdCom.getLongName(self.str_length),True,False,1.1*constant.FLOAT_MIN,1.1*constant.FLOAT_MAX]:
                    tdSql.error(f'insert into {self.ntbname} values({self.ts},{error_value})')
            elif 'binary' in col_type.lower() or 'nchar' in col_type.lower():
                for error_value in [tdCom.getLongName(str_length)]:
                    tdSql.error(f'insert into {self.ntbname} values({self.ts},"{error_value}")')
            elif col_type.lower() == 'bool':
                for error_value in [tdCom.getLongName(self.str_length)]:
                    tdSql.error(f'insert into {self.ntbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'tinyint':
                for error_value in [constant.TINYINT_MIN-1,constant.TINYINT_MAX+1,random.uniform(constant.FLOAT_MIN,constant.FLOAT_MAX),tdCom.getLongName(self.str_length),True,False]:
                    tdSql.error(f'insert into {self.ntbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'smallint':
                for error_value in [constant.SMALLINT_MIN-1,constant.SMALLINT_MAX+1,random.uniform(constant.FLOAT_MIN,constant.FLOAT_MAX),tdCom.getLongName(self.str_length),True,False]:
                    tdSql.error(f'insert into {self.ntbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'int':
                for error_value in [constant.INT_MIN-1,constant.INT_MAX+1,random.uniform(constant.FLOAT_MIN,constant.FLOAT_MAX),tdCom.getLongName(self.str_length),True,False]:
                    tdSql.error(f'insert into {self.ntbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'bigint':
                for error_value in [constant.BIGINT_MIN-1,constant.BIGINT_MAX+1,random.uniform(constant.FLOAT_MIN,constant.FLOAT_MAX),tdCom.getLongName(self.str_length),True,False]:
                    tdSql.error(f'insert into {self.ntbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'tinyint unsigned':
                for error_value in [constant.TINYINT_UN_MIN-1,constant.TINYINT_UN_MAX+1,random.uniform(constant.FLOAT_MIN,constant.FLOAT_MAX),tdCom.getLongName(self.str_length),True,False]:
                    tdSql.error(f'insert into {self.ntbname} values({self.ts},{error_value})')     
            elif col_type.lower() == 'smallint unsigned':
                for error_value in [constant.SMALLINT_UN_MIN-1,constant.SMALLINT_UN_MAX+1,random.uniform(constant.FLOAT_MIN,constant.FLOAT_MAX),tdCom.getLongName(self.str_length),True,False]:
                    tdSql.error(f'insert into {self.ntbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'int unsigned':
                for error_value in [constant.INT_UN_MIN-1,constant.INT_UN_MAX+1,random.uniform(constant.FLOAT_MIN,constant.FLOAT_MAX),tdCom.getLongName(self.str_length),True,False]:
                    tdSql.error(f'insert into {self.ntbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'bigint unsigned':
                for error_value in [constant.BIGINT_UN_MIN-1,constant.BIGINT_UN_MAX+1,random.uniform(constant.FLOAT_MIN,constant.FLOAT_MAX),tdCom.getLongName(self.str_length),True,False]:
                    tdSql.error(f'insert into {self.ntbname} values({self.ts},{error_value})')           
            tdSql.execute(f'drop table {self.ntbname}')

    def run(self):
        self.update_check_ntb()
        self.update_check_error_ntb()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())