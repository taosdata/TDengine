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
from datetime import datetime
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
        tdSql.init(conn.cursor())
        self.setsql = TDSetSql()
        self.dbname = 'db_test'
        self.ntbname = 'ntb'
        self.stbname = 'stb'
        self.ctbname = 'ctb'
        self.ts = 1537146000000
        self.str_length = 20
        self.block_update_times = 10000
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
        self.tag_dict = {
            't0':'int'
        }
        # The number of tag_values should be same as tbnum
        self.tag_values = [
            f'10',
            f'100'
        ]

    def data_check(self,tbname,col_name,col_type,value):
        tdSql.query(f'select {col_name} from {tbname}')
        if col_type.lower() == 'float' or col_type.lower() == 'double':
            if abs(tdSql.queryResult[0][0] - value) / value <= 0.0001:
                tdSql.checkEqual(tdSql.queryResult[0][0],tdSql.queryResult[0][0])
            else:
                tdLog.exit(f'{col_name} data check failure')
        elif col_type.lower() == 'timestamp':
            tdSql.checkEqual(str(tdSql.queryResult[0][0]),str(datetime.fromtimestamp(value/1000).strftime("%Y-%m-%d %H:%M:%S.%f")))
        else:
            tdSql.checkEqual(tdSql.queryResult[0][0],value)
    def update_and_check_data(self,tbname,col_name,col_type,value,dbname):
        if 'binary' in col_type.lower() or 'nchar' in col_type.lower():
            tdSql.execute(f'insert into {tbname} values({self.ts},"{value}")')
        else:
            tdSql.execute(f'insert into {tbname} values({self.ts},{value})')
        self.data_check(tbname,col_name,col_type,value)
        tdSql.execute(f'flush database {dbname}')
        tdSql.execute('reset query cache')
        self.data_check(tbname,col_name,col_type,value)
        for func in ['first','last']:
            tdSql.execute(f'select {func}({col_name}) from {tbname}')
    def error_check(self,tbname,column_dict,tb_type=None,stbname=None):
        str_length = self.str_length+1
        for col_name,col_type in column_dict.items():
            if tb_type == 'ntb':
                tdSql.execute(f'create table {tbname} (ts timestamp,{col_name} {col_type})')
            elif tb_type == 'ctb':
                tdSql.execute(f'create table {stbname} (ts timestamp,{col_name} {col_type}) tags(t0 int)')
                tdSql.execute(f'create table {tbname} using {stbname} tags(1)')
            tdSql.execute(f'insert into {tbname} values({self.ts},null)')
            if col_type.lower() == 'double':
                for error_value in [tdCom.getLongName(self.str_length),True,False,1.1*constant.DOUBLE_MIN,1.1*constant.DOUBLE_MAX]:
                    tdSql.error(f'insert into {tbname} values({self.ts},{error_value})')
                    if tb_type == 'ctb':
                        tdSql.error(f'insert into {stbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'float':
                for error_value in [tdCom.getLongName(self.str_length),True,False,1.1*constant.FLOAT_MIN,1.1*constant.FLOAT_MAX]:
                    tdSql.error(f'insert into {tbname} values({self.ts},{error_value})')
                    if tb_type == 'ctb':
                        tdSql.error(f'insert into {stbname} values({self.ts},{error_value})')
            elif 'binary' in col_type.lower() or 'nchar' in col_type.lower():
                for error_value in [tdCom.getLongName(str_length)]:
                    tdSql.error(f'insert into {tbname} values({self.ts},"{error_value}")')
                    if tb_type == 'ctb':
                        tdSql.error(f'insert into {stbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'bool':
                for error_value in [tdCom.getLongName(self.str_length)]:
                    tdSql.error(f'insert into {tbname} values({self.ts},{error_value})')
                    if tb_type == 'ctb':
                        tdSql.error(f'insert into {stbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'tinyint':
                for error_value in [constant.TINYINT_MIN-1,constant.TINYINT_MAX+1,random.uniform(constant.FLOAT_MIN,constant.FLOAT_MAX),tdCom.getLongName(self.str_length),True,False]:
                    tdSql.error(f'insert into {tbname} values({self.ts},{error_value})')
                    if tb_type == 'ctb':
                        tdSql.error(f'insert into {stbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'smallint':
                for error_value in [constant.SMALLINT_MIN-1,constant.SMALLINT_MAX+1,random.uniform(constant.FLOAT_MIN,constant.FLOAT_MAX),tdCom.getLongName(self.str_length),True,False]:
                    tdSql.error(f'insert into {tbname} values({self.ts},{error_value})')
                    if tb_type == 'ctb':
                        tdSql.error(f'insert into {stbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'int':
                for error_value in [constant.INT_MIN-1,constant.INT_MAX+1,random.uniform(constant.FLOAT_MIN,constant.FLOAT_MAX),tdCom.getLongName(self.str_length),True,False]:
                    tdSql.error(f'insert into {tbname} values({self.ts},{error_value})')
                    if tb_type == 'ctb':
                        tdSql.error(f'insert into {stbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'bigint':
                for error_value in [constant.BIGINT_MIN-1,constant.BIGINT_MAX+1,random.uniform(constant.FLOAT_MIN,constant.FLOAT_MAX),tdCom.getLongName(self.str_length),True,False]:
                    tdSql.error(f'insert into {tbname} values({self.ts},{error_value})')
                    if tb_type == 'ctb':
                        tdSql.error(f'insert into {stbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'tinyint unsigned':
                for error_value in [constant.TINYINT_UN_MIN-1,constant.TINYINT_UN_MAX+1,random.uniform(constant.FLOAT_MIN,constant.FLOAT_MAX),tdCom.getLongName(self.str_length),True,False]:
                    tdSql.error(f'insert into {tbname} values({self.ts},{error_value})')
                    if tb_type == 'ctb':
                        tdSql.error(f'insert into {stbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'smallint unsigned':
                for error_value in [constant.SMALLINT_UN_MIN-1,constant.SMALLINT_UN_MAX+1,random.uniform(constant.FLOAT_MIN,constant.FLOAT_MAX),tdCom.getLongName(self.str_length),True,False]:
                    tdSql.error(f'insert into {tbname} values({self.ts},{error_value})')
                    if tb_type == 'ctb':
                        tdSql.error(f'insert into {stbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'int unsigned':
                for error_value in [constant.INT_UN_MIN-1,constant.INT_UN_MAX+1,random.uniform(constant.FLOAT_MIN,constant.FLOAT_MAX),tdCom.getLongName(self.str_length),True,False]:
                    tdSql.error(f'insert into {tbname} values({self.ts},{error_value})')
                    if tb_type == 'ctb':
                        tdSql.error(f'insert into {stbname} values({self.ts},{error_value})')
            elif col_type.lower() == 'bigint unsigned':
                for error_value in [constant.BIGINT_UN_MIN-1,constant.BIGINT_UN_MAX+1,random.uniform(constant.FLOAT_MIN,constant.FLOAT_MAX),tdCom.getLongName(self.str_length),True,False]:
                    tdSql.error(f'insert into {tbname} values({self.ts},{error_value})')
                    if tb_type == 'ctb':
                        tdSql.error(f'insert into {stbname} values({self.ts},{error_value})')
            tdSql.execute(f'drop table {tbname}')
            if tb_type == 'ctb':
                tdSql.execute(f'drop table {stbname}')
    def update_data_check(self,tbname,column_dict,dbname,tb_type=None,stbname=None):
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
        binary_length = []
        for i in range(self.str_length+1):
            binary_length.append(i)
        nchar_length = []
        for i in range(self.str_length+1):
            nchar_length.append(i)
        for col_name,col_type in column_dict.items():
            if tb_type == 'ntb':
                tdSql.execute(f'create table {tbname} (ts timestamp,{col_name} {col_type})')
            elif tb_type == 'ctb':
                tdSql.execute(f'create table {stbname} (ts timestamp,{col_name} {col_type}) tags(t0 int)')
                tdSql.execute(f'create table {tbname} using {stbname} tags(1)')
            tdSql.execute(f'insert into {tbname} values({self.ts},null)')
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
                for i in binary_length:
                    up_binary = tdCom.getLongName(i)
                    self.update_and_check_data(tbname,col_name,col_type,up_binary,dbname)
            elif 'nchar' in col_type.lower():
                for i in nchar_length:
                    up_nchar = tdCom.getLongName(i)
                    self.update_and_check_data(tbname,col_name,col_type,up_nchar,dbname)
            elif col_type.lower() == 'timestamp':
                self.update_and_check_data(tbname,col_name,col_type,self.ts+1,dbname)
            tdSql.execute(f'insert into {tbname} values({self.ts},null)')
            tdSql.query(f'select {col_name} from {tbname}')
            tdSql.checkEqual(tdSql.queryResult[0][0],None)
            tdSql.execute(f'flush database {self.dbname}')
            tdSql.execute('reset query cache')
            tdSql.query(f'select {col_name} from {tbname}')
            tdSql.checkEqual(tdSql.queryResult[0][0],None)
            tdSql.execute(f'drop table {tbname}')
            if tb_type == 'ctb':
                tdSql.execute(f'drop table {stbname}')
    def update_check(self):
        tdSql.execute(f'drop database if exists {self.dbname}')
        tdSql.execute(f'create database {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        self.update_data_check(self.ntbname,self.column_dict,self.dbname,'ntb')
        for col_name,col_type in self.column_dict.items():
            tdSql.execute(f'create table {self.ntbname} (ts timestamp,{col_name} {col_type})')
            tdSql.execute(f'insert into {self.ntbname} values({self.ts},null)')
            if 'binary' in col_type.lower():
                up_binary = tdCom.getLongName(self.str_length+1)
                tdSql.execute(f'alter table {self.ntbname} modify column {col_name} binary({self.str_length+1})')
                self.update_and_check_data(self.ntbname,col_name,col_type,up_binary,self.dbname)
            elif 'nchar' in col_type.lower():
                up_nchar = tdCom.getLongName(self.str_length+1)
                tdSql.execute(f'alter table {self.ntbname} modify column {col_name} nchar({self.str_length+1})')
                self.update_and_check_data(self.ntbname,col_name,col_type,up_nchar,self.dbname)
            tdSql.execute(f'drop table {self.ntbname}')
        self.update_data_check(self.ctbname,self.column_dict,self.dbname,'ctb',self.stbname)
        for col_name,col_type in self.column_dict.items():
            tdSql.execute(f'create table {self.stbname} (ts timestamp,{col_name} {col_type}) tags(t0 int)')
            tdSql.execute(f'create table {self.ctbname} using {self.stbname} tags(1)')
            tdSql.execute(f'insert into {self.ctbname} values({self.ts},null)')
            if 'binary' in col_type.lower():
                up_binary = tdCom.getLongName(self.str_length+1)
                tdSql.execute(f'alter table {self.stbname} modify column {col_name} binary({self.str_length+1})')
                self.update_and_check_data(self.ctbname,col_name,col_type,up_binary,self.dbname)
            elif 'nchar' in col_type.lower():
                up_nchar = tdCom.getLongName(self.str_length+1)
                tdSql.execute(f'alter table {self.stbname} modify column {col_name} nchar({self.str_length+1})')
                self.update_and_check_data(self.ctbname,col_name,col_type,up_nchar,self.dbname)
            tdSql.execute(f'drop table {self.stbname}')

    def update_check_error(self):
        tdSql.execute(f'drop database if exists {self.dbname}')
        tdSql.execute(f'create database {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        self.error_check(self.ntbname,self.column_dict,'ntb')
        self.error_check(self.ctbname,self.column_dict,'ctb',self.stbname)

    def update_10000times_and_query(self):
        new_column_dict = {"ts": "timestamp"}
        new_column_dict.update(self.column_dict)
        tdSql.execute(f'drop database if exists {self.dbname}')
        tdSql.execute(f'create database {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(self.setsql.set_create_stable_sql(self.stbname,new_column_dict,self.tag_dict))
        tdSql.execute(f'create table {self.stbname}_1 using {self.stbname} tags({self.tag_values[0]})')
        tdSql.execute(f'insert into {self.stbname}_1 values ({self.ts}, {random.randint(1, 127)}, {random.randint(1, 127)}, {random.randint(1, 127)}, {random.randint(1, 127)}, {random.randint(1, 127)}, {random.randint(1, 127)}, {random.randint(1, 127)}, {random.randint(1, 127)}, {random.randint(1, 127)}, {random.randint(1, 127)}, {random.choice(["True", "FALSE"])}, {random.randint(1, 127)}, {random.randint(1, 127)}, now)')
        for i in range(self.block_update_times):
            tdSql.execute(f'insert into {self.stbname}_1 values ({self.ts}, {random.randint(1, 127)}, {random.randint(1, 127)}, {random.randint(1, 127)}, {random.randint(1, 127)}, {random.randint(1, 127)}, {random.randint(1, 127)}, {random.randint(1, 127)}, {random.randint(1, 127)}, {random.randint(1, 127)}, {random.randint(1, 127)}, {random.choice(["True", "FALSE"])}, {random.randint(1, 127)}, {random.randint(1, 127)}, now)')
        tdSql.query(f'select count(*) from {self.stbname}')
        tdSql.query(f'select * from {self.stbname}')

    def run(self):
        #!bug TD-17708 and TD-17709
        # for i in range(10):
            self.update_check()
            self.update_check_error()
            self.update_10000times_and_query()
            # i+=1

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
