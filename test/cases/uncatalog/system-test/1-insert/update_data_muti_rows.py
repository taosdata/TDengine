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

from new_test_framework.utils import tdLog, tdSql, constant
from new_test_framework.utils.common import tdCom
import random
from random import randint
import os
import time

class TestUpdateDataMutiRows:
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        #tdSql.init(conn.cursor(), logSql))
        cls.dbname = 'db_test'
        cls.ntbname = 'ntb'
        cls.stbname = 'stb'
        cls.rowNum = 10
        cls.tbnum = 5
        cls.ts = 1537146000000
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
            'col13': f'nchar({cls.str_length})'
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
        cls.bool_val = random.randint(0,2)%2
        cls.binary_val = tdCom.getLongName(random.randint(0,cls.str_length))
        cls.nchar_val = tdCom.getLongName(random.randint(0,cls.str_length))
        cls.data = {
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
    def update_data(self,dbname,tbname,tb_num,rows,values,col_type):
        sql = f'insert into '
        for j in range(tb_num):
            sql += f'{dbname}.{tbname}_{j} values'
            for i in range(rows):
                if 'binary' in col_type.lower() or 'nchar' in col_type.lower():
                    sql += f'({self.ts+i},"{values}")'
                else:
                    sql += f'({self.ts+i},{values})'
            sql += ' '
        tdSql.execute(sql)

    def insert_data(self,col_type,tbname,rows,data):
        for i in range(rows):
            if col_type.lower() == 'tinyint':
                tdSql.execute(f'insert into {tbname} values({self.ts+i},{data["tinyint"]})')
            elif col_type.lower() == 'smallint':
                tdSql.execute(f'insert into {tbname} values({self.ts+i},{data["smallint"]})')
            elif col_type.lower() == 'int':
                tdSql.execute(f'insert into {tbname} values({self.ts+i},{data["int"]})')
            elif col_type.lower() == 'bigint':
                tdSql.execute(f'insert into {tbname} values({self.ts+i},{data["bigint"]})')
            elif col_type.lower() == 'tinyint unsigned':
                tdSql.execute(f'insert into {tbname} values({self.ts+i},{data["tinyint unsigned"]})')
            elif col_type.lower() == 'smallint unsigned':
                tdSql.execute(f'insert into {tbname} values({self.ts+i},{data["smallint unsigned"]})')
            elif col_type.lower() == 'int unsigned':
                tdSql.execute(f'insert into {tbname} values({self.ts+i},{data["int unsigned"]})')
            elif col_type.lower() == 'bigint unsigned':
                tdSql.execute(f'insert into {tbname} values({self.ts+i},{data["bigint unsigned"]})')
            elif col_type.lower() == 'bool':
                tdSql.execute(f'insert into {tbname} values({self.ts+i},{data["bool"]})')
            elif col_type.lower() == 'float':
                tdSql.execute(f'insert into {tbname} values({self.ts+i},{data["float"]})')
            elif col_type.lower() == 'double':
                tdSql.execute(f'insert into {tbname} values({self.ts+i},{data["double"]})')
            elif 'binary' in col_type.lower():
                tdSql.execute(f'''insert into {tbname} values({self.ts+i},"{data['binary']}")''')
            elif 'nchar' in col_type.lower():
                tdSql.execute(f'''insert into {tbname} values({self.ts+i},"{data['nchar']}")''')

    def data_check(self,dbname,tbname,tbnum,rownum,data,col_name,col_type):
        if 'binary' in col_type.lower():
            self.update_data(dbname,f'{tbname}',tbnum,rownum,data['binary'],col_type)
        elif 'nchar' in col_type.lower():
            self.update_data(dbname,f'{tbname}',tbnum,rownum,data['nchar'],col_type)
        else:
            self.update_data(dbname,f'{tbname}',tbnum,rownum,data[col_type],col_type)
        tdSql.execute(f'flush database {dbname}')
        tdSql.execute('reset query cache')
        for i in range(self.tbnum):
            tdSql.query(f'select {col_name} from {dbname}.{tbname}_{i}')
            for j in range(rownum):
                if col_type.lower() == 'float' or col_type.lower() == 'double':
                    if abs(tdSql.queryResult[j][0] - data[col_type]) / data[col_type] <= 0.0001:
                        tdSql.checkEqual(tdSql.queryResult[j][0],tdSql.queryResult[j][0])
                elif 'binary' in col_type.lower():
                    tdSql.checkEqual(tdSql.queryResult[j][0],data['binary'])
                elif 'nchar' in col_type.lower():
                    tdSql.checkEqual(tdSql.queryResult[j][0],data['nchar'])
                else:
                    tdSql.checkEqual(tdSql.queryResult[j][0],data[col_type])
    def update_data_ntb(self):
        tdSql.execute(f'drop database if exists {self.dbname}')
        tdSql.execute(f'create database {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        for col_name,col_type in self.column_dict.items():
            for i in range(self.tbnum):
                tdSql.execute(f'create table {self.dbname}.{self.ntbname}_{i} (ts timestamp,{col_name} {col_type})')
                for j in range(self.rowNum):
                    tdSql.execute(f'insert into {self.dbname}.{self.ntbname}_{i} values({self.ts+j},null)' )
            tdSql.execute(f'flush database {self.dbname}')
            tdSql.execute('reset query cache')
            self.data_check(self.dbname,self.ntbname,self.tbnum,self.rowNum,self.data,col_name,col_type)
            for i in range(self.tbnum):
                tdSql.execute(f'drop table {self.ntbname}_{i}')
    def update_data_ctb(self):
        tdSql.execute(f'drop database if exists {self.dbname}')
        tdSql.execute(f'create database {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        for col_name,col_type in self.column_dict.items():
            tdSql.execute(f'create table {self.dbname}.{self.stbname} (ts timestamp,{col_name} {col_type}) tags(t0 int)')
            for i in range(self.tbnum):
                tdSql.execute(f'create table {self.dbname}.{self.stbname}_{i} using {self.dbname}.{self.stbname} tags(1)')
                for j in range(self.rowNum):
                    tdSql.execute(f'insert into {self.dbname}.{self.stbname}_{i} values({self.ts+j},null)' )
            tdSql.execute(f'flush database {self.dbname}')
            tdSql.execute('reset query cache')
            self.data_check(self.dbname,self.stbname,self.tbnum,self.rowNum,self.data,col_name,col_type)
            tdSql.execute(f'drop table {self.stbname}')
    def test_update_data_muti_rows(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
        - xxx:xxx

        History:
        - xxx
        - xxx

        """
        self.update_data_ntb()
        self.update_data_ctb()
        
        tdLog.success(f"{__file__} successfully executed")
