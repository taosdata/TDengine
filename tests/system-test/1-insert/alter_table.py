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
from util.log import *
from util.cases import *
from util.sql import *
from util import constant
from util.common import *
from util.sqlset import *

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.setsql = TDSetSql()
        self.ntbname = 'ntb'
        self.stbname = 'stb'
        self.binary_length = 20 # the length of binary for column_dict
        self.nchar_length = 20  # the length of nchar for column_dict
        self.column_dict = {
            'ts'  : 'timestamp',
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
            'col12': f'binary({self.binary_length})',
            'col13': f'nchar({self.nchar_length})'
        }
        self.tag_dict = {
            'ts_tag'  : 'timestamp',
            't1': 'tinyint',
            't2': 'smallint',
            't3': 'int',
            't4': 'bigint',
            't5': 'tinyint unsigned',
            't6': 'smallint unsigned',
            't7': 'int unsigned',
            't8': 'bigint unsigned',
            't9': 'float',
            't10': 'double',
            't11': 'bool',
            't12': f'binary({self.binary_length})',
            't13': f'nchar({self.nchar_length})'
        }
        self.tag_list = [
            f'now,1,2,3,4,5,6,7,8,9.9,10.1,true,"abcd","涛思数据"'
        ]
        self.tbnum = 1
        self.values_list = [
            f'now,1,2,3,4,5,6,7,8,9.9,10.1,true,"abcd","涛思数据"'
        ]
        self.column_add_dict = {
            'col_time'      : 'timestamp',
            'col_tinyint'   : 'tinyint',
            'col_smallint'  : 'smallint',
            'col_int'       : 'int',
            'col_bigint'    : 'bigint',
            'col_untinyint' : 'tinyint unsigned',
            'col_smallint'  : 'smallint unsigned',
            'col_int'       : 'int unsigned',
            'col_bigint'    : 'bigint unsigned',
            'col_bool'      : 'bool',
            'col_float'     : 'float',
            'col_double'    : 'double',
            'col_binary'    : f'binary({constant.BINARY_LENGTH_MAX})',
            'col_nchar'     : f'nchar({constant.NCAHR_LENGTH_MAX})'

        }
    def alter_check_ntb(self):      
        tdSql.prepare()
        tdSql.execute(self.setsql.set_create_normaltable_sql(self.ntbname,self.column_dict))
        for i in self.values_list:
            tdSql.execute(f'insert into {self.ntbname} values({i})')
        for key,values in self.column_add_dict.items():
            tdSql.execute(f'alter table {self.ntbname} add column {key} {values}')
            tdSql.query(f'describe {self.ntbname}')
            tdSql.checkRows(len(self.column_dict)+1)
            tdSql.query(f'select {key} from {self.ntbname}')
            tdSql.checkRows(len(self.values_list))
            tdSql.execute(f'alter table {self.ntbname} drop column {key}')
            tdSql.query(f'describe {self.ntbname}')
            tdSql.checkRows(len(self.column_dict))
            tdSql.error(f'select {key} from {self.ntbname} ')
        for key,values in self.column_dict.items():
            if 'binary' in values.lower():
                v = f'binary({self.binary_length+1})'
                v_error = f'binary({self.binary_length-1})'
                tdSql.error(f'alter table {self.ntbname} modify column {key} {v_error}')
                tdSql.error(f'alter table {self.ntbname} set tag {key} = "abcd1"')
                tdSql.execute(f'alter table {self.ntbname} modify column {key} {v}')
                tdSql.query(f'describe {self.ntbname}')
                result = tdCom.getOneRow(1,'VARCHAR')
                tdSql.checkEqual(result[0][2],self.binary_length+1)
            elif 'nchar' in values.lower():
                v = f'nchar({self.binary_length+1})'
                v_error = f'nchar({self.binary_length-1})'
                tdSql.error(f'alter table {self.ntbname} modify column {key} {v_error}')
                tdSql.error(f'alter table {self.ntbname} set tag {key} = "abcd1"')
                tdSql.execute(f'alter table {self.ntbname} modify column {key} {v}')
                tdSql.query(f'describe {self.ntbname}')
                result = tdCom.getOneRow(1,'NCHAR')
                tdSql.checkEqual(result[0][2],self.binary_length+1)
            else:
                for v in self.column_dict.values():
                    tdSql.error(f'alter table {self.ntbname} modify column {key} {v}')
                    tdSql.error(f'alter table {self.ntbname} set tag {key} = "abcd1"')
        for key,values in self.column_dict.items():
            rename_str = f'{tdCom.getLongName(constant.COL_NAME_LENGTH_MAX,"letters")}'
            tdSql.execute(f'alter table {self.ntbname} rename column {key} {rename_str}')
            tdSql.query(f'select {rename_str} from {self.ntbname}')
            tdSql.checkRows(1)
            tdSql.error(f'select {key} from {self.ntbname}')
        
    def alter_check_tb(self):
        tag_tinyint = random.randint(constant.TINYINT_MIN,constant.TINYINT_MAX)
        tag_smallint = random.randint(constant.SMALLINT_MIN,constant.SMALLINT_MAX)
        tag_int = random.randint(constant.INT_MIN,constant.INT_MAX)
        tag_bigint = random.randint(constant.BIGINT_MIN,constant.BIGINT_MAX)
        tag_untinyint = random.randint(constant.TINYINT_UN_MIN,constant.TINYINT_UN_MAX)
        tag_unsmallint = random.randint(constant.SMALLINT_UN_MIN,constant.SMALLINT_UN_MAX)
        tag_unint = random.randint(constant.INT_UN_MIN,constant.INT_MAX)
        tag_unbigint = random.randint(constant.BIGINT_UN_MIN,constant.BIGINT_UN_MAX)
        tag_bool = random.randint(0,100)%2
        tag_float = random.uniform(constant.FLOAT_MIN,constant.FLOAT_MAX)
        tag_double = random.uniform(constant.DOUBLE_MIN*(1E-300),constant.DOUBLE_MAX*(1E-300))
        tag_binary = tdCom.getLongName(self.binary_length)
        tag_nchar = tdCom.getLongName(self.binary_length)
        modify_column_dict = {
            'ts1'  : 'timestamp',
            'c1': 'tinyint',
            'c2': 'smallint',
            'c3': 'int',
            'c4': 'bigint',
            'c5': 'tinyint unsigned',
            'c6': 'smallint unsigned',
            'c7': 'int unsigned',
            'c8': 'bigint unsigned',
            'c9': 'float',
            'c10': 'double',
            'c11': 'bool',
            'c12': f'binary({self.binary_length})',
            'c13': f'nchar({self.nchar_length})'
        }
        tdSql.prepare()
        tdSql.execute(self.setsql.set_create_stable_sql(self.stbname,self.column_dict,self.tag_dict))
        for i in range(self.tbnum):
            tdSql.execute(f'create table {self.stbname}_{i} using {self.stbname} tags({self.tag_list[i]})')
            for j in self.values_list:
                tdSql.execute(f'insert into {self.stbname}_{i} values({j})')
        for i in range(self.tbnum):
            for k,v in modify_column_dict.items():
                tdSql.error(f'alter table {self.stbname}_{i} add column {k} {v}')
            for k in self.column_dict.keys():
                tdSql.error(f'alter table {self.stbname}_{i} drop column {k}')
            for k,v in self.column_dict.items():
                if 'binary' in v.lower():
                    values = [f'binary({self.binary_length+1})', f'binary({self.binary_length-1})']
                    for j in values:
                        tdSql.error(f'alter table {self.stbname}_{i} modify {k} {j}')
                elif 'nchar' in v.lower():
                    values = [f'nchar({self.nchar_length+1})', f'binary({self.nchar_length-1})']
                    for j in values:
                        tdSql.error(f'alter table {self.stbname}_{i} modify {k} {j}')
                else:
                    for values in self.column_dict.values():
                        tdSql.error(f'alter table {self.stbname}_{i} modify column {k} {values}')
        for k,v in self.tag_dict.items():
            if v.lower() == 'tinyint':
                self.tag_check(i,k,tag_tinyint)
                for error in [constant.TINYINT_MIN-1,constant.TINYINT_MAX+1]:
                    tdSql.error(f'alter table {self.stbname}_{i} set tag {k} = {error}')
            elif v.lower() == 'smallint':
                self.tag_check(i,k,tag_smallint)
                for error in [constant.SMALLINT_MIN-1,constant.SMALLINT_MAX+1]:
                    tdSql.error(f'alter table {self.stbname}_{i} set tag {k} = {error}')
            elif v.lower() == 'int':
                self.tag_check(i,k,tag_int)
                for error in [constant.INT_MIN-1,constant.INT_MAX+1]:
                    tdSql.error(f'alter table {self.stbname}_{i} set tag {k} = {error}')
            elif v.lower() == 'bigint':
                self.tag_check(i,k,tag_bigint)
                for error in [constant.BIGINT_MIN-1,constant.BIGINT_MAX+1]:
                    tdSql.error(f'alter table {self.stbname}_{i} set tag {k} = {error}')
            elif v.lower() == 'tinyint unsigned':
                self.tag_check(i,k,tag_untinyint)
                for error in [constant.TINYINT_UN_MIN-1,constant.TINYINT_UN_MAX+1]:
                    tdSql.error(f'alter table {self.stbname}_{i} set tag {k} = {error}')
            elif v.lower() == 'smallint unsigned':
                self.tag_check(i,k,tag_unsmallint)
                for error in [constant.SMALLINT_UN_MIN-1,constant.SMALLINT_UN_MAX+1]:
                    tdSql.error(f'alter table {self.stbname}_{i} set tag {k} = {error}')
            elif v.lower() == 'int unsigned':
                self.tag_check(i,k,tag_unint)
                for error in [constant.INT_UN_MIN-1,constant.INT_UN_MAX+1]:
                    tdSql.error(f'alter table {self.stbname}_{i} set tag {k} = {error}')  
            #! bug TD-17106
            elif v.lower() == 'bigint unsigned':
                self.tag_check(i,k,tag_unbigint)
                for error in [constant.BIGINT_UN_MIN-1,constant.BIGINT_UN_MAX+1]:
                    tdSql.error(f'alter table {self.stbname}_{i} set tag {k} = {error}') 
            elif v.lower() == 'bool':     
                self.tag_check(i,k,tag_bool)
            elif v.lower() == 'float':
                tdSql.execute(f'alter table {self.stbname}_{i} set tag {k} = {tag_float}')
                tdSql.query(f'select {k} from {self.stbname}_{i}')
                if abs(tdSql.queryResult[0][0] - tag_float)/tag_float<=0.0001:
                    tdSql.checkEqual(tdSql.queryResult[0][0],tdSql.queryResult[0][0])
                else:
                    tdLog.exit(f'select {k} from {self.stbname}_{i},data check failure')
            #! bug TD-17106    
                for error in [constant.FLOAT_MIN*1.1,constant.FLOAT_MAX*1.1]:
                    tdSql.error(f'alter table {self.stbname}_{i} set tag {k} = {error}') 
            elif v.lower() == 'double':
                tdSql.execute(f'alter table {self.stbname}_{i} set tag {k} = {tag_double}')
                tdSql.query(f'select {k} from {self.stbname}_{i}')
                if abs(tdSql.queryResult[0][0] - tag_double)/tag_double<=0.0001:
                    tdSql.checkEqual(tdSql.queryResult[0][0],tdSql.queryResult[0][0])
                else:
                    tdLog.exit(f'select {k} from {self.stbname}_{i},data check failure')
                for error in [constant.DOUBLE_MIN*1.1,constant.DOUBLE_MAX*1.1]:
                    tdSql.error(f'alter table {self.stbname}_{i} set tag {k} = {error}') 
            elif 'binary' in v.lower():
                tag_binary_error = tdCom.getLongName(self.binary_length+1)
                tdSql.error(f'alter table {self.stbname}_{i} set tag {k} = "{tag_binary_error}"')
                tdSql.execute(f'alter table {self.stbname}_{i} set tag {k} = "{tag_binary}"')
                tdSql.query(f'select {k} from {self.stbname}_{i}')
                tdSql.checkData(0,0,tag_binary)
            elif 'nchar' in v.lower():
                tag_nchar_error = tdCom.getLongName(self.nchar_length+1)
                tdSql.error(f'alter table {self.stbname}_{i} set tag {k} = "{tag_nchar_error}"')
                tdSql.execute(f'alter table {self.stbname}_{i} set tag {k} = "{tag_nchar}"')
                tdSql.query(f'select {k} from {self.stbname}_{i}')
                tdSql.checkData(0,0,tag_nchar)
    
    def tag_check(self,tb_no,tag,values):
        tdSql.execute(f'alter table {self.stbname}_{tb_no} set tag {tag} = {values}')
        tdSql.query(f'select {tag} from {self.stbname}_{tb_no}')
        tdSql.checkData(0,0,values)
        tdSql.execute(f'alter table {self.stbname}_{tb_no} set tag {tag} = null')
        tdSql.query(f'select {tag} from {self.stbname}_{tb_no}')
        tdSql.checkData(0,0,None)
    def alter_check_stb(self):
        tdSql.prepare()
        tdSql.execute(self.setsql.set_create_stable_sql(self.stbname,self.column_dict,self.tag_dict))
        for i in range(self.tbnum):
            tdSql.execute(f'create table {self.stbname}_{i} using {self.stbname} tags({self.tag_list[i]})')
            for j in self.values_list:
                tdSql.execute(f'insert into {self.stbname}_{i} values({j})')
        for key,values in self.column_add_dict.items():
            tdSql.execute(f'alter table {self.stbname} add column {key} {values}')
            tdSql.query(f'describe {self.stbname}')
            tdSql.checkRows(len(self.column_dict)+len(self.tag_dict)+1)
            for i in range(self.tbnum):
                tdSql.query(f'describe {self.stbname}_{i}')
                tdSql.checkRows(len(self.column_dict)+len(self.tag_dict)+1)
                tdSql.query(f'select {key} from {self.stbname}_{i}')
                tdSql.checkRows(len(self.values_list))
            tdSql.execute(f'alter table {self.stbname} drop column {key}')
            tdSql.query(f'describe {self.stbname}')
            tdSql.checkRows(len(self.column_dict)+len(self.tag_dict))
            for i in range(self.tbnum):
                tdSql.query(f'describe {self.stbname}_{i}')
                tdSql.checkRows(len(self.column_dict)+len(self.tag_dict))
            tdSql.error(f'select {key} from {self.stbname} ')
        for key,values in self.column_dict.items():
            if 'binary' in values.lower():
                v = f'binary({self.binary_length+1})'
                v_error = f'binary({self.binary_length-1})'
                tdSql.error(f'alter table {self.stbname} modify column {key} {v_error}')
                tdSql.error(f'alter table {self.stbname} set tag {key} = "abcd1"')
                tdSql.execute(f'alter table {self.stbname} modify column {key} {v}')
                tdSql.query(f'describe {self.stbname}')
                result = tdCom.getOneRow(1,'VARCHAR')
                tdSql.checkEqual(result[0][2],self.binary_length+1)
                for i in range(self.tbnum):
                    tdSql.query(f'describe {self.stbname}_{i}')
                    result = tdCom.getOneRow(1,'VARCHAR')
                    tdSql.checkEqual(result[0][2],self.binary_length+1)
            elif 'nchar' in values.lower():
                v = f'nchar({self.binary_length+1})'
                v_error = f'nchar({self.binary_length-1})'
                tdSql.error(f'alter table {self.stbname} modify column {key} {v_error}')
                tdSql.error(f'alter table {self.stbname} set tag {key} = "abcd1"')
                tdSql.execute(f'alter table {self.stbname} modify column {key} {v}')
                tdSql.query(f'describe {self.stbname}')
                result = tdCom.getOneRow(1,'NCHAR')
                tdSql.checkEqual(result[0][2],self.binary_length+1)
                for i in range(self.tbnum):
                    tdSql.query(f'describe {self.stbname}')
                    result = tdCom.getOneRow(1,'NCHAR')
                    tdSql.checkEqual(result[0][2],self.binary_length+1)
            else:
                for v in self.column_dict.values():
                    tdSql.error(f'alter table {self.stbname} modify column {key} {v}')
                    tdSql.error(f'alter table {self.stbname} set tag {key} = "abcd1"')
        for key,values in self.column_dict.items():
            rename_str = f'{tdCom.getLongName(constant.COL_NAME_LENGTH_MAX,"letters")}'
            tdSql.error(f'alter table {self.stbname} rename column {key} {rename_str}')
            for i in range(self.tbnum):
                tdSql.error(f'alter table {self.stbname}_{i} rename column {key} {rename_str}')
    def run(self):
        self.alter_check_ntb()
        self.alter_check_tb()
        self.alter_check_stb()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())