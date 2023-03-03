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
from util.sqlset import *
from util import constant
from util.common import *
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

    def alter_stable_check(self):
        tdSql.prepare()
        tdSql.execute(self.setsql.set_create_stable_sql(self.stbname,self.column_dict,self.tag_dict))
        tdSql.execute(self.setsql.set_create_normaltable_sql(self.ntbname,self.column_dict))
        for i in self.values_list:
            tdSql.execute(f'insert into {self.ntbname} values({i})')
        for i in range(self.tbnum):
            tdSql.execute(f'create table {self.stbname}_{i} using {self.stbname} tags({self.tag_list[i]})')
            for j in self.values_list:
                tdSql.execute(f'insert into {self.stbname}_{i} values({j})')
        for key,values in self.column_add_dict.items():
            tdSql.execute(f'alter stable {self.stbname} add column {key} {values}')
            tdSql.query(f'describe {self.stbname}')
            tdSql.checkRows(len(self.column_dict)+len(self.tag_dict)+1)
            for i in range(self.tbnum):
                tdSql.query(f'describe {self.stbname}_{i}')
                tdSql.checkRows(len(self.column_dict)+len(self.tag_dict)+1)
                tdSql.query(f'select {key} from {self.stbname}_{i}')
                tdSql.checkRows(len(self.values_list))
            for i in range(self.tbnum):
                tdSql.error(f'alter stable {self.stbname}_{i} add column {key} {values}')
                tdSql.error(f'alter stable {self.stbname}_{i} drop column {key}')
            #! bug TD-16921
            tdSql.error(f'alter stable {self.ntbname} add column {key} {values}')
            tdSql.error(f'alter stable {self.ntbname} drop column {key}')
            tdSql.execute(f'alter stable {self.stbname} drop column {key}')
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
                tdSql.error(f'alter stable {self.stbname} modify column {key} {v_error}')
                tdSql.execute(f'alter stable {self.stbname} modify column {key} {v}')
                tdSql.query(f'describe {self.stbname}')
                result = tdCom.getOneRow(1,'VARCHAR')
                tdSql.checkEqual(result[0][2],self.binary_length+1)
                for i in range(self.tbnum):
                    tdSql.query(f'describe {self.stbname}_{i}')
                    result = tdCom.getOneRow(1,'VARCHAR')
                    tdSql.checkEqual(result[0][2],self.binary_length+1)
                    tdSql.error(f'alter stable {self.stbname}_{i} modify column {key} {v}')
               #! bug TD-16921
                tdSql.error(f'alter stable {self.ntbname} modify column {key} {v}')
            elif 'nchar' in values.lower():
                v = f'nchar({self.binary_length+1})'
                v_error = f'nchar({self.binary_length-1})'
                tdSql.error(f'alter stable {self.stbname} modify column {key} {v_error}')
                tdSql.execute(f'alter stable {self.stbname} modify column {key} {v}')
                tdSql.query(f'describe {self.stbname}')
                result = tdCom.getOneRow(1,'NCHAR')
                tdSql.checkEqual(result[0][2],self.binary_length+1)
                for i in range(self.tbnum):
                    tdSql.query(f'describe {self.stbname}_{i}')
                    result = tdCom.getOneRow(1,'NCHAR')
                    tdSql.checkEqual(result[0][2],self.binary_length+1)
                    tdSql.error(f'alter stable {self.stbname}_{i} modify column {key} {v}')
                #! bug TD-16921
                tdSql.error(f'alter stable {self.ntbname} modify column {key} {v}')
            else:
                for v in self.column_dict.values():
                    tdSql.error(f'alter stable {self.stbname} modify column {key} {v}')
                    tdSql.error(f'alter stable {self.ntbname} modify column {key} {v}')
                    for i in range(self.tbnum):
                        tdSql.error(f'alter stable {self.stbname}_{i} modify column {key} {v}')
    def run(self):

        self.alter_stable_check()
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
