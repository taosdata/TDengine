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

from util.log import *
from util.cases import *
from util.sql import *
import numpy as np
from util.sqlset import TDSetSql

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(),True)
        self.rowNum = 10
        self.ts = 1537146000000
        self.setsql = TDSetSql()
        self.dbname = "db"
        self.ntbname = f"{self.dbname}.ntb"
        self.stbname = f'{self.dbname}.stb'
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
        self.binary_str = 'taosdata'
        self.nchar_str = '涛思数据'
        self.tbnum = 2
        self.tag_ts = self.ts
        self.tag_tinyint = 1
        self.tag_smallint = 2
        self.tag_int = 3
        self.tag_bigint = 4
        self.tag_utint = 5
        self.tag_usint = 6
        self.tag_uint = 7
        self.tag_ubint = 8
        self.tag_float = 9.1
        self.tag_double = 10.1
        self.tag_bool = True
        self.tag_values = [
            f'{self.tag_ts},{self.tag_tinyint},{self.tag_smallint},{self.tag_int},{self.tag_bigint},\
            {self.tag_utint},{self.tag_usint},{self.tag_uint},{self.tag_ubint},{self.tag_float},{self.tag_double},{self.tag_bool},"{self.binary_str}","{self.nchar_str}"'

        ]

        self.percent = [1,50,100]
        self.param_list = ['default','t-digest']
    def insert_data(self,column_dict,tbname,row_num):
        insert_sql = self.setsql.set_insertsql(column_dict,tbname,self.binary_str,self.nchar_str)
        for i in range(row_num):
            insert_list = []
            self.setsql.insert_values(column_dict,i,insert_sql,insert_list,self.ts)


    def function_check_ntb(self):
        tdSql.prepare()
        tdSql.execute(self.setsql.set_create_normaltable_sql(self.ntbname,self.column_dict))
        self.insert_data(self.column_dict,self.ntbname,self.rowNum)
        for k,v in self.column_dict.items():
            for percent in self.percent:
                for param in self.param_list:
                    if v.lower() in ['timestamp','bool'] or 'binary' in v.lower() or 'nchar' in v.lower():
                        tdSql.error(f'select apercentile({k},{percent},"{param}") from {self.ntbname}')
                    else:
                        tdSql.query(f"select apercentile({k},{percent},'{param}') from {self.ntbname}")
    def function_check_stb(self):
        tdSql.prepare()
        tdSql.execute(self.setsql.set_create_stable_sql(self.stbname,self.column_dict,self.tag_dict))
        for i in range(self.tbnum):
            tdSql.execute(f"create table {self.stbname}_{i} using {self.stbname} tags({self.tag_values[0]})")
            self.insert_data(self.column_dict,f'{self.stbname}_{i}',self.rowNum)
        for i in range(self.tbnum):
            for k,v in self.column_dict.items():
                for percent in self.percent:
                    for param in self.param_list:
                        if v.lower() in ['timestamp','bool'] or 'binary' in v.lower() or 'nchar' in v.lower():
                            tdSql.error(f'select apercentile({k},{percent},"{param}") from {self.stbname}_{i}')
                        else:
                            tdSql.query(f"select apercentile({k},{percent},'{param}') from {self.stbname}_{i}")
        for k,v in self.column_dict.items():
                for percent in self.percent:
                    for param in self.param_list:
                        if v.lower() in ['timestamp','bool'] or 'binary' in v.lower() or 'nchar' in v.lower():
                            tdSql.error(f'select apercentile({k},{percent},"{param}") from {self.stbname}')
                        else:
                            tdSql.query(f"select apercentile({k},{percent},'{param}') from {self.stbname}")
    def run(self):
        self.function_check_ntb()
        self.function_check_stb()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
