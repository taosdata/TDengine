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
        tdSql.init(conn.cursor())

        self.rowNum = 10
        self.ts = 1537146000000
        self.setsql = TDSetSql()
        self.dbname = 'db'
        self.ntbname = f'{self.dbname}.ntb'
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

        self.param = [1,50,100]

    def insert_data(self,column_dict,tbname,row_num):
        intData = []
        floatData = []
        insert_sql = self.setsql.set_insertsql(column_dict,tbname,self.binary_str,self.nchar_str)
        for i in range(row_num):
            insert_list = []
            self.setsql.insert_values(column_dict,i,insert_sql,insert_list,self.ts)
            intData.append(i)
            floatData.append(i + 0.1)
        return intData,floatData
    def check_tags(self,tags,param,num,value):
        tdSql.query(f'select percentile({tags}, {param}) from {self.stbname}_{num}')
        tdSql.checkEqual(tdSql.queryResult[0][0], value)
    def function_check_ntb(self):
        tdSql.execute(f'create database {self.dbname}')
        tdSql.execute(self.setsql.set_create_normaltable_sql(self.ntbname,self.column_dict))
        intData,floatData = self.insert_data(self.column_dict,self.ntbname,self.rowNum)
        for k,v in self.column_dict.items():
            for param in self.param:
                if v.lower() in ['timestamp','bool'] or 'binary' in v.lower() or 'nchar' in v.lower():
                    tdSql.error(f'select percentile({k},{param}) from {self.ntbname}')
                elif v.lower() in ['tinyint','smallint','int','bigint','tinyint unsigned','smallint unsigned','int unsigned','bigint unsigned']:
                    tdSql.query(f'select percentile({k}, {param}) from {self.ntbname}')
                    tdSql.checkData(0, 0, np.percentile(intData, param))
                else:
                    tdSql.query(f'select percentile({k}, {param}) from {self.ntbname}')
                    tdSql.checkData(0, 0, np.percentile(floatData, param))

        tdSql.query(f'select percentile(col1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100) from {self.ntbname}')
        tdSql.checkData(0, 0, '[0.900000, 1.800000, 2.700000, 3.600000, 4.500000, 5.400000, 6.300000, 7.200000, 8.100000, 9.000000]')

        tdSql.query(f'select percentile(col1, 9.9, 19.9, 29.9, 39.9, 49.9, 59.9, 69.9, 79.9, 89.9, 99.9) from {self.ntbname}')
        tdSql.checkData(0, 0, '[0.891000, 1.791000, 2.691000, 3.591000, 4.491000, 5.391000, 6.291000, 7.191000, 8.091000, 8.991000]')

        tdSql.error(f'select percentile(col1) from {self.ntbname}')
        tdSql.error(f'select percentile(col1, -1) from {self.ntbname}')
        tdSql.error(f'select percentile(col1, 101) from {self.ntbname}')
        tdSql.error(f'select percentile(col1, col2) from {self.ntbname}')
        tdSql.error(f'select percentile(1, col1) from {self.ntbname}')
        tdSql.error(f'select percentile(col1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 101) from {self.ntbname}')

        tdSql.execute(f'drop database {self.dbname}')
    def function_check_ctb(self):
        tdSql.execute(f'create database {self.dbname}')
        tdSql.execute(self.setsql.set_create_stable_sql(self.stbname,self.column_dict,self.tag_dict))
        for i in range(self.tbnum):
            tdSql.execute(f"create table {self.stbname}_{i} using {self.stbname} tags({self.tag_values[0]})")
            intData,floatData = self.insert_data(self.column_dict,f'{self.stbname}_{i}',self.rowNum)
        for i in range(self.tbnum):
            for k,v in self.column_dict.items():
                for param in self.param:
                    if v.lower() in ['timestamp','bool'] or 'binary' in v.lower() or 'nchar' in v.lower():
                        tdSql.error(f'select percentile({k},{param}) from {self.stbname}_{i}')
                    elif v.lower() in ['tinyint','smallint','int','bigint','tinyint unsigned','smallint unsigned','int unsigned','bigint unsigned']:
                        tdSql.query(f'select percentile({k}, {param}) from {self.stbname}_{i}')
                        tdSql.checkData(0, 0, np.percentile(intData, param))
                    else:
                        tdSql.query(f'select percentile({k}, {param}) from {self.stbname}_{i}')
                        tdSql.checkData(0, 0, np.percentile(floatData, param))

            for k,v in self.tag_dict.items():
                for param in self.param:
                    if v.lower() in ['timestamp','bool'] or 'binary' in v.lower() or 'nchar' in v.lower():
                        tdSql.error(f'select percentile({k},{param}) from {self.stbname}_{i}')
                    else:
                        tdSql.query(f'select {k} from {self.stbname}_{i}')
                        data_num = tdSql.queryResult[0][0]
                        tdSql.query(f'select percentile({k},{param}) from {self.stbname}_{i}')
                        tdSql.checkData(0,0,data_num)

        tdSql.query(f'select percentile(col1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100) from {self.stbname}_0')
        tdSql.checkData(0, 0, '[0.900000, 1.800000, 2.700000, 3.600000, 4.500000, 5.400000, 6.300000, 7.200000, 8.100000, 9.000000]')

        tdSql.query(f'select percentile(col1, 9.9, 19.9, 29.9, 39.9, 49.9, 59.9, 69.9, 79.9, 89.9, 99.9) from {self.stbname}_0')
        tdSql.checkData(0, 0, '[0.891000, 1.791000, 2.691000, 3.591000, 4.491000, 5.391000, 6.291000, 7.191000, 8.091000, 8.991000]')

        tdSql.error(f'select percentile(col1) from {self.stbname}_0')
        tdSql.error(f'select percentile(col1, -1) from {self.stbname}_0')
        tdSql.error(f'select percentile(col1, 101) from {self.stbname}_0')
        tdSql.error(f'select percentile(col1, col2) from {self.stbname}_0')
        tdSql.error(f'select percentile(1, col1) from {self.stbname}_0')
        tdSql.error(f'select percentile(col1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 101) from {self.stbname}_0')

        tdSql.execute(f'drop database {self.dbname}')
    def run(self):
        self.function_check_ntb()
        self.function_check_ctb()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
