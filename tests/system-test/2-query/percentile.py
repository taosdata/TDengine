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
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.rowNum = 10
        self.ts = 1537146000000
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
        self.binary_str = 'taosdata'
        self.nchar_str = '涛思数据'
        self.tbnum = 2
        self.tag_values = [
            f'1537146000000,1,2,3,4,5,6,7,8,9.1,10.1,"{self.binary_str}","{self.nchar_str}"'
            
        ]
        
        self.param = [1,50,100]
    def insert_data(self,column_dict,tbname,row_num):
        intData = []        
        floatData = []
        sql = ''
        for k, v in column_dict.items():
            if v.lower() == 'timestamp' or v.lower() == 'tinyint' or v.lower() == 'smallint' or v.lower() == 'int' or v.lower() == 'bigint' or \
            v.lower() == 'tinyint unsigned' or v.lower() == 'smallint unsigned' or v.lower() == 'int unsigned' or v.lower() == 'bigint unsigned' or v.lower() == 'bool':
                sql += '%d,'
            elif v.lower() == 'float' or v.lower() == 'double':
                sql += '%f,'
            elif 'binary' in v.lower():
                sql += f'"{self.binary_str}%d",'
            elif 'nchar' in v.lower():
                sql += f'"{self.nchar_str}%d",'
        insert_sql = f'insert into {tbname} values({sql[:-1]})'
        for i in range(row_num):
            insert_list = []
            for k, v in column_dict.items():
                if v.lower() in[ 'tinyint' , 'smallint' , 'int', 'bigint' , 'tinyint unsigned' , 'smallint unsigned' , 'int unsigned' , 'bigint unsigned'] or\
                'binary' in v.lower() or 'nchar' in v.lower():
                    insert_list.append(1 + i)
                elif v.lower() == 'float' or v.lower() == 'double':
                    insert_list.append(0.1 + i)
                elif v.lower() == 'bool':
                    insert_list.append(i % 2)
                elif v.lower() == 'timestamp':
                    insert_list.append(self.ts + i)
            tdSql.execute(insert_sql%(tuple(insert_list)))
            intData.append(i + 1)            
            floatData.append(i + 0.1)
        return intData,floatData
    def function_check_ntb(self):
        tdSql.prepare()
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
    def function_check_ctb(self):
        
        tdSql.prepare()
        tdSql.execute(self.setsql.set_create_stable_sql(self.stbname,self.column_dict,self.tag_dict))
        for i in range(self.tbnum):
            tdSql.execute(f"create table {self.stbname}_{i} using {self.stbname} tags({self.tag_values[0]})")
            tdSql.execute(self.insert_data(self.column_dict,f'{self.stbname}_{i}',self.rowNum))
        
    def run(self):
        # self.function_check_ntb()
        self.function_check_ctb()
        # tdSql.prepare()

        # intData = []        
        # floatData = []

        # tdSql.execute('''create table test(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double, 
        #             col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned)''')
        # for i in range(self.rowNum):
        #     tdSql.execute("insert into test values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)" 
        #                 % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))
        #     intData.append(i + 1)            
        #     floatData.append(i + 0.1)                        

        # # percentile verifacation 
        # tdSql.error("select percentile(ts ,20) from test")
        # tdSql.error("select percentile(col7 ,20) from test")
        # tdSql.error("select percentile(col8 ,20) from test")        
        # tdSql.error("select percentile(col9 ,20) from test") 
        # column_list = [1,2,3,4,11,12,13,14]
        # percent_list = [0,50,100]  
        # for i in column_list:  
        #     for j in percent_list:
        #         tdSql.query(f"select percentile(col{i}, {j}) from test")        
        #         tdSql.checkData(0, 0, np.percentile(intData, j)) 

        # for i in [5,6]:
        #     for j in percent_list:
        #         tdSql.query(f"select percentile(col{i}, {j}) from test")
        #         tdSql.checkData(0, 0, np.percentile(floatData, j))
        
        # tdSql.execute("create table meters (ts timestamp, voltage int) tags(loc nchar(20))")
        # tdSql.execute("create table t0 using meters tags('beijing')")
        # tdSql.execute("create table t1 using meters tags('shanghai')")
        # for i in range(self.rowNum):
        #     tdSql.execute("insert into t0 values(%d, %d)" % (self.ts + i, i + 1))            
        #     tdSql.execute("insert into t1 values(%d, %d)" % (self.ts + i, i + 1))            
        
        # # tdSql.error("select percentile(voltage, 20) from meters")


 
        # tdSql.execute("create table st(ts timestamp, k int)")
        # tdSql.execute("insert into st values(now, -100)(now+1a,-99)")
        # tdSql.query("select apercentile(k, 20) from st")
        # tdSql.checkData(0, 0, -100.00)


        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
