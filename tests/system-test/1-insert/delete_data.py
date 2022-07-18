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
from util.sqlset import TDSetSql

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.dbname = 'db_test'
        self.setsql = TDSetSql()
        self.ntbname = 'ntb'
        self.rowNum = 10
        self.tbnum = 20
        self.ts = 1537146000000
        self.binary_str = 'taosdata'
        self.nchar_str = '涛思数据'
        # first column must be timestamp
        self.column_dict = {
            'ts'  : 'timestamp',
            'col1': 'tinyint',
            
        }
        self.value = [
            f'1'
        ]

        self.param_list = [1,100]
    def insert_data(self,column_dict,tbname,row_num):
        insert_sql = self.setsql.set_insertsql(column_dict,tbname,self.binary_str,self.nchar_str)
        for i in range(row_num):
            insert_list = []
            
    def delete_all_data(self,tbname):
        tdSql.execute(f'delete from {tbname}')
        tdSql.query(f'select * from {tbname}')
        tdSql.checkRows(0)
        
    def delete_one_row(self,tbname,column_dict):
        for column_name,column_type in column_dict.items():
            if column_type.lower() == 'timestamp':
                
                tdSql.execute(f'delete from {tbname} where {column_name}={self.ts}')
                tdSql.query(f'select * from {tbname}')
                tdSql.checkRows(self.rowNum-1)
                tdSql.query(f'select * from {tbname} where {column_name}={self.ts}')
                tdSql.checkRows(0)
                tdSql.execute(f'insert into {tbname} values({self.ts},{self.value_list[0]}')
                tdSql.query(f'select * from {tbname} where {column_name}={self.ts}')
                tdSql.checkEqual(str(tdSql.queryResult[0][0]),str(datetime.datetime.fromtimestamp(value/1000).strftime("%Y-%m-%d %H:%M:%S.%f")))
        
    def delete_data_ntb(self):
        tdSql.execute(f'create database if not exists {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(self.setsql.set_create_normaltable_sql(self.ntbname,self.column_dict))
        for i in range(self.rowNum):
            tdSql.execute(f'insert into {self.ntbname} values({self.ts+i},{self.value_list[0]})')
        
        
        # self.delete_all_data(self.ntbname)
        
    def run(self):
        self.delete_data_ntb()
        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())