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
import numpy as np
from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *
class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.dbname = 'db_test'
        self.setsql = TDSetSql()
        self.ntbname = f'{self.dbname}.ntb'
        self.row_num = 10
        self.ts = 1537146000000
        self.column_dict = {
            'ts':'timestamp',
            'col1': 'tinyint',
            'col2': 'smallint',
            'col3': 'int',
            'col4': 'bigint',
            'col5': 'tinyint unsigned',
            'col6': 'smallint unsigned',
            'col7': 'int unsigned',
            'col8': 'bigint unsigned',
    
        }
    def insert_data(self,column_dict,tbname,row_num):
        insert_sql = self.setsql.set_insertsql(column_dict,tbname)
        for i in range(row_num):
            insert_list = []
            self.setsql.insert_values(column_dict,i,insert_sql,insert_list,self.ts)
    def stddev_check(self):
        stbname = f'{self.dbname}.{tdCom.getLongName(5,"letters")}'
        tag_dict = {
            't0':'int'
        }
        tag_values = [
            f'1'
            ]
        tdSql.execute(f"create database if not exists {self.dbname}")
        tdSql.execute(self.setsql.set_create_stable_sql(stbname,self.column_dict,tag_dict))
        tdSql.execute(f"create table {stbname}_1 using {stbname} tags({tag_values[0]})")
        self.insert_data(self.column_dict,f'{stbname}_1',self.row_num)
        for col in self.column_dict.keys():
            col_val_list = []
            if col.lower() != 'ts':
                tdSql.query(f'select {col} from {stbname}_1')
                for col_val in tdSql.queryResult:
                    col_val_list.append(col_val[0])
                col_std = np.std(col_val_list)
                tdSql.query(f'select stddev({col}) from {stbname}_1')
                tdSql.checkEqual(col_std,tdSql.queryResult[0][0])
        tdSql.execute(f'drop database {self.dbname}')
    def run(self): 
        self.stddev_check() 
        

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
