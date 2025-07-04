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

import string
import numpy as np
from new_test_framework.utils import tdLog, tdSql
from new_test_framework.utils.sqlset import TDSetSql

class TestStddev:
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        cls.dbname = 'db_test'
        cls.setsql = TDSetSql()
        cls.ntbname = f'{cls.dbname}.ntb'
        cls.row_num = 10
        cls.ts = 1537146000000
        cls.column_dict = {
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
        stbname = f"{self.dbname}.test_stb"
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
    def test_stddev(self):
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
 
        self.stddev_check() 

        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
