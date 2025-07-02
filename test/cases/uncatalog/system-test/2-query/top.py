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
from new_test_framework.utils import tdLog, tdSql
from new_test_framework.utils.sqlset import TDSetSql

class TestTop:
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        # cls.setsql = # TDSetSql()
        cls.dbname = 'db'
        cls.stbname = f'{cls.dbname}.stb'
        cls.ntbname = f'{cls.dbname}.ntb'
        cls.rowNum = 10
        cls.tbnum = 20
        cls.ts = 1537146000000
        cls.binary_str = 'taosdata'
        cls.nchar_str = '涛思数据'
        cls.column_dict = {
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
            'col12': 'binary(20)',
            'col13': 'nchar(20)'
        }

        cls.param_list = [1,100]

    def insert_data(self,column_dict,tbname,row_num):
        insert_sql = TDSetSql.set_insertsql(column_dict,tbname,self.binary_str,self.nchar_str)
        for i in range(row_num):
            insert_list = []
            TDSetSql.insert_values(column_dict,i,insert_sql,insert_list,self.ts)
    def top_check_data(self,tbname,tb_type):
        new_column_dict = {}
        for param in self.param_list:
            for k,v in self.column_dict.items():
                if v.lower() in ['tinyint','smallint','int','bigint','tinyint unsigned','smallint unsigned','int unsigned','bigint unsigned']:
                    tdSql.query(f'select top({k},{param}) from {tbname}')
                    if param >= self.rowNum:
                        if tb_type in ['normal_table','child_table']:
                            tdSql.checkRows(self.rowNum)
                            values_list = []
                            for i in range(self.rowNum):
                                tp = (self.rowNum-i-1,)
                                values_list.insert(0,tp)
                            tdSql.checkEqual(tdSql.queryResult,values_list)
                        elif tb_type == 'stable':
                            tdSql.checkRows(param)
                    elif param < self.rowNum:
                        if tb_type in ['normal_table','child_table']:
                            tdSql.checkRows(param)
                            values_list = []
                            for i in range(param):
                                tp = (self.rowNum-i-1,)
                                values_list.insert(0,tp)
                            tdSql.checkEqual(tdSql.queryResult,values_list)
                        elif tb_type == 'stable':
                            tdSql.checkRows(param)
                    for i in [self.param_list[0]-1,self.param_list[-1]+1]:
                        tdSql.error(f'select top({k},{i}) from {tbname}')
                    new_column_dict.update({k:v})
                elif v.lower() == 'bool' or 'binary' in v.lower() or 'nchar' in v.lower():
                    tdSql.error(f'select top({k},{param}) from {tbname}')
                tdSql.error(f'select * from {tbname} where top({k},{param})=1')
        for key in new_column_dict.keys():
            for k in self.column_dict.keys():
                if key == k :
                    continue
                else:
                    tdSql.query(f'select top({key},2),{k} from {tbname} group by tbname')
                    if tb_type == 'normal_table' or tb_type == 'child_table':
                        tdSql.checkRows(2)
                    else:
                        tdSql.checkRows(2*self.tbnum)
    def top_check_stb(self):
        
        tag_dict = {
            't0':'int'
        }
        tag_values = [
            f'1'
            ]
        tdSql.execute(f"create database if not exists {self.dbname} vgroups 2")
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(TDSetSql.set_create_stable_sql(self.stbname,self.column_dict,tag_dict))

        for i in range(self.tbnum):
            tdSql.execute(f"create table {self.stbname}_{i} using {self.stbname} tags({tag_values[0]})")
            self.insert_data(self.column_dict,f'{self.stbname}_{i}',self.rowNum)
        tdSql.query(f'select * from information_schema.ins_tables where db_name = "{self.dbname}"')
        vgroup_list = []
        for i in range(len(tdSql.queryResult)):
            vgroup_list.append(tdSql.queryResult[i][6])
        vgroup_list_set = set(vgroup_list)
        for i in vgroup_list_set:
            vgroups_num = vgroup_list.count(i)
            if vgroups_num >= 2:
                tdLog.info(f'This scene with {vgroups_num} vgroups is ok!')
            else:
                tdLog.exit(
                    'This scene does not meet the requirements with {vgroups_num} vgroup!\n')
        for i in range(self.tbnum):
            self.top_check_data(f'{self.stbname}_{i}','child_table')
        self.top_check_data(self.stbname,'stable')
        tdSql.execute(f'drop database {self.dbname}')

    def top_check_ntb(self):
        tdSql.execute(f"create database if not exists {self.dbname}")
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(TDSetSql.set_create_normaltable_sql(self.ntbname,self.column_dict))
        self.insert_data(self.column_dict,self.ntbname,self.rowNum)
        self.top_check_data(self.ntbname,'normal_table')
        tdSql.execute(f'drop database {self.dbname}')

    def test_top(self):
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

        self.top_check_ntb()
        self.top_check_stb()

        #tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
