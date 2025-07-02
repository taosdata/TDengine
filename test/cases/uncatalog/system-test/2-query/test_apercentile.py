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
from new_test_framework.utils import tdLog, tdSql, sqlset

class TestApercentile:
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        cls.rowNum = 10
        cls.ts = 1537146000000
        # cls.setsql = # TDSetSql()
        cls.dbname = "db"
        cls.ntbname = f"{cls.dbname}.ntb"
        cls.stbname = f'{cls.dbname}.stb'
        cls.binary_length = 20 
        cls.nchar_length = 20  
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
            'col12': f'binary({cls.binary_length})',
            'col13': f'nchar({cls.nchar_length})'
        }
        cls.tag_dict = {
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
            't12': f'binary({cls.binary_length})',
            't13': f'nchar({cls.nchar_length})'
        }
        cls.binary_str = 'taosdata'
        cls.nchar_str = '涛思数据'
        cls.tbnum = 2
        cls.tag_ts = cls.ts
        cls.tag_tinyint = 1
        cls.tag_smallint = 2
        cls.tag_int = 3
        cls.tag_bigint = 4
        cls.tag_utint = 5
        cls.tag_usint = 6
        cls.tag_uint = 7
        cls.tag_ubint = 8
        cls.tag_float = 9.1
        cls.tag_double = 10.1
        cls.tag_bool = True
        cls.tag_values = [
            f'{cls.tag_ts},{cls.tag_tinyint},{cls.tag_smallint},{cls.tag_int},{cls.tag_bigint},\
            {cls.tag_utint},{cls.tag_usint},{cls.tag_uint},{cls.tag_ubint},{cls.tag_float},{cls.tag_double},{cls.tag_bool},"{cls.binary_str}","{cls.nchar_str}"'

        ]
        cls.percent = [1,50,100]
        cls.param_list = ['default','t-digest']
    def insert_data(self,column_dict,tbname,row_num):
        insert_sql = sqlset.TDSetSql.set_insertsql(column_dict,tbname,self.binary_str,self.nchar_str)
        for i in range(row_num):
            insert_list = []
            sqlset.TDSetSql.insert_values(column_dict,i,insert_sql,insert_list,self.ts)

    def function_check_ntb(self):
        tdSql.prepare()
        tdSql.execute(sqlset.TDSetSql.set_create_normaltable_sql(self.ntbname,self.column_dict))
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
    def test_apercentile(self):
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

        self.function_check_ntb()
        self.function_check_stb()

        #tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
