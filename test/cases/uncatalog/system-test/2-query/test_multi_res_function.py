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

class TestMultiResFunction:
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        cls.rowNum = 10
        cls.tbnum = 20
        cls.ts = 1537146000000
        cls.binary_str = 'taosdata'
        cls.nchar_str = '涛思数据'

    def first_check_base(self):
        dbname = "db"
        tdSql.prepare(dbname)
        column_dict = {
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
        tdSql.execute(f"alter local \'keepColumnName\' \'1\'")
        tdSql.execute(f'''create table {dbname}.stb(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 tinyint unsigned, col6 smallint unsigned,
                    col7 int unsigned, col8 bigint unsigned, col9 float, col10 double, col11 bool, col12 binary(20), col13 nchar(20)) tags(loc nchar(20))''')
        tdSql.execute(f"create table {dbname}.stb_1 using {dbname}.stb tags('beijing')")
        tdSql.execute(f"create table {dbname}.stb_2 using {dbname}.stb tags('beijing')")

        column_list = ['col1','col2','col3','col4','col5','col6','col7','col8','col9','col10','col11','col12','col13']
        for i in column_list:
            for j in ['stb_1']:
                tdSql.query(f"select first({i}) from {dbname}.{j}")
                tdSql.checkRows(0)
        for n in range(self.rowNum):
            i = n
            tdSql.execute(f"insert into {dbname}.stb_1 values(%d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %f, %d, '{self.binary_str}%d', '{self.nchar_str}%d')"
                          % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1))
            
        for n in range(self.rowNum):
            i = n + 10
            tdSql.execute(f"insert into {dbname}.stb_1 values(%d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %f, %d, '{self.binary_str}%d', '{self.nchar_str}%d')"
                          % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1))
            
        for n in range(self.rowNum):
            i = n + 100
            tdSql.execute(f"insert into {dbname}.stb_2 values(%d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %f, %d, '{self.binary_str}%d', '{self.nchar_str}%d')"
                          % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1))
            
        for k, v in column_dict.items():
            
                if v == 'tinyint' or v == 'smallint' or v == 'int' or v == 'bigint' or v == 'tinyint unsigned' or v == 'smallint unsigned'\
                        or v == 'int unsigned' or v == 'bigint unsigned':
                    tdSql.query(f"select last({k})-first({k}) from {dbname}.stb")
                    tdSql.checkData(0, 0, 109)
                    tdSql.query(f"select first({k})+last({k}) from {dbname}.stb")
                    tdSql.checkData(0, 0, 111)
                    tdSql.query(f"select max({k})-first({k}) from {dbname}.stb")
                    tdSql.checkData(0, 0, 109)
                    tdSql.query(f"select max({k})-min({k}) from {dbname}.stb")
                    tdSql.checkData(0, 0, 109)
                    
                    tdSql.query(f"select last({k})-first({k}) from {dbname}.stb_1")
                    tdSql.checkData(0, 0, 19)
                    tdSql.query(f"select first({k})+last({k}) from {dbname}.stb_1")
                    tdSql.checkData(0, 0, 21)
                    tdSql.query(f"select max({k})-first({k}) from {dbname}.stb_1")
                    tdSql.checkData(0, 0, 19)
                    tdSql.query(f"select max({k})-min({k}) from {dbname}.stb_1")
                    tdSql.checkData(0, 0, 19)
                    
                # float,double
                elif v == 'float' or v == 'double':
                    tdSql.query(f"select first({k})+last({k}) from {dbname}.stb")
                    tdSql.checkData(0, 0, 109.2)
                    tdSql.query(f"select first({k})+last({k}) from {dbname}.stb_1")
                    tdSql.checkData(0, 0, 19.2)
                # bool
                elif v == 'bool':
                    continue
                # binary
                elif 'binary' in v:
                    continue
                # nchar
                elif 'nchar' in v:
                    continue

        #tdSql.execute(f'drop database {dbname}')

    def test_multi_res_function(self):
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

        self.first_check_base()

        #tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
