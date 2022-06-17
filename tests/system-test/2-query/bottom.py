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
from util.common import *



class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.rowNum = 10
        self.tbnum = 20
        self.ts = 1537146000000
        self.binary_str = 'taosdata'
        self.nchar_str = '涛思数据'
    def bottom_check_base(self):
        tdSql.prepare()
        tdSql.execute('''create table stb(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 tinyint unsigned, col6 smallint unsigned, 
                    col7 int unsigned, col8 bigint unsigned, col9 float, col10 double, col11 bool, col12 binary(20), col13 nchar(20)) tags(loc nchar(20))''')
        tdSql.execute("create table stb_1 using stb tags('beijing')")
        column_list = ['col1','col2','col3','col4','col5','col6','col7','col8']
        error_column_list = ['col11','col12','col13']
        error_param_list = [0,101]
        for i in range(self.rowNum):
            tdSql.execute(f"insert into stb_1 values(%d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %f, %d, '{self.binary_str}%d', '{self.nchar_str}%d')"
                          % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1))
        
        for i in column_list:
            tdSql.query(f'select bottom({i},2) from stb_1')
            tdSql.checkRows(2)
            tdSql.checkEqual(tdSql.queryResult,[(2,),(1,)])
            for j in error_param_list:
                tdSql.error(f'select bottom({i},{j}) from stb_1')
        for i in error_column_list:
            tdSql.error(f'select bottom({i},10) from stb_1')
        tdSql.query("select ts,bottom(col1, 2),ts from stb_1 group by tbname")
        tdSql.checkRows(2)
        tdSql.query('select bottom(col2,1) from stb_1 interval(1y) order by col2')
        tdSql.checkData(0,0,1)

        tdSql.error('select * from stb_1 where bottom(col2,1)=1')
        tdSql.execute('drop database db')
    def bottom_check_distribute(self):
        # prepare data for vgroup 4
        dbname = tdCom.getLongName(5, "letters")
        stbname = tdCom.getLongName(5, "letters")
        vgroup_num = 2
        child_table_num = 20
        tdSql.execute(f"create database if not exists {dbname} vgroups {vgroup_num}")
        tdSql.execute(f'use {dbname}')
        # build 20 child tables,every table insert 10 rows
        tdSql.execute(f'''create table {stbname}(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 tinyint unsigned, col6 smallint unsigned, 
                    col7 int unsigned, col8 bigint unsigned, col9 float, col10 double, col11 bool, col12 binary(20), col13 nchar(20)) tags(loc nchar(20))''')
        for i in range(child_table_num):
            tdSql.execute(f"create table {stbname}_{i} using {stbname} tags('beijing')")
            tdSql.execute(f"insert into {stbname}_{i}(ts) values(%d)" % (self.ts - 1-i))
        column_list = ['col1','col2','col3','col4','col5','col6','col7','col8']
        error_column_list = ['col11','col12','col13']
        error_param_list = [0,101]
        for i in [f'{stbname}', f'{dbname}.{stbname}']:
            for j in column_list:
                tdSql.query(f"select bottom({j},1) from {i}")
                tdSql.checkRows(0)
        tdSql.query('show tables')
        vgroup_list = []
        for i in range(len(tdSql.queryResult)):
            vgroup_list.append(tdSql.queryResult[i][6])
        vgroup_list_set = set(vgroup_list)

        for i in vgroup_list_set:
            vgroups_num = vgroup_list.count(i)
            if vgroups_num >=2:
                tdLog.info(f'This scene with {vgroups_num} vgroups is ok!')
                continue
            else:
                tdLog.exit(f'This scene does not meet the requirements with {vgroups_num} vgroup!\n')
        for i in range(self.rowNum):
            for j in range(child_table_num):
                tdSql.execute(f"insert into {stbname}_{j} values(%d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %f, %d, '{self.binary_str}%d', '{self.nchar_str}%d')"
                          % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1))  
        for i in column_list:
            tdSql.query(f'select bottom({i},2) from {stbname}')
            tdSql.checkRows(2)
            tdSql.checkEqual(tdSql.queryResult,[(1,),(1,)])
            for j in error_param_list:
                tdSql.error(f'select bottom({i},{j}) from {stbname}')
        for i in error_column_list:
            tdSql.error(f'select bottom({i},10) from {stbname}')
        
        tdSql.execute(f'drop database {dbname}')
    def run(self):

        self.bottom_check_base()
        self.bottom_check_distribute()
        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
