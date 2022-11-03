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
import sys
import taos
from util.common import *
from util.log import *
from util.cases import *
from util.sql import *
import numpy as np


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.rowNum = 10
        self.tbnum = 20
        self.ts = 1537146000000
        self.binary_str = 'taosdata'
        self.nchar_str = '涛思数据'

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
        tdSql.execute(f'''create table {dbname}.stb(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 tinyint unsigned, col6 smallint unsigned,
                    col7 int unsigned, col8 bigint unsigned, col9 float, col10 double, col11 bool, col12 binary(20), col13 nchar(20)) tags(loc nchar(20))''')
        tdSql.execute(f"create table {dbname}.stb_1 using {dbname}.stb tags('beijing')")
        tdSql.execute(f"insert into {dbname}.stb_1(ts) values({self.ts - 1})")
        column_list = ['col1','col2','col3','col4','col5','col6','col7','col8','col9','col10','col11','col12','col13']
        for i in ['stb_1','stb']:
            tdSql.query(f"select first(*) from {dbname}.{i}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)
        for i in column_list:
            for j in ['stb_1']:
                tdSql.query(f"select first({i}) from {dbname}.{j}")
                tdSql.checkRows(0)
        for i in range(self.rowNum):
            tdSql.execute(f"insert into {dbname}.stb_1 values(%d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %f, %d, '{self.binary_str}%d', '{self.nchar_str}%d')"
                          % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1))
        for k, v in column_dict.items():
            for j in ['stb_1', 'stb']:
                tdSql.query(f"select first({k}) from {dbname}.{j}")
                tdSql.checkRows(1)
                # tinyint,smallint,int,bigint,tinyint unsigned,smallint unsigned,int unsigned,bigint unsigned
                if v == 'tinyint' or v == 'smallint' or v == 'int' or v == 'bigint' or v == 'tinyint unsigned' or v == 'smallint unsigned'\
                        or v == 'int unsigned' or v == 'bigint unsigned':
                    tdSql.checkData(0, 0, 1)
                # float,double
                elif v == 'float' or v == 'double':
                    tdSql.checkData(0, 0, 0.1)
                # bool
                elif v == 'bool':
                    tdSql.checkData(0, 0, False)
                # binary
                elif 'binary' in v:
                    tdSql.checkData(0, 0, f'{self.binary_str}1')
                # nchar
                elif 'nchar' in v:
                    tdSql.checkData(0, 0, f'{self.nchar_str}1')
        #!bug TD-16569
        tdSql.query(f"select first(*),last(*) from {dbname}.stb where ts < 23 interval(1s)")
        tdSql.checkRows(0)
        tdSql.execute(f'drop database {dbname}')
    def first_check_stb_distribute(self):
        # prepare data for vgroup 4
        dbname = tdCom.getLongName(10, "letters")
        stbname = tdCom.getLongName(5, "letters")
        child_table_num = 20
        vgroup = 2
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
        tdSql.execute(f"create database if not exists {dbname} vgroups {vgroup}")
        tdSql.execute(f'use {dbname}')
        # build 20 child tables,every table insert 10 rows
        tdSql.execute(f'''create table {dbname}.{stbname}(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 tinyint unsigned, col6 smallint unsigned,
                    col7 int unsigned, col8 bigint unsigned, col9 float, col10 double, col11 bool, col12 binary(20), col13 nchar(20)) tags(loc nchar(20))''')
        for i in range(child_table_num):
            tdSql.execute(f"create table {dbname}.{stbname}_{i} using {dbname}.{stbname} tags('beijing')")
            tdSql.execute(f"insert into {dbname}.{stbname}_{i}(ts) values(%d)" % (self.ts - 1-i))
        #!bug TD-16561
        for i in [f'{dbname}.{stbname}']:
            tdSql.query(f"select first(*) from {i}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, None)
        tdSql.query(f"select * from information_schema.ins_tables where db_name = '{dbname}'")
        vgroup_list = []
        for i in range(len(tdSql.queryResult)):
            vgroup_list.append(tdSql.queryResult[i][6])
        vgroup_list_set = set(vgroup_list)
        # print(vgroup_list_set)
        # print(vgroup_list)
        for i in vgroup_list_set:
            vgroups_num = vgroup_list.count(i)
            if vgroups_num >=2:
                tdLog.info(f'This scene with {vgroups_num} vgroups is ok!')
                continue
            else:
                tdLog.exit(f'This scene does not meet the requirements with {vgroups_num} vgroup!\n')

        for i in range(child_table_num):
            for j in range(self.rowNum):
                tdSql.execute(f"insert into {dbname}.{stbname}_{i} values(%d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %f, %d, '{self.binary_str}%d', '{self.nchar_str}%d')"
                          % (self.ts + j + i, j + 1, j + 1, j + 1, j + 1, j + 1, j + 1, j + 1, j + 1, j + 0.1, j + 0.1, j % 2, j + 1, j + 1))

        for k, v in column_dict.items():
            for j in [f'{dbname}.{stbname}_{i}', f'{dbname}.{stbname}']:
                tdSql.query(f"select first({k}) from {j}")
                tdSql.checkRows(1)
                # tinyint,smallint,int,bigint,tinyint unsigned,smallint unsigned,int unsigned,bigint unsigned
                if v == 'tinyint' or v == 'smallint' or v == 'int' or v == 'bigint' or v == 'tinyint unsigned' or v == 'smallint unsigned'\
                        or v == 'int unsigned' or v == 'bigint unsigned':
                    tdSql.checkData(0, 0, 1)
                # float,double
                elif v == 'float' or v == 'double':
                    tdSql.checkData(0, 0, 0.1)
                # bool
                elif v == 'bool':
                    tdSql.checkData(0, 0, False)
                # binary
                elif 'binary' in v:
                    tdSql.checkData(0, 0, f'{self.binary_str}1')
                # nchar
                elif 'nchar' in v:
                    tdSql.checkData(0, 0, f'{self.nchar_str}1')
        #!bug TD-16569
        tdSql.query(f"select first(*),last(*) from {dbname}.{stbname} where ts < 23 interval(1s)")
        tdSql.checkRows(0)
        tdSql.execute(f'drop database {dbname}')

    def run(self):
        self.first_check_base()
        self.first_check_stb_distribute()


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
