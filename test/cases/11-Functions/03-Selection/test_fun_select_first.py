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
from new_test_framework.utils.common import tdCom

class TestFunFirst:
    #
    # ------------------ sim ------------------
    #
    def do_sim_first(self):
        dbPrefix = "m_fi_db"
        tbPrefix = "m_fi_tb"
        mtPrefix = "m_fi_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.prepare(db, drop=True)
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol int)")

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")
            x = 0
            sql = f"insert into {tb} values"
            while x < rowNum:
                cc = x * 60000
                ms = 1601481600000 + cc
                sql +=f" ({ms},{x})"
                x = x + 1
            tdSql.execute(sql)    
            i = i + 1

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select first(tbcol) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== step3")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(f"select first(tbcol) from {tb} where ts <= {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select first(tbcol) as b from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select first(tbcol) as b from {tb} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select first(tbcol) as b from {tb} interval(1d)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== step6")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(f"select first(tbcol) as b from {tb} where ts <= {ms} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(4, 0, 4)
        tdSql.checkRows(5)

        tdLog.info(f"=============== step7")
        tdSql.query(f"select first(tbcol) from {mt}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== step8")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(f"select first(tbcol) as c from {mt} where ts <= {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select first(tbcol) as c from {mt} where tgcol < 5")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(
            f"select first(tbcol) as c from {mt} where tgcol < 5 and ts <= {ms}"
        )
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== step9")
        tdSql.query(f"select first(tbcol) as b from {mt} interval(1m)")
        tdLog.info(f"select first(tbcol) as b from {mt} interval(1m)")
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)

        tdSql.query(f"select first(tbcol) as b from {mt} interval(1d)")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== step10")
        tdSql.query(f"select first(tbcol) as b from {mt} group by tgcol")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)
        tdSql.checkRows(tbNum)

        tdLog.info(f"=============== step11")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(
            f"select first(tbcol) as b from {mt}  where ts <= {ms} partition by tgcol interval(1m)"
        )
        tdLog.info(f"===> {tdSql.getData(1,0)}")
        tdSql.checkData(1, 0, 1)
        tdSql.checkRows(50)

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        print("\n")
        print("do_sim_first .......................... [passed]\n")

    #
    # ------------------ test_first.py ------------------
    #    
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

        sql = f"insert into {dbname}.stb_1 values"        
        for i in range(self.rowNum):
            sql += f" (%d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %f, %d, '{self.binary_str}%d', '{self.nchar_str}%d')" % (self.ts + i, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1)
        tdSql.execute(sql)

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
            sql = f"insert into {dbname}.{stbname}_{i} values"
            for j in range(self.rowNum):
                sql += f" (%d, %d, %d, %d, %d, %d, %d, %d, %d, %f, %f, %d, '{self.binary_str}%d', '{self.nchar_str}%d')"%(self.ts + j + i, j + 1, j + 1, j + 1, j + 1, j + 1, j + 1, j + 1, j + 1, j + 0.1, j + 0.1, j % 2, j + 1, j + 1)
            tdSql.execute(sql)

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

    def do_first(self):
        self.first_check_base()
        self.first_check_stb_distribute()
        print("do_first .............................. [passed]\n")


    #
    # ------------------ main ------------------
    #
    def test_func_select_first(self):
        """ Fun: first()

        1. Sim case
        2. Query on all data types
        3. Input parameter with different values
        4. Query on stable/normal table
        5. Query on null data
        6. Query on where clause
        7. Query with filter
        8. Error check

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-26 Simon Guan Migrated from tsim/compute/first.sim
            - 2025-9-25 Alex  Duan Migrated from uncatalog/system-test/2-query/test_first.py

        """
        self.do_sim_first()
        self.do_first()

        tdLog.success("%s successfully executed" % __file__)
