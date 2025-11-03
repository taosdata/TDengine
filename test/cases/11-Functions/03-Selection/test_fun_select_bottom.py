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

from new_test_framework.utils import tdLog, tdSql, common
from new_test_framework.utils.sqlset import TDSetSql

class TestBottom:
    #
    # ------------------ sim case ------------------
    #
    def do_sim_bottom(self):
        dbPrefix = "m_bo_db"
        tbPrefix = "m_bo_tb"
        mtPrefix = "m_bo_mt"
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

            sql = f"insert into {tb} values"
            x = 0
            while x < rowNum:
                cc = x * 60000
                ms = 1601481600000 + cc
                sql += f" ({ms},{x})"
                x = x + 1
            tdSql.execute(sql)
            i = i + 1

        tdLog.info(f"=============== step2")
        i = 1
        tb = tbPrefix + str(i)

        tdSql.query(f"select bottom(tbcol, 1) from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== step3")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(f"select bottom(tbcol, 1) from {tb} where ts > {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 5)

        tdLog.info(f"=============== step4")
        tdSql.query(f"select bottom(tbcol, 1) as b from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 0)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select bottom(tbcol, 2) as b from {tb}")
        tdLog.info(f"===> {tdSql.getData(0,0)}  {tdSql.getData(1,0)}")
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 0)

        tdLog.info(f"=============== step6")
        cc = 4 * 60000
        ms = 1601481600000 + cc
        tdSql.query(f"select bottom(tbcol, 2) as b from {tb} where ts > {ms}")
        tdLog.info(f"===> {tdSql.getData(0,0)}  {tdSql.getData(1,0)}")
        tdSql.checkData(0, 0, 6)
        tdSql.checkData(1, 0, 5)

        tdSql.error(f"select bottom(tbcol, 122) as b from {tb}")

        tdLog.info(f"=============== clear")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        print("\n")
        print("do_sim_bottom ......................... [passed]\n")

    #
    # ------------------ python case ------------------
    #
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        cls.dbname = 'db_test'
        cls.setsql = TDSetSql()
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
        insert_sql = self.setsql.set_insertsql(column_dict,tbname,self.binary_str,self.nchar_str)
        for i in range(row_num):
            insert_list = []
            self.setsql.insert_values(column_dict,i,insert_sql,insert_list,self.ts)

    def bottom_check_data(self,tbname,tb_type):
        new_column_dict = {}
        for param in self.param_list:
            for k,v in self.column_dict.items():
                if v.lower() in ['tinyint','smallint','int','bigint','tinyint unsigned','smallint unsigned','int unsigned','bigint unsigned']:
                    tdSql.query(f'select bottom({k},{param}) from {tbname} order by {k}')
                    if param >= self.rowNum:
                        if tb_type in ['normal_table','child_table']:
                            tdSql.checkRows(self.rowNum)
                            values_list = []
                            for i in range(self.rowNum):
                                tp = (i,)
                                values_list.append(tp)
                            tdSql.checkEqual(tdSql.queryResult,values_list)
                        elif tb_type == 'stable':
                            tdSql.checkRows(param)
                    elif param < self.rowNum:
                        if tb_type in ['normal_table','child_table']:
                            tdSql.checkRows(param)
                            values_list = []
                            for i in range(param):
                                tp = (i,)
                                values_list.append(tp)
                            tdSql.checkEqual(tdSql.queryResult,values_list)
                        elif tb_type == 'stable':
                            tdSql.checkRows(param)
                    for i in [self.param_list[0]-1,self.param_list[-1]+1]:
                        tdSql.error(f'select top({k},{i}) from {tbname}')
                    new_column_dict.update({k:v})
                elif v.lower() == 'bool' or 'binary' in v.lower() or 'nchar' in v.lower():
                    tdSql.error(f'select top({k},{param}) from {tbname}')
                tdSql.error(f'select * from {tbname} where top({k},{param})=1')
        pass

    def bottom_check_ntb(self):
        tdSql.execute(f'create database if not exists {self.dbname} vgroups 1')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(self.setsql.set_create_normaltable_sql(self.ntbname,self.column_dict))
        self.insert_data(self.column_dict,self.ntbname,self.rowNum)
        self.bottom_check_data(self.ntbname,'normal_table')
        tdSql.execute(f'drop database {self.dbname}')

    def bottom_check_stb(self):
        stbname = f'{self.dbname}.{common.tdCom.getLongName(5, "letters")}'
        tag_dict = {
            't0':'int'
        }
        tag_values = [
            f'1'
            ]
        tdSql.execute(f"create database if not exists {self.dbname} vgroups 2")
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(self.setsql.set_create_stable_sql(stbname,self.column_dict,tag_dict))
        for i in range(self.tbnum):
            tdSql.execute(f"create table {stbname}_{i} using {stbname} tags({tag_values[0]})")
            self.insert_data(self.column_dict,f'{stbname}_{i}',self.rowNum)
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
            self.bottom_check_data(f'{stbname}_{i}','child_table')
        self.bottom_check_data(f'{stbname}','stable')
        tdSql.execute(f'drop database {self.dbname}')

    def do_bottom(self):
        self.bottom_check_ntb()
        self.bottom_check_stb()
        print("do_sim_bottom ......................... [passed]\n")



    #
    # ------------------ main ------------------
    #
    def test_func_select_bottom(self):
        """ Fun: bottom()

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
            - 2025-8-26 Simon Guan Migrated from tsim/compute/bottom.sim
            - 2025-9-25 Alex  Duan Migrated from uncatalog/system-test/2-query/test_bottom.py

        """
        self.do_sim_bottom()
        self.do_bottom()
        tdLog.success("%s successfully executed" % __file__)