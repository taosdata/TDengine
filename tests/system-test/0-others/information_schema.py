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
        self.setsql = TDSetSql()
        self.dbname = 'db'
        self.stbname = 'stb'
        self.binary_length = 20 # the length of binary for column_dict
        self.nchar_length = 20  # the length of nchar for column_dict
        self.ts = 1537146000000
        self.column_dict = {
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
            'col12': f'binary({self.binary_length})',
            'col13': f'nchar({self.nchar_length})'
        }
        self.tbnum = 20
        self.rowNum = 10
        self.tag_dict = {
            't0':'int'
        }
        self.tag_values = [
            f'1'
            ]
        self.binary_str = 'taosdata'
        self.nchar_str = '涛思数据'
        self.ins_list = ['ins_dnodes','ins_mnodes','ins_modules','ins_qnodes','ins_snodes','ins_cluster','ins_databases','ins_functions',\
            'ins_indexes','ins_stables','ins_tables','ins_tags','ins_users','ins_grants','ins_vgroups','ins_configs','ins_dnode_variables',\
                'ins_topics','ins_subscriptions','ins_streams','ins_stream_tasks','ins_vnodes','ins_user_privileges']
        self.perf_list = ['perf_connections','perf_queries','perf_consumers','perf_trans','perf_apps']
    def insert_data(self,column_dict,tbname,row_num):
        insert_sql = self.setsql.set_insertsql(column_dict,tbname,self.binary_str,self.nchar_str)
        for i in range(row_num):
            insert_list = []
            self.setsql.insert_values(column_dict,i,insert_sql,insert_list,self.ts)
    def prepare_data(self):
        tdSql.execute(f"create database if not exists {self.dbname} vgroups 2")
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(self.setsql.set_create_stable_sql(self.stbname,self.column_dict,self.tag_dict))
        for i in range(self.tbnum):
            tdSql.execute(f"create table {self.stbname}_{i} using {self.stbname} tags({self.tag_values[0]})")
            self.insert_data(self.column_dict,f'{self.stbname}_{i}',self.rowNum)
    def count_check(self):
        tdSql.query('select count(*) from information_schema.ins_tables')
        tdSql.checkEqual(tdSql.queryResult[0][0],self.tbnum+len(self.ins_list)+len(self.perf_list))
        tdSql.query(f'select count(*) from information_schema.ins_tables where db_name = "{self.dbname}"')
        tdSql.checkEqual(tdSql.queryResult[0][0],self.tbnum)
        tdSql.query(f'select count(*) from information_schema.ins_tables where db_name = "{self.dbname}" and stable_name = "{self.stbname}"')
        tdSql.checkEqual(tdSql.queryResult[0][0],self.tbnum)
        tdSql.execute('create database db1')
        tdSql.execute('create table stb1 (ts timestamp,c0 int) tags(t0 int)')
        tdSql.execute('create table tb1 using stb1 tags(1)')
        tdSql.query(f'select db_name, stable_name, count(*) from information_schema.ins_tables group by db_name, stable_name')
        for i in tdSql.queryResult:
            if i[0].lower() == 'information_schema':
                tdSql.checkEqual(i[2],len(self.ins_list))
            elif i[0].lower() == self.dbname and i[1] == self.stbname:
                tdSql.checkEqual(i[2],self.tbnum)
            elif i[0].lower() == self.dbname and i[1] == 'stb1':
                tdSql.checkEqual(i[2],1)
            elif i[0].lower() == 'performance_schema':
                tdSql.checkEqual(i[2],len(self.perf_list))
        tdSql.execute('create table db1.ntb (ts timestamp,c0 int)')
        tdSql.query(f'select db_name, count(*) from information_schema.ins_tables group by db_name')
        print(tdSql.queryResult)
        for i in tdSql.queryResult:
            if i[0].lower() == 'information_schema':
                tdSql.checkEqual(i[1],len(self.ins_list))
            elif i[0].lower() == 'performance_schema':
                tdSql.checkEqual(i[1],len(self.perf_list))
            elif i[0].lower() == self.dbname:
                tdSql.checkEqual(i[1],self.tbnum+1)
    def run(self):
        self.prepare_data()
        self.count_check()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())