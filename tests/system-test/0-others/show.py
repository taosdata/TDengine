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

import re
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
        self.ins_param_list = ['dnodes','mnodes','qnodes','cluster','functions','users','grants','topics','subscriptions','streams']
        self.perf_param = ['apps','connections','consumers','queries','transactions']
        self.perf_param_list = ['apps','connections','consumers','queries','trans']
        self.dbname = "db"
        self.vgroups = 4
        self.stbname = f'`{tdCom.getLongName(5)}`'
        self.tbname = f'`{tdCom.getLongName(3)}`'
        self.db_param = {
            "database":f"{self.dbname}",
            "buffer":100,
            "cachemodel":"'none'",
            "cachesize":1,
            "comp":2,
            "maxrows":1000,
            "minrows":200,
            "pages":512,
            "pagesize":16,
            "precision":"'ms'",
            "replica":1,
            "wal_level":1,
            "wal_fsync_period":6000,
            "vgroups":self.vgroups,
            "stt_trigger":1,
            "tsdb_pagesize":16
        }
    def ins_check(self):
        tdSql.prepare()
        for param in self.ins_param_list:
            if param.lower() == 'qnodes':
                tdSql.execute('create qnode on dnode 1')
            tdSql.query(f'show {param}')
            show_result = tdSql.queryResult
            tdSql.query(f'select * from information_schema.ins_{param}')
            select_result = tdSql.queryResult
            tdSql.checkEqual(show_result,select_result)
        tdSql.execute('drop database db')
    def perf_check(self):
        tdSql.prepare()
        for param in range(len(self.perf_param_list)):
            tdSql.query(f'show {self.perf_param[param]}')
            if len(tdSql.queryResult) != 0:
                show_result = tdSql.queryResult[0][0]
                tdSql.query(f'select * from performance_schema.perf_{self.perf_param_list[param]}')
                select_result = tdSql.queryResult[0][0]
                tdSql.checkEqual(show_result,select_result)
            else :
                continue
        tdSql.execute('drop database db')
    def set_stb_sql(self,stbname,column_dict,tag_dict):
        column_sql = ''
        tag_sql = ''
        for k,v in column_dict.items():
            column_sql += f"{k} {v}, "
        for k,v in tag_dict.items():
            tag_sql += f"{k} {v}, "
        create_stb_sql = f'create stable {stbname} ({column_sql[:-2]}) tags ({tag_sql[:-2]})'
        return create_stb_sql

    def set_create_database_sql(self,sql_dict):
        create_sql = 'create'
        for key,value in sql_dict.items():
            create_sql += f' {key} {value}'
        return create_sql

    def show_create_sysdb_sql(self):
        sysdb_list = {'information_schema', 'performance_schema'}
        for db in sysdb_list:
          tdSql.query(f'show create database {db}')
          tdSql.checkEqual(f'{db}',tdSql.queryResult[0][0])
          tdSql.checkEqual(f'CREATE DATABASE `{db}`',tdSql.queryResult[0][1])

    def show_create_systb_sql(self):
        for param in self.ins_param_list:
          tdSql.query(f'show create table information_schema.ins_{param}')
          tdSql.checkEqual(f'ins_{param}',tdSql.queryResult[0][0])

          tdSql.execute(f'use information_schema')
          tdSql.query(f'show create table ins_{param}')
          tdSql.checkEqual(f'ins_{param}',tdSql.queryResult[0][0])

        for param in self.perf_param_list:
          tdSql.query(f'show create table performance_schema.perf_{param}')
          tdSql.checkEqual(f'perf_{param}',tdSql.queryResult[0][0])

          tdSql.execute(f'use performance_schema')
          tdSql.query(f'show create table perf_{param}')
          tdSql.checkEqual(f'perf_{param}',tdSql.queryResult[0][0])

    def show_create_sql(self):
        create_db_sql = self.set_create_database_sql(self.db_param)
        print(create_db_sql)
        tdSql.execute(create_db_sql)
        tdSql.query(f'show create database {self.dbname}')
        tdSql.checkEqual(self.dbname,tdSql.queryResult[0][0])
        for key,value in self.db_param.items():
            if key == 'database':
                continue
            else:
                param = f'{key} {value}'
                if param in tdSql.queryResult[0][1].lower():
                    tdLog.info(f'show create database check success with {key} {value}')
                    continue
                else:
                    tdLog.exit(f"show create database check failed with {key} {value}")
        tdSql.query('show vnodes on dnode 1')
        tdSql.checkRows(self.vgroups)
        tdSql.execute(f'use {self.dbname}')

        column_dict = {
            '`ts`': 'timestamp',
            '`col1`': 'tinyint',
            '`col2`': 'smallint',
            '`col3`': 'int',
            '`col4`': 'bigint',
            '`col5`': 'tinyint unsigned',
            '`col6`': 'smallint unsigned',
            '`col7`': 'int unsigned',
            '`col8`': 'bigint unsigned',
            '`col9`': 'float',
            '`col10`': 'double',
            '`col11`': 'bool',
            '`col12`': 'varchar(20)',
            '`col13`': 'nchar(20)'

        }
        tag_dict = {
            '`t1`': 'tinyint',
            '`t2`': 'smallint',
            '`t3`': 'int',
            '`t4`': 'bigint',
            '`t5`': 'tinyint unsigned',
            '`t6`': 'smallint unsigned',
            '`t7`': 'int unsigned',
            '`t8`': 'bigint unsigned',
            '`t9`': 'float',
            '`t10`': 'double',
            '`t11`': 'bool',
            '`t12`': 'varchar(20)',
            '`t13`': 'nchar(20)',
            '`t14`': 'timestamp'

        }
        create_table_sql = self.set_stb_sql(self.stbname,column_dict,tag_dict)
        tdSql.execute(create_table_sql)
        tdSql.query(f'show create stable {self.stbname}')
        query_result = tdSql.queryResult
        tdSql.checkEqual(query_result[0][1].lower(),create_table_sql)
        tdSql.execute(f'create table {self.tbname} using {self.stbname} tags(1,1,1,1,1,1,1,1,1.000000e+00,1.000000e+00,true,"abc","abc123",0)')
        tag_sql = '('
        for tag_keys in tag_dict.keys():
            tag_sql += f'{tag_keys}, '
        tags = f'{tag_sql[:-2]})'
        sql = f'create table {self.tbname} using {self.stbname} {tags} tags (1, 1, 1, 1, 1, 1, 1, 1, 1.000000e+00, 1.000000e+00, true, "abc", "abc123", 0)'
        tdSql.query(f'show create table {self.tbname}')
        query_result = tdSql.queryResult
        tdSql.checkEqual(query_result[0][1].lower(),sql)
        tdSql.execute(f'drop database {self.dbname}')
    def check_gitinfo(self):
        taosd_gitinfo_sql = ''
        tdSql.query('show dnode 1 variables')
        for i in tdSql.queryResult:
            if i[1].lower() == "gitinfo":
                taosd_gitinfo_sql = f"gitinfo: {i[2]}"
        taos_gitinfo_sql = ''
        tdSql.query('show local variables')
        for i in tdSql.queryResult:
            if i[0].lower() == "gitinfo":
                taos_gitinfo_sql = f"gitinfo: {i[1]}"
        taos_info = os.popen('taos -V').read()
        taos_gitinfo = re.findall("^gitinfo.*",taos_info,re.M)
        tdSql.checkEqual(taos_gitinfo_sql,taos_gitinfo[0])
        taosd_info = os.popen('taosd -V').read()
        taosd_gitinfo = re.findall("^gitinfo.*",taosd_info,re.M)
        tdSql.checkEqual(taosd_gitinfo_sql,taosd_gitinfo[0])

    def show_base(self):
        for sql in ['dnodes','mnodes','cluster']:
            tdSql.query(f'show {sql}')
            print(tdSql.queryResult)
            tdSql.checkRows(1)
        tdSql.query('show grants')
        grants_info = tdSql.queryResult
        tdSql.query('show licences')
        licences_info = tdSql.queryResult
        tdSql.checkEqual(grants_info,licences_info)

    def show_column_name(self):
        tdSql.execute("create database db;")
        tdSql.execute("use db;")
        tdSql.execute("create table ta(ts timestamp, name nchar(16), age int , address int);")
        tdSql.execute("insert into ta values(now, 'jack', 19, 23);")
        
        colName1 = ["ts","name","age","address"]
        colName2 = tdSql.getColNameList("select last(*) from ta;")
        for i in range(len(colName1)):
            if colName2[i] != f"last({colName1[i]})":
                tdLog.exit(f"column name is different.  {colName2} != last({colName1[i]} ")
                return 

        # alter option        
        tdSql.execute("alter local 'keepColumnName' '1';")
        colName3 = tdSql.getColNameList("select last(*) from ta;")
        for col in colName3:
            if colName1 != colName3:
                tdLog.exit(f"column name is different. colName1= {colName1} colName2={colName3}")
                return

    def run(self):
        self.check_gitinfo()
        self.show_base()
        self.ins_check()
        self.perf_check()
        self.show_create_sql()
        self.show_create_sysdb_sql()
        self.show_create_systb_sql()
        self.show_column_name()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
