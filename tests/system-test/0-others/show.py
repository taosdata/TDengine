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
        self.ins_param_list = ['dnodes','mnodes','qnodes','cluster','functions','users','grants','topics','subscriptions','streams']
        self.perf_param = ['apps','connections','consumers','queries','transactions']
        self.perf_param_list = ['apps','connections','consumers','queries','trans']

    def ins_check(self):
        tdSql.prepare()
        for param in self.ins_param_list:
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
    def show_sql(self):
        tdSql.prepare()
        tdSql.execute('use db')
        stbname = f'`{tdCom.getLongName(5)}`'
        tbname = f'`{tdCom.getLongName(3)}`'
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
        create_table_sql = self.set_stb_sql(stbname,column_dict,tag_dict)
        tdSql.execute(create_table_sql)
        tdSql.query(f'show create table {stbname}')
        query_result = tdSql.queryResult
        tdSql.checkEqual(query_result[0][1].lower(),create_table_sql)
        tdSql.execute(f'create table {tbname} using {stbname} tags(1,1,1,1,1,1,1,1,1.000000e+00,1.000000e+00,true,"abc","abc123",0)')
        tag_sql = '('
        for tag_keys in tag_dict.keys():
            tag_sql += f'{tag_keys}, '
        tags = f'{tag_sql[:-2]})' 
        sql = f'create table {tbname} using {stbname} {tags} tags (1, 1, 1, 1, 1, 1, 1, 1, 1.000000e+00, 1.000000e+00, true, "abc", "abc123", 0)'
        tdSql.query(f'show create table {tbname}')
        query_result = tdSql.queryResult
        tdSql.checkEqual(query_result[0][1].lower(),sql)
        tdSql.execute('drop database db')
    def run(self):
        self.ins_check()
        self.perf_check()
        self.show_sql()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())

