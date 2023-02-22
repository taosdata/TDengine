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
import random


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.setsql = TDSetSql()
        self.column_dict = {
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
        self.tag_dict = {
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

    
    def set_stb_sql(self,stbname,column_dict,tag_dict):
        column_sql = ''
        tag_sql = ''
        for k,v in column_dict.items():
            column_sql += f"{k} {v}, "
        for k,v in tag_dict.items():
            tag_sql += f"{k} {v}, "
        create_stb_sql = f'create stable {stbname} ({column_sql[:-2]}) tags ({tag_sql[:-2]})'
        return create_stb_sql
    
    # create stable and child tables
    def create_table(self, stbname, tbname, count):
        tdSql.prepare()
        tdSql.execute('use db')
        
        # create stable
        create_table_sql = self.set_stb_sql(stbname, self.column_dict, self.tag_dict)
        tdSql.execute(create_table_sql)

        # create child table
        for i in range(count):
            tags = f'{i},{i},{i},{i},{i},{i},{i},{i},{i}.000{i},{i}.000{i},true,"var{i}","nch{i}",now'
            sql  = f'create table {tbname}{i} using {stbname} tags({tags})'
            tdSql.execute(sql)
    
    
    # create stable and child tables
    def create_tagidx(self, stbname):
        for key in self.tag_dict.keys:
            sql = f'create idx_{key} on {stbname} ({key})'
            tdSql.execute(sql)

    # check show indexs
    def show_tagidx(self, stbname):
        sql = f'select index_name,column_name from information_schema.ins_indexes where db_name="db"'
        tdSql.execute(sql)
        i = 0
        for key in self.tag_dict.keys:
            tdSql.checkData(i, 0, f'idx_{key}')
            tdSql.checkData(i, 1, f'{key}')
            i += 1

    # query with tag idx
    def query_tagidx(self, stbname):
        sql = f'select * from meters where t1=10'
        tdSql.execute(sql)
        sql = f'select * from meters where t2<10'
        tdSql.execute(sql)
        sql = f'select * from meters where t3>10'
        tdSql.execute(sql)
        sql = f'select * from meters where t12="11"'
        tdSql.execute(sql)
        sql = f'select * from meters where (t4 < 10 or t5 = 20) and t6= 30'
        tdSql.execute(sql)
        sql = f'select * from meters where (t7 < 20 and t8 = 20) or t9= 30'
        tdSql.execute(sql)

    # drop child table
    def drop_tables(self, tbname, count):
        start = random.randint(1, count/2)
        end   = random.random(count/2 + 1, count - 1)
        for i in range(start, end):
            sql = f'drop table {tbname}{i}'
            tdSql.execute(sql)

    # drop tag index
    def drop_tagidx(self, stbname):
        # drop index
        for key in self.tag_dict.keys:
            sql = f'drop index idx_{key}'
            tdSql.execute(sql)
        
        # check idx result is 0
        sql = f'select index_name,column_name from information_schema.ins_indexes where db_name="db"'
        tdSql.execute(sql)
        tdSql.checkRows(0)

    # run
    def run(self):
        stable = "meters"
        tbname = "d"
        count = 1000
        self.create_table(stable, tbname, count)
        self.create_tagidx(stable)
        self.show_tagidx(stable)
        self.query_tagidx(stable)
        self.drop_tables(tbname, count)
        self.drop_tagidx(stable)
        # query after delete , expect no crash
        self.query_tagidx(stable)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())

