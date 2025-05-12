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
        tdSql.init(conn.cursor(), True)
        self.setsql = TDSetSql()
        self.column_dict = {
            'ts': 'timestamp',
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
            'col12': 'varchar(20)',
            'col13': 'nchar(20)'            
        }
        self.tag_dict = {
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
            't12': 'varchar(20)',
            't13': 'nchar(20)',
            't14': 'timestamp'            
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
            ti = i % 128
            tags = f'{ti},{ti},{i},{i},{ti},{ti},{i},{i},{i}.000{i},{i}.000{i},true,"var{i}","nch{i}",now'
            sql  = f'create table {tbname}{i} using {stbname} tags({tags})'
            tdSql.execute(sql)

        tdLog.info(f" create {count} child tables ok.")    
    
    def create_tagidx_check(self, stbname):
        err_dict = {"NULL","",",","\"","\"\"","undef","t1,t2","t12,t12"}
        for errs in err_dict:
            sql = (f'create index idx_err_check on {stbname} (%s)'% (errs))
            tdLog.info(f'  sql={sql}')
            tdSql.error(f'{sql}')
        tdLog.info(f' create tagidx check ok.')
    
    # create stable and child tables
    def create_tagidx(self, stbname):
        cnt = -1
        for key in self.tag_dict.keys():
            # first tag have default index, so skip
            if cnt == -1:
                cnt = 0
                continue; 
            sql = f'create index idx_{key} on {stbname} ({key})'
            tdLog.info(f"  sql={sql}")
            tdSql.execute(sql)
            cnt += 1
        tdLog.info(f' create {cnt} tag indexs ok.')

     # insert to child table d1 data
    def insert_data(self, tbname):
        # d1 insert 3 rows
        for i in range(3):
            sql = f'insert into {tbname}1(ts,col1) values(now+{i}s,{i});' 
            tdSql.execute(sql)
        # d20 insert 4
        for i in range(4):
            sql = f'insert into {tbname}20(ts,col1) values(now+{i}s,{i});' 
            tdSql.execute(sql)

    # check show indexs
    def show_tagidx(self, stbname):
        sql = f'select index_name,column_name from information_schema.ins_indexes where db_name="db"'
        tdSql.query(sql)
        rows = len(self.tag_dict.keys())
        tdSql.checkRows(rows)

        for i in range(rows):
            col_name = tdSql.getData(i, 1)
            idx_name = f'idx_{col_name}'
            # skip first tag
            if col_name == "t1":
                continue
            tdSql.checkData(i, 0, idx_name)

        tdLog.info(f' show {rows} tag indexs ok.')

    # query with tag idx
    def query_tagidx(self, stbname):
        sql = f'select * from meters where t1=1'
        tdSql.query(sql)
        tdSql.checkRows(3)

        sql = f'select * from meters where t2<10'
        tdSql.query(sql)
        tdSql.checkRows(3)

        sql = f'select * from meters where t2>10'
        tdSql.query(sql)
        tdSql.checkRows(4)

        sql = f'select * from meters where t3<30'
        tdSql.query(sql)
        tdSql.checkRows(7)

        sql = f'select * from meters where t12="11"'
        tdSql.query(sql)
        tdSql.checkRows(0)

        sql = f'select t12 ,t13,tbname from meters where t13="nch20"'
        tdSql.query(sql)
        tdSql.checkRows(4)

        sql = f'select * from meters where t12 like "%ab%" '
        tdSql.query(sql)
        tdSql.checkRows(0)

        sql = f'select * from meters where t13 = "d20" '
        tdSql.query(sql)
        tdSql.checkRows(0)

        sql = f'select * from meters where tbname = "d20" '
        tdSql.query(sql)
        tdSql.checkRows(4)

        sql = f'select * from meters where (t4 < 10 or t5 = 20) and t6= 30'
        tdSql.query(sql)
        tdSql.checkRows(0)

        sql = f'select * from meters where (t7 < 20 and t8 = 20) or t4 = 20'
        tdSql.query(sql)
        tdSql.checkRows(4)

    # drop child table
    def drop_tables(self, tbname, count):
        # table d1 and d20 have verify data , so can not drop
        start = random.randint(21, count/2)
        end   = random.randint(count/2 + 1, count - 1)
        for i in range(start, end):
            sql = f'drop table {tbname}{i}'
            tdSql.execute(sql)
        cnt = end - start + 1    
        tdLog.info(f' drop table from {start} to {end} count={cnt}')     

    # drop tag index
    def drop_tagidx(self, stbname):
        # drop index
        cnt = -1
        for key in self.tag_dict.keys():
            # first tag have default index, so skip
            if cnt == -1:
                cnt = 0
                continue; 
            sql = f'drop index idx_{key}'
            tdSql.execute(sql)
            cnt += 1
        
        # check idx result is 0
        sql = f'select index_name,column_name from information_schema.ins_indexes where db_name="db"'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdLog.info(f' drop {cnt} tag indexs ok.')

    # create long name idx 
    def longname_idx(self, stbname):
        long_name = "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjjkkkkkkkkkkllllllllllmmmmmmmmmm"
        key = "t3"
        sql = f'create index {long_name} on {stbname} ({key})'
        tdSql.error(sql)

        long_name = "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffff"
        key = "t3"
        sql = f'create index {long_name} on {stbname} ({key})'
        tdLog.info(f"{sql}")
        tdSql.execute(sql)
        sql = f'drop index {long_name}'
        tdLog.info(f"{sql}")
        tdSql.execute(sql)

    # run
    def run(self):
        # var
        stable = "meters"
        tbname = "d"
        count = 1000
        # do 
        self.create_table(stable, tbname, count)
        self.create_tagidx_check(stable)
        self.create_tagidx(stable)
        self.insert_data(tbname)
        self.show_tagidx(stable)
        self.query_tagidx(stable)
        self.drop_tables(tbname, count)
        self.drop_tagidx(stable)
        # query after delete , expect no crash
        self.query_tagidx(stable)
        self.longname_idx(stable)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())