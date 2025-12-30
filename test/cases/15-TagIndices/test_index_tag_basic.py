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


from new_test_framework.utils import tdLog, tdSql, TDSetSql
import random


class TestTagIndexBasic:
    #
    # ------------------- 1 ----------------
    #
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.setsql = TDSetSql()
        cls.column_dict = {
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
        cls.tag_dict = {
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

        tables = []
        # create child table
        for i in range(count):
            ti = i % 128
            tags = f'{ti},{ti},{i},{i},{ti},{ti},{i},{i},{i}.000{i},{i}.000{i},true,"var{i}","nch{i}",now'
            tables.append(f'{tbname}{i} using {stbname} tags({tags})')        
        sql = 'create table ' + ' '.join(tables)
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

    #
    # ------------------- 2 ----------------
    #
    def prepareData(self):
        self.dbname = 'db'
        self.stbname = 'st'
        # db
        tdSql.execute("create database {};".format(self.dbname))
        tdSql.execute("use {};".format(self.dbname))
        tdLog.debug("Create database %s" % self.dbname)

        # super table
        tdSql.execute("create table {} (ts timestamp, col1 int) tags (t1 int, t2 binary(16), t3 nchar(16));".format(self.stbname))
        tdLog.debug("Create super table %s" % self.stbname)

        # create index for all tags
        tdSql.execute("create index t2_st on {} (t2);".format(self.stbname))
        tdSql.execute("create index t3_st on {} (t3);".format(self.stbname))

    def do_ts4403(self):
        self.prepareData()
        # check index number
        tdSql.query("show indexes from {};".format(self.stbname))
        assert(3 == len(tdSql.queryResult))
        tdLog.debug("All tags of super table have index successfully")

        # drop default first tag index
        tdSql.execute("drop index t1_st;")
        tdSql.query("show indexes from {};".format(self.stbname))
        assert(2 == len(tdSql.queryResult))
        tdLog.debug("Delete the default index of first tag successfully")

        # create index for first tag
        tdSql.execute("create index t1_st on {} (t1);".format(self.stbname))
        tdSql.query("show indexes from {};".format(self.stbname))
        assert(3 == len(tdSql.queryResult))
        tdLog.debug("Create index for first tag successfully")

        # null as index value to create child table
        tdSql.execute("create table ct1 using {} tags(null, null, null);".format(self.stbname))
        tdSql.query("show tables;")
        assert(1 == len(tdSql.queryResult))
        tdLog.debug("Create child table with tags value as 'null' successfully")

        # redundant index with different index name for some tag
        tdSql.error("create index t2_ct1 on st (t2);")
        tdLog.debug("Verify redundant index with different index name for some tag successfully")

        # same index name for different tag
        tdSql.error("create index t2_st on st (t3);")
        tdLog.debug("Verify same index name for some tag successfully")

        # add index for multiple tags(TD-28078)
        tdSql.error("create index tt on {} (t2, t3);".format(self.stbname))
        tdLog.debug("Verify add index for multiple tags successfully")
    

    #
    # ------------------- main ----------------
    #
    
    # run
    def test_index_tag_basic(self):
        """Tagindex basic
        
        1. Create 1 stable and 1000 child tables
        2. Create tag index for each tag column except the first one
        3. Check error cases when creating tag index with invalid column names
        4. Insert data into some child tables
        5. Check create tag is successful
        6. Query data using tag index and verify results
        7. Drop a range of child tables
        8. Drop all tag indexes
        9. Attempt to create tag index with excessively long name and verify error
        10. bug TS-4403: Create/drop tag index on supertable and verify behavior
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-11-04 Alex Duan Migrated from uncatalog/system-test/0-others/test_tag_index_basic.py
            - 2025-12-15 Alex Duan Migrated from cases/uncatalog/system-test/2-query/test_test_ts_4403.py
 
        """
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
        
        self.do_ts4403()

        tdLog.success("%s successfully executed" % __file__)

