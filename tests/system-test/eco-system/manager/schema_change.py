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
import time
import traceback


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)
        self.setsql = TDSetSql()
        self.column_dict = {
            'col0': 'int',
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


    # delete 
    def delete_col(self, columns, cnt, max_col):
        # delte for random
        for i in range(cnt):
           col_cnt = len(columns)
           if col_cnt == 0:
               return 
           sel = random.randint(0, col_cnt - 1)
           sql = f"alter table meters drop column `{columns[sel]}`"
           try: 
               tdSql.execute(sql)
               tdLog.info(f" drop cur col={len(columns)} max_col={max_col} {sql}")
               del columns[sel]
           except:
               tdLog.info(f" drop column failed. {sql}")
               traceback.print_exc()
               

    # describe table   
    def describe_table(self, tbname):
        columns = []
        tags = []
        sql = f"describe {tbname}"
        row_cnt = tdSql.query(sql)
        for i in range(20, row_cnt):
            col_name  = tdSql.queryResult[i][0]
            type_name = tdSql.queryResult[i][3]
            if type_name == "TAG":
                tags.append(col_name)
            else:
                columns.append(col_name)

        return columns,tags
    
    def renames(self, tags, cnt):
        col_cnt = len(tags)
        if col_cnt < 10:
            return 
        for i in range(cnt):
            sel = random.randint(1, col_cnt-3)
            new_name = tags[sel] + "n"
            sql = f"alter table meters rename tag `{tags[sel]}` `{new_name}` "
            try:
               tdSql.execute(sql)
               tdLog.info(sql)
               tags[sel] = new_name
            except:
               tdLog.info(f" rename tag failed. {sql}")
               traceback.print_exc()
    

    def drop_tag(self, tags, cnt):        
        for i in range(cnt):
            tag_cnt = len(tags)
            sel = random.randint(1, tag_cnt-1)
            sql = f"alter table meters drop tag `{tags[sel]}` "
            try:
               tdSql.execute(sql)
               tdLog.info(sql)
               del tags[sel]
            except:
               tdLog.info(f" drop tags failed. {sql}")
               traceback.print_exc()

     # query
    def query_table(self, columns, tags):
        if len(columns) < 10 :
            return 
        if len(tags) < 10:
            return 

        sel_cols = random.sample(columns, random.randint(2,int(len(columns)/10)))
        sel_tags = random.sample(tags, random.randint(1,int(len(tags)/10)))

        field_cols = ",".join(sel_cols)
        field_tags = ",".join(sel_tags)

        #sql = f"select {field_cols},{field_tags} from meters ;"
        sql = f"select {field_cols},{field_tags} from meters"
        try:
            tdLog.info( " query sql:" + sql)
            tdSql.query("select * from meters limit 1")
        except:
            tdLog.info( " query failed :" + sql)
            traceback.print_exc()        
    
    # change table schema
    def change_schema(self, change_cnt):
        # init
        columns, tags = self.describe_table("meters")
        max_col = random.randint(200, 2000)
        tdLog.info(f" ----------- set max column = {max_col} -------------")
        for i in range(change_cnt):
            col_cnt = len(self.column_dict)
            icol = random.randint(0, col_cnt-1)
            key = f"col{icol}"
            col_name = key + f"_{i}_{random.randint(1,100)}"
            col_type = self.column_dict[key]
            sql = f'alter table meters add column `{col_name}` {col_type}'
            sql_tag = f'alter table meters add tag `t_{col_name}` {col_type}'
            
            try:
                tdSql.execute(sql)
                tdLog.info(f" add cur col={len(columns)} max_col={max_col} {sql}")
                columns.append(col_name)
                if random.randint(1, 4) == 2:
                    tdSql.execute(sql_tag)
                    tdLog.info(f" add tag tag_cnt={len(tags)} {sql_tag}")

            except:
                tdLog.info(f" add column failed. {sql}")
                traceback.print_exc()


            col_cnt = len(columns)
            # delete
            if col_cnt > max_col + 100:
                self.delete_col(columns, random.randint(1, 30), max_col)
            elif col_cnt >= max_col + 30:
                self.delete_col(columns, random.randint(1, 4), max_col)
                max_col = random.randint(200, 2000)
                tdLog.info(f" ----------- set max column = {max_col} -------------")
            elif col_cnt > max_col:
                self.delete_col(columns, random.randint(1, 3), max_col)
    


            if i % 50 == 0:
                sql = f"flush database {self.dbname};"
                tdSql.execute(sql)
                tdLog.info(f"  ***** {sql}  *****")

            # query 
            if i % 70 == 0:
                self.query_table(columns, tags)

            # drop and rename
            if i % 10 == 0:
                # update columns
                columns,tags = self.describe_table("meters")
                tdLog.info(f"  =======  describe table column count = {len(columns)} tags= {len(tags)}======")

                if random.randint(1,3) == 2:
                    self.query_table(columns, tags)

                if len(tags) > 50:
                    self.drop_tag(tags, random.randint(1, 30))

                self.renames(tags, random.randint(1, 10))

                
            # sleep
            #time.sleep(0.3)


    # run
    def run(self):
        # seed
        random.seed(int(time.time()))
        self.dbname = "schema_change"

        # switch db
        tdSql.execute(f"use {self.dbname};")

        # change meters
        self.change_schema(1000000)

        
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())