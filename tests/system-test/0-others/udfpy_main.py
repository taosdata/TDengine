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
import os


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.setsql = TDSetSql()

        # udf path
        self.udf_path = os.path.dirname(os.path.realpath(__file__)) + "/udfpy"


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
            'col13': 'nchar(20)',
            'col14': 'timestamp'
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
        self.child_count = count
        self.stbname = stbname
        self.tbname  = tbname
        
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

    def create_udfpy_impl(self, funs, filename):
        for name, outtype in funs.items():
            sql = f' create function {name} as "{self.udf_path}/{filename} {outtype} " language "Python" '
            tdSql.execute(sql)


    def create_udfpy_dicts(self, dicts, filename):
        for k,v in dicts:
            self.create_udfpy_impl(k, v, filename)

    # create_udfpy_function
    def create_udfpy_function(self):
        # function


        # scalar funciton
        self.scalar_funs = {
            'sf1': 'tinyint',
            'sf2': 'smallint',
            'sf3': 'int',
            'sf4': 'bigint',
            'sf5': 'tinyint unsigned',
            'sf6': 'smallint unsigned',
            'sf7': 'int unsigned',
            'sf8': 'bigint unsigned',
            'sf9': 'float',
            'sf10': 'double',
            'sf11': 'bool',
            'sf12': 'varchar(20)',
            'sf13': 'nchar(20)',
            'sf14': 'timestamp'            
        }
        # agg function
        self.agg_funs = {
            'af1': 'tinyint',
            'af2': 'smallint',
            'af3': 'int',
            'af4': 'bigint',
            'af5': 'tinyint unsigned',
            'af6': 'smallint unsigned',
            'af7': 'int unsigned',
            'af8': 'bigint unsigned',
            'af9': 'float',
            'af10': 'double',
            'af11': 'bool',
            'af12': 'varchar(20)',
            'af13': 'nchar(20)',
            'af14': 'timestamp'            
        }

        # files
        self.create_udfpy_function(self.scalar_funs, "fun_origin")
        self.create_udf_sf("sf_multi_args", "binary(1024)")

        #self.create_udfpy_function(self.agg_funs, None)

    def create_udf_sf(self, fun_name, out_type):
        sql = f'create function {fun_name} as {self.udf_path}{fun_name}.py {out_type} language "Python"'
        tdSql.execute(sql)

    def create_udf_af(self, fun_name, out_type, bufsize):
        sql = f'create aggregate function {fun_name} as {self.udf_path}{fun_name}.py {out_type} bufsize {bufsize} language "Python"'
        tdSql.execute(sql)


    # sql1 query result eual with sql2
    def verify_same_result(self, sql1, sql2):
        # query
        result1 = tdSql.getResult(sql1)
        tdSql.query(sql2)
        
        for i, row in enumerate(result1):
            for j , val in enumerate(row):
                tdSql.checkData(i, j, result1[i][j])

    # same value like select col1, udf_fun1(col1) from st
    def verfiy_same_value(sql):
        tdSql.query(sql)
        nrows = tdSql.getRows()
        for i in range(nrows):
            val = tdSql.getData(i, 0)
            tdSql.checkData(i, 1, val)

    # verify multi values
    def verify_same_multi_values(self, sql):
        tdSql.query(sql)
        nrows = tdSql.getRows()
        for i in range(nrows):
            udf_val = tdSql.getData(i, 0)
            vals = udf_val.split(',')
            for j,val in enumerate(vals, 1):
                tdSql.checkData(i, j, val)
            
    # query multi-args
    def query_multi_args(self):   
        cols = self.column_dict.keys() + self.tag_dict.keys()
        ncols = len(cols)
        for i in range(2, ncols):
            sample = random.sample(i)
            cols_name = ','.join(sample)
            sql = f'select  sf_multi_args({cols_name}),{cols_name} from {self.stbname}'
            self.verify_same_multi_values(sql)

    
    # query_udfpy
    def query_scalar_udfpy(self):
        # col
        for col_name, col_type in self.column_dict:
           for fun_name, out_type in self.scalar_funs:
               sql = f'select {col_name} {fun_name}({col_name}) from {self.stbname}'
               self.verify_same_value(sql)

        # multi-args
        self.query_multi_args()       

    # create aggregate 
    def create_aggr_udfpy(self):
        # all type check null
        for col_name, col_type in self.column_dict:
             self.create_udf_af(f"af_null_{col_name}", f"{col_type}", 10*1024*1024)

        # min
        self.create_udf_af(f"af_min_float", f"float", 10*1024*1024)
        self.create_udf_af(f"af_min_int", f"int", 10*1024*1024)

        # sum
        self.create_udf_af(f"af_sum_float", f"float", 100*1024*1024)
        self.create_udf_af(f"af_sum_int", f"sum", 100*1024*1024)


    # query aggregate 
    def query_aggr_udfpy(self) :
        # all type check null
        for col_name, col_type in self.column_dict:
             fun_name = f"af_null_{col_name}"
             sql = f'select {fun_name}(col_name) from {self.stbname}'
             tdSql.query(sql)
             tdSql.checkData(0, 0, "NULL")

        # min
        sql = f'select min(col3), af_min_int(col3) from {self.stbname}'
        self.verfiy_same_value(sql)
        sql = f'select min(col7), af_min_int(col7) from {self.stbname}'
        self.verfiy_same_value(sql)
        sql = f'select min(col9), af_min_float(col9) from {self.stbname}'
        self.verfiy_same_value(sql)

        # sum
        sql = f'select sum(col3), af_sum_int(col3) from {self.stbname}'
        self.verfiy_same_value(sql)
        sql = f'select sum(col7), af_sum_int(col7) from {self.stbname}'
        self.verfiy_same_value(sql)
        sql = f'select sum(col9), af_sum_float(col9) from {self.stbname}'
        self.verfiy_same_value(sql)


            
    
    # insert to child table d1 data
    def insert_data(self, tbname, rows):
        ts = 1670000000000
        for i in range(self.child_count):
            for j in range(rows):
                ti = j % 128
                cols = f'{ti},{ti},{i},{i},{ti},{ti},{i},{i},{i}.000{i},{i}.000{i},true,"var{i}","nch{i}",now'
                sql = f'insert into {tbname}{i} values({ts+j},{cols});' 
                tdSql.execute(sql)

        tdLog.info(f" insert {rows} for each child table.")


    # run
    def run(self):
        # var
        stable = "meters"
        tbname = "d"
        count = 100
        # do 
        self.create_table(stable, tbname, count)
        self.insert_data(tbname, 1000)

        # scalar
        self.create_scalar_udfpy()
        self.query_scalar_udfpy()

        # aggregate
        self.create_aggr_udfpy()
        self.query_aggr_udfpy()




    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())