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
import subprocess


class PerfDB:
    def __init__(self):
        self.sqls = []
        self.spends = []

    # execute
    def execute(self, sql):
        print(f"  perfdb execute {sql}")
        stime = time.time()
        ret = tdSql.execute(sql, 1)
        spend = time.time() - stime

        self.sqls.append(sql)
        self.spends.append(spend)
        return ret

    # query
    def query(self, sql):
        print(f"  perfdb query {sql}")
        start = time.time()
        ret = tdSql.query(sql, None, 1)
        spend = time.time() - start
        self.sqls.append(sql)
        self.spends.append(spend)
        return ret
    

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
            'col12': 'varchar(120)',
            'col13': 'nchar(100)',
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
            't12': 'varchar(120)',
            't13': 'nchar(100)',         
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
        tdSql.execute("create database db wal_retention_period 4")
        tdSql.execute('use db')
        self.child_count = count
        self.stbname = stbname
        self.tbname  = tbname
        
        # create stable
        create_table_sql = self.set_stb_sql(stbname, self.column_dict, self.tag_dict)
        tdSql.execute(create_table_sql)

        batch_size = 1000
        # create child table
        for i in range(count):
            ti = i % 128
            tags = f'{ti},{ti},{i},{i},{ti},{ti},{i},{i},{i}.000{i},{i}.000{i},true,"var{i}","nch{i}"'
            sql  = f'create table {tbname}{i} using {stbname} tags({tags});'
            tdSql.execute(sql)            
            if i % batch_size == 0:
               tdLog.info(f" create child table {i} ...")

        tdLog.info(f" create {count} child tables ok.")

    # create with dicts
    def create_sf_dicts(self, dicts, filename):
        for fun_name, out_type in dicts.items():
            sql = f' create function {fun_name} as "{self.udf_path}/{filename}" outputtype {out_type} language "Python" '
            tdSql.execute(sql)
            tdLog.info(sql)

    # create_udfpy_function
    def create_scalar_udfpy(self):
        # scalar funciton
        self.scalar_funs = {
            'sf0': 'timestamp',    
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
            'sf12': 'varchar(120)',
            'sf13': 'nchar(100)'
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
            'af12': 'varchar(120)',
            'af13': 'nchar(100)',
            'af14': 'timestamp'            
        }

        # multi_args
        self.create_sf_dicts(self.scalar_funs, "sf_origin.py")
        fun_name = "sf_multi_args"
        self.create_udf_sf(fun_name, f'{fun_name}.py', "binary(1024)")

        # all type check null
        for col_name, col_type in self.column_dict.items():
             self.create_udf_sf(f"sf_null_{col_name}", "sf_null.py", col_type)

        # concat
        fun_name = "sf_concat_var"
        self.create_udf_sf(fun_name, f'{fun_name}.py', "varchar(1024)")
        fun_name = "sf_concat_nch"
        self.create_udf_sf(fun_name, f'{fun_name}.py', "nchar(1024)")
             

    # fun_name == fun_name.py
    def create_udf_sf(self, fun_name, file_name, out_type):
        sql = f'create function {fun_name} as "{self.udf_path}/{file_name}" outputtype {out_type} language "Python" '
        tdSql.execute(sql)
        tdLog.info(sql)

    def create_udf_af(self, fun_name, file_name, out_type, bufsize):
        sql = f'create aggregate function {fun_name} as "{self.udf_path}/{file_name}" outputtype {out_type} bufsize {bufsize} language "Python" '
        tdSql.execute(sql)
        tdLog.info(sql)


    # sql1 query result eual with sql2
    def verify_same_result(self, sql1, sql2):
        # query
        result1 = tdSql.getResult(sql1)
        tdSql.query(sql2)
        
        for i, row in enumerate(result1):
            for j , val in enumerate(row):
                tdSql.checkData(i, j, result1[i][j])

    # same value like select col1, udf_fun1(col1) from st
    def verify_same_value(self, sql, col=0):
        tdSql.query(sql)
        nrows = tdSql.getRows()
        for i in range(nrows):
            val = tdSql.getData(i, col)
            tdSql.checkData(i, col + 1, val)

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
        cols = list(self.column_dict.keys()) + list(self.tag_dict.keys())
        cols.remove("col13")
        cols.remove("t13")
        cols.remove("ts")
        ncols = len(cols)
        print(cols)
        for i in range(2, ncols):
            sample = random.sample(cols, i)
            print(sample)
            cols_name = ','.join(sample)
            sql = f'select sf_multi_args({cols_name}),{cols_name} from {self.stbname} limit 10'
            self.verify_same_multi_values(sql)
            tdLog.info(sql)

    
    # query_udfpy
    def query_scalar_udfpy(self):
        # col
        for col_name, col_type in self.column_dict.items():
           for fun_name, out_type in self.scalar_funs.items():
               if col_type == out_type :                   
                    sql = f'select {col_name}, {fun_name}({col_name}) from {self.stbname} limit 10'
                    tdLog.info(sql)
                    self.verify_same_value(sql)
                    sql = f'select * from (select {col_name} as a, {fun_name}({col_name}) as b from {self.stbname}  limit 100) order by b,a desc'
                    tdLog.info(sql)
                    self.verify_same_value(sql)

        # multi-args
        self.query_multi_args()       

        # all type check null
        for col_name, col_type in self.column_dict.items():             
             fun_name = f"sf_null_{col_name}"
             sql = f'select {fun_name}({col_name}) from {self.stbname}'
             tdSql.query(sql)
             if col_type != "timestamp":
                tdSql.checkData(0, 0, "None")
             else:
                val = tdSql.getData(0, 0)
                if val is not None:
                    tdLog.exit(f" check {sql} not expect None.")

        # concat
        sql = f'select sf_concat_var(col12, t12), concat(col12, t12) from {self.stbname} limit 1000'
        self.verify_same_value(sql)
        sql = f'select sf_concat_nch(col13, t13), concat(col13, t13) from {self.stbname} limit 1000'
        self.verify_same_value(sql)

    # create aggregate 
    def create_aggr_udfpy(self):

        bufsize = 200 * 1024
        # all type check null
        for col_name, col_type in self.column_dict.items():
             self.create_udf_af(f"af_null_{col_name}", "af_null.py", col_type, bufsize)

        # min
        file_name = "af_min.py"
        fun_name = "af_min_float"
        self.create_udf_af(fun_name, file_name, f"float", bufsize)
        fun_name = "af_min_int"
        self.create_udf_af(fun_name, file_name, f"int", bufsize)

        # sum
        file_name = "af_sum.py"
        fun_name = "af_sum_float"
        self.create_udf_af(fun_name, file_name, f"float", bufsize)
        fun_name = "af_sum_int"
        self.create_udf_af(fun_name, file_name, f"int", bufsize)
        fun_name = "af_sum_bigint"
        self.create_udf_af(fun_name, file_name, f"bigint", bufsize)

        # count
        file_name = "af_count.py"
        fun_name = "af_count_float"
        self.create_udf_af(fun_name, file_name, f"float", bufsize)
        fun_name = "af_count_int"
        self.create_udf_af(fun_name, file_name, f"int", bufsize)
        fun_name = "af_count_bigint"
        self.create_udf_af(fun_name, file_name, f"bigint", bufsize)


    # query aggregate 
    def query_aggr_udfpy(self) :
        # all type check null
        for col_name, col_type in self.column_dict.items():
             fun_name = f"af_null_{col_name}"
             sql = f'select {fun_name}({col_name}) from {self.stbname}'
             tdSql.query(sql)
             if col_type != "timestamp":
                tdSql.checkData(0, 0, "None")
             else:
                val = tdSql.getData(0, 0)
                if val is not None:
                    tdLog.exit(f" check {sql} not expect None.")

        # min
        sql = f'select min(col3), af_min_int(col3) from {self.stbname}'
        self.verify_same_value(sql)
        sql = f'select min(col7), af_min_int(col7) from {self.stbname}'
        self.verify_same_value(sql)
        sql = f'select min(col9), af_min_float(col9) from {self.stbname}'
        self.verify_same_value(sql)

        # sum
        sql = f'select sum(col1), af_sum_int(col1) from d0'
        self.verify_same_value(sql)
        sql = f'select sum(col3), af_sum_bigint(col3) from {self.stbname}'
        self.verify_same_value(sql)
        sql = f'select sum(col9), af_sum_float(col9) from {self.stbname}'
        self.verify_same_value(sql)

        # count
        sql = f'select count(col1), af_count_int(col1) from {self.stbname}'
        self.verify_same_value(sql)
        sql = f'select count(col7), af_count_bigint(col7) from {self.stbname}'
        self.verify_same_value(sql)
        sql = f'select count(col8), af_count_float(col8) from {self.stbname}'
        self.verify_same_value(sql)

        # nest
        sql = f'select a+1000,b+1000 from (select count(col8) as a, af_count_float(col8) as b from {self.stbname})'
        self.verify_same_value(sql)
        # group by
        sql = f'select a+1000,b+1000 from (select count(col8) as a, af_count_float(col8) as b from {self.stbname} group by tbname)'
        self.verify_same_value(sql)
        # two filed expr
        sql = f'select sum(col1+col2),af_sum_float(col1+col2) from {self.stbname};'
        self.verify_same_value(sql)
        # interval
        sql = f'select af_sum_float(col2+col3),sum(col3+col2) from {self.stbname} interval(1s)'
        self.verify_same_value(sql)

    
    # insert to child table d1 data
    def insert_data(self, tbname, rows):
        ts = 1670000000000
        values = ""
        batch_size = 500
        child_name = ""
        for i in range(self.child_count):
            for j in range(rows):
                tj = j % 128
                cols = f'{tj},{tj},{j},{j},{tj},{tj},{j},{j},{j}.000{j},{j}.000{j},true,"var{j}","nch{j}涛思数据codepage is utf_32_le"'
                value = f'({ts+j},{cols})' 
                if values == "":
                    values = value
                else:
                    values += f",{value}"
                if j % batch_size == 0 or j + 1 == rows:
                   sql = f'insert into {tbname}{i} values {values};' 
                   tdSql.execute(sql)
                   tdLog.info(f" child table={i} rows={j} insert data.")
                   values = ""

        # partial columns upate
        sql = f'insert into {tbname}0(ts, col1, col9, col11) values(now, 100, 200, 0)'
        tdSql.execute(sql)
        sql = f'insert into {tbname}0(ts, col2, col5, col8) values(now, 100, 200, 300)'
        tdSql.execute(sql)
        sql = f'insert into {tbname}0(ts, col3, col7, col13) values(now, null, null, null)'
        tdSql.execute(sql)        
        sql = f'insert into {tbname}0(ts) values(now)'
        tdSql.execute(sql)
        tdLog.info(f" insert {rows} to child table {self.child_count} .")

   
    # create stream
    def create_stream(self):
        sql = f"create stream ma  into sta subtable(concat('sta_',tbname)) \
            as select _wstart,count(col1),af_count_bigint(col1) from {self.stbname} partition by tbname interval(1s);"
        tdSql.execute(sql)
        tdLog.info(sql)

    #  query stream
    def verify_stream(self):
        sql = f"select * from sta limit 10"
        self.verify_same_value(sql, 1)

    # create tmq
    def create_tmq(self):
        sql = f"create topic topa as select concat(col12,t12),sf_concat_var(col12,t12) from {self.stbname};"   
        tdSql.execute(sql)
        tdLog.info(sql)

    def install_taospy(self):
        tdLog.info("install taospyudf...")
        packs = ["taospyudf"]
        for pack in packs:
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-i', 'https://pypi.org/simple', '-U', pack])
        tdLog.info("call ldconfig...")
        os.system("ldconfig")
        tdLog.info("install taospyudf successfully.")

    # run
    def run(self):
        self.install_taospy()

        # var
        stable = "meters"
        tbname = "d"
        count = 10
        rows =  5000
        # do 
        self.create_table(stable, tbname, count)

        # create
        self.create_scalar_udfpy()
        self.create_aggr_udfpy()

        # create stream
        self.create_stream()

        # create tmq
        self.create_tmq()

        # insert data
        self.insert_data(tbname, rows)

        # query
        self.query_scalar_udfpy()
        self.query_aggr_udfpy()

        # show performance


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
