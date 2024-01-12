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

import random
import os
import time
import taos
import subprocess
from faker import Faker
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
from util.dnodes import tdDnodes
from util.dnodes import *

class TDTestCase:
    updatecfgDict = {'maxSQLLength':1048576,'debugFlag': 143 ,"querySmaOptimize":1}
    
    def init(self, conn, logSql, replicaVar):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.testcasePath = os.path.split(__file__)[0]
        self.testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))
        
        self.db = "case_when"

    def dropandcreateDB_random(self,database,n):
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        tdSql.execute('''drop database if exists %s ;''' %database)
        tdSql.execute('''create database %s keep 36500 ;'''%(database))
        tdSql.execute('''use %s;'''%database)

        tdSql.execute('''create stable %s.stable_1 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);'''%database)
        tdSql.execute('''create stable %s.stable_2 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);'''%database)
        
        for i in range(num_random):
            tdSql.execute('''create table %s.table_%d \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ) ;'''%(database,i))
            tdSql.execute('''create table %s.stable_1_%d using %s.stable_1 tags('stable_1_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(database,i,database,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 
         
            tdSql.execute('''create table %s.stable_%d_a using %s.stable_2 tags('stable_2_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(database,i,database,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 
            tdSql.execute('''create table %s.stable_%d_b using %s.stable_2 tags('stable_2_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(database,i,database,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))

        # insert data
        for i in range(num_random):   
            for j in range(n):         
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
                
                tdSql.execute('''insert into %s.stable_%d_a (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j, fake.random_int(min=0, max=2147483647, step=1), 
                            fake.random_int(min=0, max=9223372036854775807, step=1), 
                            fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))
            
        tdSql.query("select count(*) from %s.stable_1;" %database)
        tdSql.checkData(0,0,num_random*n)
        tdSql.query("select count(*) from %s.table_0;"%database)
        tdSql.checkData(0,0,n)
        
        
    def users_bug(self,database):    
        sql1 = "select (case when `q_smallint` >0 then 'many--' when `q_smallint`<0 then 'little' end),q_int,loc from %s.stable_1 where tbname = 'stable_1_1' limit 100;" %database
        sql2 = "select (case when `q_smallint` >0 then 'many--' when `q_smallint`<0 then 'little' end),q_int,loc from %s.stable_1_1 limit 100;"  %database
        self.constant_check(database,sql1,sql2,0)
        
        sql1 = "select (case when `q_smallint` >0 then 'many![;;][][]]' when `q_smallint`<0 then 'little' end),q_int,loc from %s.stable_1 where tbname = 'stable_1_1' limit 100;" %database
        sql2 = "select (case when `q_smallint` >0 then 'many![;;][][]]' when `q_smallint`<0 then 'little' end),q_int,loc from %s.stable_1_1 limit 100;"  %database
        self.constant_check(database,sql1,sql2,0)
        
        sql1 = "select (case when sum(q_smallint)=0 then null else sum(q_smallint) end) from %s.stable_1 where tbname = 'stable_1_1' limit 100;"  %database
        sql2 = "select (case when sum(q_smallint)=0 then null else sum(q_smallint) end) from %s.stable_1_1 limit 100;"  %database
        self.constant_check(database,sql1,sql2,0)
        
        #TD-20257
        sql1 = "select tbname,first(ts),q_int,q_smallint,q_bigint,case when q_int <0 then 1 else 0 end from %s.stable_1 where tbname = 'stable_1_1' and ts < now partition by tbname state_window(case when q_int <0 then 1 else 0 end);"  %database
        sql2 = "select tbname,first(ts),q_int,q_smallint,q_bigint,case when q_int <0 then 1 else 0 end from %s.stable_1_1 where ts < now partition by tbname state_window(case when q_int <0 then 1 else 0 end);"  %database
        self.constant_check(database,sql1,sql2,0)
        self.constant_check(database,sql1,sql2,1)
        self.constant_check(database,sql1,sql2,2)
        self.constant_check(database,sql1,sql2,3)
        self.constant_check(database,sql1,sql2,4)
        self.constant_check(database,sql1,sql2,5)
        
        #TD-20260
        sql1 = "select _wstart,avg(q_int),min(q_smallint) from %s.stable_1 where tbname = 'stable_1_1' and ts < now state_window(case when q_smallint <0 then 1 else 0 end);"  %database
        sql2 = "select _wstart,avg(q_int),min(q_smallint) from %s.stable_1_1 where ts < now state_window(case when q_smallint <0 then 1 else 0 end);"  %database
        self.constant_check(database,sql1,sql2,0)
        self.constant_check(database,sql1,sql2,1)
        self.constant_check(database,sql1,sql2,2)

    def casewhen_list(self):
        a1,a2,a3 = random.randint(-2147483647,2147483647),random.randint(-2147483647,2147483647),random.randint(-2147483647,2147483647)
        casewhen_lists = ['first  case when %d then %d end last' %(a1,a2) ,     #'first  case when 3 then 4 end last' , 
                        'first  case when 0 then %d end last' %(a1),            #'first  case when 0 then 4 end last' ,
                        'first  case when null then %d end last' %(a1) ,        #'first  case when null then 4 end last' ,
                        'first  case when 1 then %d+(%d) end last' %(a1,a2) ,     #'first  case when 1 then 4+1 end last' ,
                        'first  case when %d-(%d) then 0 end last' %(a1,a1) ,     #'first  case when 1-1 then 0 end last' ,
                        'first  case when %d+(%d) then 0 end last' %(a1,a1),      #'first  case when 1+1 then 0 end last' ,  
                        'first  case when 1 then %d-(%d)+(%d) end last' %(a1,a1,a2),  #'first  case when 1 then 1-1+2 end last' ,
                        'first  case when %d > 0 then %d < %d end last'  %(a1,a1,a2),   #'first  case when 1 > 0 then 1 < 2 end last' ,
                        'first  case when %d > %d then %d < %d end last'  %(a1,a2,a1,a2),   #'first  case when 1 > 2 then 1 < 2 end last' ,
                        'first  case when abs(%d) then abs(-(%d)) end last'  %(a1,a2) ,#'first  case when abs(3) then abs(-1) end last' ,
                        'first  case when abs(%d+(%d)) then abs(-(%d))+abs(%d) end last' %(a1,a2,a1,a2) , #'first  case when abs(1+1) then abs(-1)+abs(3) end last' ,
                        'first  case when 0 then %d else %d end last'  %(a1,a2),  #'first  case when 0 then 1 else 3 end last' ,
                        'first  case when 0 then %d when 1 then %d else %d end last'  %(a1,a1,a3),  #'first  case when 0 then 1 when 1 then 0 else 3 end last' ,
                        'first  case when 0 then %d when 1 then %d when 2 then %d end last' %(a1,a1,a3), #'first  case when 0 then 1 when 1 then 0 when 2 then 3 end last' ,
                        'first  case when \'a\' then \'b\' when null then 0 end last' ,   #'first  case when \'a\' then \'b\' when null then 0 end last' ,
                        'first  case when \'%d\' then \'b\' when null then %d end last' %(a1,a2),   #'first  case when \'2\' then \'b\' when null then 0 end last' ,
                        'first  case when \'%d\' then \'b\' else null end last' %(a1), #'first  case when \'0\' then \'b\' else null end last',
                        'first  case when \'%d\' then \'b\' else %d end last' %(a1,a2), #'first  case when \'0\' then \'b\' else 2 end last',
                        'first  case when sum(%d) then sum(%d)-sum(%d) end last'  %(a1,a1,a3), #'first  case when sum(2) then sum(2)-sum(1) end last' ,
                        'first  case when sum(%d) then abs(-(%d)) end last'  %(a1,a2), #'first  case when sum(2) then abs(-2) end last' ,
                        'first  case when q_int then ts end last' ,
                        'first  case when q_int then q_int when q_int + (%d) then q_int + (%d) else q_int is null end last' %(a1,a2) , #'first  case when q_int then q_int when q_int + 1 then q_int + 1 else q_int is null end last' ,
                        'first  case when q_int then %d when ts then ts end last'  %(a1),  #'first  case when q_int then 3 when ts then ts end last' ,
                        'first  case when %d then q_int end last'  %(a1),  #'first  case when 3 then q_int end last' ,
                        'first  case when q_int then %d when %d then %d end last'  %(a1,a1,a3),  #'first  case when q_int then 3 when 1 then 2 end last' ,
                        'first  case when sum(q_int) then sum(q_int)-abs(-(%d)) end last'  %(a1),  #'first  case when sum(q_int) then sum(q_int)-abs(-1) end last' ,
                        'first  case when q_int < %d then %d when q_int >= %d then %d else %d end last' %(a1,a2,a1,a2,a3), #'first  case when q_int < 3 then 1 when q_int >= 3 then 2 else 3 end last' ,
                        'first  cast(case q_int when q_int then q_int + (%d) else q_int is null end as double) last' %(a1), #'first  cast(case q_int when q_int then q_int + 1 else q_int is null end as double) last' ,
                        'first  sum(case q_int when q_int then q_int + (%d) else q_int is null end + (%d)) last'  %(a1,a2), #'first  sum(case q_int when q_int then q_int + 1 else q_int is null end + 1) last' ,
                        'first  case when q_int is not null then case when q_int <= %d then q_int else q_int * (%d) end else -(%d) end last'  %(a1,a1,a3),  #'first  case when q_int is not null then case when q_int <= 0 then q_int else q_int * 10 end else -1 end last' ,
                        'first  case %d when %d then %d end last'  %(a1,a2,a3),  # 'first  case 3 when 3 then 4 end last' ,
                        'first  case %d when %d then %d end last'  %(a1,a2,a3),  # 'first  case 3 when 1 then 4 end last' ,
                        'first  case %d when %d then %d else %d end last'  %(a1,a1,a2,a3),  # 'first  case 3 when 1 then 4 else 2 end last' ,
                        'first  case %d when null then %d when \'%d\' then %d end last' %(a1,a1,a2,a3) , # 'first  case 3 when null then 4 when \'3\' then 1 end last' ,
                        'first  case \'%d\' when null then %d when %d then %d end last'  %(a1,a1,a2,a3), # 'first  case \'3\' when null then 4 when 3 then 1 end last' ,
                        'first  case null when null then %d when %d then %d end last' %(a1,a2,a3), # 'first  case null when null then 4 when 3 then 1 end last' ,
                        'first  case %d.0 when null then %d when \'%d\' then %d end last' %(a1,a1,a2,a3) ,  # 'first  case 3.0 when null then 4 when \'3\' then 1 end last' ,
                        'first  case q_double when \'a\' then %d when \'%d\' then %d end last' %(a1,a2,a3) , # 'first  case q_double when \'a\' then 4 when \'0\' then 1 end last' ,
                        'first  case q_double when q_int then q_int when q_int - (%d) then q_int else %d end last' %(a1,a2),  # 'first  case q_double when q_int then q_int when q_int - 1 then q_int else 99 end last' ,
                        'first  case cast(q_double as int) when %d then q_double when q_int then %d else ts end last' %(a1,a2), #'first  case cast(q_double as int) when 0 then q_double when q_int then 11 else ts end last' ,
                        'first  case q_int + (%d) when %d then %d when %d then %d else %d end last'  %(a1,a2,a3,a1,a2,a3), #'first  case q_int + 1 when 1 then 1 when 2 then 2 else 3 end last' ,
                        'first  case when \'a\' then \'b\' when null then %d end last'  %(a1), # 'first  case when \'a\' then \'b\' when null then 0 end last' ,
                        'first  case when \'%d\' then \'b\' when null then %d end last'  %(a1,a2), # 'first  case when \'2\' then \'b\' when null then 0 end last' ,
                        'first  case when %d then \'b\' else null end last'  %(a1), # 'first  case when 0 then \'b\' else null end last' ,
                        'first  case when %d then \'b\' else %d+abs(%d) end last'  %(a1,a2,a3), # 'first  case when 0 then \'b\' else 2+abs(-2) end last' ,
                        'first  case when %d then %d end last'  %(a1,a2), # 'first  case when 3 then 4 end last' ,
                        'first  case when %d then %d end last'  %(a1,a2), # 'first  case when 0 then 4 end last' ,
                        'first  case when null then %d end last'  %(a1), # 'first  case when null then 4 end last' ,
                        'first  case when %d then %d+(%d) end last'  %(a1,a2,a3), # 'first  case when 1 then 4+1 end last' ,
                        'first  case when %d-(%d) then %d end last'  %(a1,a2,a3), # 'first  case when 1-1 then 0 end last' ,
                        'first  case when %d+(%d) then %d end last'  %(a1,a2,a3), # 'first  case when 1+1 then 0 end last' ,
                        'first  case when abs(%d) then abs(%d) end last' %(a1,a2), # 'first  case when abs(3) then abs(-1) end last' ,
                        'first  case when abs(%d+(%d)) then abs(%d)+abs(%d) end last'  %(a1,a2,a3,a1), # 'first  case when abs(1+1) then abs(-1)+abs(3) end last' ,
                        'first  case when %d then %d else %d end last' %(a1,a2,a3), # 'first  case when 0 then 1 else 3 end last' ,
                        'first  case when %d then %d when %d then %d else %d end last' %(a1,a2,a3,a1,a2), # 'first  case when 0 then 1 when 1 then 0 else 3 end last' ,
                        'first  case when %d then %d when %d then %d when %d then %d end last' %(a1,a2,a3,a1,a2,a3), # 'first  case when 0 then 1 when 1 then 0 when 2 then 3 end last' ,
                        'first  case %d when %d then %d end last'  %(a1,a1,a3),  # 'first  case 3 when 3 then 4 end last' ,
                        'first  case %d when %d then %d end last'  %(a1,a2,a3),  # 'first  case 3 when 1 then 4 end last' ,
                        'first  case %d when %d then %d else %d end last'  %(a1,a2,a3,a1),  # 'first  case 3 when 1 then 4 else 2 end last' ,
                        'first  case %d when null then %d when \'%d\' then %d end last'  %(a1,a2,a1,a3),  # 'first  case 3 when null then 4 when \'3\' then 1 end last' ,
                        'first  case null when null then %d when %d then %d end last'  %(a1,a2,a3),  # 'first  case null when null then 4 when 3 then 1 end last' ,
                        'first  case %d.0 when null then %d when \'%d\' then %d end last' %(a1,a2,a1,a3),  # 'first  case 3.0 when null then 4 when \'3\' then 1 end last' ,
                        'first  q_double,case q_double when \'a\' then %d when \'%d\' then %d end last' %(a1,a2,a3), #'first  q_double,case q_double when \'a\' then 4 when \'0\' then 1 end last' ,
                        'first  case null when null then %d when %d then %d end last'  %(a1,a2,a3), #'first  case null when null then 4 when 3 then 1 end last' ,
                        'first  q_double,q_int,case q_double when q_int then q_int when q_int - (%d ) then q_int else %d  end last'  %(a1,a2), # 'first  q_double,q_int,case q_double when q_int then q_int when q_int - 1 then q_int else 99 end last' ,
                        'first  case cast(q_double as int) when %d then q_double when q_int then %d  else ts end last'  %(a1,a2), # 'first  case cast(q_double as int) when 0 then q_double when q_int then 11 else ts end last' ,
                        'first  q_int, case q_int + (%d) when %d then %d when %d then %d else %d end last' %(a1,a1,a1,a2,a2,a3), #'first  q_int, case q_int + 1 when 1 then 1 when 2 then 2 else 3 end last' ,
                        'first  distinct loc, case t_int when t_bigint then t_ts else t_smallint + (%d) end last' %(a1), #'first  distinct loc, case t_int when t_bigint then t_ts else t_smallint + 100 end last' ,
                        ]
        #num = len(casewhen_lists)
        
        casewhen_list = str(random.sample(casewhen_lists,50)).replace("[","").replace("]","").replace("'first","").replace("last'","").replace("\"first","").replace("last\"","")
        
        return casewhen_list
    
    def base_case(self,database):
        
        for i in range(30):
            cs = self.casewhen_list().split(',')[i] 
            sql1 = "select %s from %s.stable_1 where tbname = 'stable_1_1';" % (cs ,database)
            sql2 = "select %s from %s.stable_1_1 ;" % (cs ,database)
            self.constant_check(database,sql1,sql2,0)
            

    def state_window_list(self):
        a1,a2,a3 = random.randint(-2147483647,2147483647),random.randint(-2147483647,2147483647),random.randint(-2147483647,2147483647)
        state_window_lists = ['first  case when %d then %d end last' %(a1,a2) ,     #'first  case when 3 then 4 end last' , 
                        'first  case when 0 then %d end last' %(a1),            #'first  case when 0 then 4 end last' ,
                        'first  case when null then %d end last' %(a1) ,        #'first  case when null then 4 end last' ,
                        'first  case when %d-(%d) then 0 end last' %(a1,a1) ,     #'first  case when 1-1 then 0 end last' ,
                        'first  case when %d+(%d) then 0 end last' %(a1,a1),      #'first  case when 1+1 then 0 end last' ,  
                        'first  case when %d > 0 then %d < %d end last'  %(a1,a1,a2),   #'first  case when 1 > 0 then 1 < 2 end last' ,
                        'first  case when %d > %d then %d < %d end last'  %(a1,a2,a1,a2),   #'first  case when 1 > 2 then 1 < 2 end last' ,
                        'first  case when abs(%d) then abs(-(%d)) end last'  %(a1,a2) ,#'first  case when abs(3) then abs(-1) end last' ,
                        'first  case when 0 then %d else %d end last'  %(a1,a2),  #'first  case when 0 then 1 else 3 end last' ,
                        'first  case when 0 then %d when 1 then %d else %d end last'  %(a1,a1,a3),  #'first  case when 0 then 1 when 1 then 0 else 3 end last' ,
                        'first  case when 0 then %d when 1 then %d when 2 then %d end last' %(a1,a1,a3), #'first  case when 0 then 1 when 1 then 0 when 2 then 3 end last' ,
                        'first  case when \'a\' then \'b\' when null then 0 end last' ,   #'first  case when \'a\' then \'b\' when null then 0 end last' ,
                        'first  case when \'%d\' then \'b\' when null then %d end last' %(a1,a2) ,   #'first  case when \'2\' then \'b\' when null then 0 end last' ,
                        'first  case when \'%d\' then \'b\' else null end last' %(a1), #'first  case when \'0\' then \'b\' else null end last',
                        'first  case when \'%d\' then \'b\' else %d end last' %(a1,a2), #'first  case when \'0\' then \'b\' else 2 end last',
                        'first  case when q_int then q_int when q_int + (%d) then q_int + (%d) else q_int is null end last' %(a1,a2) , #'first  case when q_int then q_int when q_int + 1 then q_int + 1 else q_int is null end last' ,
                        'first  case when q_int then %d when ts then ts end last'  %(a1),  #'first  case when q_int then 3 when ts then ts end last' ,
                        'first  case when %d then q_int end last'  %(a1),  #'first  case when 3 then q_int end last' ,
                        'first  case when q_int then %d when %d then %d end last'  %(a1,a1,a3),  #'first  case when q_int then 3 when 1 then 2 end last' ,
                        'first  case when q_int < %d then %d when q_int >= %d then %d else %d end last' %(a1,a2,a1,a2,a3), #'first  case when q_int < 3 then 1 when q_int >= 3 then 2 else 3 end last' ,
                        'first  case when q_int is not null then case when q_int <= %d then q_int else q_int * (%d) end else -(%d) end last'  %(a1,a1,a3),  #'first  case when q_int is not null then case when q_int <= 0 then q_int else q_int * 10 end else -1 end last' ,
                        'first  case %d when %d then %d end last'  %(a1,a2,a3),  # 'first  case 3 when 3 then 4 end last' ,
                        'first  case %d when %d then %d end last'  %(a1,a2,a3),  # 'first  case 3 when 1 then 4 end last' ,
                        'first  case %d when %d then %d else %d end last'  %(a1,a1,a2,a3),  # 'first  case 3 when 1 then 4 else 2 end last' ,
                        'first  case %d when null then %d when \'%d\' then %d end last' %(a1,a1,a2,a3) , # 'first  case 3 when null then 4 when \'3\' then 1 end last' ,
                        'first  case \'%d\' when null then %d when %d then %d end last'  %(a1,a1,a2,a3), # 'first  case \'3\' when null then 4 when 3 then 1 end last' ,
                        'first  case null when null then %d when %d then %d end last' %(a1,a2,a3), # 'first  case null when null then 4 when 3 then 1 end last' ,
                        'first  case %d.0 when null then %d when \'%d\' then %d end last' %(a1,a1,a2,a3) ,  # 'first  case 3.0 when null then 4 when \'3\' then 1 end last' ,
                        'first  case q_double when \'a\' then %d when \'%d\' then %d end last' %(a1,a2,a3) , # 'first  case q_double when \'a\' then 4 when \'0\' then 1 end last' ,
                        'first  case q_double when q_int then q_int when q_int - (%d) then q_int else %d end last' %(a1,a2),  # 'first  case q_double when q_int then q_int when q_int - 1 then q_int else 99 end last' ,
                        'first  case q_int + (%d) when %d then %d when %d then %d else %d end last'  %(a1,a2,a3,a1,a2,a3), #'first  case q_int + 1 when 1 then 1 when 2 then 2 else 3 end last' ,
                        'first  case when \'a\' then \'b\' when null then %d end last'  %(a1), # 'first  case when \'a\' then \'b\' when null then 0 end last' ,
                        'first  case when \'%d\' then \'b\' when null then %d end last'  %(a1,a2), # 'first  case when \'2\' then \'b\' when null then 0 end last' ,
                        'first  case when %d then \'b\' else null end last'  %(a1), # 'first  case when 0 then \'b\' else null end last' ,
                        'first  case when %d then \'b\' else %d+abs(%d) end last'  %(a1,a2,a3), # 'first  case when 0 then \'b\' else 2+abs(-2) end last' ,
                        'first  case when %d then %d end last'  %(a1,a2), # 'first  case when 3 then 4 end last' ,
                        'first  case when %d then %d end last'  %(a1,a2), # 'first  case when 0 then 4 end last' ,
                        'first  case when null then %d end last'  %(a1), # 'first  case when null then 4 end last' ,
                        #'first  case when %d then %d+(%d) end last'  %(a1,a2,a3), # 'first  case when 1 then 4+1 end last' ,
                        'first  case when %d-(%d) then %d end last'  %(a1,a2,a3), # 'first  case when 1-1 then 0 end last' ,
                        'first  case when %d+(%d) then %d end last'  %(a1,a2,a3), # 'first  case when 1+1 then 0 end last' ,
                        'first  case when abs(%d) then abs(%d) end last' %(a1,a2), # 'first  case when abs(3) then abs(-1) end last' ,
                        #'first  case when abs(%d+(%d)) then abs(%d)+abs(%d) end last'  %(a1,a2,a3,a1), # 'first  case when abs(1+1) then abs(-1)+abs(3) end last' ,
                        'first  case when %d then %d else %d end last' %(a1,a2,a3), # 'first  case when 0 then 1 else 3 end last' ,
                        'first  case when %d then %d when %d then %d else %d end last' %(a1,a2,a3,a1,a2), # 'first  case when 0 then 1 when 1 then 0 else 3 end last' ,
                        'first  case when %d then %d when %d then %d when %d then %d end last' %(a1,a2,a3,a1,a2,a3), # 'first  case when 0 then 1 when 1 then 0 when 2 then 3 end last' ,
                        'first  case %d when %d then %d end last'  %(a1,a1,a3),  # 'first  case 3 when 3 then 4 end last' ,
                        'first  case %d when %d then %d end last'  %(a1,a2,a3),  # 'first  case 3 when 1 then 4 end last' ,
                        'first  case %d when %d then %d else %d end last'  %(a1,a2,a3,a1),  # 'first  case 3 when 1 then 4 else 2 end last' ,
                        'first  case %d when null then %d when \'%d\' then %d end last'  %(a1,a2,a1,a3),  # 'first  case 3 when null then 4 when \'3\' then 1 end last' ,
                        'first  case null when null then %d when %d then %d end last'  %(a1,a2,a3),  # 'first  case null when null then 4 when 3 then 1 end last' ,
                        'first  case %d.0 when null then %d when \'%d\' then %d end last' %(a1,a2,a1,a3),  # 'first  case 3.0 when null then 4 when \'3\' then 1 end last' ,
                        'first  case null when null then %d when %d then %d end last'  %(a1,a2,a3), #'first  case null when null then 4 when 3 then 1 end last' ,
                        'first  q_int, case q_int + (%d) when %d then %d when %d then %d else %d end last' %(a1,a1,a1,a2,a2,a3), #'first  q_int, case q_int + 1 when 1 then 1 when 2 then 2 else 3 end last' ,
                        ]
        
        state_window_list = str(random.sample(state_window_lists,50)).replace("[","").replace("]","").replace("'first","").replace("last'","").replace("\"first","").replace("last\"","")
        
        return state_window_list
           
    def state_window_case(self,database):
        
        for i in range(30):
            cs = self.state_window_list().split(',')[i] 
            sql1 = "select _wstart,avg(q_int),min(q_smallint) from %s.stable_1 where tbname = 'stable_1_1' state_window(%s);" % (database,cs)
            sql2 = "select _wstart,avg(q_int),min(q_smallint) from %s.stable_1_1 state_window(%s) ;" % (database,cs)
            self.constant_check(database,sql1,sql2,0)
            self.constant_check(database,sql1,sql2,1)
            self.constant_check(database,sql1,sql2,2)
            
                      
        
    def constant_check(self,database,sql1,sql2,column):   
        #column =0 代表0列， column = n代表n-1列 
        tdLog.info("\n=============sql1:(%s)___sql2:(%s) ====================\n" %(sql1,sql2)) 
        tdSql.query(sql1) 
        queryRows = len(tdSql.queryResult) 
      
        for i in range(queryRows):
            tdSql.query(sql1) 
            sql1_value = tdSql.getData(i,column)
            tdSql.execute(" flush database %s;" %database)
            tdSql.query(sql2) 
            sql2_value = tdSql.getData(i,column)
            self.value_check(sql1_value,sql2_value)       
                      
    def value_check(self,base_value,check_value):
        if base_value==check_value:
            tdLog.info(f"checkEqual success, base_value={base_value},check_value={check_value}") 
        else :
            tdLog.exit(f"checkEqual error, base_value=={base_value},check_value={check_value}") 
                            
    def run(self):      
        fake = Faker('zh_CN')
        fake_data =  fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1)
        fake_float = fake.pyfloat()
        fake_str = fake.pystr()
        
        startTime = time.time()  
                  
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename)) 
        
        self.dropandcreateDB_random("%s" %self.db, 10)
        
        self.users_bug("%s" %self.db)
        
        self.base_case("%s" %self.db)
        
        self.state_window_case("%s" %self.db)
        
        
        
        #taos -f sql 
        print("taos -f sql start!")
        taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
        _ = subprocess.check_output(taos_cmd1, shell=True)
        print("taos -f sql over!")     
                

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))
    


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
