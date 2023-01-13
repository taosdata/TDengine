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
    updatecfgDict = {'maxSQLLength':1048576,'debugFlag': 131 ,"querySmaOptimize":1}
    
    def init(self, conn, logSql, replicaVar):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.testcasePath = os.path.split(__file__)[0]
        self.testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))
        
        self.db = "insert_null_none"

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
           
        tdSql.query("select count(*) from %s.stable_1;" %database)
        tdSql.checkData(0,0,num_random*n)
        tdSql.query("select count(*) from %s.table_0;"%database)
        tdSql.checkData(0,0,n)
        
    def qint_none(self,database,n):
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 1,  
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +1, 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
    
    def qint_null(self,database,n):
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %s, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 2, 'NULL', 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %s, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +2, 'NULL', 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))

    def qint_none_null(self,database,n):
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 3,  
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +3, 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %s, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 4, 'NULL', 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %s, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +4, 'NULL', 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))

    def qint_none_value(self,database,n):
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 5,  
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +5, 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
                      
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
                
    def qint_null_value(self,database,n):
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
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
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %s, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 6, 'NULL', 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %s, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +6, 'NULL', 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))


    def qint_none_null_value(self,database,n):
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 7,  
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +7, 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
               
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
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %s, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 8, 'NULL', 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %s, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 8, 'NULL', 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
                 

    def qbigint_none(self,database,n):
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 1,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +1, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
    
    def qbigint_null(self,database,n):
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %s, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 2, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 'NULL', 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %s, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +2, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 'NULL', 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))

    def qbigint_none_null(self,database,n):
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 3,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +3, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %s, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 4, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 'NULL', 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %s, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +4, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),'NULL',  
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))

    def qbigint_none_value(self,database,n):
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 5,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +5, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
                      
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
                
    def qbigint_null_value(self,database,n):
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
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
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %s, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 6, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),'NULL',  
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %s, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +6, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),'NULL',  
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))


    def qbigint_none_null_value(self,database,n):
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 7,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +7, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
               
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
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %s, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 8, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),'NULL',  
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %s, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 8, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),'NULL',  
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))


    def qsmallint_none(self,database,n):
        tdLog.info("qsmallint_none")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 1,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +1, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
    
    def qsmallint_null(self,database,n):
        tdLog.info("qsmallint_null")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %s, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 2, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) ,'NULL',  fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %s, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +2, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) ,'NULL',  fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))

    def qsmallint_none_null(self,database,n):
        tdLog.info("qsmallint_none_null")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 3,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +3, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %s, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 4, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , 'NULL', fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %s, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +4, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),
                            fake.random_int(min=-32767, max=32767, step=1) ,'NULL',   fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))

    def qsmallint_none_value(self,database,n):
        tdLog.info("qsmallint_none_value")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 5,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +5, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
                      
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
                
    def qsmallint_null_value(self,database,n):
        tdLog.info("qsmallint_null_value")       
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
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
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %s, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 6, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),
                            fake.random_int(min=-32767, max=32767, step=1) ,'NULL',   fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %s, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +6, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),  
                            fake.random_int(min=-32767, max=32767, step=1) ,'NULL', fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))


    def qsmallint_none_null_value(self,database,n):
        tdLog.info("qsmallint_none_null_value")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 7,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +7, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
               
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
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %s, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 8, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , 'NULL', fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %s, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 8, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) ,'NULL',  fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
 
 

    def qtinyint_none(self,database,n):
        tdLog.info("qtinyint_none")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 1,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +1, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
    
    def qtinyint_null(self,database,n):
        tdLog.info("qtinyint_null")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %s, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 2, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 'NULL', 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %s, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +2, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 'NULL', 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))

    def qtinyint_none_null(self,database,n):
        tdLog.info("qtinyint_none_null")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 3,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +3, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %s, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 4, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,  'NULL',
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %s, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +4, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),
                            fake.random_int(min=-32767, max=32767, step=1) ,  fake.random_int(min=-127, max=127, step=1) , 'NULL', 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))

    def qtinyint_none_value(self,database,n):
        tdLog.info("qtinyint_none_value")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 5,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +5, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
                      
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
                
    def qtinyint_null_value(self,database,n):
        tdLog.info("qtinyint_null_value")       
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
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
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %s, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 6, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),
                            fake.random_int(min=-32767, max=32767, step=1) ,  fake.random_int(min=-127, max=127, step=1) , 'NULL', 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %s, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +6, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),  
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 'NULL',
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))


    def qtinyint_none_null_value(self,database,n):
        tdLog.info("qtinyint_none_null_value")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 7,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +7, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
               
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
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %s, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 8, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) ,  fake.random_int(min=-127, max=127, step=1) , 'NULL',
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %s, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 8, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 'NULL', 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))

 
    def qfloat_none(self,database,n):
        tdLog.info("qfloat_none")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 1,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +1, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
    
    def qfloat_null(self,database,n):
        tdLog.info("qfloat_null")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %s, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 2, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) ,'NULL', 
                            fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %s, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +2, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,fake.random_int(min=-127, max=127, step=1) , 'NULL', 
                            fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))

    def qfloat_none_null(self,database,n):
        tdLog.info("qfloat_none_null")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 3,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +3, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %s, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 4, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,  fake.random_int(min=-127, max=127, step=1) ,'NULL',
                            fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %s, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +4, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),
                            fake.random_int(min=-32767, max=32767, step=1) ,  fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) ,'NULL', 
                            fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))

    def qfloat_none_value(self,database,n):
        tdLog.info("qfloat_none_value")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 5,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +5, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
                      
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
                
    def qfloat_null_value(self,database,n):
        tdLog.info("qfloat_null_value")       
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %d, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat(), fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %d, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat(), fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %s, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 6, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),
                            fake.random_int(min=-32767, max=32767, step=1) ,  fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) ,'NULL', 
                            fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %s, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +6, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),  
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) ,'NULL',
                            fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))


    def qfloat_none_null_value(self,database,n):
        tdLog.info("qfloat_none_null_value")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 7,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +7, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
               
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
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %s, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 8, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) ,  fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) ,'NULL',
                            fake.pyfloat() ,  fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %s, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 8, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) ,'NULL', 
                            fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))


    def qdouble_none(self,database,n):
        tdLog.info("qdouble_none")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 1,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +1, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
    
    def qdouble_null(self,database,n):
        tdLog.info("qdouble_null")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %s, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 2, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() ,'NULL', fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %s, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +2, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() ,'NULL', fake.pystr() , fake.pystr() , ts + i ))

    def qdouble_none_null(self,database,n):
        tdLog.info("qdouble_none_null")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 3,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +3, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %s, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 4, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,  fake.random_int(min=-127, max=127, step=1) ,
                            fake.pyfloat() ,'NULL',  fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %s, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +4, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),
                            fake.random_int(min=-32767, max=32767, step=1) ,  fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                            fake.pyfloat() ,'NULL' , fake.pystr() , fake.pystr() , ts + i ))

    def qdouble_none_value(self,database,n):
        tdLog.info("qdouble_none_value")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 5,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +5, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
                      
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
                
    def qdouble_null_value(self,database,n):
        tdLog.info("qdouble_null_value")       
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %d, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat(), fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %d, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat(), fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %s, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 6, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),
                            fake.random_int(min=-32767, max=32767, step=1) ,  fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1), 
                            fake.pyfloat() ,'NULL', fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %s, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +6, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),  
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1),
                            fake.pyfloat() ,'NULL', fake.pystr() , fake.pystr() , ts + i ))


    def qdouble_none_null_value(self,database,n):
        tdLog.info("qdouble_none_null_value")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 7,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +7, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
               
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
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %s, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 8, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) ,  fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                            fake.pyfloat() ,'NULL',  fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %s, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 8, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() ,'NULL', fake.pystr() , fake.pystr() , ts + i ))





    def qbinary_none(self,database,n):
        tdLog.info("qbinary_none")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float, q_double , q_bool  , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 1,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool  , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +1, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat() , fake.pystr() , ts + i ))
    
    def qbinary_null(self,database,n):
        tdLog.info("qbinary_null")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float  , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %s, 0, %s, 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 2, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() ,'NULL', fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %s, 0, %s, 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +2, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() ,'NULL', fake.pystr() , ts + i ))

    def qbinary_none_null(self,database,n):
        tdLog.info("qbinary_none_null")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float, q_double , q_bool  , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 3,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool  , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +3, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat() , fake.pystr() , ts + i ))
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 0, %s, 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 4, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,  fake.random_int(min=-127, max=127, step=1) ,
                            fake.pyfloat() , fake.pyfloat() ,'NULL', fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, 0, %s, 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +4, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),
                            fake.random_int(min=-32767, max=32767, step=1) ,  fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() ,'NULL', fake.pystr() , ts + i ))

    def qbinary_none_value(self,database,n):
        tdLog.info("qbinary_none_value")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float, q_double , q_bool  , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 5,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool  , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, 0,  'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +5, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat() , fake.pystr() , ts + i ))
                      
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
                
    def qbinary_null_value(self,database,n):
        tdLog.info("qbinary_null_value")       
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %d, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %d, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 0, %s, 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 6, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),
                            fake.random_int(min=-32767, max=32767, step=1) ,  fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() ,'NULL', fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, 0, %s, 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +6, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),  
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                            fake.pyfloat() ,fake.pyfloat() ,'NULL', fake.pystr() , ts + i ))


    def qbinary_none_null_value(self,database,n):
        tdLog.info("qbinary_none_null_value")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float, q_double , q_bool  , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 7,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool  , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +7, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat() , fake.pystr() , ts + i ))
               
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
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 0, %s, 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 8, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) ,  fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                            fake.pyfloat() , fake.pyfloat() ,'NULL' , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, 0, %s, 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 8, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat(), 'NULL' , fake.pystr() , ts + i ))



    def qnchar_none(self,database,n):
        tdLog.info("qnchar_none")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float, q_double , q_bool  , q_binary, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 1,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool  , q_binary, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +1, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat() , fake.pystr() , ts + i ))
    
    def qnchar_null(self,database,n):
        tdLog.info("qnchar_null")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float  , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s',%s,  %d) ;''' 
                            % (database,i,ts + i*1000 + j + 2, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() ,fake.pystr() , 'NULL', ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s',%s,  %d) ;''' 
                            % (database,i,ts + i*1000 + j +2, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() ,fake.pystr() , 'NULL', ts + i ))

    def qnchar_none_null(self,database,n):
        tdLog.info("qnchar_none_null")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float, q_double , q_bool  , q_binary, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 3,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool  , q_binary, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +3, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat() , fake.pystr() , ts + i ))
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s',%s,  %d) ;''' 
                            % (database,i,ts + i*1000 + j + 4, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,  fake.random_int(min=-127, max=127, step=1) ,
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() ,'NULL', ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s',%s,  %d) ;''' 
                            % (database,i,ts + i*1000 + j +4, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),
                            fake.random_int(min=-32767, max=32767, step=1) ,  fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() ,'NULL', ts + i ))

    def qnchar_none_value(self,database,n):
        tdLog.info("qnchar_none_value")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float, q_double , q_bool  , q_binary ,q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 5,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool  , q_binary, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, 0,  'binary.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +5, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat() , fake.pystr() , ts + i ))
                      
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
                
    def qnchar_null_value(self,database,n):
        tdLog.info("qnchar_null_value")       
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
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
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s',%s, %d) ;''' 
                            % (database,i,ts + i*1000 + j + 6, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),
                            fake.random_int(min=-32767, max=32767, step=1) ,  fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() ,fake.pystr() , 'NULL', ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, 0,  'binary.%s',%s, %d) ;''' 
                            % (database,i,ts + i*1000 + j +6, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),  
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                            fake.pyfloat() ,fake.pyfloat() ,fake.pystr() ,'NULL',  ts + i ))


    def qnchar_none_null_value(self,database,n):
        tdLog.info("qnchar_none_null_value")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float, q_double , q_bool  , q_binary , q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 7,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool  , q_binary , q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +7, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat() , fake.pystr() , ts + i ))
               
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
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 0,  'binary.%s', %s , %d) ;''' 
                            % (database,i,ts + i*1000 + j + 8, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) ,  fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                            fake.pyfloat() , fake.pyfloat() ,fake.pystr() , 'NULL' , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, 0,  'binary.%s', %s , %d) ;''' 
                            % (database,i,ts + i*1000 + j + 8, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat(), fake.pystr() , 'NULL' , ts + i ))

    def qts_none(self,database,n):
        tdLog.info("qts_none")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float, q_double , q_bool  , q_binary, q_nchar)\
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s') ;''' 
                            % (database,i,ts + i*1000 + j + 1,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr()))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool  , q_binary, q_nchar) \
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s') ;''' 
                            % (database,i,ts + i*1000 + j +1, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr()))
    
    def qts_null(self,database,n):
        tdLog.info("qts_null")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float  , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s',  %s) ;''' 
                            % (database,i,ts + i*1000 + j + 2, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() ,fake.pystr(),fake.pystr() , 'NULL'))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s',  %s) ;''' 
                            % (database,i,ts + i*1000 + j +2, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() ,fake.pystr(),fake.pystr() , 'NULL' ))

    def qts_none_null(self,database,n):
        tdLog.info("qts_none_null")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float, q_double , q_bool  , q_binary, q_nchar)\
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s') ;''' 
                            % (database,i,ts + i*1000 + j + 3,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr()))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool  , q_binary, q_nchar) \
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s') ;''' 
                            % (database,i,ts + i*1000 + j +3, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr()))
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s',  %s) ;''' 
                            % (database,i,ts + i*1000 + j + 4, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,  fake.random_int(min=-127, max=127, step=1) ,
                            fake.pyfloat() , fake.pyfloat() , fake.pystr()  , fake.pystr(),'NULL'))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s',  %s) ;''' 
                            % (database,i,ts + i*1000 + j +4, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),
                            fake.random_int(min=-32767, max=32767, step=1) ,  fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr()  , fake.pystr(),'NULL' ))

    def qts_none_value(self,database,n):
        tdLog.info("qts_none_value")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float, q_double , q_bool  , q_binary, q_nchar)\
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s') ;''' 
                            % (database,i,ts + i*1000 + j + 5,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr()))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool  , q_binary, q_nchar) \
                            values(%d, %d, %d, %d, %d, %f, %f, 0,  'binary.%s', 'nchar.%s') ;''' 
                            % (database,i,ts + i*1000 + j +5, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr()))
                      
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
                
    def qts_null_value(self,database,n):
        tdLog.info("qts_null_value")       
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
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
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s','nchar.%s', %s) ;''' 
                            % (database,i,ts + i*1000 + j + 6, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),
                            fake.random_int(min=-32767, max=32767, step=1) ,  fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() ,fake.pystr() ,fake.pystr(), 'NULL'))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, 0,  'binary.%s','nchar.%s', %s) ;''' 
                            % (database,i,ts + i*1000 + j +6, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),  
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                            fake.pyfloat() ,fake.pyfloat() ,fake.pystr() ,fake.pystr(),'NULL' ))


    def qts_none_null_value(self,database,n):
        tdLog.info("qts_none_null_value")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float, q_double , q_bool  , q_binary , q_nchar)\
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s') ;''' 
                            % (database,i,ts + i*1000 + j + 7,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr()))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool  , q_binary , q_nchar) \
                            values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s') ;''' 
                            % (database,i,ts + i*1000 + j +7, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() ))
               
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
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 0,  'binary.%s', 'nchar.%s' , %s) ;''' 
                            % (database,i,ts + i*1000 + j + 8, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) ,  fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                            fake.pyfloat() , fake.pyfloat() ,fake.pystr() ,fake.pystr() , 'NULL' ))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, 0,  'binary.%s', 'nchar.%s' , %s) ;''' 
                            % (database,i,ts + i*1000 + j + 8, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat(), fake.pystr() ,fake.pystr() , 'NULL' ))
                

    def qbool_none(self,database,n):
        tdLog.info("qbool_none")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float, q_double , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 1,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat(), fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +1, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat(), fake.pystr() , fake.pystr() , ts + i ))
    
    def qbool_null(self,database,n):
        tdLog.info("qbool_null")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, %s, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 2, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() ,fake.pyfloat() ,'NULL', fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, %s, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +2, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() ,fake.pyfloat() ,'NULL', fake.pystr() , fake.pystr() , ts + i ))

    def qbool_none_null(self,database,n):
        tdLog.info("qbool_none_null")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float, q_double , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 3,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat(), fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float, q_double , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +3, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat(), fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, %s, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 4, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) ,  fake.random_int(min=-127, max=127, step=1) ,
                            fake.pyfloat() ,fake.pyfloat() ,'NULL', fake.pystr() , fake.pystr() , ts + i ))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, %s, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +4, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),
                            fake.random_int(min=-32767, max=32767, step=1) ,  fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                            fake.pyfloat() ,fake.pyfloat() ,'NULL', fake.pystr() , fake.pystr() , ts + i  ))

    def qbool_none_value(self,database,n):
        tdLog.info("qbool_none_value")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float, q_double , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 'binary.%s', 'nchar.%s', %d);''' 
                            % (database,i,ts + i*1000 + j + 5,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat(), fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float, q_double , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +5, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat(), fake.pystr() , fake.pystr() , ts + i ))
                      
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
                
    def qbool_null_value(self,database,n):
        tdLog.info("qbool_null_value")       
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %d, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat(), fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %d, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat(), fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i ))
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, %s, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j + 6, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),
                            fake.random_int(min=-32767, max=32767, step=1) ,  fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1), 
                            fake.pyfloat() ,fake.pyfloat() ,'NULL', fake.pystr() , fake.pystr() , ts + i ))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, %s, 'binary.%s', 'nchar.%s', %d) ;''' 
                            % (database,i,ts + i*1000 + j +6, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1),  
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1),
                            fake.pyfloat() ,fake.pyfloat() ,'NULL', fake.pystr() , fake.pystr() , ts + i  ))


    def qbool_none_null_value(self,database,n):
        tdLog.info("qbool_none_null_value")
        ts = 1630000000000
        num_random = 10
        fake = Faker('zh_CN')
        # insert data
        for i in range(num_random):   
            for j in range(n):                    
                tdSql.execute('''insert into %s.stable_1_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float, q_double , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 'binary.%s', 'nchar.%s', %d)  ;''' 
                            % (database,i,ts + i*1000 + j + 7,  
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i))

                tdSql.execute('''insert into %s.table_%d  (ts  , q_int , q_bigint , q_smallint , q_tinyint , q_float, q_double , q_binary , q_nchar, q_ts)\
                            values(%d, %d, %d, %d, %d, %f, %f, 'binary.%s', 'nchar.%s', %d)  ;''' 
                            % (database,i,ts + i*1000 + j +7, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.random_int(min=-127, max=127, step=1) , fake.pyfloat()  , fake.pyfloat(), fake.pystr() , fake.pystr() , ts + i ))
               
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
                      
                tdSql.execute('''insert into %s.stable_1_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, %s, 'binary.%s', 'nchar.%s', %d );''' 
                            % (database,i,ts + i*1000 + j + 8, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) ,  fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) ,
                            fake.pyfloat(),fake.pyfloat() ,'NULL', fake.pystr() , fake.pystr() , ts + i ))

                tdSql.execute('''insert into %s.table_%d  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) \
                            values(%d, %d, %d, %d, %d, %f, %f, %s, 'binary.%s', 'nchar.%s', %d );''' 
                            % (database,i,ts + i*1000 + j + 8, 
                            fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() ,fake.pyfloat() ,'NULL', fake.pystr() , fake.pystr() , ts + i  ))                
                                                                                                                                                                                                                                                                 
    def sqls(self,database):    
        sql = "select * from %s.stable_1 ;" %database
        tdSql.query(sql)
        sql = "select * from %s.table_1 ;" %database
        tdSql.query(sql)
         
        sql1 = "select count(*) from %s.stable_1 ;" %database
        tdSql.query(sql1) 
        sql1_value = tdSql.getData(0,0)
        
        sql2 = "select count(ts) from %s.stable_1 ;" %database
        self.constant_check(sql1,sql2)
        
        sql3 = "select count(q_int) from %s.stable_1 ;" %database
        self.constant_check(sql1,sql3)
        
        sql4 = "select count(q_bigint) from %s.stable_1 ;" %database
        self.constant_check(sql1,sql4)
        
        sql5 = "select count(q_smallint) from %s.stable_1 ;" %database
        self.constant_check(sql1,sql5)
        
        sql6 = "select count(q_tinyint) from %s.stable_1 ;" %database
        self.constant_check(sql1,sql6)
        
        sql7 = "select count(q_float) from %s.stable_1 ;" %database
        self.constant_check(sql1,sql7)
        
        sql8 = "select count(q_double) from %s.stable_1 ;" %database
        self.constant_check(sql1,sql8)
        
        sql9 = "select count(q_bool) from %s.stable_1 ;" %database
        self.constant_check(sql1,sql9)
        
        sql10 = "select count(q_binary) from %s.stable_1 ;" %database
        self.constant_check(sql1,sql10)
        
        sql11 = "select count(q_nchar) from %s.stable_1 ;" %database
        self.constant_check(sql1,sql11)
        
        sql12 = "select count(q_ts) from %s.stable_1 ;" %database
        self.constant_check(sql1,sql12)
        
    def check_flushdb(self,database):
        self.sqls(database);
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);      
        
        self.qint_none(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);        
        
        self.qint_null(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);       
        
        self.qint_none_null(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);        
        
        self.qint_none_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);      
        
        self.qint_null_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);    
        
        self.qint_none_null_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);    
        
        
        self.qbigint_none(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);        
        
        self.qbigint_null(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);       
        
        self.qbigint_none_null(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);        
        
        self.qbigint_none_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);      
        
        self.qbigint_null_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);    
        
        self.qbigint_none_null_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database); 
        
        
        self.qsmallint_none(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);        
        
        self.qsmallint_null(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);       
        
        self.qsmallint_none_null(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);        
        
        self.qsmallint_none_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);      
        
        self.qsmallint_null_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);    
        
        self.qsmallint_none_null_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database); 
        
        
        self.qtinyint_none(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);        
        
        self.qtinyint_null(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);       
        
        self.qtinyint_none_null(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);        
        
        self.qtinyint_none_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);      
        
        self.qtinyint_null_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);    
        
        self.qtinyint_none_null_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database); 
        
        
        self.qfloat_none(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);        
        
        self.qfloat_null(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);       
        
        self.qfloat_none_null(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);        
        
        self.qfloat_none_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);      
        
        self.qfloat_null_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);    
        
        self.qfloat_none_null_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database); 
        
        
        self.qdouble_none(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);        
        
        self.qdouble_null(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);       
        
        self.qdouble_none_null(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);        
        
        self.qdouble_none_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);      
        
        self.qdouble_null_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);    
        
        self.qdouble_none_null_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database); 
        
        self.qbinary_none(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);        
        
        self.qbinary_null(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);       
        
        self.qbinary_none_null(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);        
        
        self.qbinary_none_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);      
        
        self.qbinary_null_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);    
        
        self.qbinary_none_null_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database); 
        
        self.qnchar_none(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);        
        
        self.qnchar_null(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);       
        
        self.qnchar_none_null(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);        
        
        self.qnchar_none_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);      
        
        self.qnchar_null_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);    
        
        self.qnchar_none_null_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database); 
        
        
        self.qts_none(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);        
        
        self.qts_null(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);       
        
        self.qts_none_null(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);        
        
        self.qts_none_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);      
        
        self.qts_null_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);    
        
        self.qts_none_null_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database); 
        
        
        self.qbool_none(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);        
        
        self.qbool_null(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);       
        
        self.qbool_none_null(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);        
        
        self.qbool_none_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);      
        
        self.qbool_null_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database);    
        
        self.qbool_none_null_value(database, 10);  
        self.sqls(database); 
        tdSql.execute(" flush database %s;" %database)
        tdLog.info("flush database success")
        self.sqls(database); 
        
        
    def constant_check(self,sql1,sql2):   
        tdLog.info("\n=============sql1:(%s)___sql2:(%s) ====================\n" %(sql1,sql2)) 
        tdSql.query(sql1) 
        sql1_value = tdSql.getData(0,0)
        tdSql.query(sql2) 
        sql2_value = tdSql.getData(0,0)
        self.value_check(sql1_value,sql2_value)       
                      
    def value_check(self,base_value,check_value):
        if base_value==check_value:
            tdLog.info(f"checkEqual success, base_value={base_value},check_value={check_value}") 
        elif base_value > check_value:
            tdLog.info(f"checkEqual success,but count not include NULL, base_value=={base_value},check_value={check_value}") 
        else :
            tdLog.exit(f"checkEqual error, base_value=={base_value},check_value={check_value}") 
                            
    def run(self):      
        
        startTime = time.time()  
                  
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename)) 
        
        self.dropandcreateDB_random("%s" %self.db, 10)
        
        self.check_flushdb("%s" %self.db)
        
        
        # #taos -f sql 
        # startTime1 = time.time()
        # print("taos -f sql start!")
        # taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
        # _ = subprocess.check_output(taos_cmd1, shell=True)
        # print("taos -f sql over!")     
        # endTime1 = time.time()
        # print("total time %ds" % (endTime1 - startTime1))        

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))
    


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
