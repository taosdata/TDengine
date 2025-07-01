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
        tdSql.init(conn.cursor(), logSql)

        self.testcasePath = os.path.split(__file__)[0]
        self.testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))
        
        self.db = "ind_sel"

    def dropandcreateDB_random(self,database,n,vgroups):
        ts = 1630000000000
        num_random = 100
        fake = Faker('zh_CN')
        tdSql.execute('''drop database if exists %s ;''' %database)
        tdSql.execute('''create database %s keep 36500 vgroups %d ;'''%(database,vgroups))
        tdSql.execute('''use %s;'''%database)

        tdSql.execute('''create stable %s.stable_1 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_binary1 binary(100) , q_nchar1 nchar(100) ,q_binary2 binary(100) , q_nchar2 nchar(100) ,q_binary3 binary(100) , q_nchar3 nchar(100) ,q_binary4 binary(100) , q_nchar4 nchar(100) ,\
                q_binary5 binary(100) , q_nchar5 nchar(100) ,q_binary6 binary(100) , q_nchar6 nchar(100) ,q_binary7 binary(100) , q_nchar7 nchar(100) ,q_binary8 binary(100) , q_nchar8 nchar(100) ,\
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);'''%database)
        tdSql.execute('''create stable %s.stable_2 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);'''%database)
        
        for i in range(10*n):
            tdSql.execute('''create table %s.bj_%d (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ) ;'''%(database,i))
            tdSql.execute('''create table %s.sh_%d (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ) ;'''%(database,i))
            tdSql.execute('''create table %s.bj_table_%d_r (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ) ;'''%(database,i))
            tdSql.execute('''create table %s.sh_table_%d_r (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ) ;'''%(database,i))
            tdSql.execute('''create table %s.hn_table_%d_r \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_binary1 binary(100) , q_nchar1 nchar(100) ,q_binary2 binary(100) , q_nchar2 nchar(100) ,q_binary3 binary(100) , q_nchar3 nchar(100) ,q_binary4 binary(100) , q_nchar4 nchar(100) ,\
                    q_binary5 binary(100) , q_nchar5 nchar(100) ,q_binary6 binary(100) , q_nchar6 nchar(100) ,q_binary7 binary(100) , q_nchar7 nchar(100) ,q_binary8 binary(100) , q_nchar8 nchar(100) ,\
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;'''%(database,i))
            tdSql.execute('''create table %s.bj_stable_1_%d using %s.stable_1 tags('bj_stable_1_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(database,i,database,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 
            tdSql.execute('''create table %s.sh_table_%d_a using %s.stable_1 tags('sh_a_table_1_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(database,i,database,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 
            tdSql.execute('''create table %s.sh_table_%d_b using %s.stable_1 tags('sh_b_table_1_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(database,i,database,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
            tdSql.execute('''create table %s.sh_table_%d_c using %s.stable_1 tags('sh_c_table_1_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(database,i,database,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
            
            tdSql.execute('''create table %s.bj_table_%d_a using %s.stable_1 tags('bj_a_table_1_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(database,i,database,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 
            tdSql.execute('''create table %s.bj_table_%d_b using %s.stable_1 tags('bj_b_table_1_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(database,i,database,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
            tdSql.execute('''create table %s.bj_table_%d_c using %s.stable_1 tags('bj_c_table_1_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(database,i,database,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))

            
            tdSql.execute('''create table %s.tj_table_%d_a using %s.stable_2 tags('tj_a_table_2_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(database,i,database,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 
            tdSql.execute('''create table %s.tj_table_%d_b using %s.stable_2 tags('tj_b_table_2_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(database,i,database,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))

        # insert data
        for i in range(num_random*n):        
            tdSql.execute('''insert into %s.bj_stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8) \
                        values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (database,ts + i*1000+1, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , 
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))
            tdSql.execute('''insert into  %s.hn_table_1_r (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8) \
                        values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (database,ts + i*1000+1, fake.random_int(min=-2147483647, max=2147483647, step=1) , 
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1) , 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i, fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , 
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))

            tdSql.execute('''insert into %s.bj_stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8)\
                        values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (database,ts + i*1000+2, fake.random_int(min=0, max=2147483647, step=1), 
                        fake.random_int(min=0, max=9223372036854775807, step=1), 
                        fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i, fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , 
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))
            tdSql.execute('''insert into %s.hn_table_2_r (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8) \
                        values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (database,ts + i*1000+2, fake.random_int(min=0, max=2147483647, step=1), 
                        fake.random_int(min=0, max=9223372036854775807, step=1), 
                        fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i, fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , 
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))

            tdSql.execute('''insert into %s.bj_stable_1_3 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8) \
                        values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (database,ts + i*1000+3, fake.random_int(min=-0, max=2147483647, step=1), 
                        fake.random_int(min=-0, max=9223372036854775807, step=1), 
                        fake.random_int(min=-0, max=32767, step=1) , fake.random_int(min=-0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i, fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , 
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))

            tdSql.execute('''insert into %s.bj_stable_1_4 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8) \
                        values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (database,ts + i*1000 +4, fake.random_int(min=-0, max=2147483647, step=1), 
                        fake.random_int(min=-0, max=9223372036854775807, step=1), 
                        fake.random_int(min=-0, max=32767, step=1) , fake.random_int(min=-0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i, fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , 
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))

            tdSql.execute('''insert into %s.bj_stable_1_5 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8) \
                        values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (database,ts + i*1000 +5, fake.random_int(min=-0, max=2147483647, step=1), 
                        fake.random_int(min=-0, max=9223372036854775807, step=1), 
                        fake.random_int(min=-0, max=32767, step=1) , fake.random_int(min=-0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i, fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , 
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))

        tdSql.query("select count(*) from %s.stable_1;" %database)
        tdSql.checkData(0,0,5*num_random*n)
        tdSql.query("select count(*) from %s.hn_table_1_r;"%database)
        tdSql.checkData(0,0,num_random*n)
        
    def func_index_check(self,database,func,column):  
        fake = Faker('zh_CN')
        fake_data =  fake.random_int(min=1, max=20, step=1) 
        tdLog.info("\n=============constant(%s)_check ====================\n" %func)  
        tdSql.execute(" create sma index %s.sma_index_name1 on %s.stable_1 function(%s(%s)) interval(%dm); " %(database,database,func,column,fake_data))  
        sql = " select %s(%s) from %s.stable_1 interval(1m) "%(func,column,database)
        tdLog.info(sql)
        tdSql.query(sql) 
        queryRows = len(tdSql.queryResult)    
        for i in range(queryRows):
            print("row=%d,  result=%s " %(i,tdSql.queryResult[i][0]))
            flush_before_value = tdSql.queryResult[i][0]
        
        tdLog.info("\n=============flush database ====================\n")
        
        tdSql.execute(" flush database %s;" %database)
        
        tdSql.query(sql)  
        queryRows = len(tdSql.queryResult)    
        for i in range(queryRows):
            print("row=%d,  result=%s " %(i,tdSql.queryResult[i][0]))
            flush_after_value = tdSql.queryResult[i][0]
        
        self.value_check(flush_before_value,flush_after_value)
        
        tdLog.info("\n=============drop index ====================\n")
        
        tdSql.execute(" drop index %s.sma_index_name1;" %database)
        
        tdSql.query(sql)  
        queryRows = len(tdSql.queryResult)    
        for i in range(queryRows):
            print("row=%d,  result=%s " %(i,tdSql.queryResult[i][0]))
            drop_index_value = tdSql.queryResult[i][0]
        
        self.value_check(flush_before_value,drop_index_value)
    
    def constant_speical_check(self,database,func,column):    
        tdLog.info("\n=============constant(%s)_check ====================\n" %column) 
        sql_no_from = " select %s(%s) ; "%(func,column)

        tdLog.info(sql_no_from)
        tdSql.query(sql_no_from) 
        queryRows = len(tdSql.queryResult)    
        for i in range(queryRows):
            print("row=%d,  result=%s " %(i,tdSql.queryResult[i][0]))
            flush_before_value_no_from = tdSql.queryResult[i][0]
        
        tdLog.info("\n=============flush database ====================\n")
        
        tdSql.execute(" flush database %s;" %database)
        
        tdSql.query(sql_no_from)  
        queryRows = len(tdSql.queryResult)    
        for i in range(queryRows):
            print("row=%d,  result=%s " %(i,tdSql.queryResult[i][0]))
            flush_after_value_no_from = tdSql.queryResult[i][0]
        
        self.value_check(flush_before_value_no_from,flush_after_value_no_from)
               
        
    def constant_check(self,database,func,column):    
        tdLog.info("\n=============constant(%s)_check ====================\n" %column) 
        sql = " select %s(%s) from %s.stable_1 "%(func,column,database)
        sql_no_from = " select %s(%s)  "%(func,column)
        
        tdLog.info(sql)
        tdSql.query(sql) 
        queryRows = len(tdSql.queryResult)    
        for i in range(queryRows):
            print("row=%d,  result=%s " %(i,tdSql.queryResult[i][0]))
            flush_before_value = tdSql.queryResult[i][0]
            
        tdLog.info(sql_no_from)
        tdSql.query(sql_no_from) 
        queryRows = len(tdSql.queryResult)    
        for i in range(queryRows):
            print("row=%d,  result=%s " %(i,tdSql.queryResult[i][0]))
            flush_before_value_no_from = tdSql.queryResult[i][0]
        
        tdLog.info("\n=============flush database ====================\n")
        
        tdSql.execute(" flush database %s;" %database)
        
        tdSql.query(sql)  
        queryRows = len(tdSql.queryResult)    
        for i in range(queryRows):
            print("row=%d,  result=%s " %(i,tdSql.queryResult[i][0]))
            flush_after_value = tdSql.queryResult[i][0]
        
        self.value_check(flush_before_value,flush_after_value)
        
        tdSql.query(sql_no_from)  
        queryRows = len(tdSql.queryResult)    
        for i in range(queryRows):
            print("row=%d,  result=%s " %(i,tdSql.queryResult[i][0]))
            flush_after_value_no_from = tdSql.queryResult[i][0]
        
        #self.value_check(flush_before_value_no_from,flush_after_value_no_from)#越界后值不唯一
        
    def constant_table_check(self,database,func,column):    
        tdLog.info("\n=============constant(%s)_check ====================\n" %column) 
        sql = " select %s(%s) from %s.bj_stable_1_1 "%(func,column,database)
        sql_no_from = " select %s(%s)  "%(func,column)
        
        tdLog.info(sql)
        tdSql.query(sql) 
        queryRows = len(tdSql.queryResult)    
        for i in range(queryRows):
            print("row=%d,  result=%s " %(i,tdSql.queryResult[i][0]))
            flush_before_value = tdSql.queryResult[i][0]
            
        tdLog.info(sql_no_from)
        tdSql.query(sql_no_from) 
        queryRows = len(tdSql.queryResult)    
        for i in range(queryRows):
            print("row=%d,  result=%s " %(i,tdSql.queryResult[i][0]))
            flush_before_value_no_from = tdSql.queryResult[i][0]
        
        tdLog.info("\n=============flush database ====================\n")
        
        tdSql.execute(" flush database %s;" %database)
        
        tdSql.query(sql)  
        queryRows = len(tdSql.queryResult)    
        for i in range(queryRows):
            print("row=%d,  result=%s " %(i,tdSql.queryResult[i][0]))
            flush_after_value = tdSql.queryResult[i][0]
        
        self.value_check(flush_before_value,flush_after_value)
        
        tdSql.query(sql_no_from)  
        queryRows = len(tdSql.queryResult)    
        for i in range(queryRows):
            print("row=%d,  result=%s " %(i,tdSql.queryResult[i][0]))
            flush_after_value_no_from = tdSql.queryResult[i][0]
        
        #self.value_check(flush_before_value_no_from,flush_after_value_no_from)#越界后值不唯一
        
    def constant_str_check(self,database,func,column):    
        tdLog.info("\n=============constant(%s)_check ====================\n" %column) 
        sql = " select %s('%s') from %s.stable_1 "%(func,column,database)
        tdLog.info(sql)
        tdSql.query(sql)  
        queryRows = len(tdSql.queryResult)    
        for i in range(queryRows):
            print("row=%d,  result=%s " %(i,tdSql.queryResult[i][0]))
            flush_before_value = tdSql.queryResult[i][0]
        
        tdLog.info("\n=============flush database ====================\n")
        
        tdSql.execute(" flush database %s;" %database)
        
        tdSql.query(sql)  
        queryRows = len(tdSql.queryResult)    
        for i in range(queryRows):
            print("row=%d,  result=%s " %(i,tdSql.queryResult[i][0]))
            flush_after_value = tdSql.queryResult[i][0]
        
        self.value_check(flush_before_value,flush_after_value)  
                 
        
    def constant_error_check(self,database,func,column):    
        tdLog.info("\n=============constant(%s)_check ====================\n" %column) 
        error_sql = " select %s('%s')  "%(func,column)
        tdLog.info(error_sql)
        tdSql.error(error_sql)  
        
        tdLog.info("\n=============flush database ====================\n")
        
        tdSql.execute(" flush database %s;" %database)
        
        tdSql.error(error_sql) 
        
        error_sql1 = " select %s('%s') from %s.stable_1 "%(func,column,database)
        tdLog.info(error_sql1)
        tdSql.error(error_sql1) 
        error_sql2 = " select %s('%s') from %s.bj_stable_1_1 "%(func,column,database)
        tdLog.info(error_sql2)
        tdSql.error(error_sql2) 
        
    def derivative_sql(self,database):    
        fake = Faker('zh_CN')
        fake_data =  fake.random_int(min=1, max=10000000000000, step=1)
        tdLog.info("\n=============derivative sql ====================\n" )  
        sql = " select derivative(%s,%ds,0) from %s.stable_1 "%('q_smallint',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,0) from %s.stable_1 "%('q_bigint',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,0) from %s.stable_1 "%('q_tinyint',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,0) from %s.stable_1 "%('q_int',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,0) from %s.stable_1 "%('q_float',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,0) from %s.stable_1 "%('q_double',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)  
         
        sql = " select derivative(%s,%ds,1) from %s.stable_1 "%('q_smallint',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,1) from %s.stable_1 "%('q_bigint',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,1) from %s.stable_1 "%('q_tinyint',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,1) from %s.stable_1 "%('q_int',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,1) from %s.stable_1 "%('q_float',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,1) from %s.stable_1 "%('q_double',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)    
        
        sql = " select derivative(%s,%ds,0) from %s.stable_1 order by ts"%('q_smallint',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,0) from %s.stable_1 order by ts "%('q_bigint',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,0) from %s.stable_1 order by ts "%('q_tinyint',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,0) from %s.stable_1 order by ts "%('q_int',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,0) from %s.stable_1 order by ts "%('q_float',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,0) from %s.stable_1 order by ts "%('q_double',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)  
         
        sql = " select derivative(%s,%ds,1) from %s.stable_1 order by ts "%('q_smallint',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,1) from %s.stable_1 order by ts "%('q_bigint',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,1) from %s.stable_1 order by ts "%('q_tinyint',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,1) from %s.stable_1 order by ts "%('q_int',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,1) from %s.stable_1 order by ts "%('q_float',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,1) from %s.stable_1 order by ts "%('q_double',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)  
        
        sql = " select derivative(%s,%ds,0) from %s.stable_1 order by ts desc"%('q_smallint',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,0) from %s.stable_1 order by ts desc "%('q_bigint',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,0) from %s.stable_1 order by ts desc "%('q_tinyint',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,0) from %s.stable_1 order by ts desc "%('q_int',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,0) from %s.stable_1 order by ts desc "%('q_float',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,0) from %s.stable_1 order by ts desc "%('q_double',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)  
         
        sql = " select derivative(%s,%ds,1) from %s.stable_1 order by ts desc "%('q_smallint',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,1) from %s.stable_1 order by ts desc "%('q_bigint',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,1) from %s.stable_1 order by ts desc "%('q_tinyint',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,1) from %s.stable_1 order by ts desc "%('q_int',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,1) from %s.stable_1 order by ts desc "%('q_float',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,1) from %s.stable_1 order by ts desc"%('q_double',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql) 
        
        
        sql = " select derivative(%s,%ds,1) from %s.stable_1 order by ts desc limit 3000"%('q_smallint',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,1) from %s.stable_1 order by ts desc limit 3000 "%('q_bigint',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,1) from %s.stable_1 order by ts desc limit 3000 "%('q_tinyint',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,1) from %s.stable_1 order by ts desc limit 3000 "%('q_int',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,1) from %s.stable_1 order by ts desc limit 3000 "%('q_float',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql)
        
        sql = " select derivative(%s,%ds,1) from %s.stable_1 order by ts desc limit 3000"%('q_double',fake_data,database) 
        self.derivative_data_check("%s" %self.db,"%s" %sql) 
        
    def derivative_data_check(self,database,sql):    
        tdLog.info("\n=============derivative_data(%s)_check ====================\n" %sql) 
        tdLog.info(sql)
        tdSql.query(sql) 
        queryRows = len(tdSql.queryResult)    
        for i in range(queryRows):
            #print("row=%d,  result=%s " %(i,tdSql.queryResult[i][0]))
            flush_before_value = tdSql.queryResult[i][0]
            #flush_before_value1 = tdSql.queryResult[i][1]
        
        tdLog.info("\n=============flush database ====================\n")
        
        tdSql.execute(" flush database %s;" %database)
        
        tdSql.query(sql)  
        queryRows = len(tdSql.queryResult)    
        for i in range(queryRows):
            #print("row=%d,  result=%s " %(i,tdSql.queryResult[i][0]))
            flush_after_value = tdSql.queryResult[i][0]
            #flush_after_value1 = tdSql.queryResult[i][1]
        
        self.value_check(flush_before_value,flush_after_value)
        #self.value_check(flush_before_value1,flush_after_value1)
        
                      
    def value_check(self,flush_before_value,flush_after_value):
        if flush_before_value==flush_after_value:
            tdLog.info(f"checkEqual success, flush_before_value={flush_before_value},flush_after_value={flush_after_value}") 
        else :
            tdLog.exit(f"checkEqual error, flush_before_value=={flush_before_value},flush_after_value={flush_after_value}") 
        #pass
                            
    def run(self):      
        fake = Faker('zh_CN')
        fake_data =  fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1)
        fake_float = fake.pyfloat()
        fake_str = fake.pystr()
        
        startTime = time.time()  
                  
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename)) 
        
        self.dropandcreateDB_random("%s" %self.db, 1,2)
        
        self.constant_speical_check("%s" %self.db,'','%d' %fake_data)
        self.constant_speical_check("%s" %self.db,'','%f' %fake_float)
        self.constant_speical_check("%s" %self.db,'','\'%s\'' %fake_str)
        self.constant_speical_check("%s" %self.db,'','NULL')
        
        #TD-19818
        self.func_index_check("%s" %self.db,'max','q_int')
        self.func_index_check("%s" %self.db,'max','q_bigint')
        self.func_index_check("%s" %self.db,'max','q_smallint')
        self.func_index_check("%s" %self.db,'max','q_tinyint')
        self.func_index_check("%s" %self.db,'max','q_float')
        self.func_index_check("%s" %self.db,'max','q_double')        
        
        self.func_index_check("%s" %self.db,'min','q_int')
        self.func_index_check("%s" %self.db,'min','q_bigint')
        self.func_index_check("%s" %self.db,'min','q_smallint')
        self.func_index_check("%s" %self.db,'min','q_tinyint')
        self.func_index_check("%s" %self.db,'min','q_float')
        self.func_index_check("%s" %self.db,'min','q_double') 
                
        self.func_index_check("%s" %self.db,'SUM','q_int')
        self.func_index_check("%s" %self.db,'SUM','q_bigint')
        self.func_index_check("%s" %self.db,'SUM','q_smallint')
        self.func_index_check("%s" %self.db,'SUM','q_tinyint')
        self.func_index_check("%s" %self.db,'SUM','q_float')
        self.func_index_check("%s" %self.db,'SUM','q_double')
        
        #TD-19854
        self.constant_check("%s" %self.db,'count','%d' %fake_data)
        self.constant_check("%s" %self.db,'count','%f' %fake_float)        
        self.constant_str_check("%s" %self.db,'count','%s' %fake_str)  
        self.constant_check("%s" %self.db,'count','(cast(%d as int))' %fake_data)          
        self.constant_check("%s" %self.db,'count','(cast(%f as int))' %fake_float)
        self.constant_check("%s" %self.db,'count','(cast(%d as smallint))' %fake_data)          
        self.constant_check("%s" %self.db,'count','(cast(%f as smallint))' %fake_float)
        self.constant_check("%s" %self.db,'count','(cast(%d as bigint))' %fake_data)          
        self.constant_check("%s" %self.db,'count','(cast(%f as bigint))' %fake_float)
        self.constant_check("%s" %self.db,'count','(cast(%d as tinyint))' %fake_data)          
        self.constant_check("%s" %self.db,'count','(cast(%f as tinyint))' %fake_float)
        self.constant_check("%s" %self.db,'count','(cast(%d as float))' %fake_data)          
        self.constant_check("%s" %self.db,'count','(cast(%f as float))' %fake_float)
        self.constant_check("%s" %self.db,'count','(cast(%d as double))' %fake_data)          
        self.constant_check("%s" %self.db,'count','(cast(%f as double))' %fake_float)
        
        self.constant_check("%s" %self.db,'sum','%d' %fake_data)
        self.constant_check("%s" %self.db,'sum','%f' %fake_float)        
        self.constant_error_check("%s" %self.db,'sum','%s' %fake_str)                
        self.constant_check("%s" %self.db,'sum','(cast(%d as int))' %fake_data)          
        self.constant_check("%s" %self.db,'sum','(cast(%f as int))' %fake_float)
        self.constant_check("%s" %self.db,'sum','(cast(%d as smallint))' %fake_data)          
        self.constant_check("%s" %self.db,'sum','(cast(%f as smallint))' %fake_float)
        self.constant_check("%s" %self.db,'sum','(cast(%d as bigint))' %fake_data)          
        self.constant_check("%s" %self.db,'sum','(cast(%f as bigint))' %fake_float)
        self.constant_check("%s" %self.db,'sum','(cast(%d as tinyint))' %fake_data)          
        self.constant_check("%s" %self.db,'sum','(cast(%f as tinyint))' %fake_float)
        self.constant_check("%s" %self.db,'sum','(cast(%d as float))' %fake_data)          
        self.constant_check("%s" %self.db,'sum','(cast(%f as float))' %fake_float)
        self.constant_check("%s" %self.db,'sum','(cast(%d as double))' %fake_data)          
        self.constant_check("%s" %self.db,'sum','(cast(%f as double))' %fake_float)
        
        self.constant_check("%s" %self.db,'avg','%d' %fake_data)
        self.constant_check("%s" %self.db,'avg','%f' %fake_float)        
        self.constant_error_check("%s" %self.db,'avg','%s' %fake_str)                
        self.constant_check("%s" %self.db,'avg','(cast(%d as int))' %fake_data)          
        self.constant_check("%s" %self.db,'avg','(cast(%f as int))' %fake_float)
        self.constant_check("%s" %self.db,'avg','(cast(%d as smallint))' %fake_data)          
        self.constant_check("%s" %self.db,'avg','(cast(%f as smallint))' %fake_float)
        self.constant_check("%s" %self.db,'avg','(cast(%d as bigint))' %fake_data)          
        self.constant_check("%s" %self.db,'avg','(cast(%f as bigint))' %fake_float)
        self.constant_check("%s" %self.db,'avg','(cast(%d as tinyint))' %fake_data)          
        self.constant_check("%s" %self.db,'avg','(cast(%f as tinyint))' %fake_float)
        self.constant_check("%s" %self.db,'avg','(cast(%d as float))' %fake_data)          
        self.constant_check("%s" %self.db,'avg','(cast(%f as float))' %fake_float)
        self.constant_check("%s" %self.db,'avg','(cast(%d as double))' %fake_data)          
        self.constant_check("%s" %self.db,'avg','(cast(%f as double))' %fake_float)
        
        self.constant_check("%s" %self.db,'stddev','%d' %fake_data)
        self.constant_check("%s" %self.db,'stddev','%f' %fake_float)        
        self.constant_error_check("%s" %self.db,'stddev','%s' %fake_str)                
        self.constant_check("%s" %self.db,'stddev','(cast(%d as int))' %fake_data)          
        self.constant_check("%s" %self.db,'stddev','(cast(%f as int))' %fake_float)
        self.constant_check("%s" %self.db,'stddev','(cast(%d as smallint))' %fake_data)          
        self.constant_check("%s" %self.db,'stddev','(cast(%f as smallint))' %fake_float)
        self.constant_check("%s" %self.db,'stddev','(cast(%d as bigint))' %fake_data)          
        self.constant_check("%s" %self.db,'stddev','(cast(%f as bigint))' %fake_float)
        self.constant_check("%s" %self.db,'stddev','(cast(%d as tinyint))' %fake_data)          
        self.constant_check("%s" %self.db,'stddev','(cast(%f as tinyint))' %fake_float)
        self.constant_check("%s" %self.db,'stddev','(cast(%d as float))' %fake_data)          
        self.constant_check("%s" %self.db,'stddev','(cast(%f as float))' %fake_float)
        self.constant_check("%s" %self.db,'stddev','(cast(%d as double))' %fake_data)          
        self.constant_check("%s" %self.db,'stddev','(cast(%f as double))' %fake_float)
        
        self.constant_check("%s" %self.db,'spread','%d' %fake_data)
        self.constant_check("%s" %self.db,'spread','%f' %fake_float)        
        self.constant_error_check("%s" %self.db,'spread','%s' %fake_str)                
        self.constant_check("%s" %self.db,'spread','(cast(%d as int))' %fake_data)          
        self.constant_check("%s" %self.db,'spread','(cast(%f as int))' %fake_float)
        self.constant_check("%s" %self.db,'spread','(cast(%d as smallint))' %fake_data)          
        self.constant_check("%s" %self.db,'spread','(cast(%f as smallint))' %fake_float)
        self.constant_check("%s" %self.db,'spread','(cast(%d as bigint))' %fake_data)          
        self.constant_check("%s" %self.db,'spread','(cast(%f as bigint))' %fake_float)
        self.constant_check("%s" %self.db,'spread','(cast(%d as tinyint))' %fake_data)          
        self.constant_check("%s" %self.db,'spread','(cast(%f as tinyint))' %fake_float)
        self.constant_check("%s" %self.db,'spread','(cast(%d as float))' %fake_data)          
        self.constant_check("%s" %self.db,'spread','(cast(%f as float))' %fake_float)
        self.constant_check("%s" %self.db,'spread','(cast(%d as double))' %fake_data)          
        self.constant_check("%s" %self.db,'spread','(cast(%f as double))' %fake_float)
        
        self.constant_check("%s" %self.db,'hyperloglog','%d' %fake_data)
        self.constant_check("%s" %self.db,'hyperloglog','%f' %fake_float)        
        self.constant_str_check("%s" %self.db,'hyperloglog','%s' %fake_str)                
        self.constant_check("%s" %self.db,'hyperloglog','(cast(%d as int))' %fake_data)          
        self.constant_check("%s" %self.db,'hyperloglog','(cast(%f as int))' %fake_float)
        self.constant_check("%s" %self.db,'hyperloglog','(cast(%d as smallint))' %fake_data)          
        self.constant_check("%s" %self.db,'hyperloglog','(cast(%f as smallint))' %fake_float)
        self.constant_check("%s" %self.db,'hyperloglog','(cast(%d as bigint))' %fake_data)          
        self.constant_check("%s" %self.db,'hyperloglog','(cast(%f as bigint))' %fake_float)
        self.constant_check("%s" %self.db,'hyperloglog','(cast(%d as tinyint))' %fake_data)          
        self.constant_check("%s" %self.db,'hyperloglog','(cast(%f as tinyint))' %fake_float)
        self.constant_check("%s" %self.db,'hyperloglog','(cast(%d as float))' %fake_data)          
        self.constant_check("%s" %self.db,'hyperloglog','(cast(%f as float))' %fake_float)
        self.constant_check("%s" %self.db,'hyperloglog','(cast(%d as double))' %fake_data)          
        self.constant_check("%s" %self.db,'hyperloglog','(cast(%f as double))' %fake_float)
        
        self.constant_error_check("%s" %self.db,'elapsed','%d' %fake_data)
        self.constant_error_check("%s" %self.db,'elapsed','%f' %fake_float)        
        self.constant_error_check("%s" %self.db,'elapsed','%s' %fake_str)                
        self.constant_error_check("%s" %self.db,'elapsed','(cast(%d as int))' %fake_data)          
        self.constant_error_check("%s" %self.db,'elapsed','(cast(%f as int))' %fake_float)
        self.constant_error_check("%s" %self.db,'elapsed','(cast(%d as smallint))' %fake_data)          
        self.constant_error_check("%s" %self.db,'elapsed','(cast(%f as smallint))' %fake_float)
        self.constant_error_check("%s" %self.db,'elapsed','(cast(%d as bigint))' %fake_data)          
        self.constant_error_check("%s" %self.db,'elapsed','(cast(%f as bigint))' %fake_float)
        self.constant_error_check("%s" %self.db,'elapsed','(cast(%d as tinyint))' %fake_data)          
        self.constant_error_check("%s" %self.db,'elapsed','(cast(%f as tinyint))' %fake_float)
        self.constant_error_check("%s" %self.db,'elapsed','(cast(%d as float))' %fake_data)          
        self.constant_error_check("%s" %self.db,'elapsed','(cast(%f as float))' %fake_float)
        self.constant_error_check("%s" %self.db,'elapsed','(cast(%d as double))' %fake_data)          
        self.constant_error_check("%s" %self.db,'elapsed','(cast(%f as double))' %fake_float)
        
        percentile_data =  fake.random_int(min=-0, max=100, step=1)
        self.constant_table_check("%s" %self.db,'percentile','%d,%d' %(fake_data,percentile_data))
        self.constant_table_check("%s" %self.db,'percentile','%f,%d' %(fake_float,percentile_data))        
        self.constant_error_check("%s" %self.db,'percentile','%s,%d' %(fake_str,percentile_data))                 
        self.constant_table_check("%s" %self.db,'percentile','(cast(%d as int)),%d' %(fake_data,percentile_data))          
        self.constant_table_check("%s" %self.db,'percentile','(cast(%f as int)),%d' %(fake_float,percentile_data))
        self.constant_table_check("%s" %self.db,'percentile','(cast(%d as smallint)),%d' %(fake_data,percentile_data))          
        self.constant_table_check("%s" %self.db,'percentile','(cast(%f as smallint)),%d' %(fake_float,percentile_data))
        self.constant_table_check("%s" %self.db,'percentile','(cast(%d as bigint)),%d' %(fake_data,percentile_data))          
        self.constant_table_check("%s" %self.db,'percentile','(cast(%f as bigint)),%d' %(fake_float,percentile_data))
        self.constant_table_check("%s" %self.db,'percentile','(cast(%d as tinyint)),%d' %(fake_data,percentile_data))          
        self.constant_table_check("%s" %self.db,'percentile','(cast(%f as tinyint)),%d' %(fake_float,percentile_data))
        self.constant_table_check("%s" %self.db,'percentile','(cast(%d as float)),%d' %(fake_data,percentile_data))          
        self.constant_table_check("%s" %self.db,'percentile','(cast(%f as float)),%d' %(fake_float,percentile_data))
        self.constant_table_check("%s" %self.db,'percentile','(cast(%d as double)),%d' %(fake_data,percentile_data))          
        self.constant_table_check("%s" %self.db,'percentile','(cast(%f as double)),%d' %(fake_float,percentile_data))
        
        self.constant_table_check("%s" %self.db,'apercentile','%d,%d' %(fake_data,percentile_data))
        self.constant_check("%s" %self.db,'apercentile','%f,%d' %(fake_float,percentile_data))        
        self.constant_error_check("%s" %self.db,'apercentile','%s,%d' %(fake_str,percentile_data))                 
        self.constant_table_check("%s" %self.db,'apercentile','(cast(%d as int)),%d' %(fake_data,percentile_data))          
        self.constant_check("%s" %self.db,'apercentile','(cast(%f as int)),%d' %(fake_float,percentile_data))
        self.constant_check("%s" %self.db,'apercentile','(cast(%d as smallint)),%d' %(fake_data,percentile_data))          
        self.constant_table_check("%s" %self.db,'apercentile','(cast(%f as smallint)),%d' %(fake_float,percentile_data))
        self.constant_table_check("%s" %self.db,'apercentile','(cast(%d as bigint)),%d' %(fake_data,percentile_data))          
        self.constant_check("%s" %self.db,'apercentile','(cast(%f as bigint)),%d' %(fake_float,percentile_data))
        self.constant_check("%s" %self.db,'apercentile','(cast(%d as tinyint)),%d' %(fake_data,percentile_data))          
        self.constant_table_check("%s" %self.db,'apercentile','(cast(%f as tinyint)),%d' %(fake_float,percentile_data))
        self.constant_table_check("%s" %self.db,'apercentile','(cast(%d as float)),%d' %(fake_data,percentile_data))          
        self.constant_check("%s" %self.db,'apercentile','(cast(%f as float)),%d' %(fake_float,percentile_data))
        self.constant_check("%s" %self.db,'apercentile','(cast(%d as double)),%d' %(fake_data,percentile_data))          
        self.constant_table_check("%s" %self.db,'apercentile','(cast(%f as double)),%d' %(fake_float,percentile_data))
        
        percentile_data =  fake.random_int(min=-0, max=1, step=1)
        self.constant_table_check("%s" %self.db,'histogram',"%d,'user_input','[-10000,0,10000]',%d" %(fake_data,percentile_data))
        self.constant_check("%s" %self.db,'histogram',"%f,'user_input','[-10000,0,10000]',%d" %(fake_float,percentile_data))        
        self.constant_error_check("%s" %self.db,'histogram',"%s,'user_input','[-10000,0,10000]',%d" %(fake_str,percentile_data))           
        self.constant_table_check("%s" %self.db,'histogram',"(cast(%d as int)),'user_input','[-10000,0,10000]',%d" %(fake_data,percentile_data))          
        self.constant_check("%s" %self.db,'histogram',"(cast(%f as int)),'user_input','[-10000,0,10000]',%d" %(fake_float,percentile_data))
        self.constant_check("%s" %self.db,'histogram',"(cast(%d as smallint)),'user_input','[-10000,0,10000]',%d" %(fake_data,percentile_data))          
        self.constant_table_check("%s" %self.db,'histogram',"(cast(%f as smallint)),'user_input','[-10000,0,10000]',%d" %(fake_float,percentile_data))
        self.constant_table_check("%s" %self.db,'histogram',"(cast(%d as bigint)),'user_input','[-10000,0,10000]',%d" %(fake_data,percentile_data))          
        self.constant_check("%s" %self.db,'histogram',"(cast(%f as bigint)),'user_input','[-10000,0,10000]',%d" %(fake_float,percentile_data))
        self.constant_check("%s" %self.db,'histogram',"(cast(%d as tinyint)),'user_input','[-10000,0,10000]',%d" %(fake_data,percentile_data))          
        self.constant_table_check("%s" %self.db,'histogram',"(cast(%f as tinyint)),'user_input','[-10000,0,10000]',%d" %(fake_float,percentile_data))
        self.constant_table_check("%s" %self.db,'histogram',"(cast(%d as float)),'user_input','[-10000,0,10000]',%d" %(fake_data,percentile_data))          
        self.constant_check("%s" %self.db,'histogram',"(cast(%f as float)),'user_input','[-10000,0,10000]',%d" %(fake_float,percentile_data))
        self.constant_check("%s" %self.db,'histogram',"(cast(%d as double)),'user_input','[-10000,0,10000]',%d" %(fake_data,percentile_data))          
        self.constant_table_check("%s" %self.db,'histogram',"(cast(%f as double)),'user_input','[-10000,0,10000]',%d" %(fake_float,percentile_data)) 
      
        #TD-19843
        self.derivative_sql("%s" %self.db)
        
        
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
