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
    updatecfgDict = {'maxSQLLength':1048576,'debugFlag': 135}
    
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.testcasePath = os.path.split(__file__)[0]
        self.testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))
        
        self.db = "pre_suf"

    def dropandcreateDB_random(self,database,n,vgroups,table_prefix,table_suffix,check_result_positive,check_result_negative):
        #check_result_positive 检查前缀后缀是正数的，check_result_negative 检查前缀后缀是负数的(TS-3249)
        tdLog.info(f"create start:n:{n},vgroups:{vgroups},table_prefix:{table_prefix},table_suffix:{table_suffix},check_result_positive:{check_result_positive},check_result_negative:{check_result_negative}") 
        ts = 1630000000000
        num_random = 100
        fake = Faker('zh_CN')
        tdSql.execute('''drop database if exists %s ;''' %database)
        tdSql.execute('''create database %s keep 36500 vgroups %d table_prefix %d table_suffix %d;'''%(database,vgroups,table_prefix,table_suffix))
        tdSql.execute('''use %s;'''%database)

        tdSql.execute('''create stable stable_1 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_binary1 binary(100) , q_nchar1 nchar(100) ,q_binary2 binary(100) , q_nchar2 nchar(100) ,q_binary3 binary(100) , q_nchar3 nchar(100) ,q_binary4 binary(100) , q_nchar4 nchar(100) ,\
                q_binary5 binary(100) , q_nchar5 nchar(100) ,q_binary6 binary(100) , q_nchar6 nchar(100) ,q_binary7 binary(100) , q_nchar7 nchar(100) ,q_binary8 binary(100) , q_nchar8 nchar(100) ,\
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')
        tdSql.execute('''create stable stable_2 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')
        
        #positive
        for i in range(10*n):
            tdSql.execute('''create table bj_%d (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ) ;'''%i)
            tdSql.execute('''create table sh_%d (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ) ;'''%i)
            tdSql.execute('''create table bj_table_%d_r (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ) ;'''%i)
            tdSql.execute('''create table sh_table_%d_r (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ) ;'''%i)
            tdSql.execute('''create table hn_table_%d_r \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_binary1 binary(100) , q_nchar1 nchar(100) ,q_binary2 binary(100) , q_nchar2 nchar(100) ,q_binary3 binary(100) , q_nchar3 nchar(100) ,q_binary4 binary(100) , q_nchar4 nchar(100) ,\
                    q_binary5 binary(100) , q_nchar5 nchar(100) ,q_binary6 binary(100) , q_nchar6 nchar(100) ,q_binary7 binary(100) , q_nchar7 nchar(100) ,q_binary8 binary(100) , q_nchar8 nchar(100) ,\
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;'''%i)
            tdSql.execute('''create table bj_stable_1_%d using stable_1 tags('bj_stable_1_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(i,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 
            tdSql.execute('''create table sh_table_%d_a using stable_1 tags('sh_a_table_1_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(i,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 
            tdSql.execute('''create table sh_table_%d_b using stable_1 tags('sh_b_table_1_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(i,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
            tdSql.execute('''create table sh_table_%d_c using stable_1 tags('sh_c_table_1_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(i,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
            
            tdSql.execute('''create table bj_table_%d_a using stable_1 tags('bj_a_table_1_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(i,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 
            tdSql.execute('''create table bj_table_%d_b using stable_1 tags('bj_b_table_1_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(i,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
            tdSql.execute('''create table bj_table_%d_c using stable_1 tags('bj_c_table_1_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(i,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))

            
            tdSql.execute('''create table tj_table_%d_a using stable_2 tags('tj_a_table_2_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(i,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 
            tdSql.execute('''create table tj_table_%d_b using stable_2 tags('tj_b_table_2_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(i,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))

        #negative
        for i in range(10*n):
            tdSql.execute('''create table bj_table_%d_r_negative (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ) ;'''%i)
            tdSql.execute('''create table sh_table_%d_r_negative (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ) ;'''%i)
            tdSql.execute('''create table hn_table_%d_r_negative \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_binary1 binary(100) , q_nchar1 nchar(100) ,q_binary2 binary(100) , q_nchar2 nchar(100) ,q_binary3 binary(100) , q_nchar3 nchar(100) ,q_binary4 binary(100) , q_nchar4 nchar(100) ,\
                    q_binary5 binary(100) , q_nchar5 nchar(100) ,q_binary6 binary(100) , q_nchar6 nchar(100) ,q_binary7 binary(100) , q_nchar7 nchar(100) ,q_binary8 binary(100) , q_nchar8 nchar(100) ,\
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;'''%i)
            
            tdSql.execute('''create table bj_stable_1_%d_negative using stable_1 tags('bj_stable_1_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(i,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 
            tdSql.execute('''create table sh_table_%d_a_negative using stable_1 tags('sh_a_table_1_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(i,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 
            tdSql.execute('''create table sh_table_%d_b_negative using stable_1 tags('sh_b_table_1_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(i,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
            tdSql.execute('''create table sh_table_%d_c_negative using stable_1 tags('sh_c_table_1_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(i,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
            
            tdSql.execute('''create table bj_table_%d_a_negative using stable_1 tags('bj_a_table_1_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(i,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 
            tdSql.execute('''create table bj_table_%d_b_negative using stable_1 tags('bj_b_table_1_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(i,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
            tdSql.execute('''create table bj_table_%d_c_negative using stable_1 tags('bj_c_table_1_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(i,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))

            
            tdSql.execute('''create table tj_table_%d_a_negative using stable_2 tags('tj_a_table_2_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(i,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 
            tdSql.execute('''create table tj_table_%d_b_negative using stable_2 tags('tj_b_table_2_%d', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(i,i,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1)))
            
        # create stream
        tdSql.execute('''create stream current_stream trigger at_once IGNORE EXPIRED 0 into stream_max_stable_1 as select _wstart as startts, _wend as wend, max(q_int) as max_int, min(q_bigint) as min_int from stable_1 where ts is not null interval (5s);''')
        
        # insert data positive
        for i in range(num_random*n):        
            tdSql.execute('''insert into bj_stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8) \
                        values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (ts + i*1000, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , 
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))
            tdSql.execute('''insert into  hn_table_1_r (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8) \
                        values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (ts + i*1000, fake.random_int(min=-2147483647, max=2147483647, step=1) , 
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1) , 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i, fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , 
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))

            tdSql.execute('''insert into bj_stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8)\
                        values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (ts + i*1000, fake.random_int(min=0, max=2147483647, step=1), 
                        fake.random_int(min=0, max=9223372036854775807, step=1), 
                        fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i, fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , 
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))
            tdSql.execute('''insert into hn_table_2_r (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8) \
                        values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (ts + i*1000, fake.random_int(min=0, max=2147483647, step=1), 
                        fake.random_int(min=0, max=9223372036854775807, step=1), 
                        fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i, fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , 
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))

            tdSql.execute('''insert into bj_stable_1_3 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8) \
                        values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (ts + i*1000, fake.random_int(min=-0, max=2147483647, step=1), 
                        fake.random_int(min=-0, max=9223372036854775807, step=1), 
                        fake.random_int(min=-0, max=32767, step=1) , fake.random_int(min=-0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i, fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , 
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))

            tdSql.execute('''insert into bj_stable_1_4 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8) \
                        values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (ts + i*1000 +1, fake.random_int(min=-0, max=2147483647, step=1), 
                        fake.random_int(min=-0, max=9223372036854775807, step=1), 
                        fake.random_int(min=-0, max=32767, step=1) , fake.random_int(min=-0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i, fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , 
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))

            tdSql.execute('''insert into bj_stable_1_5 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8) \
                        values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (ts + i*1000 +10, fake.random_int(min=-0, max=2147483647, step=1), 
                        fake.random_int(min=-0, max=9223372036854775807, step=1), 
                        fake.random_int(min=-0, max=32767, step=1) , fake.random_int(min=-0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i, fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , 
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))

        # insert data negative
        for i in range(num_random*n):        
            tdSql.execute('''insert into bj_stable_1_1_negative  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8) \
                        values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (ts + i*1000, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , 
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))

            tdSql.execute('''insert into bj_stable_1_2_negative (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8)\
                        values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (ts + i*1000, fake.random_int(min=0, max=2147483647, step=1), 
                        fake.random_int(min=0, max=9223372036854775807, step=1), 
                        fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i, fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , 
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))

            tdSql.execute('''insert into bj_stable_1_3_negative (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8) \
                        values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (ts + i*1000, fake.random_int(min=-0, max=2147483647, step=1), 
                        fake.random_int(min=-0, max=9223372036854775807, step=1), 
                        fake.random_int(min=-0, max=32767, step=1) , fake.random_int(min=-0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i, fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , 
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))

            tdSql.execute('''insert into bj_stable_1_4_negative (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8) \
                        values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (ts + i*1000 +1, fake.random_int(min=-0, max=2147483647, step=1), 
                        fake.random_int(min=-0, max=9223372036854775807, step=1), 
                        fake.random_int(min=-0, max=32767, step=1) , fake.random_int(min=-0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i, fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , 
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))

            tdSql.execute('''insert into bj_stable_1_5_negative (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts,\
                        q_binary1 , q_nchar1 , q_binary2 , q_nchar2 , q_binary3 , q_nchar3 , q_binary4 , q_nchar4 , q_binary5 , q_nchar5 , q_binary6 , q_nchar6 , q_binary7 , q_nchar7, q_binary8 , q_nchar8) \
                        values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d, 'binary1.%s', 'nchar1.%s', 'binary2.%s', 'nchar2.%s', 'binary3.%s', 'nchar3.%s', \
                        'binary4.%s', 'nchar4.%s', 'binary5.%s', 'nchar5.%s', 'binary6.%s', 'nchar6.%s', 'binary7.%s', 'nchar7.%s', 'binary8.%s', 'nchar8.%s') ;''' 
                        % (ts + i*1000 +10, fake.random_int(min=-0, max=2147483647, step=1), 
                        fake.random_int(min=-0, max=9223372036854775807, step=1), 
                        fake.random_int(min=-0, max=32767, step=1) , fake.random_int(min=-0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.pystr() , ts + i, fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , 
                        fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr() , fake.pystr()))
            
        tdSql.query("select count(*) from stable_1;")
        tdSql.checkData(0,0,10*num_random*n)
        tdSql.query("select count(*) from hn_table_1_r;")
        tdSql.checkData(0,0,num_random*n)
        
        sleep(5)
        # stream data check
        tdSql.query("select startts,wend,max_int from stream_max_stable_1 ;")
        tdSql.checkRows(20)
        tdSql.query("select sum(max_int) from stream_max_stable_1 ;")
        stream_data_1 = tdSql.queryResult[0][0]
        tdSql.query("select sum(min_int) from stream_max_stable_1 ;")
        stream_data_2 = tdSql.queryResult[0][0]
        tdSql.query("select sum(max_int),sum(min_int) from (select _wstart as startts, _wend as wend, max(q_int) as max_int, min(q_bigint) as min_int from stable_1 where ts is not null interval (5s));")
        sql_data_1 = tdSql.queryResult[0][0]
        sql_data_2 = tdSql.queryResult[0][1]
        
        self.stream_value_check(stream_data_1,sql_data_1)       
        self.stream_value_check(stream_data_2,sql_data_2)
        
        tdSql.query("select sum(max_int),sum(min_int) from (select _wstart as startts, _wend as wend, max(q_int) as max_int, min(q_bigint) as min_int from stable_1 interval (5s));")
        sql_data_1 = tdSql.queryResult[0][0]
        sql_data_2 = tdSql.queryResult[0][1]
        
        self.stream_value_check(stream_data_1,sql_data_1)       
        self.stream_value_check(stream_data_2,sql_data_2)
        
        tdSql.query("select max(max_int) from stream_max_stable_1 ;")
        stream_data_1 = tdSql.queryResult[0][0]
        tdSql.query("select min(min_int) from stream_max_stable_1 ;")
        stream_data_2 = tdSql.queryResult[0][0]
        tdSql.query("select max(q_int) as max_int, min(q_bigint) as min_int from stable_1;")
        sql_data_1 = tdSql.queryResult[0][0]
        sql_data_2 = tdSql.queryResult[0][1]
        
        self.stream_value_check(stream_data_1,sql_data_1)       
        self.stream_value_check(stream_data_2,sql_data_2)
        
        
        tdSql.query(" select * from information_schema.ins_databases where name = '%s';" %database)
        tdLog.info(tdSql.queryResult)
        
        tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' limit 3;" %database)  
        queryRows = len(tdSql.queryResult)    
        for i in range(queryRows):
            tdLog.info("row=%d,  vgroup_id=%s,  tbname=%s " %(i,tdSql.queryResult[i][1],tdSql.queryResult[i][0]))
        
        tdLog.info("\n=============flush database ====================\n")
        
        tdSql.execute(" flush database %s;" %database)
        
        tdSql.query(" select * from information_schema.ins_databases where name = '%s';" %database)
        tdLog.info(tdSql.queryResult)
               
        tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' limit 3;" %database)
        queryRows = len(tdSql.queryResult)    
        for i in range(queryRows):
            tdLog.info("row=%d,  vgroup_id=%s,  tbname=%s " %(i,tdSql.queryResult[i][1],tdSql.queryResult[i][0]))


        # check in one vgroup
        if check_result_positive == 'Y':
            #base table : sh_table_0_a
            tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='sh_table_0_a';" %(database))
            base_value_table_name = tdSql.queryResult[0][0]
            base_value_table_vgroup = tdSql.queryResult[0][1]
            
            #check table :sh_table_i_a
            check_rows = tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name like 'sh_table_%%_a';" %(database))
            for i in range(check_rows):
                tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='sh_table_%d_a';" %(database,i))
                self.value_check(base_value_table_name,base_value_table_vgroup)
                        
            #check table :sh_table_i_b
            check_rows = tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name like 'sh_table_%%_b';" %(database))
            for i in range(check_rows):
                tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='sh_table_%d_b';" %(database,i))
                self.value_check(base_value_table_name,base_value_table_vgroup) 
                       
            #check table :sh_table_i_c
            check_rows = tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name like 'sh_table_%%_c';" %(database))
            for i in range(check_rows):
                tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='sh_table_%d_c';" %(database,i))
                self.value_check(base_value_table_name,base_value_table_vgroup)
                    
            #check table :sh_table_i_r
            check_rows = tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name like 'sh_table_%%_r';" %(database))
            for i in range(check_rows):
                tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='sh_table_%d_r';" %(database,i))
                self.value_check(base_value_table_name,base_value_table_vgroup)
                
            #check table :bj_table_i_a
            check_rows = tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name like 'bj_table_%%_a';" %(database))
            for i in range(check_rows):
                tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='bj_table_%d_a';" %(database,i))
                self.value_check(base_value_table_name,base_value_table_vgroup)
                
            #check table :bj_table_i_b
            check_rows = tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name like 'bj_table_%%_b';" %(database))
            for i in range(check_rows):
                tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='bj_table_%d_b';" %(database,i))
                self.value_check(base_value_table_name,base_value_table_vgroup)
                
            #check table :bj_table_i_c
            check_rows = tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name like 'bj_table_%%_c';" %(database))
            for i in range(check_rows):
                tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='bj_table_%d_c';" %(database,i))
                self.value_check(base_value_table_name,base_value_table_vgroup)
                
            #check table :bj_table_i_r
            check_rows = tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name like 'bj_table_%%_r';" %(database))
            for i in range(check_rows):
                tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='bj_table_%d_r';" %(database,i))
                self.value_check(base_value_table_name,base_value_table_vgroup)
                
            #check table :hn_table_i_r
            check_rows = tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name like 'hn_table_%%_r';" %(database))
            for i in range(check_rows):
                tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='hn_table_%d_r';" %(database,i))
                self.value_check(base_value_table_name,base_value_table_vgroup)
                
            #check table :tj_table_i_a
            check_rows = tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name like 'tj_table_%%_a';" %(database))
            for i in range(check_rows):
                tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='tj_table_%d_a';" %(database,i))
                self.value_check(base_value_table_name,base_value_table_vgroup)
                
            #check table :tj_table_i_b
            check_rows = tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name like 'tj_table_%%_b';" %(database))
            for i in range(check_rows):
                tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='tj_table_%d_b';" %(database,i))
                self.value_check(base_value_table_name,base_value_table_vgroup)
                         
        elif check_result_negative == 'Y':
            #base table : sh_table_0_a
            tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='sh_table_0_a_negative';" %(database))
            base_value_table_name = tdSql.queryResult[0][0]
            base_value_table_vgroup = tdSql.queryResult[0][1]
            
            #check table :sh_table_i_a
            check_rows = tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name like 'sh_table_%%_a_negative';" %(database))
            for i in range(check_rows):
                tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='sh_table_%d_a_negative';" %(database,i))
                self.value_check(base_value_table_name,base_value_table_vgroup)
                        
            #check table :sh_table_i_b
            check_rows = tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name like 'sh_table_%%_b_negative';" %(database))
            for i in range(check_rows):
                tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='sh_table_%d_b_negative';" %(database,i))
                self.value_check(base_value_table_name,base_value_table_vgroup) 
                       
            #check table :sh_table_i_c
            check_rows = tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name like 'sh_table_%%_c_negative';" %(database))
            for i in range(check_rows):
                tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='sh_table_%d_c_negative';" %(database,i))
                self.value_check(base_value_table_name,base_value_table_vgroup)
                    
            #check table :sh_table_i_r
            check_rows = tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name like 'sh_table_%%_r_negative';" %(database))
            for i in range(check_rows):
                tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='sh_table_%d_r_negative';" %(database,i))
                self.value_check(base_value_table_name,base_value_table_vgroup)
                
            #base table : bj_table_0_a
            tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='bj_table_0_a_negative';" %(database))
            base_value_table_name = tdSql.queryResult[0][0]
            base_value_table_vgroup = tdSql.queryResult[0][1]
                
            #check table :bj_table_i_a
            check_rows = tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name like 'bj_table_%%_a_negative';" %(database))
            for i in range(check_rows):
                tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='bj_table_%d_a_negative';" %(database,i))
                self.value_check(base_value_table_name,base_value_table_vgroup)
                
            #check table :bj_table_i_b
            check_rows = tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name like 'bj_table_%%_b_negative';" %(database))
            for i in range(check_rows):
                tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='bj_table_%d_b_negative';" %(database,i))
                self.value_check(base_value_table_name,base_value_table_vgroup)
                
            #check table :bj_table_i_c
            check_rows = tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name like 'bj_table_%%_c_negative';" %(database))
            for i in range(check_rows):
                tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='bj_table_%d_c_negative';" %(database,i))
                self.value_check(base_value_table_name,base_value_table_vgroup)
                
            #check table :bj_table_i_r
            check_rows = tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name like 'bj_table_%%_r_negative';" %(database))
            for i in range(check_rows):
                tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='bj_table_%d_r_negative';" %(database,i))
                self.value_check(base_value_table_name,base_value_table_vgroup)
                
            #base table : hn_table_0_r
            tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='hn_table_0_r_negative';" %(database))
            base_value_table_name = tdSql.queryResult[0][0]
            base_value_table_vgroup = tdSql.queryResult[0][1]
            
            #check table :hn_table_i_r
            check_rows = tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name like 'hn_table_%%_r_negative';" %(database))
            for i in range(check_rows):
                tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='hn_table_%d_r_negative';" %(database,i))
                self.value_check(base_value_table_name,base_value_table_vgroup)
                
                
            #base table : tj_table_0_r
            tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='tj_table_0_a_negative';" %(database))
            base_value_table_name = tdSql.queryResult[0][0]
            base_value_table_vgroup = tdSql.queryResult[0][1]
            
            #check table :tj_table_i_a
            check_rows = tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name like 'tj_table_%%_a_negative';" %(database))
            for i in range(check_rows):
                tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='tj_table_%d_a_negative';" %(database,i))
                self.value_check(base_value_table_name,base_value_table_vgroup)
                
            #check table :tj_table_i_b
            check_rows = tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name like 'tj_table_%%_b_negative';" %(database))
            for i in range(check_rows):
                tdSql.query(" select table_name,vgroup_id from information_schema.ins_tables where db_name = '%s' and table_name='tj_table_%d_b_negative';" %(database,i))
                self.value_check(base_value_table_name,base_value_table_vgroup)
                
                
        else:
            pass
        
        tdLog.info(f"create end:n:{n},vgroups:{vgroups},table_prefix:{table_prefix},table_suffix:{table_suffix},check_result_positive:{check_result_positive},check_result_negative:{check_result_negative}") 
        
    
    def value_check(self,base_value_table_name,base_value_table_vgroup):
        check_value_table_name = tdSql.queryResult[0][0]
        check_value_table_vgroup = tdSql.queryResult[0][1]
        #tdLog.info(f"{base_value_table_name},{base_value_table_vgroup},{check_value_table_name},{check_value_table_vgroup}")

        if base_value_table_vgroup==check_value_table_vgroup:
            tdLog.info(f"checkEqual success, base_table_name={base_value_table_name},base_table_host={base_value_table_vgroup} ,check_table_name={check_value_table_name},check_table_host={check_value_table_vgroup}") 
        else :
            tdLog.exit(f"checkEqual error, base_table_name=={base_value_table_name},base_table_host={base_value_table_vgroup} ,check_table_name={check_value_table_name},check_table_host={check_value_table_vgroup}") 
    
    def stream_value_check(self,stream_data,sql_data):
        if stream_data==sql_data:
            tdLog.info(f"checkEqual success, stream_data={stream_data},sql_data={sql_data}") 
        else :
            tdLog.exit(f"checkEqual error, stream_data=={stream_data},sql_data={sql_data}") 
                            
    def run(self):       
        startTime = time.time()  
                  
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename)) 
        
        #(self,database,n,vgroups,table_prefix,table_suffix,check_result_positive,check_result_negative):
        #check_result_positive 检查前缀后缀是正数的，check_result_negative 检查前缀后缀是负数的(TS-3249)
        # self.dropandcreateDB_random("%s" %self.db, 1,2,0,0,'N','N')
        # self.dropandcreateDB_random("%s" %self.db, 1,2,0,2,'N','N')
        # self.dropandcreateDB_random("%s" %self.db, 1,2,2,0,'N','N')        
        
        self.dropandcreateDB_random("%s" %self.db, 1,random.randint(1,5),random.randint(0,3),random.randint(0,3),'N','N')
        self.dropandcreateDB_random("%s" %self.db, 1,random.randint(1,5),random.randint(-10,0),random.randint(-10,0),'N','N')
        self.dropandcreateDB_random("%s" %self.db, 1,random.randint(1,5),random.randint(-191,0),random.randint(-191,0),'N','N')
        self.dropandcreateDB_random("%s" %self.db, 1,random.randint(1,5),random.randint(0,100),random.randint(0,91),'N','N')
        
        # self.dropandcreateDB_random("%s" %self.db, 1,2,3,3,'Y','N')  
        # self.dropandcreateDB_random("%s" %self.db, 1,3,3,3,'Y','N')  
        # self.dropandcreateDB_random("%s" %self.db, 1,4,4,4,'Y','N')
        # self.dropandcreateDB_random("%s" %self.db, 1,5,5,5,'Y','N')       
        self.dropandcreateDB_random("%s" %self.db, 1,random.randint(1,5),random.randint(3,5),random.randint(3,5),'Y','N')
                
        self.dropandcreateDB_random("%s" %self.db, 1,random.randint(1,5),random.randint(-5,-1),0,'N','Y')        
        self.dropandcreateDB_random("%s" %self.db, 1,random.randint(1,5),random.randint(-5,-1),random.randint(-9,-0),'N','Y')
        
        
        # #taos -f sql
        print("taos -f sql start!")
        taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
        _ = subprocess.check_output(taos_cmd1, shell=True)
        time.sleep(5)
        print("taos -f sql over!")     
                

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))
    


    def stop(self):
        tdSql.close()
        time.sleep(5)
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
