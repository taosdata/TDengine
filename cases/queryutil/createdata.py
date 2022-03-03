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
# import string
# import os
# import sys
# import time
# import taos
# import itertools
from itertools import product
from itertools import combinations
from faker import Faker
import subprocess

from taostest import TDCase

class TDCreateData():
    def __init__(self, tdSql, logger):
        self.tdSql =  tdSql
        self.logger = logger

    def desc(self) -> str:
        case_description = '''
        case1<xyguo>:data create 
        ''' 
        return case_description

    def tags(self) :
		
        return ""

    def author(self) -> str:

        return "Guo Xiangyang"
        
    def restartDnodes(self):
        self.tdDnodes.stop(1)
        self.tdDnodes.start(1)

    def dropandcreateDB_random(self,database,n):
        self.ts = 1630000000000
        self.num_random = 100
        fake = Faker('zh_CN')
        self.tdSql.execute('''drop database if exists %s ;''' %database)
        self.tdSql.execute('''create database %s keep 36500;'''%database)
        self.tdSql.execute('''use %s;'''%database)

        self.tdSql.execute('''create stable stable_1 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')
        self.tdSql.execute('''create stable stable_2 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')
        
        self.tdSql.execute('''create stable stable_null_data (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')

        self.tdSql.execute('''create stable stable_null_childtable (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')
        
        self.tdSql.execute('''create table stable_1_1 using stable_1 tags('stable_1_1', '0' , '0' , '0' , '0' , 0 , 'binary1' , 'nchar1' , '0' , '0' ,'0')''')
        self.tdSql.execute('''create table stable_1_2 using stable_1 tags('stable_1_2', '2147483647' , '9223372036854775807' , '32767' , '127' , 1 , 'binary2' , 'nchar2' , '2' , '22' , \'1999-09-09 09:09:09.090\')''')
        self.tdSql.execute('''create table stable_1_3 using stable_1 tags('stable_1_3', '-2147483647' , '-9223372036854775807' , '-32767' , '-127' , false , 'binary3' , 'nchar3nchar3' , '-3.3' , '-33.33' , \'2099-09-09 09:09:09.090\')''')
        self.tdSql.execute('''create table stable_1_4 using stable_1 tags('stable_1_4', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0')''')

        self.tdSql.execute('''create table stable_2_1 using stable_2 tags('stable_2_1' , '0' , '0' , '0' , '0' , 0 , 'binary21' , 'nchar21' , '0' , '0' ,'0')''')
        self.tdSql.execute('''create table stable_2_2 using stable_2 tags('stable_2_2' , '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0')''')

        self.tdSql.execute('''create table stable_null_data_1 using stable_null_data tags('stable_null_data_1', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0')''')

        #regular table
        self.tdSql.execute('''create table regular_table_1 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')
        self.tdSql.execute('''create table regular_table_2 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')
        self.tdSql.execute('''create table regular_table_3 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')

        self.tdSql.execute('''create table regular_table_null \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')


        for i in range(self.num_random*n):        
            self.tdSql.execute('''insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d)''' 
                        % (self.ts + i*1000, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))
            self.tdSql.execute('''insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d)''' 
                        % (self.ts + i*1000, fake.random_int(min=-2147483647, max=2147483647, step=1) , 
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1) , 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))

            self.tdSql.execute('''insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d)''' 
                        % (self.ts + i*1000, fake.random_int(min=0, max=2147483647, step=1), 
                        fake.random_int(min=0, max=9223372036854775807, step=1), 
                        fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))
            self.tdSql.execute('''insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d)''' 
                        % (self.ts + i*1000, fake.random_int(min=0, max=2147483647, step=1), 
                        fake.random_int(min=0, max=9223372036854775807, step=1), 
                        fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))

            self.tdSql.execute('''insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d)''' 
                        % (self.ts + i*1000, fake.random_int(min=-2147483647, max=0, step=1), 
                        fake.random_int(min=-9223372036854775807, max=0, step=1), 
                        fake.random_int(min=-32767, max=0, step=1) , fake.random_int(min=-127, max=0, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))

            # self.tdSql.execute('''insert into regular_table_3 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d)''' 
            #             % (self.ts + i*1000, fake.random_int(min=-2147483647, max=0, step=1), 
            #             fake.random_int(min=-9223372036854775807, max=0, step=1), 
            #             fake.random_int(min=-32767, max=0, step=1) , fake.random_int(min=-127, max=0, step=1) , 
            #             fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))

        self.tdSql.query("select count(*) from stable_1;")
        self.tdSql.checkData(0,0,2*self.num_random*n)
        self.tdSql.query("select count(*) from regular_table_1;")
        self.tdSql.checkData(0,0,self.num_random*n)
               
    def dropandcreateDB_null(self):
        self.num_null = 100
        self.ts = 1630000000000
        self.tdSql.execute('''drop database if exists db ;''')
        self.tdSql.execute('''create database db keep 36500;''')
        self.tdSql.execute('''use db;''')

        self.tdSql.execute('''create stable stable_1 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(20) , q_nchar nchar(20) , q_ts timestamp , 
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) 
                    tags(loc nchar(20) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(20) , t_nchar nchar(20) ,t_float float , t_double double , t_ts timestamp);''')
        self.tdSql.execute('''create stable stable_2 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(20) , q_nchar nchar(20) , q_ts timestamp , 
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) 
                    tags(loc nchar(20) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(20) , t_nchar nchar(20) ,t_float float , t_double double , t_ts timestamp);''')
        
        self.tdSql.execute('''create table table_1 using stable_1 tags('table_1', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0')''')
        self.tdSql.execute('''create table table_2 using stable_1 tags('table_2', '2147483647' , '9223372036854775807' , '32767' , '127' , 1 , 'binary2' , 'nchar2' , '2' , '22' , \'1999-09-09 09:09:09.090\')''')
        self.tdSql.execute('''create table table_3 using stable_1 tags('table_3', '-2147483647' , '-9223372036854775807' , '-32767' , '-127' , false , 'binary3' , 'nchar3nchar3' , '-3.3' , '-33.33' , \'2099-09-09 09:09:09.090\')''')
        self.tdSql.execute('''create table table_21 using stable_2 tags('table_21' , '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0')''')
        
        self.tdSql.execute('''create table regular_table_1
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(20) , q_nchar nchar(20) , q_ts timestamp , 
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')
        self.tdSql.execute('''create table regular_table_2
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(20) , q_nchar nchar(20) , q_ts timestamp , 
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')
        self.tdSql.execute('''create table regular_table_3
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(20) , q_nchar nchar(20) , q_ts timestamp , 
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')
        
        for i in range(self.num_null):        
            self.tdSql.execute('''insert into table_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*10000, i, i, i, i, i, i, i, i, self.ts + i))
            self.tdSql.execute('''insert into table_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*3000, i, i, i, i, i, i, i, i, self.ts + i))
            self.tdSql.execute('''insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*10000, i, i, i, i, i, i, i, i, self.ts + i))
            self.tdSql.execute('''insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*3000 , i, i, i, i, i, i, i, i, self.ts + i))

            self.tdSql.execute('''insert into table_21  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*10000, i, i, i, i, i, i, i, i, self.ts + i))
            self.tdSql.execute('''insert into table_21  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*3000, i, i, i, i, i, i, i, i, self.ts + i))

            self.tdSql.execute('''insert into table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*10000, 2147483647-i, 9223372036854775807-i, 32767-i, 127-i, i, i, i, i, self.ts + i))
            self.tdSql.execute('''insert into table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*3000, 2147483647-i, 9223372036854775807-i, 32767-i, 127-i, i, i, i, i, self.ts + i))
            self.tdSql.execute('''insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*10000, 2147483647-i, 9223372036854775807-i, 32767-i, 127-i, i, i, i, i, self.ts + i))
            self.tdSql.execute('''insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*3000 , 2147483647-i, 9223372036854775807-i, 32767-i, 127-i, i, i, i, i, self.ts + i))

            self.tdSql.execute('''insert into table_3 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*10000, -2147483647+i, -9223372036854775807+i, -32767+i, -127+i, -i, -i, i, i, self.ts + i))
            self.tdSql.execute('''insert into table_3 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*3000, -2147483647+i, -9223372036854775807+i, -32767+i, -127+i, -i, -i, i, i, self.ts + i))
            self.tdSql.execute('''insert into regular_table_3 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*10000, -2147483647+i, -9223372036854775807+i, -32767+i, -127+i, -i, -i, i, i, self.ts + i))
            self.tdSql.execute('''insert into regular_table_3 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*3000 , -2147483647+i, -9223372036854775807+i, -32767+i, -127+i, -i, -i, i, i, self.ts + i))

        self.tdSql.query("select count(*) from stable_1;")
        self.tdSql.checkData(0,0,570)
        self.tdSql.query("select count(*) from regular_table_1;")
        self.tdSql.checkData(0,0,190)

    def alter_column(self,sql):
        pass

    def alter_tag(self,sql):
        pass


    def result_0(self,sql):
        #self.logger.info(sql) 
        self.tdSql.query(sql)
        self.tdSql.checkRow(0)
        
    def dataequal(self, sql1,row1,col1, sql2,row2,col2):
        self.sql1 = sql1
        list1 =[]
        self.tdSql.query(sql1)
        for i1 in range(row1):
            for j1 in range(col1):
                list1.append(self.tdSql.getData(i1,j1))
        
        self.tdSql.execute("reset query cache;")
        self.sql2 = sql2  
        list2 =[]
        self.tdSql.query(sql2)
        for i2 in range(row2):
            for j2 in range(col2):
                list2.append(self.tdSql.getData(i2,j2))
       
        if  (list1 == list2) and len(list2)>0:
            pass
            #self.tdLog.info(("sql1:'%s' result = sql2:'%s' result") %(sql1,sql2))
            #self.logger.info(("sql1:'%s' result = sql2:'%s' result") %(sql1,sql2))
        else:
            pass
            #self.tdLog.info(("sql1:'%s' result != sql2:'%s' result") %(sql1,sql2))
            #self.logger.info(("sql1:'%s' result != sql2:'%s' result") %(sql1,sql2))
            return self.tdSql.checkEqual(list1,list2)
                   
    def data2in1(self, sql1,row1,col1, sql2,row2,col2):
        self.sql1 = sql1
        list1 =[]
        self.tdSql.query(sql1)
        for i1 in range(row1):
            for j1 in range(col1):
                list1.append(self.tdSql.getData(i1,j1))
        #print(list1)

        self.tdSql.execute("reset query cache;")
        self.sql2 = sql2  
        list2 =[]
        self.tdSql.query(sql2)
        for i2 in range(row2):
            for j2 in range(col2):                
                list2.append(self.tdSql.getData(i2,j2))
                #print(len(list2))
                #print(list2)                
        
        if len(list2) == 0 :
            self.result_0(sql2)
        elif (set(list2) <= set(list1)) :
            self.tdLog.info(("sql1:'%s' result include sql2:'%s' result") %(sql1,sql2))
        else:
            self.tdLog.info(("sql1:'%s' result not include sql2:'%s' row:'%s' col'%s' result '%s'") %(sql1,sql2,i2,j2,self.tdSql.getData(i2,j2)))
            return self.tdSql.checkEqual(list1,self.tdSql.getData(i2,j2))
