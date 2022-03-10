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
import string
import os
import sys
import time
import taos
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
from util.dnodes import tdDnodes
from util.dnodes import *
import itertools
from itertools import product
from itertools import combinations
from faker import Faker
import subprocess

class TDTestCase:
    def caseDescription(self):
        '''
        case1<xyguo>[TD-12204]:slect * from ** order by ts can cause core:src/query/src/qExtbuffer.c 
        ''' 
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        os.system("rm -rf 2-query/TD-12204.py.sql")
    
    def restartDnodes(self):
        tdDnodes.stop(1)
        tdDnodes.start(1)

    def dropandcreateDB_random(self,n):
        self.ts = 1630000000000
        self.num_random = 1000
        fake = Faker('zh_CN')
        for i in range(n):
            tdSql.execute('''drop database if exists db ;''')
            tdSql.execute('''create database db keep 36500;''')
            tdSql.execute('''use db;''')

            tdSql.execute('''create stable stable_1 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                    tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')
            tdSql.execute('''create stable stable_2 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                    tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);''')
            
            tdSql.execute('''create table table_1 using stable_1 tags('table_1', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0')''')
            tdSql.execute('''create table table_2 using stable_1 tags('table_2', '2147483647' , '9223372036854775807' , '32767' , '127' , 1 , 'binary2' , 'nchar2' , '2' , '22' , \'1999-09-09 09:09:09.090\')''')
            tdSql.execute('''create table table_3 using stable_1 tags('table_3', '-2147483647' , '-9223372036854775807' , '-32767' , '-127' , false , 'binary3' , 'nchar3nchar3' , '-3.3' , '-33.33' , \'2099-09-09 09:09:09.090\')''')
            tdSql.execute('''create table table_21 using stable_2 tags('table_21' , '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0')''')

            #regular table
            tdSql.execute('''create table regular_table_1 \
                        (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                        q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')
            tdSql.execute('''create table regular_table_2 \
                        (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                        q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')
            tdSql.execute('''create table regular_table_3 \
                        (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                        q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')


            for i in range(self.num_random):        
                tdSql.execute('''insert into table_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*1000, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))
                tdSql.execute('''insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*1000, fake.random_int(min=-2147483647, max=2147483647, step=1) , 
                            fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1) , 
                            fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))

                tdSql.execute('''insert into table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*1000, fake.random_int(min=0, max=2147483647, step=1), 
                            fake.random_int(min=0, max=9223372036854775807, step=1), 
                            fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))
                tdSql.execute('''insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*1000, fake.random_int(min=0, max=2147483647, step=1), 
                            fake.random_int(min=0, max=9223372036854775807, step=1), 
                            fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))

                tdSql.execute('''insert into table_3 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*1000, fake.random_int(min=-2147483647, max=0, step=1), 
                            fake.random_int(min=-9223372036854775807, max=0, step=1), 
                            fake.random_int(min=-32767, max=0, step=1) , fake.random_int(min=-127, max=0, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))
                tdSql.execute('''insert into regular_table_3 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*1000, fake.random_int(min=-2147483647, max=0, step=1), 
                            fake.random_int(min=-9223372036854775807, max=0, step=1), 
                            fake.random_int(min=-32767, max=0, step=1) , fake.random_int(min=-127, max=0, step=1) , 
                            fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))

            tdSql.query("select count(*) from stable_1;")
            tdSql.checkData(0,0,3000)
            tdSql.query("select count(*) from regular_table_1;")
            tdSql.checkData(0,0,1000)
               
    def dropandcreateDB_null(self):
        self.num_null = 100
        self.ts = 1630000000000
        tdSql.execute('''drop database if exists db ;''')
        tdSql.execute('''create database db keep 36500;''')
        tdSql.execute('''use db;''')

        tdSql.execute('''create stable stable_1 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(20) , q_nchar nchar(20) , q_ts timestamp , 
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) 
                    tags(loc nchar(20) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(20) , t_nchar nchar(20) ,t_float float , t_double double , t_ts timestamp);''')
        tdSql.execute('''create stable stable_2 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(20) , q_nchar nchar(20) , q_ts timestamp , 
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) 
                    tags(loc nchar(20) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(20) , t_nchar nchar(20) ,t_float float , t_double double , t_ts timestamp);''')
        
        tdSql.execute('''create table table_1 using stable_1 tags('table_1', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0')''')
        tdSql.execute('''create table table_2 using stable_1 tags('table_2', '2147483647' , '9223372036854775807' , '32767' , '127' , 1 , 'binary2' , 'nchar2' , '2' , '22' , \'1999-09-09 09:09:09.090\')''')
        tdSql.execute('''create table table_3 using stable_1 tags('table_3', '-2147483647' , '-9223372036854775807' , '-32767' , '-127' , false , 'binary3' , 'nchar3nchar3' , '-3.3' , '-33.33' , \'2099-09-09 09:09:09.090\')''')
        tdSql.execute('''create table table_21 using stable_2 tags('table_21' , '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0')''')
        
        tdSql.execute('''create table regular_table_1
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(20) , q_nchar nchar(20) , q_ts timestamp , 
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')
        tdSql.execute('''create table regular_table_2
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(20) , q_nchar nchar(20) , q_ts timestamp , 
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')
        tdSql.execute('''create table regular_table_3
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(20) , q_nchar nchar(20) , q_ts timestamp , 
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;''')
        
        for i in range(self.num_null):        
            tdSql.execute('''insert into table_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*10000, i, i, i, i, i, i, i, i, self.ts + i))
            tdSql.execute('''insert into table_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*3000, i, i, i, i, i, i, i, i, self.ts + i))
            tdSql.execute('''insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*10000, i, i, i, i, i, i, i, i, self.ts + i))
            tdSql.execute('''insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*3000 , i, i, i, i, i, i, i, i, self.ts + i))

            tdSql.execute('''insert into table_21  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*10000, i, i, i, i, i, i, i, i, self.ts + i))
            tdSql.execute('''insert into table_21  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*3000, i, i, i, i, i, i, i, i, self.ts + i))

            tdSql.execute('''insert into table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*10000, 2147483647-i, 9223372036854775807-i, 32767-i, 127-i, i, i, i, i, self.ts + i))
            tdSql.execute('''insert into table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*3000, 2147483647-i, 9223372036854775807-i, 32767-i, 127-i, i, i, i, i, self.ts + i))
            tdSql.execute('''insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*10000, 2147483647-i, 9223372036854775807-i, 32767-i, 127-i, i, i, i, i, self.ts + i))
            tdSql.execute('''insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*3000 , 2147483647-i, 9223372036854775807-i, 32767-i, 127-i, i, i, i, i, self.ts + i))

            tdSql.execute('''insert into table_3 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*10000, -2147483647+i, -9223372036854775807+i, -32767+i, -127+i, -i, -i, i, i, self.ts + i))
            tdSql.execute('''insert into table_3 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*3000, -2147483647+i, -9223372036854775807+i, -32767+i, -127+i, -i, -i, i, i, self.ts + i))
            tdSql.execute('''insert into regular_table_3 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*10000, -2147483647+i, -9223372036854775807+i, -32767+i, -127+i, -i, -i, i, i, self.ts + i))
            tdSql.execute('''insert into regular_table_3 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d)''' 
                            % (self.ts + i*3000 , -2147483647+i, -9223372036854775807+i, -32767+i, -127+i, -i, -i, i, i, self.ts + i))

        tdSql.query("select count(*) from stable_1;")
        tdSql.checkData(0,0,570)
        tdSql.query("select count(*) from regular_table_1;")
        tdSql.checkData(0,0,190)


    def result_0(self,sql):
        tdLog.info(sql)       
        tdSql.query(sql)
        tdSql.checkRows(0)
 
    def dataequal(self, sql1,row1,col1, sql2,row2,col2):
        self.sql1 = sql1
        list1 =[]
        tdSql.query(sql1)
        for i1 in range(row1):
            for j1 in range(col1):
                list1.append(tdSql.getData(i1,j1))
        #print(list1)
        
        tdSql.execute("reset query cache;")
        self.sql2 = sql2  
        list2 =[]
        tdSql.query(sql2)
        #print(tdSql.queryResult)
        for i2 in range(row2):
            for j2 in range(col2):
                list2.append(tdSql.getData(i2,j2))
        #print(list2)
       
        if  (list1 == list2) and len(list2)>0:
	        tdLog.info(("sql1:'%s' result = sql2:'%s' result") %(sql1,sql2))
        else:
            tdLog.info(("sql1:'%s' result != sql2:'%s' result") %(sql1,sql2))
            return tdSql.checkEqual(list1,list2)
                   
    def data2in1(self, sql1,row1,col1, sql2,row2,col2):
        self.sql1 = sql1
        list1 =[]
        tdSql.query(sql1)
        for i1 in range(row1):
            for j1 in range(col1):
                list1.append(tdSql.getData(i1,j1))

        tdSql.execute("reset query cache;")
        self.sql2 = sql2  
        list2 =[]
        tdSql.query(sql2)
        for i2 in range(row2):
            for j2 in range(col2):
                list2.append(tdSql.getData(i2,j2))
       
        if  (set(list2) <= set(list1)) and len(list2)>0:
	        tdLog.info(("sql1:'%s' result include sql2:'%s' result") %(sql1,sql2))
        else:
            tdLog.info(("sql1:'%s' result not include sql2:'%s' result") %(sql1,sql2))
            return tdSql.checkEqual(list1,list2)


    def regular_where(self):       
        q_int_where = ['q_bigint >= -9223372036854775807 and ' , 'q_bigint <= 9223372036854775807 and ','q_smallint >= -32767 and ', 'q_smallint <= 32767 and ',
        'q_tinyint >= -127 and ' , 'q_tinyint <= 127 and ' , 'q_int <= 2147483647 and ' , 'q_int >= -2147483647 and ',
        'q_tinyint != 128 and ',
        'q_bigint between  -9223372036854775807 and 9223372036854775807 and ',' q_int between -2147483647 and 2147483647 and ',
        'q_smallint between -32767 and 32767 and ', 'q_tinyint between -127 and 127  and ',
        'q_bigint is not null and ' , 'q_int is not null and ' , 'q_smallint is not null and ' , 'q_tinyint is not null and ' ,]

        q_fl_do_where = ['q_float >= -3.4E38 and ','q_float <= 3.4E38 and ', 'q_double >= -1.7E308 and ','q_double <= 1.7E308 and ', 
        'q_float between -3.4E38 and 3.4E38 and ','q_double between -1.7E308 and 1.7E308 and ' ,
        'q_float is not null and ' ,'q_double is not null and ' ,]

        q_nc_bi_bo_ts_where = [ 'q_bool is not null and ' ,'q_binary is not null and ' ,'q_nchar is not null and ' ,'q_ts is not null and ' ,]

        q_where = random.sample(q_int_where,2) + random.sample(q_fl_do_where,1) + random.sample(q_nc_bi_bo_ts_where,1)
        print(q_where)
        return q_where
        

    def regular_where_all(self):       
        q_int_where_add = ['q_bigint >= 0 and ' , 'q_smallint >= 0 and ', 'q_tinyint >= 0 and ' ,  'q_int >= 0 and ',
        'q_bigint between  0 and 9223372036854775807 and ',' q_int between 0 and 2147483647 and ',
        'q_smallint between 0 and 32767 and ', 'q_tinyint between 0 and 127  and ',
        'q_bigint is not null and ' , 'q_int is not null and ' ,]

        q_fl_do_where_add = ['q_float >= 0 and ', 'q_double >= 0 and ' , 'q_float between 0 and 3.4E38 and ','q_double between 0 and 1.7E308 and ' ,
        'q_float is not null and ' ,]

        q_nc_bi_bo_ts_where_add = ['q_nchar is not null and ' ,'q_ts is not null and ' ,]

        q_where_add = random.sample(q_int_where_add,2) + random.sample(q_fl_do_where_add,1) + random.sample(q_nc_bi_bo_ts_where_add,1)
        
        q_int_where_sub = ['q_bigint <= 0 and ' , 'q_smallint <= 0 and ', 'q_tinyint <= 0 and ' ,  'q_int <= 0 and ',
        'q_bigint between -9223372036854775807 and 0 and ',' q_int between -2147483647 and 0 and ',
        'q_smallint between -32767 and 0 and ', 'q_tinyint between -127 and 0 and ',
        'q_smallint is not null and ' , 'q_tinyint is not null and ' ,]

        q_fl_do_where_sub = ['q_float <= 0 and ', 'q_double <= 0 and ' , 'q_float between -3.4E38 and 0 and ','q_double between -1.7E308 and 0 and ' ,
        'q_double is not null and ' ,]

        q_nc_bi_bo_ts_where_sub = ['q_bool is not null and ' ,'q_binary is not null and ' ,]

        q_where_sub = random.sample(q_int_where_sub,2) + random.sample(q_fl_do_where_sub,1) + random.sample(q_nc_bi_bo_ts_where_sub,1)

        return(q_where_add,q_where_sub)

    def stable_where(self):       
        q_where = self.regular_where()

        t_int_where = ['t_bigint >= -9223372036854775807 and ' , 't_bigint <= 9223372036854775807 and ','t_smallint >= -32767 and ', 't_smallint <= 32767 and ',
        't_tinyint >= -127 and ' , 't_tinyint <= 127 and ' , 't_int <= 2147483647 and ' , 't_int >= -2147483647 and ',
        't_tinyint != 128 and ',
        't_bigint between  -9223372036854775807 and 9223372036854775807 and ',' t_int between -2147483647 and 2147483647 and ',
        't_smallint between -32767 and 32767 and ', 't_tinyint between -127 and 127  and ',
        't_bigint is not null and ' , 't_int is not null and ' , 't_smallint is not null and ' , 't_tinyint is not null and ' ,]

        t_fl_do_where = ['t_float >= -3.4E38 and ','t_float <= 3.4E38 and ', 't_double >= -1.7E308 and ','t_double <= 1.7E308 and ', 
        't_float between -3.4E38 and 3.4E38 and ','t_double between -1.7E308 and 1.7E308 and ' ,
        't_float is not null and ' ,'t_double is not null and ' ,]

        t_nc_bi_bo_ts_where = [ 't_bool is not null and ' ,'t_binary is not null and ' ,'t_nchar is not null and ' ,'t_ts is not null and ' ,]

        t_where = random.sample(t_int_where,2) + random.sample(t_fl_do_where,1) + random.sample(t_nc_bi_bo_ts_where,1)
        
        qt_where = q_where + t_where
        print(qt_where)
        return qt_where


    def stable_where_all(self):  
        regular_where_all = self.regular_where_all()

        t_int_where_add = ['t_bigint >= 0 and ' , 't_smallint >= 0 and ', 't_tinyint >= 0 and ' ,  't_int >= 0 and ',
        't_bigint between  1 and 9223372036854775807 and ',' t_int between 1 and 2147483647 and ',
        't_smallint between 1 and 32767 and ', 't_tinyint between 1 and 127  and ',
        't_bigint is not null and ' , 't_int is not null and ' ,]

        t_fl_do_where_add = ['t_float >= 0 and ', 't_double >= 0 and ' , 't_float between 1 and 3.4E38 and ','t_double between 1 and 1.7E308 and ' ,
        't_float is not null and ' ,]

        t_nc_bi_bo_ts_where_add = ['t_nchar is not null and ' ,'t_ts is not null and ' ,]

        qt_where_add = random.sample(t_int_where_add,1) + random.sample(t_fl_do_where_add,1) + random.sample(t_nc_bi_bo_ts_where_add,1) + random.sample(regular_where_all[0],2)
        
        t_int_where_sub = ['t_bigint <= 0 and ' , 't_smallint <= 0 and ', 't_tinyint <= 0 and ' ,  't_int <= 0 and ',
        't_bigint between -9223372036854775807 and -1 and ',' t_int between -2147483647 and -1 and ',
        't_smallint between -32767 and -1 and ', 't_tinyint between -127 and -1 and ',
        't_smallint is not null and ' , 't_tinyint is not null and ' ,]

        t_fl_do_where_sub = ['t_float <= 0 and ', 't_double <= 0 and ' , 't_float between -3.4E38 and -1 and ','t_double between -1.7E308 and -1 and ' ,
        't_double is not null and ' ,]

        t_nc_bi_bo_ts_where_sub = ['t_bool is not null and ' ,'t_binary is not null and ' ,]

        qt_where_sub = random.sample(t_int_where_sub,1) + random.sample(t_fl_do_where_sub,1) + random.sample(t_nc_bi_bo_ts_where_sub,1) + random.sample(regular_where_all[1],2)

        return(qt_where_add,qt_where_sub)


    def run(self):
        tdSql.prepare()

        dcDB = self.dropandcreateDB_random(1)
       
        stable_where_all = self.stable_where_all()
        print(stable_where_all)
        for i in range(2,len(stable_where_all[0])+1):
            qt_where_add_new = list(combinations(stable_where_all[0],i))
            for qt_where_add_new in qt_where_add_new:
                qt_where_add_new = str(qt_where_add_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","").replace("=","")
    
        for j in range(2,len(stable_where_all[1])+1):
            qt_where_sub_new = list(combinations(stable_where_all[1],j))
            for qt_where_sub_new in qt_where_sub_new:
                qt_where_sub_new = str(qt_where_sub_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","").replace("=","")
                sql = "select * from stable_1 where %s %s ts < now +1s order by ts " %(qt_where_add_new,qt_where_sub_new)

                tdSql.query(sql)

        conn1 = taos.connect(host="127.0.0.1", user="root", password="taosdata", config="/etc/taos/")
        print(conn1)
        cur1 = conn1.cursor()
        tdSql.init(cur1, True)        
        cur1.execute('use db ;')
        sql = 'select * from stable_1 limit 10;'
        cur1.execute(sql)
        for data in cur1:
            print("ts = %s" %data[0])
        
        print(conn1)

        for i in range(2):
            try:
                taos_cmd1 = "taos -f 2-query/TD-12204.py.sql"
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")

                print(i)
                print(conn1)

                for i in range(5):
                    cur1.execute('use db ;')
                    sql = 'select * from stable_1 where t_smallint between 0 and 32767 and  t_float between 0 and 3.4E38 and  t_nchar is not null and  q_smallint between 0 and 32767 and  q_nchar is not null and  t_binary is not null and  q_tinyint is not null and  ts < now +1s order by ts ;;;'
                    
                    cur1.execute(sql)
                    for data in cur1:
                        print("ts = %s" %data[0])

            except Exception as e:
                raise e   

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())