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


from itertools import product
from itertools import combinations
from faker import Faker
import time, datetime
from taostest import TDCase
import subprocess
import os
import random
from taostest.util.common import TDCom
from taostest.util.remote import Remote
import threading

class TDTestQuery(TDCase):

    def tags(self) :
	
        return ""

    def author(self) -> str:

        return "Guo Xiangyang"

    def desc(self) -> str:
        case_description = '''
        case1:# meters all query
        '''
        return case_description        

        
    def drop_db(self,dbname):
        #delete:
        table_list = ['stable_1','stable_2','stable_null_data','stable_null_childtable','stable_1','stable_2','regular_table_1','stable_1_1','regular_table_2',\
            'regular_table_3','regular_table_1','regular_table_2','regular_table_null','stable_2_1','stable_2_2','stable_2_2','stable_1_3','stable_1_4',]
        for i in table_list:
            self.tdSql.execute("delete from {}.{};".format(dbname, i))
            self.tdSql.execute("flush database {};".format(dbname))
            self.tdSql.execute("reset query cache;")
            self.tdSql.query("select * from {}.{};".format(dbname, i))
            self.tdSql.checkRow(0)
        
        #drop:
        self.tdSql.execute('''drop database if exists %s ;''' %dbname)

    def show_local_variables(self):
        self.tdSql.query('''show local variables;''')
        for i in range(self.tdSql.query_row):
            self.logger.info("%s - %s"% (self.tdSql.query_data[i][0], self.tdSql.query_data[i][1]))

    def dropandcreateDB_random(self,database,n,replica):
        self.ts = 1630000000000
        self.num_random = 100
        fake = Faker('zh_CN')
        self.tdSql.execute('''drop database if exists %s ;''' %database)
        self.tdSql.execute('''create database %s keep 36500 replica %d;'''%(database,replica))
        # self.show_local_variables()
        # self.tdCommon.createDb(database, True, keep=36500)
        self.tdSql.execute('''use %s;'''%database)

        self.tdSql.execute('''create stable %s.stable_1 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);'''%(database))
        self.tdSql.execute('''create stable %s.stable_2 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);'''%(database))
        
        self.tdSql.execute('''create stable %s.stable_null_data (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);'''%(database))

        self.tdSql.execute('''create stable %s.stable_null_childtable (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);'''%(database))
        
        #self.tdSql.execute('''create table stable_1_1 using stable_1 tags('stable_1_1', '0' , '0' , '0' , '0' , 0 , 'binary1' , 'nchar1' , '0' , '0' ,'0') ;''')
        self.tdSql.execute('''create table %s.stable_1_1 using %s.stable_1 tags('stable_1_1', '%d' , '%d', '%d' , '%d' , 0 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'0') ;''' 
                      %(database,database,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat())) 
        #self.tdSql.execute('''create table stable_1_2 using stable_1 tags('stable_1_2', '2147483647' , '9223372036854775807' , '32767' , '127' , 1 , 'binary2' , 'nchar2' , '2' , '22' , \'1999-09-09 09:09:09.090\') ;''')
        self.tdSql.execute('''create table %s.stable_1_2 using %s.stable_1 tags('stable_1_2', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' , \'1999-09-09 09:09:09.090\') ;''' 
                      %(database,database,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat())) 
        self.tdSql.execute('''create table %s.stable_1_3 using %s.stable_1 tags('stable_1_3', '-2147483647' , '-9223372036854775807' , '-32767' , '-127' , false , 'binary3' , 'nchar3nchar3' , '-3.3' , '-33.33' , \'2099-09-09 09:09:09.090\') ;'''%(database,database))
        self.tdSql.execute('''create table %s.stable_1_4 using %s.stable_1 tags('stable_1_4', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0') ;'''%(database,database))
        self.tdSql.execute('''create table %s.stable_1_5 using %s.stable_1 tags('stable_1_5', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(database,database,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 
        self.tdSql.execute('''create table %s.stable_1_6 using %s.stable_1 tags('stable_1_6', '%d' , '%d', '%d' , '%d' , 1 , 'binary1.%s' , 'nchar1.%s' , '%f', '%f' ,'%d') ;''' 
                      %(database,database,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 

        #self.tdSql.execute('''create table stable_2_1 using stable_2 tags('stable_2_1' , '0' , '0' , '0' , '0' , 0 , 'binary21' , 'nchar21' , '0' , '0' ,'0') ;''')
        self.tdSql.execute('''create table %s.stable_2_1 using %s.stable_2 tags('stable_2_1', '%d' , '%d', '%d' , '%d' , 1 , 'binary2.%s' , 'nchar2.%s' , '%f', '%f' ,'0') ;''' 
                      %(database,database,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat())) 
        self.tdSql.execute('''create table %s.stable_2_2 using %s.stable_2 tags('stable_2_2' , '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0') ;'''%(database,database))
        self.tdSql.execute('''create table %s.stable_2_3 using %s.stable_2 tags('stable_2_3', '%d' , '%d', '%d' , '%d' , 1 , 'binary2.%s' , 'nchar2.%s' , '%f', '%f' ,'%d') ;''' 
                      %(database,database,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 
        self.tdSql.execute('''create table %s.stable_2_4 using %s.stable_2 tags('stable_2_4', '%d' , '%d', '%d' , '%d' , 1 , 'binary2.%s' , 'nchar2.%s' , '%f', '%f' ,'%d') ;''' 
                      %(database,database,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 
        self.tdSql.execute('''create table %s.stable_2_5 using %s.stable_2 tags('stable_2_5', '%d' , '%d', '%d' , '%d' , 1 , 'binary2.%s' , 'nchar2.%s' , '%f', '%f' ,'%d') ;''' 
                      %(database,database,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 
        self.tdSql.execute('''create table %s.stable_2_6 using %s.stable_2 tags('stable_2_6', '%d' , '%d', '%d' , '%d' , 1 , 'binary2.%s' , 'nchar2.%s' , '%f', '%f' ,'%d') ;''' 
                      %(database,database,fake.random_int(min=-2147483647, max=2147483647, step=1), fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pystr() ,fake.pystr() ,fake.pyfloat(),fake.pyfloat(),fake.random_int(min=-2147483647, max=2147483647, step=1))) 

        self.tdSql.execute('''create table %s.stable_null_data_1 using %s.stable_null_data tags('stable_null_data_1', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0') ;'''%(database,database))

        #regular table
        self.tdSql.execute('''create table %s.regular_table_1 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;'''%database)
        self.tdSql.execute('''create table %s.regular_table_2 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;'''%database)
        self.tdSql.execute('''create table %s.regular_table_3 \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;'''%database)

        self.tdSql.execute('''create table %s.regular_table_null \
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;'''%database)


        for i in range(self.num_random*n):        
            self.tdSql.execute('''insert into %s.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (database,self.ts + i*15000000, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))
            self.tdSql.execute('''insert into  %s.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (database,self.ts + i*15000000, fake.random_int(min=-2147483647, max=2147483647, step=1) , 
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1) , 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))

            self.tdSql.execute('''insert into %s.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (database,self.ts + i*15000000 -1, fake.random_int(min=0, max=2147483647, step=1), 
                        fake.random_int(min=0, max=9223372036854775807, step=1), 
                        fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))
            self.tdSql.execute('''insert into %s.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (database,self.ts + i*15000000, fake.random_int(min=0, max=2147483647, step=1), 
                        fake.random_int(min=0, max=9223372036854775807, step=1), 
                        fake.random_int(min=0, max=32767, step=1) , fake.random_int(min=0, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))
            
            self.tdSql.execute('''insert into %s.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (database,self.ts + i*15000000 +1, fake.random_int(min=-2147483647, max=0, step=1), 
                        fake.random_int(min=-9223372036854775807, max=0, step=1), 
                        fake.random_int(min=-32767, max=0, step=1) , fake.random_int(min=-127, max=0, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i +1))
            self.tdSql.execute('''insert into %s.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (database,self.ts + i*15000000 +1, fake.random_int(min=-2147483647, max=0, step=1), 
                        fake.random_int(min=-9223372036854775807, max=0, step=1), 
                        fake.random_int(min=-32767, max=0, step=1) , fake.random_int(min=-127, max=0, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i +1))

            self.tdSql.execute('''insert into %s.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 0, 'binary.%s', 'nchar.%s', %d) ;''' 
                        % (database,self.ts + i*15000000, fake.random_int(min=-2147483647, max=2147483647, step=1), 
                        fake.random_int(min=-9223372036854775807, max=9223372036854775807, step=1), 
                        fake.random_int(min=-32767, max=32767, step=1) , fake.random_int(min=-127, max=127, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))

            self.tdSql.execute('''insert into %s.regular_table_3 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(%d, %d, %d, %d, %d, %f, %f, 1, 'binary.%s', 'nchar.%s', %d)''' 
                        % (database,self.ts + i*1000, fake.random_int(min=-2147483647, max=0, step=1), 
                        fake.random_int(min=-9223372036854775807, max=0, step=1), 
                        fake.random_int(min=-32767, max=0, step=1) , fake.random_int(min=-127, max=0, step=1) , 
                        fake.pyfloat() , fake.pyfloat() , fake.pystr() , fake.address() , self.ts + i))

        i = random.randint(0,1)
        if i ==0:
            self.logger.info("======this case test use flush database =========")
            self.tdSql.execute("flush database %s;" %database)       
        elif i ==1:
            self.logger.info("===!!!===this case test not use flush database =====!!!====")
        
        self.tdSql.query("select count(*) from %s.stable_1;"%database)
        self.tdSql.checkData(0,0,3*self.num_random*n)
        self.tdSql.query("select count(*) from %s.regular_table_1;"%database)
        self.tdSql.checkData(0,0,self.num_random*n)

    def drop_db_common(self,dbname,replica): 
        # #每个库的通用检查
        self.dropandcreateDB_random(dbname,random.randint(1,20),replica)
        self.sql_select(dbname)
        
        self.tdSql.execute("flush database %s;" %dbname) 
        
        self.drop_db(dbname)
        
        self.tdSql.error("flush database %s;" %dbname) 
        self.tdSql.error("select * from %s.stable_1;" %dbname)

    def sql_select(self,dbname): 
        self.tdSql.query("select count(*) from %s.stable_1;"%dbname)
        self.tdSql.query("select count(*) from %s.regular_table_1;"%dbname)
        
        self.tdSql.query("select * from %s.stable_1;"%dbname)
        self.tdSql.query("select * from %s.regular_table_1;"%dbname)
        
        self.tdSql.query("select first(*) from %s.stable_1;"%dbname)
        self.tdSql.query("select first(*) from %s.regular_table_1;"%dbname)
        
        self.tdSql.query("select last(*) from %s.stable_1;"%dbname)
        self.tdSql.query("select last(*) from %s.regular_table_1;"%dbname)
        
        self.tdSql.query("select last_row(*) from %s.stable_1;"%dbname)
        self.tdSql.query("select last_row(*) from %s.regular_table_1;"%dbname)
        
        self.tdSql.query("select sum(q_int) from %s.stable_1;"%dbname)
        self.tdSql.query("select sum(q_int) from %s.regular_table_1;"%dbname)
        
        self.tdSql.query("select max(q_bigint) from %s.stable_1;"%dbname)
        self.tdSql.query("select max(q_bigint) from %s.regular_table_1;"%dbname)
        
        self.tdSql.query("select * from %s.stable_1 order by ts;"%dbname)
        self.tdSql.query("select * from %s.regular_table_1 order by ts;"%dbname)
        
        self.tdSql.query("select * from %s.stable_1  order by ts desc;"%dbname)
        self.tdSql.query("select * from %s.regular_table_1 order by ts desc"%dbname)
        
        self.tdSql.query("select * from %s.stable_1 order by ts limit 10000 offset 10000;"%dbname)
        self.tdSql.query("select * from %s.regular_table_1 order by ts limit 10000 offset 10000;"%dbname)
        
        self.tdSql.query("select * from %s.stable_1  order by ts desc limit 10000 offset 10000;"%dbname)
        self.tdSql.query("select * from %s.regular_table_1 order by ts desc limit 10000 offset 10000"%dbname)
        
        self.tdSql.query("select top(q_double,100) from %s.stable_1;"%dbname)
        self.tdSql.query("select top(q_double,100) from %s.regular_table_1;"%dbname)
        
        self.tdSql.query("select bottom(q_float,100) from %s.stable_1;"%dbname)
        self.tdSql.query("select bottom(q_float,100) from %s.regular_table_1;"%dbname)
        
    def countdb_10w_table100_row1000(self,replica,func):
        dbname = 'db1_10w'
        if func == 'drop':
            self.drop_db_common(dbname,replica)
        elif func == 'count':
            self.count_db_common(dbname,replica)                   

    def countdb_10w_table1w_row10(self,replica,func):
        dbname = 'db1_10w'
        if func == 'drop':
            self.drop_db_common(dbname,replica)
        elif func == 'count':
            self.count_db_common(dbname,replica)   

    def countdb_20w_table1w_row20(self,replica,func):
        dbname = 'db1_20w'
        if func == 'drop':
            self.drop_db_common(dbname,replica)
        elif func == 'count':
            self.count_db_common(dbname,replica)    

    def countdb_40w_table1w_row40(self,replica,func):
        dbname = 'db1_40w'
        if func == 'drop':
            self.drop_db_common(dbname,replica)
        elif func == 'count':
            self.count_db_common(dbname,replica)  

    def countdb_80w_table1w_row80(self,replica,func):
        dbname = 'db1_80w'
        if func == 'drop':
            self.drop_db_common(dbname,replica)
        elif func == 'count':
            self.count_db_common(dbname,replica)                    

    def countdb_100w_table1w_row100(self,replica,func):
        dbname = 'db1_100w'
        if func == 'drop':
            self.drop_db_common(dbname,replica)
        elif func == 'count':
            self.count_db_common(dbname,replica)           

    def countdb_200w_table1w_row200(self,replica,func):
        dbname = 'db1_200w'
        if func == 'drop':
            self.drop_db_common(dbname,replica)
        elif func == 'count':
            self.count_db_common(dbname,replica)   

    def countdb_400w_table1w_row400(self,replica,func):
        dbname = 'db1_400w'
        if func == 'drop':
            self.drop_db_common(dbname,replica)
        elif func == 'count':
            self.count_db_common(dbname,replica)   

    def countdb_800w_table1w_row800(self,replica,func):
        dbname = 'db1_800w'
        if func == 'drop':
            self.drop_db_common(dbname,replica)
        elif func == 'count':
            self.count_db_common(dbname,replica)                 

    def countdb_1000w_table1w_row1000(self,replica,func):
        dbname = 'db1_1000w'
        if func == 'drop':
            self.drop_db_common(dbname,replica)
        elif func == 'count':
            self.count_db_common(dbname,replica)   

    def countdb_2000w_table1w_row2000(self,replica,func):
        dbname = 'db1_2000w'
        if func == 'drop':
            self.drop_db_common(dbname,replica)
        elif func == 'count':
            self.count_db_common(dbname,replica)    

    def countdb_4000w_table1w_row4000(self,replica,func):
        dbname = 'db1_4000w'
        if func == 'drop':
            self.drop_db_common(dbname,replica)
        elif func == 'count':
            self.count_db_common(dbname,replica)  

    def countdb_8000w_table1w_row8000(self,replica,func):
        dbname = 'db1_8000w'
        if func == 'drop':
            self.drop_db_common(dbname,replica)
        elif func == 'count':
            self.count_db_common(dbname,replica)    

    def countdb_10000w_table1w_row1w(self,replica,func):
        dbname = 'db1_10000w'
        if func == 'drop':
            self.drop_db_common(dbname,replica)
        elif func == 'count':
            self.count_db_common(dbname,replica) 
                      
    def db_10w(self):        
        self.countdb_10w_table1w_row10(replica=1,func='drop')
        self.countdb_10w_table1w_row10(replica=3,func='drop')
        self.countdb_10w_table1w_row10(replica=1,func='drop')
        self.countdb_10w_table1w_row10(replica=3,func='drop')
        
    def db_20w(self):  
        self.countdb_20w_table1w_row20(replica=1,func='drop')
        self.countdb_20w_table1w_row20(replica=1,func='drop')
        self.countdb_20w_table1w_row20(replica=3,func='drop')
        self.countdb_20w_table1w_row20(replica=3,func='drop')
        
    def db_40w(self):  
        self.countdb_40w_table1w_row40(replica=1,func='drop')
        self.countdb_40w_table1w_row40(replica=3,func='drop')
        self.countdb_40w_table1w_row40(replica=1,func='drop')
        self.countdb_40w_table1w_row40(replica=3,func='drop')
            
    def db_80w(self):  
        self.countdb_80w_table1w_row80(replica=1,func='drop')
        self.countdb_80w_table1w_row80(replica=1,func='drop')
        self.countdb_80w_table1w_row80(replica=3,func='drop')
        self.countdb_80w_table1w_row80(replica=3,func='drop')
    
    def db_100w(self):        
        self.countdb_100w_table1w_row100(replica=1,func='drop')
        self.countdb_100w_table1w_row100(replica=3,func='drop')
        self.countdb_100w_table1w_row100(replica=1,func='drop')
        self.countdb_100w_table1w_row100(replica=3,func='drop')
        
    def db_200w(self):  
        self.countdb_200w_table1w_row200(replica=1,func='drop')
        self.countdb_200w_table1w_row200(replica=1,func='drop')
        self.countdb_200w_table1w_row200(replica=3,func='drop')
        self.countdb_200w_table1w_row200(replica=3,func='drop')
        
    def db_400w(self):  
        self.countdb_400w_table1w_row400(replica=1,func='drop')
        self.countdb_400w_table1w_row400(replica=3,func='drop')
        self.countdb_400w_table1w_row400(replica=1,func='drop')
        self.countdb_400w_table1w_row400(replica=3,func='drop')
            
    def db_800w(self):  
        self.countdb_800w_table1w_row800(replica=1,func='drop')
        self.countdb_800w_table1w_row800(replica=1,func='drop')
        self.countdb_800w_table1w_row800(replica=3,func='drop')
        self.countdb_800w_table1w_row800(replica=3,func='drop')
    
    def db_1000w(self):        
        self.countdb_1000w_table1w_row1000(replica=1,func='drop')
        self.countdb_1000w_table1w_row1000(replica=3,func='drop')
        self.countdb_1000w_table1w_row1000(replica=1,func='drop')
        self.countdb_1000w_table1w_row1000(replica=3,func='drop')
        
    def db_2000w(self):  
        self.countdb_2000w_table1w_row2000(replica=1,func='drop')
        self.countdb_2000w_table1w_row2000(replica=1,func='drop')
        self.countdb_2000w_table1w_row2000(replica=3,func='drop')
        self.countdb_2000w_table1w_row2000(replica=3,func='drop')
        
    def db_4000w(self):  
        self.countdb_4000w_table1w_row4000(replica=1,func='drop')
        self.countdb_4000w_table1w_row4000(replica=3,func='drop')
        self.countdb_4000w_table1w_row4000(replica=1,func='drop')
        self.countdb_4000w_table1w_row4000(replica=3,func='drop')
            
    def db_8000w(self):  
        self.countdb_8000w_table1w_row8000(replica=1,func='drop')
        self.countdb_8000w_table1w_row8000(replica=1,func='drop')
        self.countdb_8000w_table1w_row8000(replica=3,func='drop')
        self.countdb_8000w_table1w_row8000(replica=3,func='drop')
            
    def db_10000w(self):  
        self.countdb_10000w_table1w_row1w(replica=1,func='drop')
        self.countdb_10000w_table1w_row1w(replica=3,func='drop')
        self.countdb_10000w_table1w_row1w(replica=1,func='drop')
        self.countdb_10000w_table1w_row1w(replica=1,func='drop')
        self.countdb_10000w_table1w_row1w(replica=3,func='drop')
        self.countdb_10000w_table1w_row1w(replica=3,func='drop')
                                                                    
    def run(self):
        startTime = time.time() 
        
        while(1):     
            t1 = threading.Thread(target=self.db_10w) 
            t2 = threading.Thread(target=self.db_20w) 
            t3 = threading.Thread(target=self.db_40w) 
            t4 = threading.Thread(target=self.db_80w) 
            t5 = threading.Thread(target=self.db_100w) 
            t6 = threading.Thread(target=self.db_200w) 
            t7 = threading.Thread(target=self.db_400w) 
            t8 = threading.Thread(target=self.db_800w) 
            t9 = threading.Thread(target=self.db_1000w)
            t10 = threading.Thread(target=self.db_2000w) 
            t11 = threading.Thread(target=self.db_4000w) 
            t12 = threading.Thread(target=self.db_8000w) 
            t13 = threading.Thread(target=self.db_10000w) 
            
            t1.start() 
            t2.start() 
            t3.start()  
            t4.start() 
            t5.start() 
            t6.start()
            t7.start() 
            t8.start() 
            t9.start()
            t10.start() 
            t11.start() 
            t12.start() 
            t13.start() 
            
            
            t1.join()
            t2.join()
            t3.join()
            t4.join()
            t5.join()
            t6.join()
            t7.join()
            t8.join()
            t9.join()
            t10.join()
            t11.join()
            t12.join()
            t13.join()
            
        endTime = time.time()
        
    
        self.logger.info("total time %ds" % (endTime - startTime))
    

