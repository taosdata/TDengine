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

from new_test_framework.utils import tdLog, tdSql, etool, sc
import os
import time
import subprocess
from faker import Faker

class TestTd12478:
    def caseDescription(self):
        '''
        case1<xyguo>[TD-12434]:taosdump null nchar/binary length can cause core:taos-tools/src/taosdump.c 
        case2<xyguo>[TD-12478]:taos_stmt_execute() failed! reason: WAL size exceeds limit 
        ''' 
        return
    
    def restartDnodes(self):
        sc.dnodeStop(1)
        sc.dnodeStart(1)

    def dropandcreateDB_random(self,n):
        self.ts = 1630000000000
        
        fake = Faker('zh_CN')
        self.num_random = fake.random_int(min=1000, max=5000, step=1)
        print(self.num_random)
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
            tdSql.checkData(0,0,3*self.num_random)
            tdSql.query("select count(*) from regular_table_1;")
            tdSql.checkData(0,0,self.num_random)

    def test_TD_12478(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """
        tdSql.prepare()

        dcDB = self.dropandcreateDB_random(1)
       
        assert os.system("taosdump -D db") == 0

        assert os.system("taosdump -i . -g") == 0

        tdSql.query("select count(*) from stable_1;")
        tdSql.checkData(0,0,3*self.num_random)
        tdSql.query("select count(*) from regular_table_1;")
        tdSql.checkData(0,0,self.num_random)
        tdSql.query("select count(*) from regular_table_2;")
        tdSql.checkData(0,0,self.num_random)
        tdSql.query("select count(*) from regular_table_3;")
        tdSql.checkData(0,0,self.num_random)

        

        tdLog.success("%s successfully executed" % __file__)


