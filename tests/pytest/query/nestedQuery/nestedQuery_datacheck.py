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
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
from util.dnodes import tdDnodes
from util.dnodes import *

class TDTestCase:
    updatecfgDict={'maxSQLLength':1048576}

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        os.system("rm -rf query/nestedQuery/nestedQuery_datacheck.py.sql")
        now = time.time()
        self.ts = 1630000000000
        self.num = 100
        self.fornum = 3

    def get_random_string(self, length):
        letters = string.ascii_lowercase
        result_str = ''.join(random.choice(letters) for i in range(length))
        return result_str
    
    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root)-len("/build/bin")]
                    break
        return buildPath
    
    def restartDnodes(self):
        tdDnodes.stop(1)
        tdDnodes.start(1)

    def dropandcreateDB(self,n):
        for i in range(n):
            tdSql.execute('''drop database if exists db ;''')
            tdSql.execute('''create database db keep 36500;''')
            tdSql.execute('''use db;''')

            tdSql.execute('''create stable stable_1
                        (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint, 
                        q_bool bool , q_binary binary(20) , q_nchar nchar(20) ,q_float float , q_double double , q_ts timestamp) 
                        tags(loc nchar(20) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, 
                        t_bool bool , t_binary binary(20) , t_nchar nchar(20) ,t_float float , t_double double , t_ts timestamp);''')
            tdSql.execute('''create stable stable_2
                        (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint, 
                        q_bool bool , q_binary binary(20) , q_nchar nchar(20) ,q_float float , q_double double , q_ts timestamp) 
                        tags(loc nchar(20) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, 
                        t_bool bool , t_binary binary(20) , t_nchar nchar(20) ,t_float float , t_double double , t_ts timestamp);''')


            tdSql.execute('''create table table_0 using stable_1 
                        tags('table_0' , '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0')''')
            tdSql.execute('''create table table_1 using stable_1 
                        tags('table_1' , '2147483647' , '9223372036854775807' , '32767' , '127' , 1 , 
                        'binary1' , 'nchar1' , '1' , '11' , \'1999-09-09 09:09:09.090\')''')
            tdSql.execute('''create table table_2 using stable_1 
                        tags('table_2' , '-2147483647' , '-9223372036854775807' , '-32767' , '-127' , false , 
                        'binary2' , 'nchar2nchar2' , '-2.2' , '-22.22' , \'2099-09-09 09:09:09.090\')''')
            tdSql.execute('''create table table_21 using stable_2 
                        tags('table_21' , '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0')''')

            #regular table
            tdSql.execute('''create table regular_table_1
                        (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint, 
                        q_bool bool , q_binary binary(20) , q_nchar nchar(20) ,q_float float , q_double double , q_ts timestamp) ;''')
            tdSql.execute('''create table regular_table_2
                        (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint, 
                        q_bool bool , q_binary binary(20) , q_nchar nchar(20) ,q_float float , q_double double , q_ts timestamp) ;''')


            for i in range(self.num):        
                tdSql.execute('''insert into table_0 values(%d, %d, %d, %d, %d, 0, 'binary.%s', 'nchar.%s', %f, %f, %d)''' 
                                % (self.ts + i*10000000, i, i, i, i, i, i, i, i, self.ts + i))
                tdSql.execute('''insert into table_1 values(%d, %d, %d, %d, %d, 1, 'binary1.%s', 'nchar1.%s', %f, %f, %d)''' 
                                % (self.ts + i*10000000+1, 2147483647-i, 9223372036854775807-i, 32767-i, 127-i, 
                                i, i, i, i, 1262304000001 + i))
                tdSql.execute('''insert into table_2 values(%d, %d, %d, %d, %d, true, 'binary2.%s', 'nchar2nchar2.%s', %f, %f, %d)''' 
                                % (self.ts + i*10000000+2, -2147483647+i, -9223372036854775807+i, -32767+i, -127+i, 
                                i, i, -i, -i, 1577836800001 + i))
               
                tdSql.execute('''insert into table_21 values(%d, %d, %d, %d, %d, 0, 'binary.%s', 'nchar.%s', %f, %f, %d)''' 
                                % (self.ts + i*10000000, i, i, i, i, i, i, i, i, self.ts + i))

                tdSql.execute('''insert into regular_table_1 values(%d, %d, %d, %d, %d, 0, 'binary.%s', 'nchar.%s', %f, %f, %d)''' 
                                % (self.ts + i*10000000, i, i, i, i, i, i, i, i, self.ts + i))

                tdSql.execute('''insert into regular_table_2 values(%d, %d, %d, %d, %d, 1, 'binary1.%s', 'nchar1.%s', %f, %f, %d)''' 
                                % (self.ts + i*10000000 , 2147483647-i, 9223372036854775807-i, 32767-i, 127-i, 
                                i, i, i, i, 1262304000001 + i))
                tdSql.execute('''insert into regular_table_2 values(%d, %d, %d, %d, %d, true, 'binary2.%s', 'nchar2nchar2.%s', %f, %f, %d)''' 
                                % (self.ts + i*10000000 +2, -2147483647+i, -9223372036854775807+i, -32767+i, -127+i, 
                                i, i, -i, -i, 1577836800001 + i))
               
    def regular1_checkall_0(self,sql):
        tdLog.info(sql)       
        tdSql.query(sql)
        tdSql.checkData(0,0,'2021-08-27 01:46:40.000')
        tdSql.checkData(0,1,0)
        tdSql.checkData(0,2,0)
        tdSql.checkData(0,3,0)
        tdSql.checkData(0,4,0)
        tdSql.checkData(0,5,'False')
        tdSql.checkData(0,6,'binary.0')
        tdSql.checkData(0,7,'nchar.0')
        tdSql.checkData(0,8,0)
        tdSql.checkData(0,9,0)
        tdSql.checkData(0,10,'2021-08-27 01:46:40.000')

    def regular1_checkall_100(self,sql):
        tdLog.info(sql)      
        tdSql.query(sql)
        tdSql.checkData(99,0,'2021-09-07 12:46:40.000')
        tdSql.checkData(99,1,99)
        tdSql.checkData(99,2,99)
        tdSql.checkData(99,3,99)
        tdSql.checkData(99,4,99)
        tdSql.checkData(99,5,'False')
        tdSql.checkData(99,6,'binary.99')
        tdSql.checkData(99,7,'nchar.99')
        tdSql.checkData(99,8,99)
        tdSql.checkData(99,9,99)
        tdSql.checkData(99,10,'2021-08-27 01:46:40.099')

    def regular_join_checkall_0(self,sql):
        self.regular1_checkall_0(sql)
        tdSql.checkData(0,11,'2021-08-27 01:46:40.000')
        tdSql.checkData(0,12,2147483647)
        tdSql.checkData(0,13,9223372036854775807)
        tdSql.checkData(0,14,32767)
        tdSql.checkData(0,15,127)
        tdSql.checkData(0,16,'True')
        tdSql.checkData(0,17,'binary1.0')
        tdSql.checkData(0,18,'nchar1.0')
        tdSql.checkData(0,19,0)
        tdSql.checkData(0,20,0)
        tdSql.checkData(0,21,'2010-01-01 08:00:00.001')

    def regular_join_checkall_100(self,sql):
        self.regular1_checkall_100(sql)
        tdSql.checkData(99,11,'2021-09-07 12:46:40.000')
        tdSql.checkData(99,12,2147483548)
        tdSql.checkData(99,13,9223372036854775708)
        tdSql.checkData(99,14,32668)
        tdSql.checkData(99,15,28)
        tdSql.checkData(99,16,'True')
        tdSql.checkData(99,17,'binary1.99')
        tdSql.checkData(99,18,'nchar1.99')
        tdSql.checkData(99,19,99)
        tdSql.checkData(99,20,99)
        tdSql.checkData(99,21,'2010-01-01 08:00:00.100')

    def stable1_checkall_0(self,sql):
        self.regular1_checkall_0(sql)

    def stable1_checkall_300(self,sql):
        tdLog.info(sql)       
        tdSql.query(sql)
        tdSql.checkData(299,0,'2021-09-07 12:46:40.002')
        tdSql.checkData(299,1,-2147483548)
        tdSql.checkData(299,2,-9223372036854775708)
        tdSql.checkData(299,3,-32668)
        tdSql.checkData(299,4,-28)
        tdSql.checkData(299,5,'True')
        tdSql.checkData(299,6,'binary2.99')
        tdSql.checkData(299,7,'nchar2nchar2.99')
        tdSql.checkData(299,8,-99)
        tdSql.checkData(299,9,-99)
        tdSql.checkData(299,10,'2010-01-01 08:00:00.100')

    def stable_join_checkall_0(self,sql):
        self.regular1_checkall_0(sql)
        tdSql.checkData(0,22,'2021-08-27 01:46:40.000')
        tdSql.checkData(0,23,0)
        tdSql.checkData(0,24,0)
        tdSql.checkData(0,25,0)
        tdSql.checkData(0,26,0)
        tdSql.checkData(0,27,'False')
        tdSql.checkData(0,28,'binary.0')
        tdSql.checkData(0,29,'nchar.0')
        tdSql.checkData(0,30,0)
        tdSql.checkData(0,31,0)
        tdSql.checkData(0,32,'2021-08-27 01:46:40.000')

    def stable_join_checkall_100(self,sql):
        tdSql.checkData(99,0,'2021-09-07 12:46:40.000')
        tdSql.checkData(99,1,99)
        tdSql.checkData(99,2,99)
        tdSql.checkData(99,3,99)
        tdSql.checkData(99,4,99)
        tdSql.checkData(99,5,'False')
        tdSql.checkData(99,6,'binary.99')
        tdSql.checkData(99,7,'nchar.99')
        tdSql.checkData(99,8,99)
        tdSql.checkData(99,9,99)
        tdSql.checkData(99,10,'2021-08-27 01:46:40.099')
        tdSql.checkData(99,22,'2021-09-07 12:46:40.000')
        tdSql.checkData(99,23,99)
        tdSql.checkData(99,24,99)
        tdSql.checkData(99,25,99)
        tdSql.checkData(99,26,99)
        tdSql.checkData(99,27,'False')
        tdSql.checkData(99,28,'binary.99')
        tdSql.checkData(99,29,'nchar.99')
        tdSql.checkData(99,30,99)
        tdSql.checkData(99,31,99)
        tdSql.checkData(99,32,'2021-08-27 01:46:40.099')


    def run(self):
        tdSql.prepare()
        # test case for https://jira.taosdata.com:18080/browse/TD-5665
        os.system("rm -rf nestedQuery.py.sql")
        startTime = time.time() 

        dcDB = self.dropandcreateDB(1)
    
        # regular column select
        q_select= ['ts' , '*' , 'q_int', 'q_bigint' , 'q_bigint' , 'q_smallint' , 'q_tinyint' , 'q_bool' , 'q_binary' , 'q_nchar' ,'q_float' , 'q_double' ,'q_ts ']

        # tag column select
        t_select= ['*' , 'loc' ,'t_int', 't_bigint' , 't_bigint' , 't_smallint' , 't_tinyint' , 't_bool' , 't_binary' , 't_nchar' ,'t_float' , 't_double' ,'t_ts ']

        # regular and tag column select
        qt_select=  q_select + t_select

        # distinct regular column select
        dq_select= ['distinct q_int', 'distinct q_bigint' , 'distinct q_smallint' , 'distinct q_tinyint' , 
                'distinct q_bool' , 'distinct q_binary' , 'distinct q_nchar' ,'distinct q_float' , 'distinct q_double' ,'distinct q_ts ']

        # distinct tag column select
        dt_select= ['distinct loc', 'distinct t_int', 'distinct t_bigint'  , 'distinct t_smallint' , 'distinct t_tinyint' , 
                'distinct t_bool' , 'distinct t_binary' , 'distinct t_nchar' ,'distinct t_float' , 'distinct t_double' ,'distinct t_ts ']

        # distinct regular and tag column select
        dqt_select= dq_select + dt_select

        # special column select
        s_r_select= ['_c0', '_C0' ]
        s_s_select= ['tbname' , '_c0', '_C0' ]

        # regular column where 
        q_where = ['ts < now +1s','q_bigint >= -9223372036854775807 and q_bigint <= 9223372036854775807', 'q_int <= 2147483647 and q_int >= -2147483647',
        'q_smallint >= -32767 and q_smallint <= 32767','q_tinyint >= -127 and q_tinyint <= 127','q_float >= -100000 and q_float <= 100000', 
        'q_double >= -1000000000 and q_double <= 1000000000', 'q_binary like \'binary%\'  or q_binary = \'0\' ' , 'q_nchar like \'nchar%\' or q_nchar = \'0\' ' , 
        'q_bool = true or  q_bool = false' , 'q_bool in (0 , 1)' , 'q_bool in ( true , false)' , 'q_bool = 0 or q_bool = 1',
        'q_bigint between  -9223372036854775807 and 9223372036854775807',' q_int between -2147483647 and 2147483647','q_smallint between -32767 and 32767', 
        'q_tinyint between -127 and 127 ','q_float between -100000 and 100000','q_double between -1000000000 and 1000000000']
        #TD-6201 ,'q_bool between 0 and 1'

        # regular column where for test union,join
        q_u_where = ['t1.ts < now +1s' , 't2.ts < now +1s','t1.q_bigint >= -9223372036854775807 and t1.q_bigint <= 9223372036854775807 and t2.q_bigint >= -9223372036854775807 and t2.q_bigint <= 9223372036854775807',
        't1.q_int <= 2147483647 and t1.q_int >= -2147483647 and t2.q_int <= 2147483647 and t2.q_int >= -2147483647',
        't1.q_smallint >= -32767 and t1.q_smallint <= 32767 and t2.q_smallint >= -32767 and t2.q_smallint <= 32767',
        't1.q_tinyint >= -127 and t1.q_tinyint <= 127 and t2.q_tinyint >= -127 and t2.q_tinyint <= 127',
        't1.q_float >= -100000 and t1.q_float <= 100000 and t2.q_float >= -100000 and t2.q_float <= 100000',
        't1.q_double >= -1000000000 and t1.q_double <= 1000000000 and t2.q_double >= -1000000000 and t2.q_double <= 1000000000', 
        't1.q_binary like \'binary%\'  and t2.q_binary like \'binary%\'  ' , 
        't1.q_nchar like \'nchar%\' and t2.q_nchar like \'nchar%\' ' , 
        't1.q_bool in (0 , 1) and t2.q_bool in (0 , 1)' , 't1.q_bool in ( true , false) and t2.q_bool in ( true , false)' , 
        't1.q_bigint between  -9223372036854775807 and 9223372036854775807 and t2.q_bigint between  -9223372036854775807 and 9223372036854775807',
        't1.q_int between -2147483647 and 2147483647 and t2.q_int between -2147483647 and 2147483647',
        't1.q_smallint between -32767 and 32767 and t2.q_smallint between -32767 and 32767', 
        't1.q_tinyint between -127 and 127 and t2.q_tinyint between -127 and 127 ','t1.q_float between -100000 and 100000 and t2.q_float between -100000 and 100000',
        't1.q_double between -1000000000 and 1000000000 and t2.q_double between -1000000000 and 1000000000']
        #TD-6201 ,'t1.q_bool between 0 and 1 or t2.q_bool between 0 and 1']
        #'t1.q_bool = true and  t1.q_bool = false and t2.q_bool = true and  t2.q_bool = false' , 't1.q_bool = 0 and t1.q_bool = 1 and t2.q_bool = 0 and t2.q_bool = 1' ,

        q_u_or_where = ['t1.q_binary like \'binary%\'  or t1.q_binary = \'0\'  or t2.q_binary like \'binary%\'  or t2.q_binary = \'0\' ' , 
        't1.q_nchar like \'nchar%\' or t1.q_nchar = \'0\' or t2.q_nchar like \'nchar%\' or t2.q_nchar = \'0\' ' , 't1.q_bool = true or  t1.q_bool = false or t2.q_bool = true or  t2.q_bool = false' , 
        't1.q_bool in (0 , 1) or t2.q_bool in (0 , 1)' , 't1.q_bool in ( true , false) or t2.q_bool in ( true , false)' , 't1.q_bool = 0 or t1.q_bool = 1 or t2.q_bool = 0 or t2.q_bool = 1' ,
        't1.q_bigint between  -9223372036854775807 and 9223372036854775807 or t2.q_bigint between  -9223372036854775807 and 9223372036854775807',
        't1.q_int between -2147483647 and 2147483647 or t2.q_int between -2147483647 and 2147483647',
        't1.q_smallint between -32767 and 32767 or t2.q_smallint between -32767 and 32767', 
        't1.q_tinyint between -127 and 127 or t2.q_tinyint between -127 and 127 ','t1.q_float between -100000 and 100000 or t2.q_float between -100000 and 100000',
        't1.q_double between -1000000000 and 1000000000 or t2.q_double between -1000000000 and 1000000000']

        # tag column where
        t_where = ['ts < now +1s','t_bigint >= -9223372036854775807 and t_bigint <= 9223372036854775807','t_int <= 2147483647 and t_int >= -2147483647',
        't_smallint >= -32767 and t_smallint <= 32767','q_tinyint >= -127 and t_tinyint <= 127','t_float >= -100000 and t_float <= 100000',
        't_double >= -1000000000 and t_double <= 1000000000', 't_binary like \'binary%\'   or t_binary = \'0\' ' , 't_nchar like \'nchar%\' or t_nchar = \'0\'' , 
        't_bool = true or  t_bool = false' , 't_bool in (0 , 1)' , 't_bool in ( true , false)' , 't_bool = 0 or t_bool = 1',
        't_bigint between  -9223372036854775807 and 9223372036854775807',' t_int between -2147483647 and 2147483647','t_smallint between -32767 and 32767', 
        't_tinyint between -127 and 127 ','t_float between -100000 and 100000','t_double between -1000000000 and 1000000000']
        #TD-6201,'t_bool between 0 and 1'

        # tag column where for test  union,join | this is not support
        t_u_where = ['t1.ts < now +1s' , 't2.ts < now +1s','t1.t_bigint >= -9223372036854775807 and t1.t_bigint <= 9223372036854775807 and t2.t_bigint >= -9223372036854775807 and t2.t_bigint <= 9223372036854775807',
        't1.t_int <= 2147483647 and t1.t_int >= -2147483647 and t2.t_int <= 2147483647 and t2.t_int >= -2147483647',
        't1.t_smallint >= -32767 and t1.t_smallint <= 32767 and t2.t_smallint >= -32767 and t2.t_smallint <= 32767',
        't1.t_tinyint >= -127 and t1.t_tinyint <= 127 and t2.t_tinyint >= -127 and t2.t_tinyint <= 127',
        't1.t_float >= -100000 and t1.t_float <= 100000 and t2.t_float >= -100000 and t2.t_float <= 100000',
        't1.t_double >= -1000000000 and t1.t_double <= 1000000000 and t2.t_double >= -1000000000 and t2.t_double <= 1000000000', 
        't1.t_binary like \'binary%\'  or t1.t_binary = \'0\'  or t2.t_binary like \'binary%\'  or t2.t_binary = \'0\' ' , 
        't1.t_nchar like \'nchar%\' or t1.t_nchar = \'0\' or t2.t_nchar like \'nchar%\' or t2.t_nchar = \'0\' ' , 't1.t_bool = true or  t1.t_bool = false or t2.t_bool = true or  t2.t_bool = false' , 
        't1.t_bool in (0 , 1) and t2.t_bool in (0 , 1)' , 't1.t_bool in ( true , false) and t2.t_bool in ( true , false)' , 't1.t_bool = 0 or t1.t_bool = 1 or t2.t_bool = 0 or t2.t_bool = 1',
        't1.t_bigint between  -9223372036854775807 and 9223372036854775807 and t2.t_bigint between  -9223372036854775807 and 9223372036854775807',
        't1.t_int between -2147483647 and 2147483647 and t2.t_int between -2147483647 and 2147483647',
        't1.t_smallint between -32767 and 32767 and t2.t_smallint between -32767 and 32767', 
        't1.t_tinyint between -127 and 127 and t2.t_tinyint between -127 and 127 ','t1.t_float between -100000 and 100000 and t2.t_float between -100000 and 100000',
        't1.t_double between -1000000000 and 1000000000 and t2.t_double between -1000000000 and 1000000000']
        #TD-6201,'t1.t_bool between 0 and 1 or t2.q_bool between 0 and 1']

        t_u_or_where = ['t1.t_binary like \'binary%\'  or t1.t_binary = \'0\'  or t2.t_binary like \'binary%\'  or t2.t_binary = \'0\' ' , 
        't1.t_nchar like \'nchar%\' or t1.t_nchar = \'0\' or t2.t_nchar like \'nchar%\' or t2.t_nchar = \'0\' ' , 't1.t_bool = true or  t1.t_bool = false or t2.t_bool = true or  t2.t_bool = false' , 
        't1.t_bool in (0 , 1) or t2.t_bool in (0 , 1)' , 't1.t_bool in ( true , false) or t2.t_bool in ( true , false)' , 't1.t_bool = 0 or t1.t_bool = 1 or t2.t_bool = 0 or t2.t_bool = 1',
        't1.t_bigint between  -9223372036854775807 and 9223372036854775807 or t2.t_bigint between  -9223372036854775807 and 9223372036854775807',
        't1.t_int between -2147483647 and 2147483647 or t2.t_int between -2147483647 and 2147483647',
        't1.t_smallint between -32767 and 32767 or t2.t_smallint between -32767 and 32767', 
        't1.t_tinyint between -127 and 127 or t2.t_tinyint between -127 and 127 ','t1.t_float between -100000 and 100000 or t2.t_float between -100000 and 100000',
        't1.t_double between -1000000000 and 1000000000 or t2.t_double between -1000000000 and 1000000000']

        # regular and tag column where 
        qt_where = q_where + t_where
        qt_u_where = q_u_where + t_u_where
        # now,qt_u_or_where is not support
        qt_u_or_where = q_u_or_where + t_u_or_where

        # tag column where for test super join | this is  support  , 't1.t_bool = t2.t_bool ' ？？？
        t_join_where = ['t1.t_bigint = t2.t_bigint ', 't1.t_int = t2.t_int ', 't1.t_smallint = t2.t_smallint ', 't1.t_tinyint = t2.t_tinyint ',
                    't1.t_float = t2.t_float ', 't1.t_double = t2.t_double ', 't1.t_binary = t2.t_binary ' , 't1.t_nchar = t2.t_nchar  ' ]

        # session && fill
        session_where = ['session(ts,10a)' , 'session(ts,10s)', 'session(ts,10m)' , 'session(ts,10h)','session(ts,10d)' , 'session(ts,10w)']
        session_u_where = ['session(t1.ts,10a)' , 'session(t1.ts,10s)', 'session(t1.ts,10m)' , 'session(t1.ts,10h)','session(t1.ts,10d)' , 'session(t1.ts,10w)',
                    'session(t2.ts,10a)' , 'session(t2.ts,10s)', 'session(t2.ts,10m)' , 'session(t2.ts,10h)','session(t2.ts,10d)' , 'session(t2.ts,10w)']
        
        fill_where = ['FILL(NONE)','FILL(PREV)','FILL(NULL)','FILL(LINEAR)','FILL(NEXT)','FILL(VALUE, 1.23)']
        
        state_window = ['STATE_WINDOW(q_tinyint)','STATE_WINDOW(q_bigint)','STATE_WINDOW(q_int)','STATE_WINDOW(q_bool)','STATE_WINDOW(q_smallint)']
        state_u_window = ['STATE_WINDOW(t1.q_tinyint)','STATE_WINDOW(t1.q_bigint)','STATE_WINDOW(t1.q_int)','STATE_WINDOW(t1.q_bool)','STATE_WINDOW(t1.q_smallint)',
                    'STATE_WINDOW(t2.q_tinyint)','STATE_WINDOW(t2.q_bigint)','STATE_WINDOW(t2.q_int)','STATE_WINDOW(t2.q_bool)','STATE_WINDOW(t2.q_smallint)']

        # order by where 
        order_where = ['order by ts' , 'order by ts asc']
        order_u_where = ['order by t1.ts' , 'order by t1.ts asc' , 'order by t2.ts' , 'order by t2.ts asc']
        order_desc_where = ['order by ts' , 'order by ts asc' , 'order by ts desc' ]
        orders_desc_where = ['order by ts' , 'order by ts asc' , 'order by ts desc' , 'order by loc' , 'order by loc asc' , 'order by loc desc']

        # group by where,not include null-tag 
        group_where = ['group by tbname , loc' , 'group by tbname', 'group by tbname, t_bigint', 'group by tbname,t_int', 'group by tbname, t_smallint', 'group by tbname,t_tinyint', 
                    'group by tbname,t_float', 'group by tbname,t_double' , 'group by tbname,t_binary', 'group by tbname,t_nchar', 'group by tbname,t_bool' ,'group by tbname ,loc ,t_bigint',
                    'group by tbname,t_binary ,t_nchar ,t_bool' , 'group by tbname,t_int ,t_smallint ,t_tinyint' , 'group by tbname,t_float ,t_double ' ]
        having_support = ['having count(q_int) > 0','having count(q_bigint) > 0','having count(q_smallint) > 0','having count(q_tinyint) > 0','having count(q_float) > 0','having count(q_double) > 0','having count(q_bool) > 0',
                    'having avg(q_int) > 0','having avg(q_bigint) > 0','having avg(q_smallint) > 0','having avg(q_tinyint) > 0','having avg(q_float) > 0','having avg(q_double) > 0',
                    'having sum(q_int) > 0','having sum(q_bigint) > 0','having sum(q_smallint) > 0','having sum(q_tinyint) > 0','having sum(q_float) > 0','having sum(q_double) > 0',
                    'having STDDEV(q_int) > 0','having STDDEV(q_bigint) > 0','having STDDEV(q_smallint) > 0','having STDDEV(q_tinyint) > 0','having STDDEV(q_float) > 0','having STDDEV(q_double) > 0',
                    'having TWA(q_int) > 0','having  TWA(q_bigint) > 0','having  TWA(q_smallint) > 0','having  TWA(q_tinyint) > 0','having  TWA(q_float) > 0','having  TWA(q_double) > 0',
                    'having IRATE(q_int) > 0','having IRATE(q_bigint) > 0','having IRATE(q_smallint) > 0','having IRATE(q_tinyint) > 0','having IRATE(q_float) > 0','having IRATE(q_double) > 0',
                    'having MIN(q_int) > 0','having MIN(q_bigint) > 0','having MIN(q_smallint) > 0','having MIN(q_tinyint) > 0','having MIN(q_float) > 0','having MIN(q_double) > 0',
                    'having MAX(q_int) > 0','having MAX(q_bigint) > 0','having MAX(q_smallint) > 0','having MAX(q_tinyint) > 0','having MAX(q_float) > 0','having MAX(q_double) > 0',
                    'having FIRST(q_int) > 0','having FIRST(q_bigint) > 0','having FIRST(q_smallint) > 0','having FIRST(q_tinyint) > 0','having FIRST(q_float) > 0','having FIRST(q_double) > 0',
                    'having LAST(q_int) > 0','having LAST(q_bigint) > 0','having LAST(q_smallint) > 0','having LAST(q_tinyint) > 0','having LAST(q_float) > 0','having LAST(q_double) > 0',
                    'having APERCENTILE(q_int,10) > 0','having APERCENTILE(q_bigint,10) > 0','having APERCENTILE(q_smallint,10) > 0','having APERCENTILE(q_tinyint,10) > 0','having APERCENTILE(q_float,10) > 0','having APERCENTILE(q_double,10) > 0']
        having_not_support = ['having TOP(q_int,10) > 0','having TOP(q_bigint,10) > 0','having TOP(q_smallint,10) > 0','having TOP(q_tinyint,10) > 0','having TOP(q_float,10) > 0','having TOP(q_double,10) > 0','having TOP(q_bool,10) > 0',
                    'having BOTTOM(q_int,10) > 0','having BOTTOM(q_bigint,10) > 0','having BOTTOM(q_smallint,10) > 0','having BOTTOM(q_tinyint,10) > 0','having BOTTOM(q_float,10) > 0','having BOTTOM(q_double,10) > 0','having BOTTOM(q_bool,10) > 0',
                    'having LEASTSQUARES(q_int) > 0','having  LEASTSQUARES(q_bigint) > 0','having  LEASTSQUARES(q_smallint) > 0','having  LEASTSQUARES(q_tinyint) > 0','having  LEASTSQUARES(q_float) > 0','having  LEASTSQUARES(q_double) > 0','having  LEASTSQUARES(q_bool) > 0',
                    'having FIRST(q_bool) > 0','having IRATE(q_bool) > 0','having PERCENTILE(q_bool,10) > 0','having avg(q_bool) > 0','having LAST_ROW(q_bool) > 0','having sum(q_bool) > 0','having STDDEV(q_bool) > 0','having APERCENTILE(q_bool,10) > 0','having  TWA(q_bool) > 0','having LAST(q_bool) > 0',                    
                    'having PERCENTILE(q_int,10) > 0','having PERCENTILE(q_bigint,10) > 0','having PERCENTILE(q_smallint,10) > 0','having PERCENTILE(q_tinyint,10) > 0','having PERCENTILE(q_float,10) > 0','having PERCENTILE(q_double,10) > 0']
        having_tagnot_support = ['having LAST_ROW(q_int) > 0','having LAST_ROW(q_bigint) > 0','having LAST_ROW(q_smallint) > 0','having LAST_ROW(q_tinyint) > 0','having LAST_ROW(q_float) > 0','having LAST_ROW(q_double) > 0']    

        # limit offset where
        limit_where = ['limit 1 offset 1' , 'limit 1' , 'limit 2 offset 1' , 'limit 2', 'limit 12 offset 1' , 'limit 20', 'limit 20 offset 10' , 'limit 200']
        limit1_where = ['limit 1 offset 1' , 'limit 1' ]
        limit_u_where = ['limit 100 offset 10' , 'limit 50' , 'limit 100' , 'limit 10' ]

        # slimit soffset where
        slimit_where = ['slimit 1 soffset 1' , 'slimit 1' , 'slimit 2 soffset 1' , 'slimit 2']
        slimit1_where = ['slimit 2 soffset 1' , 'slimit 1' ]
        
        # aggregate function include [all:count(*)\avg\sum\stddev ||regualr:twa\irate\leastsquares ||group by tbname:twa\irate\]       
        # select function include [all: min\max\first(*)\last(*)\top\bottom\apercentile\last_row(*)(not with interval)\interp(*)(FILL) ||regualr: percentile]
        # calculation function include [all:spread\+-*/ ||regualr:diff\derivative ||group by tbname:diff\derivative\]
        # **_ns_**  express is not support stable, therefore, separated from regular tables
        # calc_select_all   calc_select_regular  calc_select_in_ts  calc_select_fill  calc_select_not_interval
        # calc_aggregate_all   calc_aggregate_regular   calc_aggregate_groupbytbname
        # calc_calculate_all   calc_calculate_regular   calc_calculate_groupbytbname

        # calc_select_all   calc_select_regular  calc_select_in_ts  calc_select_fill  calc_select_not_interval
        # select function include [all: min\max\first(*)\last(*)\top\bottom\apercentile\last_row(*)(not with interval)\interp(*)(FILL) ||regualr: percentile]
        
        calc_select_all = ['bottom(q_int,20)' , 'bottom(q_bigint,20)' , 'bottom(q_smallint,20)' , 'bottom(q_tinyint,20)' ,'bottom(q_float,20)' , 'bottom(q_double,20)' ,
                    'top(q_int,20)' , 'top(q_bigint,20)' , 'top(q_smallint,20)' ,'top(q_tinyint,20)' ,'top(q_float,20)' ,'top(q_double,20)' ,
                    'first(q_int)' , 'first(q_bigint)' , 'first(q_smallint)' , 'first(q_tinyint)' , 'first(q_float)' ,'first(q_double)' ,'first(q_binary)' ,'first(q_nchar)' ,'first(q_bool)' ,'first(q_ts)' ,
                    'last(q_int)' ,  'last(q_bigint)' , 'last(q_smallint)'  , 'last(q_tinyint)' , 'last(q_float)'  ,'last(q_double)' , 'last(q_binary)' ,'last(q_nchar)' ,'last(q_bool)' ,'last(q_ts)' ,
                    'min(q_int)' , 'min(q_bigint)' , 'min(q_smallint)' , 'min(q_tinyint)' , 'min(q_float)' ,'min(q_double)' ,
                    'max(q_int)' ,  'max(q_bigint)' , 'max(q_smallint)' , 'max(q_tinyint)' ,'max(q_float)' ,'max(q_double)' ,
                    'apercentile(q_int,20)' ,  'apercentile(q_bigint,20)'  ,'apercentile(q_smallint,20)'  ,'apercentile(q_tinyint,20)' ,'apercentile(q_float,20)'  ,'apercentile(q_double,20)' ,
                    'last_row(q_int)' ,  'last_row(q_bigint)' , 'last_row(q_smallint)' , 'last_row(q_tinyint)' , 'last_row(q_float)' , 
                    'last_row(q_double)' , 'last_row(q_bool)' ,'last_row(q_binary)' ,'last_row(q_nchar)' ,'last_row(q_ts)']

        calc_select_in_ts = ['bottom(q_int,20)' , 'bottom(q_bigint,20)' , 'bottom(q_smallint,20)' , 'bottom(q_tinyint,20)' ,'bottom(q_float,20)' , 'bottom(q_double,20)' ,
                    'top(q_int,20)' , 'top(q_bigint,20)' , 'top(q_smallint,20)' ,'top(q_tinyint,20)' ,'top(q_float,20)' ,'top(q_double,20)' ,
                    'first(q_int)' , 'first(q_bigint)' , 'first(q_smallint)' , 'first(q_tinyint)' , 'first(q_float)' ,'first(q_double)' ,'first(q_binary)' ,'first(q_nchar)' ,'first(q_bool)' ,'first(q_ts)' ,
                    'last(q_int)' ,  'last(q_bigint)' , 'last(q_smallint)'  , 'last(q_tinyint)' , 'last(q_float)'  ,'last(q_double)' , 'last(q_binary)' ,'last(q_nchar)' ,'last(q_bool)' ,'last(q_ts)' ]

        calc_select_in = ['min(q_int)' , 'min(q_bigint)' , 'min(q_smallint)' , 'min(q_tinyint)' , 'min(q_float)' ,'min(q_double)' ,
                    'max(q_int)' ,  'max(q_bigint)' , 'max(q_smallint)' , 'max(q_tinyint)' ,'max(q_float)' ,'max(q_double)' ,
                    'apercentile(q_int,20)' ,  'apercentile(q_bigint,20)'  ,'apercentile(q_smallint,20)'  ,'apercentile(q_tinyint,20)' ,'apercentile(q_float,20)'  ,'apercentile(q_double,20)' ,
                    'last_row(q_int)' ,  'last_row(q_bigint)' , 'last_row(q_smallint)' , 'last_row(q_tinyint)' , 'last_row(q_float)' , 
                    'last_row(q_double)' , 'last_row(q_bool)' ,'last_row(q_binary)' ,'last_row(q_nchar)' ,'last_row(q_ts)']
        

        calc_select_regular = [ 'PERCENTILE(q_int,10)' ,'PERCENTILE(q_bigint,20)' , 'PERCENTILE(q_smallint,30)' ,'PERCENTILE(q_tinyint,40)' ,'PERCENTILE(q_float,50)' ,'PERCENTILE(q_double,60)']

                    
        calc_select_fill = ['INTERP(q_bool)' ,'INTERP(q_binary)' ,'INTERP(q_nchar)' ,'INTERP(q_ts)', 'INTERP(q_int)' ,'INTERP(*)' ,'INTERP(q_bigint)' ,'INTERP(q_smallint)' ,'INTERP(q_tinyint)', 'INTERP(q_float)' ,'INTERP(q_double)']
        interp_where = ['ts = now' , 'ts = \'2020-09-13 20:26:40.000\'' , 'ts = \'2020-09-13 20:26:40.009\'' ,'tbname in (\'table_1\') and ts = now' ,'tbname in (\'table_0\' ,\'table_1\',\'table_2\',\'table_3\',\'table_4\',\'table_5\') and ts =  \'2020-09-13 20:26:40.000\'','tbname like \'table%\'  and ts =  \'2020-09-13 20:26:40.002\'']

        #two table join
        calc_select_in_ts_j = ['bottom(t1.q_int,20)' , 'bottom(t1.q_bigint,20)' , 'bottom(t1.q_smallint,20)' , 'bottom(t1.q_tinyint,20)' ,'bottom(t1.q_float,20)' , 'bottom(t1.q_double,20)' ,
                    'top(t1.q_int,20)' , 'top(t1.q_bigint,20)' , 'top(t1.q_smallint,20)' ,'top(t1.q_tinyint,20)' ,'top(t1.q_float,20)' ,'top(t1.q_double,20)' ,
                    'first(t1.q_int)' , 'first(t1.q_bigint)' , 'first(t1.q_smallint)' , 'first(t1.q_tinyint)' , 'first(t1.q_float)' ,'first(t1.q_double)' ,'first(t1.q_binary)' ,'first(t1.q_nchar)' ,'first(t1.q_bool)' ,'first(t1.q_ts)' ,
                    'last(t1.q_int)' ,  'last(t1.q_bigint)' , 'last(t1.q_smallint)'  , 'last(t1.q_tinyint)' , 'last(t1.q_float)'  ,'last(t1.q_double)' , 'last(t1.q_binary)' ,'last(t1.q_nchar)' ,'last(t1.q_bool)' ,'last(t1.q_ts)' ,
                    'bottom(t2.q_int,20)' , 'bottom(t2.q_bigint,20)' , 'bottom(t2.q_smallint,20)' , 'bottom(t2.q_tinyint,20)' ,'bottom(t2.q_float,20)' , 'bottom(t2.q_double,20)' ,
                    'top(t2.q_int,20)' , 'top(t2.q_bigint,20)' , 'top(t2.q_smallint,20)' ,'top(t2.q_tinyint,20)' ,'top(t2.q_float,20)' ,'top(t2.q_double,20)' ,
                    'first(t2.q_int)' , 'first(t2.q_bigint)' , 'first(t2.q_smallint)' , 'first(t2.q_tinyint)' , 'first(t2.q_float)' ,'first(t2.q_double)' ,'first(t2.q_binary)' ,'first(t2.q_nchar)' ,'first(t2.q_bool)' ,'first(t2.q_ts)' ,
                    'last(t2.q_int)' ,  'last(t2.q_bigint)' , 'last(t2.q_smallint)'  , 'last(t2.q_tinyint)' , 'last(t2.q_float)'  ,'last(t2.q_double)' , 'last(t2.q_binary)' ,'last(t2.q_nchar)' ,'last(t2.q_bool)' ,'last(t2.q_ts)']

        calc_select_in_j = ['min(t1.q_int)' , 'min(t1.q_bigint)' , 'min(t1.q_smallint)' , 'min(t1.q_tinyint)' , 'min(t1.q_float)' ,'min(t1.q_double)' ,
                    'max(t1.q_int)' ,  'max(t1.q_bigint)' , 'max(t1.q_smallint)' , 'max(t1.q_tinyint)' ,'max(t1.q_float)' ,'max(t1.q_double)' ,
                    'apercentile(t1.q_int,20)' ,  'apercentile(t1.q_bigint,20)'  ,'apercentile(t1.q_smallint,20)'  ,'apercentile(t1.q_tinyint,20)' ,'apercentile(t1.q_float,20)'  ,'apercentile(t1.q_double,20)' ,
                    'last_row(t1.q_int)' ,  'last_row(t1.q_bigint)' , 'last_row(t1.q_smallint)' , 'last_row(t1.q_tinyint)' , 'last_row(t1.q_float)' , 
                    'last_row(t1.q_double)' , 'last_row(t1.q_bool)' ,'last_row(t1.q_binary)' ,'last_row(t1.q_nchar)' ,'last_row(t1.q_ts)' ,
                    'min(t2.q_int)' , 'min(t2.q_bigint)' , 'min(t2.q_smallint)' , 'min(t2.q_tinyint)' , 'min(t2.q_float)' ,'min(t2.q_double)' ,
                    'max(t2.q_int)' ,  'max(t2.q_bigint)' , 'max(t2.q_smallint)' , 'max(t2.q_tinyint)' ,'max(t2.q_float)' ,'max(t2.q_double)' ,
                    'apercentile(t2.q_int,20)' ,  'apercentile(t2.q_bigint,20)'  ,'apercentile(t2.q_smallint,20)'  ,'apercentile(t2.q_tinyint,20)' ,'apercentile(t2.q_float,20)'  ,'apercentile(t2.q_double,20)' ,
                    'last_row(t2.q_int)' ,  'last_row(t2.q_bigint)' , 'last_row(t2.q_smallint)' , 'last_row(t2.q_tinyint)' , 'last_row(t2.q_float)' , 
                    'last_row(t2.q_double)' , 'last_row(t2.q_bool)' ,'last_row(t2.q_binary)' ,'last_row(t2.q_nchar)' ,'last_row(t2.q_ts)']
        
        calc_select_all_j = calc_select_in_ts_j + calc_select_in_j

        calc_select_regular_j = [ 'PERCENTILE(t1.q_int,10)' ,'PERCENTILE(t1.q_bigint,20)' , 'PERCENTILE(t1.q_smallint,30)' ,'PERCENTILE(t1.q_tinyint,40)' ,'PERCENTILE(t1.q_float,50)' ,'PERCENTILE(t1.q_double,60)' ,
                    'PERCENTILE(t2.q_int,10)' ,'PERCENTILE(t2.q_bigint,20)' , 'PERCENTILE(t2.q_smallint,30)' ,'PERCENTILE(t2.q_tinyint,40)' ,'PERCENTILE(t2.q_float,50)' ,'PERCENTILE(t2.q_double,60)']

                    
        calc_select_fill_j = ['INTERP(t1.q_bool)' ,'INTERP(t1.q_binary)' ,'INTERP(t1.q_nchar)' ,'INTERP(t1.q_ts)', 'INTERP(t1.q_int)' ,'INTERP(t1.*)' ,'INTERP(t1.q_bigint)' ,'INTERP(t1.q_smallint)' ,'INTERP(t1.q_tinyint)', 'INTERP(t1.q_float)' ,'INTERP(t1.q_double)' ,
                    'INTERP(t2.q_bool)' ,'INTERP(t2.q_binary)' ,'INTERP(t2.q_nchar)' ,'INTERP(t2.q_ts)', 'INTERP(t2.q_int)' ,'INTERP(t2.*)' ,'INTERP(t2.q_bigint)' ,'INTERP(t2.q_smallint)' ,'INTERP(t2.q_tinyint)', 'INTERP(t2.q_float)' ,'INTERP(t2.q_double)']
        interp_where_j = ['t1.ts = now' , 't1.ts = \'2020-09-13 20:26:40.000\'' , 't1.ts = \'2020-09-13 20:26:40.009\'' ,'t2.ts = now' , 't2.ts = \'2020-09-13 20:26:40.000\'' , 't2.ts = \'2020-09-13 20:26:40.009\'' ,
                    't1.tbname in (\'table_1\') and t1.ts = now' ,'t1.tbname in (\'table_0\' ,\'table_1\',\'table_2\',\'table_3\',\'table_4\',\'table_5\') and t1.ts =  \'2020-09-13 20:26:40.000\'','t1.tbname like \'table%\'  and t1.ts =  \'2020-09-13 20:26:40.002\'',
                    't2.tbname in (\'table_1\') and t2.ts = now' ,'t2.tbname in (\'table_0\' ,\'table_1\',\'table_2\',\'table_3\',\'table_4\',\'table_5\') and t2.ts =  \'2020-09-13 20:26:40.000\'','t2.tbname like \'table%\'  and t2.ts =  \'2020-09-13 20:26:40.002\'']

        # calc_aggregate_all   calc_aggregate_regular   calc_aggregate_groupbytbname  APERCENTILE\PERCENTILE
        # aggregate function include [all:count(*)\avg\sum\stddev ||regualr:twa\irate\leastsquares ||group by tbname:twa\irate\]
        calc_aggregate_all = ['count(*)' , 'count(q_int)' ,'count(q_bigint)' , 'count(q_smallint)' ,'count(q_tinyint)' ,'count(q_float)' ,
                    'count(q_double)' ,'count(q_binary)' ,'count(q_nchar)' ,'count(q_bool)' ,'count(q_ts)' ,
                    'avg(q_int)' ,'avg(q_bigint)' , 'avg(q_smallint)' ,'avg(q_tinyint)' ,'avg(q_float)' ,'avg(q_double)' ,
                    'sum(q_int)' ,'sum(q_bigint)' , 'sum(q_smallint)' ,'sum(q_tinyint)' ,'sum(q_float)' ,'sum(q_double)' ,
                    'STDDEV(q_int)' ,'STDDEV(q_bigint)' , 'STDDEV(q_smallint)' ,'STDDEV(q_tinyint)' ,'STDDEV(q_float)' ,'STDDEV(q_double)',
                    'APERCENTILE(q_int,10)' ,'APERCENTILE(q_bigint,20)' , 'APERCENTILE(q_smallint,30)' ,'APERCENTILE(q_tinyint,40)' ,'APERCENTILE(q_float,50)' ,'APERCENTILE(q_double,60)']

        calc_aggregate_regular = ['twa(q_int)' ,'twa(q_bigint)' , 'twa(q_smallint)' ,'twa(q_tinyint)' ,'twa (q_float)' ,'twa(q_double)' ,
                    'IRATE(q_int)' ,'IRATE(q_bigint)' , 'IRATE(q_smallint)' ,'IRATE(q_tinyint)' ,'IRATE (q_float)' ,'IRATE(q_double)' ,
                    'LEASTSQUARES(q_int,15,3)' , 'LEASTSQUARES(q_bigint,10,1)' , 'LEASTSQUARES(q_smallint,20,3)' ,'LEASTSQUARES(q_tinyint,10,4)' ,'LEASTSQUARES(q_float,6,4)' ,'LEASTSQUARES(q_double,3,1)' ,
                    'PERCENTILE(q_int,10)' ,'PERCENTILE(q_bigint,20)' , 'PERCENTILE(q_smallint,30)' ,'PERCENTILE(q_tinyint,40)' ,'PERCENTILE(q_float,50)' ,'PERCENTILE(q_double,60)']

        calc_aggregate_groupbytbname = ['twa(q_int)' ,'twa(q_bigint)' , 'twa(q_smallint)' ,'twa(q_tinyint)' ,'twa (q_float)' ,'twa(q_double)' ,
                    'IRATE(q_int)' ,'IRATE(q_bigint)' , 'IRATE(q_smallint)' ,'IRATE(q_tinyint)' ,'IRATE (q_float)' ,'IRATE(q_double)' ] 

        #two table join
        calc_aggregate_all_j = ['count(t1.*)' , 'count(t1.q_int)' ,'count(t1.q_bigint)' , 'count(t1.q_smallint)' ,'count(t1.q_tinyint)' ,'count(t1.q_float)' ,
                    'count(t1.q_double)' ,'count(t1.q_binary)' ,'count(t1.q_nchar)' ,'count(t1.q_bool)' ,'count(t1.q_ts)' ,
                    'avg(t1.q_int)' ,'avg(t1.q_bigint)' , 'avg(t1.q_smallint)' ,'avg(t1.q_tinyint)' ,'avg(t1.q_float)' ,'avg(t1.q_double)' ,
                    'sum(t1.q_int)' ,'sum(t1.q_bigint)' , 'sum(t1.q_smallint)' ,'sum(t1.q_tinyint)' ,'sum(t1.q_float)' ,'sum(t1.q_double)' ,
                    'STDDEV(t1.q_int)' ,'STDDEV(t1.q_bigint)' , 'STDDEV(t1.q_smallint)' ,'STDDEV(t1.q_tinyint)' ,'STDDEV(t1.q_float)' ,'STDDEV(t1.q_double)',
                    'APERCENTILE(t1.q_int,10)' ,'APERCENTILE(t1.q_bigint,20)' , 'APERCENTILE(t1.q_smallint,30)' ,'APERCENTILE(t1.q_tinyint,40)' ,'APERCENTILE(t1.q_float,50)' ,'APERCENTILE(t1.q_double,60)' ,
                    'count(t2.*)' , 'count(t2.q_int)' ,'count(t2.q_bigint)' , 'count(t2.q_smallint)' ,'count(t2.q_tinyint)' ,'count(t2.q_float)' ,
                    'count(t2.q_double)' ,'count(t2.q_binary)' ,'count(t2.q_nchar)' ,'count(t2.q_bool)' ,'count(t2.q_ts)' ,
                    'avg(t2.q_int)' ,'avg(t2.q_bigint)' , 'avg(t2.q_smallint)' ,'avg(t2.q_tinyint)' ,'avg(t2.q_float)' ,'avg(t2.q_double)' ,
                    'sum(t2.q_int)' ,'sum(t2.q_bigint)' , 'sum(t2.q_smallint)' ,'sum(t2.q_tinyint)' ,'sum(t2.q_float)' ,'sum(t2.q_double)' ,
                    'STDDEV(t2.q_int)' ,'STDDEV(t2.q_bigint)' , 'STDDEV(t2.q_smallint)' ,'STDDEV(t2.q_tinyint)' ,'STDDEV(t2.q_float)' ,'STDDEV(t2.q_double)',
                    'APERCENTILE(t2.q_int,10)' ,'APERCENTILE(t2.q_bigint,20)' , 'APERCENTILE(t2.q_smallint,30)' ,'APERCENTILE(t2.q_tinyint,40)' ,'APERCENTILE(t2.q_float,50)' ,'APERCENTILE(t2.q_double,60)']

        calc_aggregate_regular_j = ['twa(t1.q_int)' ,'twa(t1.q_bigint)' , 'twa(t1.q_smallint)' ,'twa(t1.q_tinyint)' ,'twa (t1.q_float)' ,'twa(t1.q_double)' ,
                    'IRATE(t1.q_int)' ,'IRATE(t1.q_bigint)' , 'IRATE(t1.q_smallint)' ,'IRATE(t1.q_tinyint)' ,'IRATE (t1.q_float)' ,'IRATE(t1.q_double)' ,
                    'LEASTSQUARES(t1.q_int,15,3)' , 'LEASTSQUARES(t1.q_bigint,10,1)' , 'LEASTSQUARES(t1.q_smallint,20,3)' ,'LEASTSQUARES(t1.q_tinyint,10,4)' ,'LEASTSQUARES(t1.q_float,6,4)' ,'LEASTSQUARES(t1.q_double,3,1)' ,
                    'PERCENTILE(t1.q_int,10)' ,'PERCENTILE(t1.q_bigint,20)' , 'PERCENTILE(t1.q_smallint,30)' ,'PERCENTILE(t1.q_tinyint,40)' ,'PERCENTILE(t1.q_float,50)' ,'PERCENTILE(t1.q_double,60)' ,
                    'twa(t2.q_int)' ,'twa(t2.q_bigint)' , 'twa(t2.q_smallint)' ,'twa(t2.q_tinyint)' ,'twa (t2.q_float)' ,'twa(t2.q_double)' ,
                    'IRATE(t2.q_int)' ,'IRATE(t2.q_bigint)' , 'IRATE(t2.q_smallint)' ,'IRATE(t2.q_tinyint)' ,'IRATE (t2.q_float)' ,'IRATE(t2.q_double)',
                    'LEASTSQUARES(t2.q_int,15,3)' , 'LEASTSQUARES(t2.q_bigint,10,1)' , 'LEASTSQUARES(t2.q_smallint,20,3)' ,'LEASTSQUARES(t2.q_tinyint,10,4)' ,'LEASTSQUARES(t2.q_float,6,4)' ,'LEASTSQUARES(t2.q_double,3,1)' ,
                    'PERCENTILE(t2.q_int,10)' ,'PERCENTILE(t2.q_bigint,20)' , 'PERCENTILE(t2.q_smallint,30)' ,'PERCENTILE(t2.q_tinyint,40)' ,'PERCENTILE(t2.q_float,50)' ,'PERCENTILE(t2.q_double,60)']

        calc_aggregate_groupbytbname_j = ['twa(t1.q_int)' ,'twa(t1.q_bigint)' , 'twa(t1.q_smallint)' ,'twa(t1.q_tinyint)' ,'twa (t1.q_float)' ,'twa(t1.q_double)' ,
                    'IRATE(t1.q_int)' ,'IRATE(t1.q_bigint)' , 'IRATE(t1.q_smallint)' ,'IRATE(t1.q_tinyint)' ,'IRATE (t1.q_float)' ,'IRATE(t1.q_double)' ,
                    'twa(t2.q_int)' ,'twa(t2.q_bigint)' , 'twa(t2.q_smallint)' ,'twa(t2.q_tinyint)' ,'twa (t2.q_float)' ,'twa(t2.q_double)' ,
                    'IRATE(t2.q_int)' ,'IRATE(t2.q_bigint)' , 'IRATE(t2.q_smallint)' ,'IRATE(t2.q_tinyint)' ,'IRATE (t2.q_float)' ,'IRATE(t2.q_double)' ] 
        
        # calc_calculate_all   calc_calculate_regular   calc_calculate_groupbytbname
        # calculation function include [all:spread\+-*/ ||regualr:diff\derivative ||group by tbname:diff\derivative\]
        calc_calculate_all = ['SPREAD(ts)'  , 'SPREAD(q_ts)'  , 'SPREAD(q_int)' ,'SPREAD(q_bigint)' , 'SPREAD(q_smallint)' ,'SPREAD(q_tinyint)' ,'SPREAD(q_float)' ,'SPREAD(q_double)' , 
                     '(SPREAD(q_int) + SPREAD(q_bigint))' , '(SPREAD(q_smallint) - SPREAD(q_float))', '(SPREAD(q_double) * SPREAD(q_tinyint))' , '(SPREAD(q_double) / SPREAD(q_float))']
        calc_calculate_regular = ['DIFF(q_int)' ,'DIFF(q_bigint)' , 'DIFF(q_smallint)' ,'DIFF(q_tinyint)' ,'DIFF(q_float)' ,'DIFF(q_double)' ,
                    'DERIVATIVE(q_int,15s,0)' , 'DERIVATIVE(q_bigint,10s,1)' , 'DERIVATIVE(q_smallint,20s,0)' ,'DERIVATIVE(q_tinyint,10s,1)' ,'DERIVATIVE(q_float,6s,0)' ,'DERIVATIVE(q_double,3s,1)' ]
        calc_calculate_groupbytbname = calc_calculate_regular
        
        #two table join
        calc_calculate_all_j = ['SPREAD(t1.ts)'  , 'SPREAD(t1.q_ts)'  , 'SPREAD(t1.q_int)' ,'SPREAD(t1.q_bigint)' , 'SPREAD(t1.q_smallint)' ,'SPREAD(t1.q_tinyint)' ,'SPREAD(t1.q_float)' ,'SPREAD(t1.q_double)' ,
                    'SPREAD(t2.ts)'  , 'SPREAD(t2.q_ts)'  , 'SPREAD(t2.q_int)' ,'SPREAD(t2.q_bigint)' , 'SPREAD(t2.q_smallint)' ,'SPREAD(t2.q_tinyint)' ,'SPREAD(t2.q_float)' ,'SPREAD(t2.q_double)' ,
                    '(SPREAD(t1.q_int) + SPREAD(t1.q_bigint))' , '(SPREAD(t1.q_tinyint) - SPREAD(t1.q_float))', '(SPREAD(t1.q_double) * SPREAD(t1.q_tinyint))' , '(SPREAD(t1.q_double) / SPREAD(t1.q_tinyint))',
                    '(SPREAD(t2.q_int) + SPREAD(t2.q_bigint))' , '(SPREAD(t2.q_smallint) - SPREAD(t2.q_float))', '(SPREAD(t2.q_double) * SPREAD(t2.q_tinyint))' , '(SPREAD(t2.q_double) / SPREAD(t2.q_tinyint))',
                    '(SPREAD(t1.q_int) + SPREAD(t1.q_smallint))' , '(SPREAD(t2.q_smallint) - SPREAD(t2.q_float))', '(SPREAD(t1.q_double) * SPREAD(t1.q_tinyint))' , '(SPREAD(t1.q_double) / SPREAD(t1.q_float))']
        calc_calculate_regular_j = ['DIFF(t1.q_int)' ,'DIFF(t1.q_bigint)' , 'DIFF(t1.q_smallint)' ,'DIFF(t1.q_tinyint)' ,'DIFF(t1.q_float)' ,'DIFF(t1.q_double)' ,
                    'DERIVATIVE(t1.q_int,15s,0)' , 'DERIVATIVE(t1.q_bigint,10s,1)' , 'DERIVATIVE(t1.q_smallint,20s,0)' ,'DERIVATIVE(t1.q_tinyint,10s,1)' ,'DERIVATIVE(t1.q_float,6s,0)' ,'DERIVATIVE(t1.q_double,3s,1)' ,
                    'DIFF(t2.q_int)' ,'DIFF(t2.q_bigint)' , 'DIFF(t2.q_smallint)' ,'DIFF(t2.q_tinyint)' ,'DIFF(t2.q_float)' ,'DIFF(t2.q_double)' ,
                    'DERIVATIVE(t2.q_int,15s,0)' , 'DERIVATIVE(t2.q_bigint,10s,1)' , 'DERIVATIVE(t2.q_smallint,20s,0)' ,'DERIVATIVE(t2.q_tinyint,10s,1)' ,'DERIVATIVE(t2.q_float,6s,0)' ,'DERIVATIVE(t2.q_double,3s,1)' ]
        calc_calculate_groupbytbname_j = calc_calculate_regular_j


        #inter  && calc_aggregate_all\calc_aggregate_regular\calc_select_all
        interval_sliding = ['interval(4w) sliding(1w) ','interval(1w) sliding(1d) ','interval(1d) sliding(1h) ' ,
                    'interval(1h) sliding(1m) ','interval(1m) sliding(1s) ','interval(1s) sliding(10a) ',
                    'interval(1y) ','interval(1n) ','interval(1w) ','interval(1d) ','interval(1h) ','interval(1m) ','interval(1s) ' ,'interval(10a)',
                    'interval(1y,1n) ','interval(1n,1w) ','interval(1w,1d) ','interval(1d,1h) ','interval(1h,1m) ','interval(1m,1s) ','interval(1s,10a) ' ,'interval(100a,30a)']


        for i in range(self.fornum):
            tdSql.query("select 1-1 from table_0;")
            sql = "select count(*) from (select count(*) from stable_1 where ts>= 1620000000000 interval(1d) group by tbname) interval(1d);"
            tdLog.info(sql)    
            tdSql.query(sql)
            tdSql.checkData(0,0,'2021-08-27 00:00:00.000')
            tdSql.checkData(0,1,3)
            tdSql.checkData(1,0,'2021-08-28 00:00:00.000')
            tdSql.checkData(1,1,3)
            tdSql.checkData(2,0,'2021-08-29 00:00:00.000')
            tdSql.checkRows(12)

            #sql = "select  * from  ( select  * from regular_table_1 where q_tinyint >= -127 and q_tinyint <= 127 order by ts );"
            tdSql.query("select 1-2 from table_0;")
            sql = "select  * from  ( select  * from regular_table_1 where " 
            sql += "%s );" % random.choice(q_where)
            datacheck = self.regular1_checkall_0(sql)           
            tdSql.checkRows(100)
            datacheck = self.regular1_checkall_100(sql)
            
            #sql = "select  * from  ( select  * from regular_table_1 ) where q_binary like 'binary%'  or q_binary = '0'  order by ts asc ;"
            tdSql.query("select 1-3 from table_0;")
            sql = "select  * from  ( select  * from regular_table_1 ) where " 
            sql += "%s ;" % random.choice(q_where)
            datacheck = self.regular1_checkall_0(sql)           
            tdSql.checkRows(100)
            datacheck = self.regular1_checkall_100(sql)

            #sql = select  * from  ( select  * from regular_table_1 t1 , regular_table_2 t2 where t1.ts = t2.ts and t1.q_double >= -1000000000 and t1.q_double <= 1000000000 and t2.q_double >= -1000000000 and t2.q_double <= 1000000000 order by t2.ts asc );;
            tdSql.query("select 1-4 from table_0;")
            sql = "select  * from  ( select  * from regular_table_1 t1 , regular_table_2 t2  where t1.ts = t2.ts and " 
            sql += "%s )" % random.choice(q_u_where)
            datacheck = self.regular_join_checkall_0(sql)           
            tdSql.checkRows(100)
            datacheck = self.regular_join_checkall_100(sql)




            #sql = "select  * from  ( select  * from stable_1 where q_tinyint >= -127 and q_tinyint <= 127 order by ts );"
            tdSql.query("select 2-1 from stable_1;")
            sql = "select  * from  ( select  * from stable_1  where " 
            sql += "%s );" % random.choice(qt_where)
            datacheck = self.stable1_checkall_0(sql)           
            tdSql.checkRows(300)
            datacheck = self.stable1_checkall_300(sql)
            
            #sql = "select  * from  ( select  * from stable_1 )  order by ts asc ;"
            tdSql.query("select 2-2 from stable_1;")
            sql = "select  * from  ( select  * from stable_1 ) where " 
            sql += "%s ;" % random.choice(qt_where)
            datacheck = self.stable1_checkall_0(sql)           
            tdSql.checkRows(300)
            datacheck = self.stable1_checkall_300(sql)  

            #sql = "select  * from  ( select  * from table_0 ) where q_binary like 'binary%'  or q_binary = '0'  order by ts asc ;"
            tdSql.query("select 2-3 from stable_1;")
            sql = "select  * from  ( select  * from table_0 ) where " 
            sql += "%s ;" % random.choice(q_where)
            datacheck = self.regular1_checkall_0(sql)           
            tdSql.checkRows(100)
            datacheck = self.regular1_checkall_100(sql)   

            #sql = "select  * from  ( select  * from table_0 where q_binary like 'binary%'  or q_binary = '0'  order by ts asc );"
            tdSql.query("select 2-4 from stable_1;")
            sql = "select  * from  ( select  * from table_0  where " 
            sql += "%s );" % random.choice(q_where)
            datacheck = self.regular1_checkall_0(sql)           
            tdSql.checkRows(100)
            datacheck = self.regular1_checkall_100(sql)    

            #sql = select  *  from  ( select  * from stable_1 t1 , stable_2 t2 where t1.ts = t2.ts and t1.t_int = t2.t_int ) ;;
            tdSql.query("select 2-5 from stable_1;")
            sql = "select  * from  ( select  * from stable_1 t1 , stable_2 t2  where t1.ts = t2.ts and " 
            sql += "%s )" % random.choice(t_join_where)
            datacheck = self.stable_join_checkall_0(sql)           
            tdSql.checkRows(100)
            datacheck = self.stable_join_checkall_100(sql)       
 

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))

        


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())