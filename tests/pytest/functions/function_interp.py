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

import sys
import taos
import numpy as np
import string
import os
import time
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        
        self.ts = 1630000000000
        self.num = 10

    def function_introduction(self):
        tdLog.info('select * from table|stable[group by tbname]|regular_table')
        tdLog.info('select interp_select from table|stable[group by tbname]|regular_table')
        tdLog.info('select interp_select from table|stable[group by tbname]|regular_table ORDER BY ts DESC')
        tdLog.info('select interp_select from table|stable[group by tbname]|regular_table where ts> ts_min')
        tdLog.info('select interp_select from table|stable[group by tbname]|regular_table where ts> ts_max')
        tdLog.info('select interp_select from table|stable[group by tbname]|regular_table [range(ts_min,ts_max)]')
        tdLog.info('select interp_select from table|stable[group by tbname]|regular_table [EVERY(s)]')
        tdLog.info('select interp_select from table|stable[group by tbname]|regular_table [FILL(LINEAR,NEXT,PREV,VALUE,NULL)]')
        tdLog.info('select interp_select from table|stable[group by tbname]|regular_table [range(ts_min,ts_max)] [FILL(LINEAR,NEXT,PREV,VALUE,NULL)]')
        tdLog.info('select interp_select from table|stable[group by tbname]|regular_table [EVERY(s)] [FILL(LINEAR,NEXT,PREV,VALUE,NULL)]')
        tdLog.info('select interp_select from table|stable[group by tbname]|regular_table [range(ts_min,ts_max)] [EVERY(s)] [FILL(LINEAR,NEXT,PREV,VALUE,NULL)]')
        tdLog.info('select interp_select from table|stable[group by tbname]|regular_table [where condition] [range(ts_min,ts_max)] [EVERY(s)] [FILL(LINEAR,NEXT,PREV,VALUE,NULL)]')
        tdLog.info('select * from (select interp_select from table|stable[group by tbname]|regular_table)')
        tdLog.info('select interp_select from (select * from table|stable[group by tbname]|regular_table)')
        tdLog.info('select * from (select interp_select from table|stable[group by tbname]|regular_table [where condition] [range(ts_min,ts_max)] [EVERY(s)] [FILL(LINEAR,NEXT,PREV,VALUE,NULL)])')
        tdLog.info('select interp_select from (select * from table|stable[group by tbname]|regular_table [where condition] [range(ts_min,ts_max)] [EVERY(s)] [FILL(LINEAR,NEXT,PREV,VALUE,NULL)])')
        tdLog.info('select * from (select interp_select from table|stable[group by tbname]|regular_table) [where condition] [range(ts_min,ts_max)] [EVERY(s)] [FILL(LINEAR,NEXT,PREV,VALUE,NULL)]')
        tdLog.info('select interp_select from (select * from table|stable[group by tbname]|regular_table) [where condition] [range(ts_min,ts_max)] [EVERY(s)] [FILL(LINEAR,NEXT,PREV,VALUE,NULL)]')
        tdLog.info('select * from (select interp_select from table|stable[group by tbname]|regular_table [where condition] [range(ts_min,ts_max)] [EVERY(s)] [FILL(LINEAR,NEXT,PREV,VALUE,NULL)]) a,\
                (select interp_select from table|stable[group by tbname]|regular_table [where condition] [range(ts_min,ts_max)] [EVERY(s)] [FILL(LINEAR,NEXT,PREV,VALUE,NULL)]) b where a.ts=b.ts')
        tdLog.info('error select interp_select sql')
    
    def restartDnodes(self):
        tdDnodes.stop(1)
        tdDnodes.start(1)

    def dropandcreateDB(self):
        tdSql.execute('''drop database if exists db ;''')
        tdSql.execute('''create database db keep 36500;''')
        tdSql.execute('''use db;''')

        tdSql.execute('''create stable stable_1
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint, q_float float , q_double double ) 
                    tags(loc nchar(20));''')
        tdSql.execute('''create table table_1 using stable_1 tags('table_1')''')
        tdSql.execute('''create table table_2 using stable_1 tags('table_2')''')
        tdSql.execute('''create table table_3 using stable_1 tags('table_3')''')
        tdSql.execute('''create table regular_table_1
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double ) ;''')
        tdSql.execute('''create table regular_table_2
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double ) ;''')
        tdSql.execute('''create table regular_table_3
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double ) ;''')
        
        for i in range(self.num):        
            tdSql.execute('''insert into table_1 values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*10000, i, i, i, i, i, i))
            tdSql.execute('''insert into table_1 values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*3000, i, i, i, i, i, i))
            tdSql.execute('''insert into regular_table_1 values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*10000, i, i, i, i, i, i))
            tdSql.execute('''insert into regular_table_1 values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*3000 , i, i, i, i, i, i))

            tdSql.execute('''insert into table_2 values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*10000, 2147483647-i, 9223372036854775807-i, 32767-i, 127-i, i, i))
            tdSql.execute('''insert into table_2 values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*3000, 2147483647-i, 9223372036854775807-i, 32767-i, 127-i, i, i))
            tdSql.execute('''insert into regular_table_2 values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*10000, 2147483647-i, 9223372036854775807-i, 32767-i, 127-i, i, i))
            tdSql.execute('''insert into regular_table_2 values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*3000 , 2147483647-i, 9223372036854775807-i, 32767-i, 127-i, i, i))

            tdSql.execute('''insert into table_3 values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*10000, -2147483647+i, -9223372036854775807+i, -32767+i, -127+i, -i, -i))
            tdSql.execute('''insert into table_3 values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*3000, -2147483647+i, -9223372036854775807+i, -32767+i, -127+i, -i, -i))
            tdSql.execute('''insert into regular_table_3 values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*10000, -2147483647+i, -9223372036854775807+i, -32767+i, -127+i, -i, -i))
            tdSql.execute('''insert into regular_table_3 values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*3000 , -2147483647+i, -9223372036854775807+i, -32767+i, -127+i, -i, -i))

    def dropandcreateDB_null(self):
        tdSql.execute('''drop database if exists db ;''')
        tdSql.execute('''create database db keep 36500;''')
        tdSql.execute('''use db;''')

        tdSql.execute('''create stable stable_1
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint, q_float float , q_double double ,
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double ,
                    q_bool bool , q_binary binary(20) , q_nchar nchar(20) , q_ts timestamp) 
                    tags(loc nchar(20) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, 
                        t_bool bool , t_binary binary(20) , t_nchar nchar(20) ,t_float float , t_double double , t_ts timestamp);''')
        tdSql.execute('''create stable stable_2
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint, q_float float , q_double double ,
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double ) 
                    tags(loc nchar(20));''')
        tdSql.execute('''create table table_1 using stable_1 tags('table_1', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0')''')
        tdSql.execute('''create table table_2 using stable_1 tags('table_2', '2147483647' , '9223372036854775807' , '32767' , '127' , 1 , 
                        'binary1' , 'nchar1' , '1' , '11' , \'1999-09-09 09:09:09.090\')''')
        tdSql.execute('''create table table_3 using stable_1 tags('table_3', '-2147483647' , '-9223372036854775807' , '-32767' , '-127' , false , 
                        'binary2' , 'nchar2nchar2' , '-2.2' , '-22.22' , \'2099-09-09 09:09:09.090\')''')
        tdSql.execute('''create table table_21 using stable_2 tags('table_21')''')
        tdSql.execute('''create table regular_table_1
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double ,
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double ,
                    q_bool bool , q_binary binary(20) , q_nchar nchar(20) , q_ts timestamp) ;''')
        tdSql.execute('''create table regular_table_2
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double ,
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double ,
                    q_bool bool , q_binary binary(20) , q_nchar nchar(20) , q_ts timestamp) ;''')
        tdSql.execute('''create table regular_table_3
                    (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double ,
                    q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double ,
                    q_bool bool , q_binary binary(20) , q_nchar nchar(20) , q_ts timestamp) ;''')
        
        for i in range(self.num):        
            tdSql.execute('''insert into table_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double) values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*10000, i, i, i, i, i, i))
            tdSql.execute('''insert into table_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double) values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*3000, i, i, i, i, i, i))
            tdSql.execute('''insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double) values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*10000, i, i, i, i, i, i))
            tdSql.execute('''insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double) values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*3000 , i, i, i, i, i, i))

            tdSql.execute('''insert into table_21  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double) values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*10000, i, i, i, i, i, i))
            tdSql.execute('''insert into table_21  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double) values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*3000, i, i, i, i, i, i))

            tdSql.execute('''insert into table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double) values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*10000, 2147483647-i, 9223372036854775807-i, 32767-i, 127-i, i, i))
            tdSql.execute('''insert into table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double) values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*3000, 2147483647-i, 9223372036854775807-i, 32767-i, 127-i, i, i))
            tdSql.execute('''insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double) values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*10000, 2147483647-i, 9223372036854775807-i, 32767-i, 127-i, i, i))
            tdSql.execute('''insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double) values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*3000 , 2147483647-i, 9223372036854775807-i, 32767-i, 127-i, i, i))

            tdSql.execute('''insert into table_3 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double) values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*10000, -2147483647+i, -9223372036854775807+i, -32767+i, -127+i, -i, -i))
            tdSql.execute('''insert into table_3 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double) values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*3000, -2147483647+i, -9223372036854775807+i, -32767+i, -127+i, -i, -i))
            tdSql.execute('''insert into regular_table_3 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double) values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*10000, -2147483647+i, -9223372036854775807+i, -32767+i, -127+i, -i, -i))
            tdSql.execute('''insert into regular_table_3 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double) values(%d, %d, %d, %d, %d, %f, %f)''' 
                            % (self.ts + i*3000 , -2147483647+i, -9223372036854775807+i, -32767+i, -127+i, -i, -i))

    def result_0(self,sql):
        tdLog.info(sql)       
        tdSql.query(sql)
        tdSql.checkRows(0)
        dcDB = self.dropandcreateDB_null()
    
    def regular1_checkall_0_base(self,sql):
        tdLog.info(sql)       
        tdSql.query(sql)
        tdSql.checkData(0,0,'2021-08-27 01:46:40.000')
        tdSql.checkData(0,1,0)
        tdSql.checkData(0,2,0)
        tdSql.checkData(0,3,0)
        tdSql.checkData(0,4,0)
        tdSql.checkData(0,5,0)
        tdSql.checkData(0,6,0)
        tdSql.checkData(0,7,'None')
        tdSql.checkData(0,8,'None')
        tdSql.checkData(0,9,'None')
        tdSql.checkData(0,10,'None')
        tdSql.checkData(0,11,'None')
        tdSql.checkData(0,12,'None')
        
    def regular1_checkall_0(self,sql):
        tdLog.info(sql)       
        tdSql.query(sql)
        tdSql.checkData(0,0,'2021-08-27 01:46:40.000')
        tdSql.checkData(0,1,0)
        tdSql.checkData(0,2,0)
        tdSql.checkData(0,3,0)
        tdSql.checkData(0,4,0)
        tdSql.checkData(0,5,0)
        tdSql.checkData(0,6,0)
        tdSql.checkData(0,7,'None')
        tdSql.checkData(0,8,'None')
        tdSql.checkData(0,9,'None')
        tdSql.checkData(0,10,'None')
        tdSql.checkData(0,11,'None')
        tdSql.checkData(0,12,'None')
        tdSql.checkData(0,13,0)
        tdSql.checkData(0,14,0)
        tdSql.checkData(0,15,0)
        tdSql.checkData(0,16,0)
        tdSql.checkData(0,17,0)
        tdSql.checkData(0,18,0)
        tdSql.checkData(0,19,'None')
        tdSql.checkData(0,20,'None')
        tdSql.checkData(0,21,'None')
        tdSql.checkData(0,22,'None')
        tdSql.checkData(0,23,'None')
        tdSql.checkData(0,24,'None')

    def regular1_checkall_0_LINEAR(self,sql):
        tdLog.info(sql)       
        tdSql.query(sql)
        tdSql.checkData(0,0,'2021-08-27 01:46:41.500')
        tdSql.checkData(0,1,0)
        tdSql.checkData(0,2,0)
        tdSql.checkData(0,3,0)
        tdSql.checkData(0,4,0)
        tdSql.checkData(0,5,0.5)
        tdSql.checkData(0,6,0.5)
        tdSql.checkData(0,7,'None')
        tdSql.checkData(0,8,'None')
        tdSql.checkData(0,9,'None')
        tdSql.checkData(0,10,'None')
        tdSql.checkData(0,11,'None')
        tdSql.checkData(0,12,'None')
        tdSql.checkData(0,13,0)
        tdSql.checkData(0,14,0)
        tdSql.checkData(0,15,0)
        tdSql.checkData(0,16,0)
        tdSql.checkData(0,17,0.5)
        tdSql.checkData(0,18,0.5)
        tdSql.checkData(0,19,'None')
        tdSql.checkData(0,20,'None')
        tdSql.checkData(0,21,'None')
        tdSql.checkData(0,22,'None')
        tdSql.checkData(0,23,'None')
        tdSql.checkData(0,24,'None')

    def regular1_checkall_0_NEXT(self,sql):
        tdLog.info(sql)       
        tdSql.query(sql)
        tdSql.checkData(0,0,'2021-08-27 01:46:41.500')
        tdSql.checkData(0,1,1)
        tdSql.checkData(0,2,1)
        tdSql.checkData(0,3,1)
        tdSql.checkData(0,4,1)
        tdSql.checkData(0,5,1)
        tdSql.checkData(0,6,1)
        tdSql.checkData(0,7,'None')
        tdSql.checkData(0,8,'None')
        tdSql.checkData(0,9,'None')
        tdSql.checkData(0,10,'None')
        tdSql.checkData(0,11,'None')
        tdSql.checkData(0,12,'None')
        tdSql.checkData(0,13,1)
        tdSql.checkData(0,14,1)
        tdSql.checkData(0,15,1)
        tdSql.checkData(0,16,1)
        tdSql.checkData(0,17,1)
        tdSql.checkData(0,18,1)
        tdSql.checkData(0,19,'None')
        tdSql.checkData(0,20,'None')
        tdSql.checkData(0,21,'None')
        tdSql.checkData(0,22,'None')
        tdSql.checkData(0,23,'None')
        tdSql.checkData(0,24,'None')

    def regular1_checkall_0_VALUE100(self,sql):
        tdLog.info(sql)       
        tdSql.query(sql)
        tdSql.checkData(0,0,'01:46:30.000')
        tdSql.checkData(0,1,100)
        tdSql.checkData(0,2,100)
        tdSql.checkData(0,3,100)
        tdSql.checkData(0,4,100)
        tdSql.checkData(0,5,100)
        tdSql.checkData(0,6,100)
        # all data interp 100
        tdSql.checkData(0,7,100)
        tdSql.checkData(0,8,100)
        tdSql.checkData(0,9,100)
        tdSql.checkData(0,10,100)
        tdSql.checkData(0,11,100)
        tdSql.checkData(0,12,100)

        tdSql.checkData(0,13,100)
        tdSql.checkData(0,14,100)
        tdSql.checkData(0,15,100)
        tdSql.checkData(0,16,100)
        tdSql.checkData(0,17,100)
        tdSql.checkData(0,17,100)
        tdSql.checkData(0,19,100)
        tdSql.checkData(0,20,100)
        tdSql.checkData(0,21,100)
        tdSql.checkData(0,22,100)
        tdSql.checkData(0,23,100)
        tdSql.checkData(0,24,100)

    def regular1_checkall_0_NULL(self,sql):
        tdLog.info(sql)       
        tdSql.query(sql)
        tdSql.checkData(0,0,'01:46:30.000')
        tdSql.checkData(0,1,None)
        tdSql.checkData(0,2,None)
        tdSql.checkData(0,3,None)
        tdSql.checkData(0,4,None)
        tdSql.checkData(0,5,None)
        tdSql.checkData(0,6,None)
        tdSql.checkData(0,7,'None')
        tdSql.checkData(0,8,'None')
        tdSql.checkData(0,9,'None')
        tdSql.checkData(0,10,'None')
        tdSql.checkData(0,11,'None')
        tdSql.checkData(0,12,'None')
        tdSql.checkData(0,13,None)
        tdSql.checkData(0,14,None)
        tdSql.checkData(0,15,None)
        tdSql.checkData(0,16,None)
        tdSql.checkData(0,17,None)
        tdSql.checkData(0,18,None)
        tdSql.checkData(0,19,'None')
        tdSql.checkData(0,20,'None')
        tdSql.checkData(0,21,'None')
        tdSql.checkData(0,22,'None')
        tdSql.checkData(0,23,'None')
        tdSql.checkData(0,24,'None')

    def regular2_checkall_0(self,sql):
        tdLog.info(sql)      
        tdSql.query(sql)
        tdSql.checkData(0,0,'2021-08-27 01:46:40.000')
        tdSql.checkData(0,1,2147483647)
        tdSql.checkData(0,2,9223372036854775807)
        tdSql.checkData(0,3,32767)
        tdSql.checkData(0,4,127)
        tdSql.checkData(0,5,0)
        tdSql.checkData(0,6,0)
        tdSql.checkData(0,7,'None')
        tdSql.checkData(0,8,'None')
        tdSql.checkData(0,9,'None')
        tdSql.checkData(0,10,'None')
        tdSql.checkData(0,11,'None')
        tdSql.checkData(0,12,'None')
        tdSql.checkData(0,13,2147483647)
        tdSql.checkData(0,14,9223372036854775807)
        tdSql.checkData(0,15,32767)
        tdSql.checkData(0,16,127)
        tdSql.checkData(0,17,0)
        tdSql.checkData(0,18,0)
        tdSql.checkData(0,19,'None')
        tdSql.checkData(0,20,'None')
        tdSql.checkData(0,21,'None')
        tdSql.checkData(0,22,'None')
        tdSql.checkData(0,23,'None')
        tdSql.checkData(0,24,'None')

    def regular2_checkall_0_LINEAR(self,sql):
        tdLog.info(sql)       
        tdSql.query(sql)
        #print(tdSql.queryResult)
        tdSql.checkData(0,0,'2021-08-27 01:46:41.500')
        tdSql.checkData(0,1,2147483646)
        tdSql.checkData(0,2,9223372036854775806)
        tdSql.checkData(0,3,32766)
        tdSql.checkData(0,4,126)
        tdSql.checkData(0,5,0.5)
        tdSql.checkData(0,6,0.5)
        tdSql.checkData(0,7,'None')
        tdSql.checkData(0,8,'None')
        tdSql.checkData(0,9,'None')
        tdSql.checkData(0,10,'None')
        tdSql.checkData(0,11,'None')
        tdSql.checkData(0,12,'None')
        tdSql.checkData(0,13,2147483646)
        tdSql.checkData(0,14,9223372036854775806)
        tdSql.checkData(0,15,32766)
        tdSql.checkData(0,16,126)
        tdSql.checkData(0,17,0.5)
        tdSql.checkData(0,18,0.5)
        tdSql.checkData(0,19,'None')
        tdSql.checkData(0,20,'None')
        tdSql.checkData(0,21,'None')
        tdSql.checkData(0,22,'None')
        tdSql.checkData(0,23,'None')
        tdSql.checkData(0,24,'None')

    def regular2_checkall_0_NEXT(self,sql):
        tdLog.info(sql)       
        tdSql.query(sql)
        #print(tdSql.queryResult)
        tdSql.checkData(0,0,'2021-08-27 01:46:41.500')
        tdSql.checkData(0,1,2147483646)
        tdSql.checkData(0,2,9223372036854775806)
        tdSql.checkData(0,3,32766)
        tdSql.checkData(0,4,126)
        tdSql.checkData(0,5,1)
        tdSql.checkData(0,6,1)
        tdSql.checkData(0,7,'None')
        tdSql.checkData(0,8,'None')
        tdSql.checkData(0,9,'None')
        tdSql.checkData(0,10,'None')
        tdSql.checkData(0,11,'None')
        tdSql.checkData(0,12,'None')
        tdSql.checkData(0,13,2147483646)
        tdSql.checkData(0,14,9223372036854775806)
        tdSql.checkData(0,15,32766)
        tdSql.checkData(0,16,126)
        tdSql.checkData(0,17,1)
        tdSql.checkData(0,18,1)
        tdSql.checkData(0,19,'None')
        tdSql.checkData(0,20,'None')
        tdSql.checkData(0,21,'None')
        tdSql.checkData(0,22,'None')
        tdSql.checkData(0,23,'None')
        tdSql.checkData(0,24,'None')

    def regular3_checkall_0(self,sql):
        tdLog.info(sql)      
        tdSql.query(sql)
        tdSql.checkData(0,0,'2021-08-27 01:46:40.000')
        tdSql.checkData(0,1,-2147483647)
        tdSql.checkData(0,2,-9223372036854775807)
        tdSql.checkData(0,3,-32767)
        tdSql.checkData(0,4,-127)
        tdSql.checkData(0,5,-0)
        tdSql.checkData(0,6,-0)
        tdSql.checkData(0,7,'None')
        tdSql.checkData(0,8,'None')
        tdSql.checkData(0,9,'None')
        tdSql.checkData(0,10,'None')
        tdSql.checkData(0,11,'None')
        tdSql.checkData(0,12,'None')
        tdSql.checkData(0,13,-2147483647)
        tdSql.checkData(0,14,-9223372036854775807)
        tdSql.checkData(0,15,-32767)
        tdSql.checkData(0,16,-127)
        tdSql.checkData(0,17,-0)
        tdSql.checkData(0,18,-0)
        tdSql.checkData(0,19,'None')
        tdSql.checkData(0,20,'None')
        tdSql.checkData(0,21,'None')
        tdSql.checkData(0,22,'None')
        tdSql.checkData(0,23,'None')
        tdSql.checkData(0,24,'None')

    def regular3_checkall_0_LINEAR(self,sql):
        tdLog.info(sql)       
        tdSql.query(sql)
        #print(tdSql.queryResult)
        tdSql.checkData(0,0,'2021-08-27 01:46:41.500')
        tdSql.checkData(0,1,-2147483646)
        tdSql.checkData(0,2,-9223372036854775806)
        tdSql.checkData(0,3,-32766)
        tdSql.checkData(0,4,-126)
        tdSql.checkData(0,5,-0.5)
        tdSql.checkData(0,6,-0.5)
        tdSql.checkData(0,7,'None')
        tdSql.checkData(0,8,'None')
        tdSql.checkData(0,9,'None')
        tdSql.checkData(0,10,'None')
        tdSql.checkData(0,11,'None')
        tdSql.checkData(0,12,'None')
        tdSql.checkData(0,13,-2147483646)
        tdSql.checkData(0,14,-9223372036854775806)
        tdSql.checkData(0,15,-32766)
        tdSql.checkData(0,16,-126)
        tdSql.checkData(0,17,-0.5)
        tdSql.checkData(0,18,-0.5)
        tdSql.checkData(0,19,'None')
        tdSql.checkData(0,20,'None')
        tdSql.checkData(0,21,'None')
        tdSql.checkData(0,22,'None')
        tdSql.checkData(0,23,'None')
        tdSql.checkData(0,24,'None')

    def regular3_checkall_0_NEXT(self,sql):
        tdLog.info(sql)       
        tdSql.query(sql)
        #print(tdSql.queryResult)
        tdSql.checkData(0,0,'2021-08-27 01:46:41.500')
        tdSql.checkData(0,1,-2147483646)
        tdSql.checkData(0,2,-9223372036854775806)
        tdSql.checkData(0,3,-32766)
        tdSql.checkData(0,4,-126)
        tdSql.checkData(0,5,-1)
        tdSql.checkData(0,6,-1)
        tdSql.checkData(0,7,'None')
        tdSql.checkData(0,8,'None')
        tdSql.checkData(0,9,'None')
        tdSql.checkData(0,10,'None')
        tdSql.checkData(0,11,'None')
        tdSql.checkData(0,12,'None')
        tdSql.checkData(0,13,-2147483646)
        tdSql.checkData(0,14,-9223372036854775806)
        tdSql.checkData(0,15,-32766)
        tdSql.checkData(0,16,-126)
        tdSql.checkData(0,17,-1)
        tdSql.checkData(0,18,-1)
        tdSql.checkData(0,19,'None')
        tdSql.checkData(0,20,'None')
        tdSql.checkData(0,21,'None')
        tdSql.checkData(0,22,'None')
        tdSql.checkData(0,23,'None')
        tdSql.checkData(0,24,'None')

    def regular1_checkall_9(self,sql):
        tdLog.info(sql)      
        tdSql.query(sql)
        tdSql.checkData(0,0,'2021-08-27 01:48:10.000')
        tdSql.checkData(0,1,9)
        tdSql.checkData(0,2,9)
        tdSql.checkData(0,3,9)
        tdSql.checkData(0,4,9)
        tdSql.checkData(0,5,9)
        tdSql.checkData(0,6,9)
        tdSql.checkData(0,7,'None')
        tdSql.checkData(0,8,'None')
        tdSql.checkData(0,9,'None')
        tdSql.checkData(0,10,'None')
        tdSql.checkData(0,11,'None')
        tdSql.checkData(0,12,'None')
        tdSql.checkData(0,13,9)
        tdSql.checkData(0,14,9)
        tdSql.checkData(0,15,9)
        tdSql.checkData(0,16,9)
        tdSql.checkData(0,17,9)
        tdSql.checkData(0,18,9)
        tdSql.checkData(0,19,'None')
        tdSql.checkData(0,20,'None')
        tdSql.checkData(0,21,'None')
        tdSql.checkData(0,22,'None')
        tdSql.checkData(0,23,'None')
        tdSql.checkData(0,24,'None')

    def regular2_checkall_9(self,sql):
        tdLog.info(sql)      
        tdSql.query(sql)
        tdSql.checkData(0,0,'2021-08-27 01:48:10.000')
        tdSql.checkData(0,1,2147483638)
        tdSql.checkData(0,2,9223372036854775798)
        tdSql.checkData(0,3,32758)
        tdSql.checkData(0,4,118)
        tdSql.checkData(0,5,9)
        tdSql.checkData(0,6,9)
        tdSql.checkData(0,7,'None')
        tdSql.checkData(0,8,'None')
        tdSql.checkData(0,9,'None')
        tdSql.checkData(0,10,'None')
        tdSql.checkData(0,11,'None')
        tdSql.checkData(0,12,'None')
        tdSql.checkData(0,13,2147483638)
        tdSql.checkData(0,14,9223372036854775798)
        tdSql.checkData(0,15,32758)
        tdSql.checkData(0,16,118)
        tdSql.checkData(0,17,9)
        tdSql.checkData(0,18,9)
        tdSql.checkData(0,19,'None')
        tdSql.checkData(0,20,'None')
        tdSql.checkData(0,21,'None')
        tdSql.checkData(0,22,'None')
        tdSql.checkData(0,23,'None')
        tdSql.checkData(0,24,'None')

    def regular3_checkall_9(self,sql):
        tdLog.info(sql)      
        tdSql.query(sql)
        tdSql.checkData(0,0,'2021-08-27 01:48:10.000')
        tdSql.checkData(0,1,-2147483638)
        tdSql.checkData(0,2,-9223372036854775798)
        tdSql.checkData(0,3,-32758)
        tdSql.checkData(0,4,-118)
        tdSql.checkData(0,5,-9)
        tdSql.checkData(0,6,-9)
        tdSql.checkData(0,7,'None')
        tdSql.checkData(0,8,'None')
        tdSql.checkData(0,9,'None')
        tdSql.checkData(0,10,'None')
        tdSql.checkData(0,11,'None')
        tdSql.checkData(0,12,'None')
        tdSql.checkData(0,13,-2147483638)
        tdSql.checkData(0,14,-9223372036854775798)
        tdSql.checkData(0,15,-32758)
        tdSql.checkData(0,16,-118)
        tdSql.checkData(0,17,-9)
        tdSql.checkData(0,18,-9)
        tdSql.checkData(0,19,'None')
        tdSql.checkData(0,20,'None')
        tdSql.checkData(0,21,'None')
        tdSql.checkData(0,22,'None')
        tdSql.checkData(0,23,'None')
        tdSql.checkData(0,24,'None')

    def regular1_checkall_20_base(self,sql):
        tdLog.info(sql)      
        tdSql.query(sql)
        tdSql.checkData(18,0,'2021-08-27 01:48:10.000')
        tdSql.checkData(18,1,9)
        tdSql.checkData(18,2,9)
        tdSql.checkData(18,3,9)
        tdSql.checkData(18,4,9)
        tdSql.checkData(18,5,9)
        tdSql.checkData(18,6,9)
        tdSql.checkData(18,7,'None')
        tdSql.checkData(18,8,'None')
        tdSql.checkData(18,9,'None')
        tdSql.checkData(18,10,'None')
        tdSql.checkData(18,11,'None')
        tdSql.checkData(18,12,'None')

    def regular1_checkall_20(self,sql):
        tdLog.info(sql)      
        tdSql.query(sql)
        tdSql.checkData(18,0,'2021-08-27 01:48:10.000')
        tdSql.checkData(18,1,9)
        tdSql.checkData(18,2,9)
        tdSql.checkData(18,3,9)
        tdSql.checkData(18,4,9)
        tdSql.checkData(18,5,9)
        tdSql.checkData(18,6,9)
        tdSql.checkData(18,7,'None')
        tdSql.checkData(18,8,'None')
        tdSql.checkData(18,9,'None')
        tdSql.checkData(18,10,'None')
        tdSql.checkData(18,11,'None')
        tdSql.checkData(18,12,'None')
        tdSql.checkData(18,13,9)
        tdSql.checkData(18,14,9)
        tdSql.checkData(18,15,9)
        tdSql.checkData(18,16,9)
        tdSql.checkData(18,17,9)
        tdSql.checkData(18,18,9)
        tdSql.checkData(18,19,'None')
        tdSql.checkData(18,20,'None')
        tdSql.checkData(18,21,'None')
        tdSql.checkData(18,22,'None')
        tdSql.checkData(18,23,'None')
        tdSql.checkData(18,24,'None')

    def regular2_checkall_20(self,sql):
        tdLog.info(sql)      
        tdSql.query(sql)
        tdSql.checkData(18,0,'2021-08-27 01:48:10.000')
        tdSql.checkData(18,1,2147483638)
        tdSql.checkData(18,2,9223372036854775798)
        tdSql.checkData(18,3,32758)
        tdSql.checkData(18,4,118)
        tdSql.checkData(18,5,9)
        tdSql.checkData(18,6,9)
        tdSql.checkData(18,7,'None')
        tdSql.checkData(18,8,'None')
        tdSql.checkData(18,9,'None')
        tdSql.checkData(18,10,'None')
        tdSql.checkData(18,11,'None')
        tdSql.checkData(18,12,'None')
        tdSql.checkData(18,13,2147483638)
        tdSql.checkData(18,14,9223372036854775798)
        tdSql.checkData(18,15,32758)
        tdSql.checkData(18,16,118)
        tdSql.checkData(18,17,9)
        tdSql.checkData(18,18,9)
        tdSql.checkData(18,19,'None')
        tdSql.checkData(18,20,'None')
        tdSql.checkData(18,21,'None')
        tdSql.checkData(18,22,'None')
        tdSql.checkData(18,23,'None')
        tdSql.checkData(18,24,'None')

    def regular3_checkall_20(self,sql):
        tdLog.info(sql)      
        tdSql.query(sql)
        tdSql.checkData(18,0,'2021-08-27 01:48:10.000')
        tdSql.checkData(18,1,-2147483638)
        tdSql.checkData(18,2,-9223372036854775798)
        tdSql.checkData(18,3,-32758)
        tdSql.checkData(18,4,-118)
        tdSql.checkData(18,5,-9)
        tdSql.checkData(18,6,-9)
        tdSql.checkData(18,7,'None')
        tdSql.checkData(18,8,'None')
        tdSql.checkData(18,9,'None')
        tdSql.checkData(18,10,'None')
        tdSql.checkData(18,11,'None')
        tdSql.checkData(18,12,'None')
        tdSql.checkData(18,13,-2147483638)
        tdSql.checkData(18,14,-9223372036854775798)
        tdSql.checkData(18,15,-32758)
        tdSql.checkData(18,16,-118)
        tdSql.checkData(18,17,-9)
        tdSql.checkData(18,18,-9)
        tdSql.checkData(18,19,'None')
        tdSql.checkData(18,20,'None')
        tdSql.checkData(18,21,'None')
        tdSql.checkData(18,22,'None')
        tdSql.checkData(18,23,'None')
        tdSql.checkData(18,24,'None')

    def stable1_checkall_0(self,sql):
        self.regular1_checkall_0(sql)


    def run(self):
        tdSql.prepare()
        os.system("rm -rf functions/function_interp.py.sql")
        startTime = time.time() 
        dcDB = self.dropandcreateDB_null()

        print("==============step1, regualr table ==============")

        interp_select = 'interp(q_int),interp(q_bigint),interp(q_smallint),interp(q_tinyint),interp(q_float),interp(q_double),\
                        interp(q_int_null),interp(q_bigint_null),interp(q_smallint_null),interp(q_tinyint_null),interp(q_float_null),interp(q_double_null),\
                        interp(q_int,q_bigint,q_smallint,q_tinyint,q_float,q_double),interp(q_int_null,q_bigint_null,q_smallint_null,q_tinyint_null,q_float_null,q_double_null)'
        
        sql = "select  * from regular_table_1 ;"
        datacheck = self.regular1_checkall_0_base(sql)           
        tdSql.checkRows(19)
        datacheck = self.regular1_checkall_20_base(sql)      

        sql = "select %s from regular_table_1 ;" %interp_select
        datacheck = self.regular1_checkall_0(sql)  
        sql = "select %s from regular_table_2 ;" %interp_select
        datacheck = self.regular2_checkall_0(sql)  
        sql = "select %s from regular_table_3 ;" %interp_select
        datacheck = self.regular3_checkall_0(sql) 
        
        sql = "select %s from regular_table_1 ORDER BY ts DESC;" %interp_select
        datacheck = self.regular1_checkall_0(sql)  
        sql = "select %s from regular_table_2 ORDER BY ts DESC;" %interp_select
        datacheck = self.regular2_checkall_0(sql)  
        sql = "select %s from regular_table_3 ORDER BY ts DESC;" %interp_select
        datacheck = self.regular3_checkall_0(sql) 

        sql = "select %s from regular_table_1 where "  %interp_select
        sql += "ts > %s  ;" % (self.ts - 10) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from regular_table_2 where "   %interp_select
        sql += "ts > %s  ;" % (self.ts - 10) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from regular_table_3 where "  %interp_select
        sql += "ts > %s  ;" % (self.ts - 10) 
        datacheck = self.regular3_checkall_0(sql)   

        sql = "select %s from regular_table_1 where "  %interp_select
        sql += "ts > %s  ;" % (self.ts + 85000) 
        datacheck = self.regular1_checkall_9(sql)  
        sql = "select %s from regular_table_2 where "  %interp_select
        sql += "ts > %s  ;" % (self.ts + 85000) 
        datacheck = self.regular2_checkall_9(sql)  
        sql = "select %s from regular_table_3 where "  %interp_select
        sql += "ts > %s  ;" % (self.ts + 85000) 
        datacheck = self.regular3_checkall_9(sql) 

        # range
        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s');" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_2  " % interp_select    
        sql += "range('%s' , '%s');" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_3  " % interp_select    
        sql += "range('%s' , '%s');" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql)

        sql = "select %s from regular_table_1  " % interp_select    
        sql += "range('%s' , '%s');" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_2  "  %interp_select
        sql += "range('%s' , '%s');" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_3  "  %interp_select
        sql += "range('%s' , '%s');" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from regular_table_1  "  %interp_select
        sql += "range('%s' , '%s');" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from regular_table_2  "  %interp_select
        sql += "range('%s' , '%s');" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from regular_table_3  "  %interp_select
        sql += "range('%s' , '%s');" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from regular_table_1  "  %interp_select
        sql += "range('%s' , '%s');" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_2  "  %interp_select
        sql += "range('%s' , '%s');" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_3  "  %interp_select
        sql += "range('%s' , '%s');" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.result_0(sql)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s');" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s');" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s');" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # FILL(LINEAR)
        sql = "select %s from regular_table_1  FILL(LINEAR)"  %interp_select
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from regular_table_2  FILL(LINEAR)"  %interp_select
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from regular_table_3  FILL(LINEAR)"  %interp_select
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_LINEAR(sql) 
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_LINEAR(sql) 
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_LINEAR(sql) 

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # FILL(LINEAR) + EVERY( s)
        sql = "select %s from regular_table_1  EVERY(1s) FILL(LINEAR);"  %interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(91)
        sql = "select %s from regular_table_2  EVERY(1s) FILL(LINEAR);"  %interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(91) 
        sql = "select %s from regular_table_3  EVERY(1s) FILL(LINEAR);"  %interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(91) 

        sql = "select %s from regular_table_1  EVERY(5s) FILL(LINEAR);"  %interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(19)
        sql = "select %s from regular_table_2  EVERY(5s) FILL(LINEAR);"  %interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(19) 
        sql = "select %s from regular_table_3  EVERY(5s) FILL(LINEAR);"  %interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(19) 

        # range + FILL(LINEAR) + EVERY( s)
        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(91)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(91)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(91)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(91)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(91)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(91)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_9(sql) 
        tdSql.checkRows(91)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_9(sql) 
        tdSql.checkRows(91)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_9(sql)
        tdSql.checkRows(91)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # where + range + FILL(LINEAR) + EVERY( s)
        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(11)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(11)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0_NEXT(sql)
        tdSql.checkRows(11)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # FILL(NEXT)
        sql = "select %s from regular_table_1  FILL(NEXT)"  %interp_select
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from regular_table_2  FILL(NEXT)"  %interp_select
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from regular_table_3  FILL(NEXT)"  %interp_select
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular3_checkall_0(sql) 

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_NEXT(sql) 

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # FILL(NEXT) + EVERY( s)
        sql = "select %s from regular_table_1  EVERY(1s) FILL(NEXT);"  %interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(91)
        sql = "select %s from regular_table_2  EVERY(1s) FILL(NEXT);"  %interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(91) 
        sql = "select %s from regular_table_3  EVERY(1s) FILL(NEXT);"  %interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(91) 

        sql = "select %s from regular_table_1  EVERY(5s) FILL(NEXT);"  %interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(19)
        sql = "select %s from regular_table_2  EVERY(5s) FILL(NEXT);"  %interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(19) 
        sql = "select %s from regular_table_3  EVERY(5s) FILL(NEXT);"  %interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(19) 

        # range + FILL(NEXT) + EVERY( s)
        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(101)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(101)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(101)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(1)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(91)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(91)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(91)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_9(sql) 
        tdSql.checkRows(91)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_9(sql) 
        tdSql.checkRows(91)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_9(sql)
        tdSql.checkRows(91)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # where + range + FILL(NEXT) + EVERY( s)
        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(21)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(21)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(21)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(1)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(11)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0_NEXT(sql)
        tdSql.checkRows(11)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # FILL(PREV)
        sql = "select %s from regular_table_1  FILL(PREV)"  %interp_select
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from regular_table_2  FILL(PREV)"  %interp_select
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from regular_table_3  FILL(PREV)"  %interp_select
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql) 
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql) 
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0(sql) 

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_9(sql) 
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_9(sql) 
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular3_checkall_9(sql) 

        # FILL(PREV) + EVERY( s)
        sql = "select %s from regular_table_1  EVERY(1s) FILL(PREV);"  %interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(91)
        sql = "select %s from regular_table_2  EVERY(1s) FILL(PREV);"  %interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(91) 
        sql = "select %s from regular_table_3  EVERY(1s) FILL(PREV);"  %interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(91) 

        sql = "select %s from regular_table_1  EVERY(5s) FILL(PREV);"  %interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(19)
        sql = "select %s from regular_table_2  EVERY(5s) FILL(PREV);"  %interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(19) 
        sql = "select %s from regular_table_3  EVERY(5s) FILL(PREV);"  %interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(19) 

        # range + FILL(PREV) + EVERY( s)
        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(101)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(101)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(101)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_9(sql) 
        tdSql.checkRows(201)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_9(sql) 
        tdSql.checkRows(201)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_9(sql)
        tdSql.checkRows(201)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(2)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(2)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(2)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_9(sql) 
        tdSql.checkRows(51)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_9(sql) 
        tdSql.checkRows(51)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular3_checkall_9(sql) 
        tdSql.checkRows(51)

        # where + range + FILL(PREV) + EVERY( s)
        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(101)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(101)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(101)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(201)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(201)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0_NEXT(sql)
        tdSql.checkRows(201)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(2)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(2)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(2)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(51)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(51)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(51)

        # FILL(VALUE，100)
        sql = "select %s from regular_table_1  FILL(VALUE, 100)"  %interp_select
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from regular_table_2  FILL(VALUE, 100)"  %interp_select
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from regular_table_3  FILL(VALUE, 100)"  %interp_select
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_VALUE100(sql) 

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_VALUE100(sql)

        # FILL(VALUE，100) + EVERY( s)
        sql = "select %s from regular_table_1  EVERY(1s) FILL(VALUE, 100);"  %interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(91)
        sql = "select %s from regular_table_2  EVERY(1s) FILL(VALUE, 100);"  %interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(91) 
        sql = "select %s from regular_table_3  EVERY(1s) FILL(VALUE, 100);"  %interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(91) 

        sql = "select %s from regular_table_1  EVERY(5s) FILL(VALUE, 100);"  %interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(19)
        sql = "select %s from regular_table_2  EVERY(5s) FILL(VALUE, 100);"  %interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(19) 
        sql = "select %s from regular_table_3  EVERY(5s) FILL(VALUE, 100);"  %interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(19) 

        # range + FILL(VALUE, 100) + EVERY( s)
        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(201)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(201)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(201)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(2)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(2)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(2)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(51)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(51)

        # where + range + FILL(VALUE, 100) + EVERY( s)
        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(201)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(201)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(201)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(2)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(2)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(2)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)

        # FILL(NULL)
        sql = "select %s from regular_table_1  FILL(NULL)"  %interp_select
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from regular_table_2  FILL(NULL)"  %interp_select
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from regular_table_3  FILL(NULL)"  %interp_select
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_NULL(sql) 
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_NULL(sql) 

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_NULL(sql)


        # FILL(NULL) + EVERY( s)
        sql = "select %s from regular_table_1  EVERY(1s) FILL(NULL);"  %interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(91)
        sql = "select %s from regular_table_2  EVERY(1s) FILL(NULL);"  %interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(91) 
        sql = "select %s from regular_table_3  EVERY(1s) FILL(NULL);"  %interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(91) 

        sql = "select %s from regular_table_1  EVERY(5s) FILL(NULL);"  %interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(19)
        sql = "select %s from regular_table_2  EVERY(5s) FILL(NULL);"  %interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(19) 
        sql = "select %s from regular_table_3  EVERY(5s) FILL(NULL);"  %interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(19) 

        # range + FILL(NULL) + EVERY( s)
        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(201)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(201)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(201)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(2)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(2)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(2)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(51)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(51)

        # where + range + FILL(NULL) + EVERY( s)
        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(201)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(201)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(201)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(2)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(2)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(2)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)


        # EVERY(1s)
        sql = "select %s from regular_table_1  EVERY(1s);"  %interp_select
        datacheck = self.regular1_checkall_0(sql) 
        datacheck = self.regular1_checkall_20(sql) 
        sql = "select %s from regular_table_2  EVERY(1s);"  %interp_select
        datacheck = self.regular2_checkall_0(sql) 
        datacheck = self.regular2_checkall_20(sql) 
        sql = "select %s from regular_table_3  EVERY(1s);"  %interp_select
        datacheck = self.regular3_checkall_0(sql)
        datacheck = self.regular3_checkall_20(sql) 

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        datacheck = self.regular1_checkall_20(sql) 
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        datacheck = self.regular2_checkall_20(sql) 
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql)
        datacheck = self.regular3_checkall_20(sql) 

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts + 10000 ,  self.ts + 25000) 
        datacheck = self.regular1_checkall_0_NEXT(sql)
        tdSql.checkRows(7)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts + 10000 ,  self.ts + 25000)  
        datacheck = self.regular2_checkall_0_NEXT(sql)
        tdSql.checkRows(7)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts + 10000 ,  self.ts + 25000)   
        datacheck = self.regular3_checkall_0_NEXT(sql)
        tdSql.checkRows(7)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # EVERY(2s)
        sql = "select %s from regular_table_1  EVERY(2s);"  %interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(14)
        sql = "select %s from regular_table_2  EVERY(2s);"  %interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(14)
        sql = "select %s from regular_table_3  EVERY(2s);"  %interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(14)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(14)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(14)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(14)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts + 10000 ,  self.ts + 25000) 
        datacheck = self.regular1_checkall_0_NEXT(sql)
        tdSql.checkRows(5)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts + 10000 ,  self.ts + 25000)  
        datacheck = self.regular2_checkall_0_NEXT(sql)
        tdSql.checkRows(5)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts + 10000 ,  self.ts + 25000)   
        datacheck = self.regular3_checkall_0_NEXT(sql)
        tdSql.checkRows(5)

        sql = "select %s from regular_table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from regular_table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # EVERY(5s)
        sql = "select %s from regular_table_1  EVERY(5s);"  %interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from regular_table_2  EVERY(5s);"  %interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from regular_table_3  EVERY(5s);"  %interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(11)

        # EVERY(100s)
        sql = "select %s from regular_table_1  EVERY(100s);"  %interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_2  EVERY(100s);"  %interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from regular_table_3  EVERY(100s);"  %interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)


        # error
        tdSql.error("select interp(*) from regular_table_1")
        tdSql.error("select interp(*) from regular_table_2 FILL(NEXT)") 
        sql = "select interp(*) from regular_table_3 where ts >= '%s' FILL(NULL);" % self.ts
        tdSql.error(sql)  
        sql = "select interp(*) from regular_table_1 where ts <= '%s' FILL(LINEAR);" % self.ts
        tdSql.error(sql)  
        sql = "select interp(*) from regular_table_2 where ts > '%s' and ts < now every(1s) fill(PREV);" % self.ts
        tdSql.error(sql)  

        sql = "select %s from regular_table_1  " % interp_select  
        sql += "range('%s' , '%s') EVERY(1s) FILL (LINEAR);" % (self.ts + 150000 ,  self.ts + 100000) 
        tdSql.error(sql)
        sql = "select %s from regular_table_2  " % interp_select  
        sql += "range('%s' , '%s') EVERY(2s) FILL (PREV);" % (self.ts + 150000 ,  self.ts - 100000) 
        tdSql.error(sql)
        sql = "select %s from regular_table_3  " % interp_select  
        sql += "range('%s' , '%s') EVERY(5s) FILL (NEXT);" % (self.ts - 150000 ,  self.ts - 200000) 
        tdSql.error(sql)

        sql = "select %s from regular_table_1  " % interp_select  
        sql += "range('%s' , '%s') EVERY(10s) FILL (NULL);" % (self.ts + 150000 ,  self.ts + 100000) 
        tdSql.error(sql)
        sql = "select %s from regular_table_2  " % interp_select  
        sql += "range('%s' , '%s') EVERY(60s) FILL (VALUE，100);" % (self.ts + 150000 ,  self.ts - 100000) 
        tdSql.error(sql)
        sql = "select %s from regular_table_3  " % interp_select  
        sql += "range('%s' , '%s') EVERY(120s) FILL (LINEAR) ORDER BY ts DESC;" % (self.ts - 150000 ,  self.ts - 200000) 
        tdSql.error(sql)
       
        # Nested Query
        sql = "select * from (select %s from regular_table_1) ;" % interp_select 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select * from (select %s from regular_table_2) ;" % interp_select 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select * from (select %s from regular_table_3) ;" % interp_select 
        datacheck = self.regular3_checkall_0(sql) 

        sql = "select %s from (select * from regular_table_1) ;" % interp_select 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from (select * from regular_table_2) ;" % interp_select 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from (select * from regular_table_3) ;" % interp_select 
        datacheck = self.regular3_checkall_0(sql) 

        # Nested Query + where + range + FILL(LINEAR) + EVERY( s) (1)
        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(11)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(11)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # Nested Query + where + range + FILL(LINEAR) + EVERY( s)(2)
        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from (select * from regular_table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(11)

        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from (select * from regular_table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from regular_table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from (select * from regular_table_3 )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(11)

        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)
        sql = "select %s from (select * from regular_table_3  )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)

        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from (select * from regular_table_3  )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # Nested Query + where + range + FILL(LINEAR) + EVERY( s)(3)
        sql = "select * from (select %s from regular_table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        sql += "(select %s from regular_table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)

        sql = "select * from (select %s from regular_table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        sql += "(select %s from regular_table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        datacheck = self.result_0(sql)

        sql = "select * from (select %s from regular_table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        sql += "(select %s from regular_table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(1)

        sql = "select * from (select %s from regular_table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        sql += "(select %s from regular_table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)        

        sql = "select * from (select %s from regular_table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        sql += "(select %s from regular_table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        datacheck = self.regular2_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)   

        sql = "select * from (select %s from regular_table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        sql += "(select %s from regular_table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        datacheck = self.result_0(sql)   

        # Nested Query + where + range + FILL(NEXT) + EVERY( s) (1)
        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(21)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(21)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(21)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(1)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(11)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # Nested Query + where + range + FILL(NEXT) + EVERY( s)(2)
        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(21)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(21)
        sql = "select %s from (select * from regular_table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(21)

        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from regular_table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(1)

        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from regular_table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from (select * from regular_table_3 )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(11)

        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)
        sql = "select %s from (select * from regular_table_3  )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)

        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from (select * from regular_table_3  )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # Nested Query + where + range + FILL(NEXT) + EVERY( s)(3)
        sql = "select * from (select %s from regular_table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        sql += "(select %s from regular_table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(21)

        sql = "select * from (select %s from regular_table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        sql += "(select %s from regular_table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)

        sql = "select * from (select %s from regular_table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        sql += "(select %s from regular_table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(1)

        sql = "select * from (select %s from regular_table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        sql += "(select %s from regular_table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)        

        sql = "select * from (select %s from regular_table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        sql += "(select %s from regular_table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)   

        sql = "select * from (select %s from regular_table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        sql += "(select %s from regular_table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        datacheck = self.result_0(sql)   

        # Nested Query + where + range + FILL(PREV) + EVERY( s) (1)
        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(101)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(101)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(101)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(201)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(201)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(201)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(2)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(2)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(2)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(51)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(51)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(51)

        # Nested Query + where + range + FILL(PREV) + EVERY( s)(2)
        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(101)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(101)
        sql = "select %s from (select * from regular_table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(101)

        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from (select * from regular_table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from regular_table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from (select * from regular_table_3 )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(2)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(2)
        sql = "select %s from (select * from regular_table_3  )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(2)

        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(51)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(51)
        sql = "select %s from (select * from regular_table_3  )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(51)

        # Nested Query + where + range + FILL(PREV) + EVERY( s)(3)
        sql = "select * from (select %s from regular_table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        sql += "(select %s from regular_table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(101)

        sql = "select * from (select %s from regular_table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        sql += "(select %s from regular_table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        datacheck = self.result_0(sql)

        sql = "select * from (select %s from regular_table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        sql += "(select %s from regular_table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(1)

        sql = "select * from (select %s from regular_table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        sql += "(select %s from regular_table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)        

        sql = "select * from (select %s from regular_table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        sql += "(select %s from regular_table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(2)   

        sql = "select * from (select %s from regular_table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        sql += "(select %s from regular_table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(51)  

        # Nested Query + where + range + FILL(VALUE, 100) + EVERY( s) (1)
        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(201)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(201)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(201)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(2)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(2)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(2)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)

        # Nested Query + where + range + FILL(VALUE, 100) + EVERY( s)(2)
        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)
        sql = "select %s from (select * from regular_table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)

        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from regular_table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)

        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from regular_table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from (select * from regular_table_3 )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(2)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(2)
        sql = "select %s from (select * from regular_table_3  )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(2)

        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)
        sql = "select %s from (select * from regular_table_3  )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)

        # Nested Query + where + range + FILL(VALUE, 100) + EVERY( s)(3)
        sql = "select * from (select %s from regular_table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        sql += "(select %s from regular_table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)

        sql = "select * from (select %s from regular_table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        sql += "(select %s from regular_table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)

        sql = "select * from (select %s from regular_table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        sql += "(select %s from regular_table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(1)

        sql = "select * from (select %s from regular_table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        sql += "(select %s from regular_table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)        

        sql = "select * from (select %s from regular_table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        sql += "(select %s from regular_table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(2)    

        sql = "select * from (select %s from regular_table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        sql += "(select %s from regular_table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)    

        # Nested Query + where + range + FILL(NULL) + EVERY( s) (1)
        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(201)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(201)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(201)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(2)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(2)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(2)

        sql = "select * from (select %s from regular_table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)
        sql = "select * from (select %s from regular_table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)
        sql = "select * from (select %s from regular_table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)

        # Nested Query + where + range + FILL(NULL) + EVERY( s)(2)
        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)
        sql = "select %s from (select * from regular_table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)

        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from regular_table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)

        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from regular_table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from (select * from regular_table_3 )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(2)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(2)
        sql = "select %s from (select * from regular_table_3  )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(2)

        sql = "select %s from (select * from regular_table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)
        sql = "select %s from (select * from regular_table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)
        sql = "select %s from (select * from regular_table_3  )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)

        # Nested Query + where + range + FILL(NULL) + EVERY( s)(3)
        sql = "select * from (select %s from regular_table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        sql += "(select %s from regular_table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)

        sql = "select * from (select %s from regular_table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        sql += "(select %s from regular_table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)

        sql = "select * from (select %s from regular_table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        sql += "(select %s from regular_table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(1)

        sql = "select * from (select %s from regular_table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        sql += "(select %s from regular_table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)        

        sql = "select * from (select %s from regular_table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        sql += "(select %s from regular_table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(2)    

        sql = "select * from (select %s from regular_table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        sql += "(select %s from regular_table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)    

        print("==============step2, super table: child table==============")

        dcDB = self.dropandcreateDB()
        
        dcDB = self.dropandcreateDB_null()

        dcRestart = self.restartDnodes()

        interp_select = 'interp(q_int),interp(q_bigint),interp(q_smallint),interp(q_tinyint),interp(q_float),interp(q_double),\
                        interp(q_int_null),interp(q_bigint_null),interp(q_smallint_null),interp(q_tinyint_null),interp(q_float_null),interp(q_double_null),\
                        interp(q_int,q_bigint,q_smallint,q_tinyint,q_float,q_double),interp(q_int_null,q_bigint_null,q_smallint_null,q_tinyint_null,q_float_null,q_double_null),loc'

        sql = "select  * from table_1 ;"
        datacheck = self.regular1_checkall_0_base(sql)           
        tdSql.checkRows(19)
        datacheck = self.regular1_checkall_20_base(sql)       

        sql = "select %s from table_1 ;" % interp_select
        datacheck = self.regular1_checkall_0(sql)   
        sql = "select %s from table_2 ;" % interp_select
        datacheck = self.regular2_checkall_0(sql)  
        sql = "select %s from table_3 ;" % interp_select
        datacheck = self.regular3_checkall_0(sql) 

        sql = "select %s from table_1 ORDER BY ts DESC;" % interp_select
        datacheck = self.regular1_checkall_0(sql)  
        sql = "select %s from table_2 ORDER BY ts DESC;" % interp_select
        datacheck = self.regular2_checkall_0(sql)  
        sql = "select %s from table_3 ORDER BY ts DESC;" % interp_select
        datacheck = self.regular3_checkall_0(sql) 

        sql = "select %s from table_1 where "  % interp_select
        sql += "ts > %s  ;" % (self.ts - 10) 
        datacheck = self.regular1_checkall_0(sql)   
        sql = "select %s from table_2 where "  % interp_select
        sql += "ts > %s  ;" % (self.ts - 10) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from table_3 where "  % interp_select
        sql += "ts > %s  ;" % (self.ts - 10) 
        datacheck = self.regular3_checkall_0(sql) 

        sql = "select %s from table_1 where "  % interp_select
        sql += "ts > %s  ;" % (self.ts + 85000) 
        datacheck = self.regular1_checkall_9(sql)  
        sql = "select %s from table_2 where "  % interp_select
        sql += "ts > %s  ;" % (self.ts + 85000) 
        datacheck = self.regular2_checkall_9(sql)  
        sql = "select %s from table_3 where "  % interp_select
        sql += "ts > %s  ;" % (self.ts + 85000) 
        datacheck = self.regular3_checkall_9(sql) 

        # range
        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s');" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s');" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s');" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s');" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s');" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s');" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s');" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s');" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s');" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s');" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s');" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.result_0(sql)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s');" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.result_0(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s');" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s');" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s');" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # FILL(LINEAR)
        sql = "select %s from table_1  FILL(LINEAR)"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from table_2  FILL(LINEAR)"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from table_3  FILL(LINEAR)"  % interp_select
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_LINEAR(sql) 
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_LINEAR(sql) 
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_LINEAR(sql) 

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR);" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # FILL(LINEAR) + EVERY( s)
        sql = "select %s from table_1  EVERY(1s) FILL(LINEAR);"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(91)
        sql = "select %s from table_2  EVERY(1s) FILL(LINEAR);"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(91) 
        sql = "select %s from table_3  EVERY(1s) FILL(LINEAR);"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(91) 

        sql = "select %s from table_1  EVERY(5s) FILL(LINEAR);"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(19)
        sql = "select %s from table_2  EVERY(5s) FILL(LINEAR);"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(19) 
        sql = "select %s from table_3  EVERY(5s) FILL(LINEAR);"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(19) 

        # range + FILL(LINEAR) + EVERY( s)
        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(91)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(91)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(91)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(91)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(91)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(91)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_9(sql) 
        tdSql.checkRows(91)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_9(sql) 
        tdSql.checkRows(91)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_9(sql)
        tdSql.checkRows(91)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # where + range + FILL(LINEAR) + EVERY( s)
        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(11)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(11)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0_NEXT(sql)
        tdSql.checkRows(11)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # FILL(NEXT)
        sql = "select %s from table_1  FILL(NEXT)"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from table_2  FILL(NEXT)"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from table_3  FILL(NEXT)"  % interp_select
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular3_checkall_0(sql) 

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_NEXT(sql) 

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT);" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # FILL(NEXT) + EVERY( s)
        sql = "select %s from table_1  EVERY(1s) FILL(NEXT);"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(91)
        sql = "select %s from table_2  EVERY(1s) FILL(NEXT);"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(91) 
        sql = "select %s from table_3  EVERY(1s) FILL(NEXT);"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(91) 

        sql = "select %s from table_1  EVERY(5s) FILL(NEXT);"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(19)
        sql = "select %s from table_2  EVERY(5s) FILL(NEXT);"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(19) 
        sql = "select %s from table_3  EVERY(5s) FILL(NEXT);"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(19) 

        # range + FILL(NEXT) + EVERY( s)
        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(101)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(101)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(101)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(1)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(91)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(91)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(91)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_9(sql) 
        tdSql.checkRows(91)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_9(sql) 
        tdSql.checkRows(91)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_9(sql)
        tdSql.checkRows(91)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # where + range + FILL(NEXT) + EVERY( s)
        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(21)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(21)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(21)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(1)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(11)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0_NEXT(sql)
        tdSql.checkRows(11)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # FILL(PREV)
        sql = "select %s from table_1  FILL(PREV)"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from table_2  FILL(PREV)"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from table_3  FILL(PREV)"  % interp_select
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql) 
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql) 
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0(sql) 

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_9(sql) 
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_9(sql) 
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV);" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular3_checkall_9(sql) 

        # FILL(PREV) + EVERY( s)
        sql = "select %s from table_1  EVERY(1s) FILL(PREV);"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(91)
        sql = "select %s from table_2  EVERY(1s) FILL(PREV);"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(91) 
        sql = "select %s from table_3  EVERY(1s) FILL(PREV);"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(91) 

        sql = "select %s from table_1  EVERY(5s) FILL(PREV);"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(19)
        sql = "select %s from table_2  EVERY(5s) FILL(PREV);"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(19) 
        sql = "select %s from table_3  EVERY(5s) FILL(PREV);"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(19) 

        # range + FILL(PREV) + EVERY( s)
        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(101)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(101)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(101)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_9(sql) 
        tdSql.checkRows(201)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_9(sql) 
        tdSql.checkRows(201)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_9(sql)
        tdSql.checkRows(201)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(2)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(2)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(2)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_9(sql) 
        tdSql.checkRows(51)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_9(sql) 
        tdSql.checkRows(51)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular3_checkall_9(sql) 
        tdSql.checkRows(51)

        # where + range + FILL(PREV) + EVERY( s)
        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(101)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(101)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(101)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(201)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(201)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0_NEXT(sql)
        tdSql.checkRows(201)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(2)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(2)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(2)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(51)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(51)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(51)

        # FILL(VALUE，100)
        sql = "select %s from table_1  FILL(VALUE, 100)"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from table_2  FILL(VALUE, 100)"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from table_3  FILL(VALUE, 100)"  % interp_select
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_VALUE100(sql) 

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100);" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_VALUE100(sql)

        # FILL(VALUE，100) + EVERY( s)
        sql = "select %s from table_1  EVERY(1s) FILL(VALUE, 100);"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(91)
        sql = "select %s from table_2  EVERY(1s) FILL(VALUE, 100);"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(91) 
        sql = "select %s from table_3  EVERY(1s) FILL(VALUE, 100);"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(91) 

        sql = "select %s from table_1  EVERY(5s) FILL(VALUE, 100);"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(19)
        sql = "select %s from table_2  EVERY(5s) FILL(VALUE, 100);"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(19) 
        sql = "select %s from table_3  EVERY(5s) FILL(VALUE, 100);"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(19) 

        # FILL(NULL) + EVERY( s)
        sql = "select %s from table_1  EVERY(1s) FILL(NULL);"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(91)
        sql = "select %s from table_2  EVERY(1s) FILL(NULL);"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(91) 
        sql = "select %s from table_3  EVERY(1s) FILL(NULL);"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(91) 

        sql = "select %s from table_1  EVERY(5s) FILL(NULL);"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(19)
        sql = "select %s from table_2  EVERY(5s) FILL(NULL);"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(19) 
        sql = "select %s from table_3  EVERY(5s) FILL(NULL);"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(19) 

        # range + FILL(VALUE, 100) + EVERY( s)
        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(201)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(201)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(201)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(2)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(2)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(2)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(51)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(51)

        # where + range + FILL(VALUE, 100) + EVERY( s)
        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(201)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(201)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(201)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(2)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(2)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(2)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)

        # FILL(NULL)
        sql = "select %s from table_1  FILL(NULL)"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from table_2  FILL(NULL)"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from table_3  FILL(NULL)"  % interp_select
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_NULL(sql) 
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_NULL(sql) 

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL);" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_NULL(sql)

        # range + FILL(NULL) + EVERY( s)
        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(201)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(201)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL) ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(201)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(2)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(2)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(2)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(51)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(51)

        # where + range + FILL(NULL) + EVERY( s)
        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(201)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(201)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(201)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(2)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(2)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(2)

        sql = "select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)
        sql = "select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)
        sql = "select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)

        # EVERY(1s)
        sql = "select %s from table_1  EVERY(1s);"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        datacheck = self.regular1_checkall_20(sql) 
        sql = "select %s from table_2  EVERY(1s);"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        datacheck = self.regular2_checkall_20(sql) 
        sql = "select %s from table_3  EVERY(1s);"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        datacheck = self.regular3_checkall_20(sql) 

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        datacheck = self.regular1_checkall_20(sql) 
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        datacheck = self.regular2_checkall_20(sql) 
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql)
        datacheck = self.regular3_checkall_20(sql) 

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts + 10000 ,  self.ts + 25000) 
        datacheck = self.regular1_checkall_0_NEXT(sql)
        tdSql.checkRows(7)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts + 10000 ,  self.ts + 25000)  
        datacheck = self.regular2_checkall_0_NEXT(sql)
        tdSql.checkRows(7)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts + 10000 ,  self.ts + 25000)   
        datacheck = self.regular3_checkall_0_NEXT(sql)
        tdSql.checkRows(7)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s);" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # EVERY(2s)
        sql = "select %s from table_1  EVERY(2s);"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(14)
        sql = "select %s from table_2  EVERY(2s);"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(14)
        sql = "select %s from table_3  EVERY(2s);"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(14)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(14)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(14)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(14)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts + 10000 ,  self.ts + 25000) 
        datacheck = self.regular1_checkall_0_NEXT(sql)
        tdSql.checkRows(5)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts + 10000 ,  self.ts + 25000)  
        datacheck = self.regular2_checkall_0_NEXT(sql)
        tdSql.checkRows(5)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts + 10000 ,  self.ts + 25000)   
        datacheck = self.regular3_checkall_0_NEXT(sql)
        tdSql.checkRows(5)

        sql = "select %s from table_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_2  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from table_3  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s);" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # EVERY(5s)
        sql = "select %s from table_1  EVERY(5s);"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from table_2  EVERY(5s);"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from table_3  EVERY(5s);"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(11)

        # EVERY(100s)
        sql = "select %s from table_1  EVERY(100s);"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_2  EVERY(100s);"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from table_3  EVERY(100s);"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        # error
        tdSql.error("select interp(*) from table_1")
        tdSql.error("select interp(*) from table_2 FILL(NEXT)") 
        sql = "select interp(*) from table_3 where ts >= '%s' FILL(NULL);" % self.ts
        tdSql.error(sql)  
        sql = "select interp(*) from table_1 where ts <= '%s' FILL(LINEAR);" % self.ts
        tdSql.error(sql)  
        sql = "select interp(*) from table_2 where ts > '%s' and ts < now every(1s) fill(PREV);" % self.ts
        tdSql.error(sql)  

        sql = "select %s from table_1  " % interp_select  
        sql += "range('%s' , '%s') EVERY(1s) FILL (LINEAR);" % (self.ts + 150000 ,  self.ts + 100000) 
        tdSql.error(sql)
        sql = "select %s from table_2  " % interp_select  
        sql += "range('%s' , '%s') EVERY(2s) FILL (PREV);" % (self.ts + 150000 ,  self.ts - 100000) 
        tdSql.error(sql)
        sql = "select %s from table_3  " % interp_select  
        sql += "range('%s' , '%s') EVERY(5s) FILL (NEXT);" % (self.ts - 150000 ,  self.ts - 200000) 
        tdSql.error(sql)

        sql = "select %s from table_1  " % interp_select  
        sql += "range('%s' , '%s') EVERY(10s) FILL (NULL);" % (self.ts + 150000 ,  self.ts + 100000) 
        tdSql.error(sql)
        sql = "select %s from table_2  " % interp_select  
        sql += "range('%s' , '%s') EVERY(60s) FILL (VALUE，100);" % (self.ts + 150000 ,  self.ts - 100000) 
        tdSql.error(sql)
        sql = "select %s from table_3  " % interp_select  
        sql += "range('%s' , '%s') EVERY(120s) FILL (LINEAR) ORDER BY ts DESC;" % (self.ts - 150000 ,  self.ts - 200000) 
        tdSql.error(sql)

        # Nested Query
        interp_select = 'interp(q_int),interp(q_bigint),interp(q_smallint),interp(q_tinyint),interp(q_float),interp(q_double),\
                        interp(q_int_null),interp(q_bigint_null),interp(q_smallint_null),interp(q_tinyint_null),interp(q_float_null),interp(q_double_null),\
                        interp(q_int,q_bigint,q_smallint,q_tinyint,q_float,q_double),interp(q_int_null,q_bigint_null,q_smallint_null,q_tinyint_null,q_float_null,q_double_null)'

        sql = "select * from (select %s from table_1) ;" % interp_select 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select * from (select %s from table_2) ;" % interp_select 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select * from (select %s from table_3) ;" % interp_select 
        datacheck = self.regular3_checkall_0(sql) 

        sql = "select %s from (select * from table_1) ;" % interp_select 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from (select * from table_2) ;" % interp_select 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from (select * from table_3) ;" % interp_select 
        datacheck = self.regular3_checkall_0(sql) 

        # Nested Query + where + range + FILL(LINEAR) + EVERY( s) (1)
        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(11)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(11)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # Nested Query + where + range + FILL(LINEAR) + EVERY( s)(2)
        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from (select * from table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(11)

        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from (select * from table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from (select * from table_3 )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(11)

        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)
        sql = "select %s from (select * from table_3  )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)

        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from (select * from table_3  )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # Nested Query + where + range + FILL(LINEAR) + EVERY( s)(3)
        sql = "select * from (select %s from table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        sql += "(select %s from table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)

        sql = "select * from (select %s from table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        sql += "(select %s from table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        datacheck = self.result_0(sql)

        sql = "select * from (select %s from table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        sql += "(select %s from table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(1)

        sql = "select * from (select %s from table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        sql += "(select %s from table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)        

        sql = "select * from (select %s from table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        sql += "(select %s from table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        datacheck = self.regular2_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)   

        sql = "select * from (select %s from table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        sql += "(select %s from table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        datacheck = self.result_0(sql)   

        # Nested Query + where + range + FILL(NEXT) + EVERY( s) (1)
        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(21)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(21)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(21)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(1)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(11)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # Nested Query + where + range + FILL(NEXT) + EVERY( s)(2)
        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(21)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(21)
        sql = "select %s from (select * from table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(21)

        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(1)

        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from (select * from table_3 )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(11)

        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)
        sql = "select %s from (select * from table_3  )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)

        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from (select * from table_3  )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # Nested Query + where + range + FILL(NEXT) + EVERY( s)(3)
        sql = "select * from (select %s from table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        sql += "(select %s from table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(21)

        sql = "select * from (select %s from table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        sql += "(select %s from table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)

        sql = "select * from (select %s from table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        sql += "(select %s from table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(1)

        sql = "select * from (select %s from table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        sql += "(select %s from table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(11)        

        sql = "select * from (select %s from table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        sql += "(select %s from table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)   

        sql = "select * from (select %s from table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        sql += "(select %s from table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        datacheck = self.result_0(sql)   

        # Nested Query + where + range + FILL(PREV) + EVERY( s) (1)
        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(101)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(101)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(101)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(201)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(201)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(201)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(2)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(2)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(2)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(51)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(51)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(51)

        # Nested Query + where + range + FILL(PREV) + EVERY( s)(2)
        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(101)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(101)
        sql = "select %s from (select * from table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(101)

        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from (select * from table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from (select * from table_3 )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(2)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(2)
        sql = "select %s from (select * from table_3  )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(2)

        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(51)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(51)
        sql = "select %s from (select * from table_3  )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(51)

        # Nested Query + where + range + FILL(PREV) + EVERY( s)(3)
        sql = "select * from (select %s from table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        sql += "(select %s from table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(101)

        sql = "select * from (select %s from table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        sql += "(select %s from table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        datacheck = self.result_0(sql)

        sql = "select * from (select %s from table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        sql += "(select %s from table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(1)

        sql = "select * from (select %s from table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        sql += "(select %s from table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)        

        sql = "select * from (select %s from table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        sql += "(select %s from table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(2)   

        sql = "select * from (select %s from table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        sql += "(select %s from table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(51)  

        # Nested Query + where + range + FILL(VALUE, 100) + EVERY( s) (1)
        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(201)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(201)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(201)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(2)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(2)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(2)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)

        # Nested Query + where + range + FILL(VALUE, 100) + EVERY( s)(2)
        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)
        sql = "select %s from (select * from table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)

        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)

        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from (select * from table_3 )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(2)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(2)
        sql = "select %s from (select * from table_3  )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(2)

        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)
        sql = "select %s from (select * from table_3  )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)

        # Nested Query + where + range + FILL(VALUE, 100) + EVERY( s)(3)
        sql = "select * from (select %s from table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        sql += "(select %s from table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)

        sql = "select * from (select %s from table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        sql += "(select %s from table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)

        sql = "select * from (select %s from table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        sql += "(select %s from table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(1)

        sql = "select * from (select %s from table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        sql += "(select %s from table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)        

        sql = "select * from (select %s from table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        sql += "(select %s from table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(2)    

        sql = "select * from (select %s from table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        sql += "(select %s from table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)    

        # Nested Query + where + range + FILL(NULL) + EVERY( s) (1)
        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(201)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(201)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(201)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(2)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(2)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(2)

        sql = "select * from (select %s from table_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)
        sql = "select * from (select %s from table_2  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)
        sql = "select * from (select %s from table_3  " % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL));" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)

        # Nested Query + where + range + FILL(NULL) + EVERY( s)(2)
        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)
        sql = "select %s from (select * from table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)

        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)

        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from (select * from table_3  )" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from (select * from table_3 )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(2)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(2)
        sql = "select %s from (select * from table_3  )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(2)

        sql = "select %s from (select * from table_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)
        sql = "select %s from (select * from table_2  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)
        sql = "select %s from (select * from table_3  )" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)

        # Nested Query + where + range + FILL(NULL) + EVERY( s)(3)
        sql = "select * from (select %s from table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        sql += "(select %s from table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)

        sql = "select * from (select %s from table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        sql += "(select %s from table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)

        sql = "select * from (select %s from table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        sql += "(select %s from table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(1)

        sql = "select * from (select %s from table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        sql += "(select %s from table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(201)        

        sql = "select * from (select %s from table_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        sql += "(select %s from table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(2)    

        sql = "select * from (select %s from table_3 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        sql += "(select %s from table_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)    

        print("==============step3, super table ==============")

        dcDB = self.dropandcreateDB()

        dcRestart = self.restartDnodes()
        
        dcDB = self.dropandcreateDB_null()

        interp_select = 'interp(q_int),interp(q_bigint),interp(q_smallint),interp(q_tinyint),interp(q_float),interp(q_double),\
                        interp(q_int_null),interp(q_bigint_null),interp(q_smallint_null),interp(q_tinyint_null),interp(q_float_null),interp(q_double_null),\
                        interp(q_int,q_bigint,q_smallint,q_tinyint,q_float,q_double),interp(q_int_null,q_bigint_null,q_smallint_null,q_tinyint_null,q_float_null,q_double_null),loc'

        sql = "select  * from stable_1 ;"  
        datacheck = self.regular1_checkall_0_base(sql)           
        tdSql.checkRows(57)
        datacheck = self.regular1_checkall_20_base(sql)    

        sql = "select %s from stable_1 group by tbname;" % interp_select
        datacheck = self.regular1_checkall_0(sql)   
        sql = "select %s from stable_1 where tbname in ('table_2') group by tbname;" % interp_select
        datacheck = self.regular2_checkall_0(sql)  
        sql = "select %s from stable_1 where tbname in ('table_3') group by tbname;" % interp_select
        datacheck = self.regular3_checkall_0(sql) 

        sql = "select %s from stable_1 group by tbname ORDER BY ts DESC;" % interp_select
        datacheck = self.regular1_checkall_0(sql)  
        sql = "select %s from stable_1 where tbname in ('table_2') group by tbname ORDER BY ts DESC;" % interp_select
        datacheck = self.regular2_checkall_0(sql)  
        sql = "select %s from stable_1 where tbname in ('table_3') group by tbname ORDER BY ts DESC;" % interp_select
        datacheck = self.regular3_checkall_0(sql) 

        sql = "select %s from stable_1 where "  % interp_select
        sql += "ts > %s  group by tbname ;" % (self.ts - 10) 
        datacheck = self.regular1_checkall_0(sql)   
        sql = "select %s from stable_1 where "  % interp_select
        sql += "ts > %s  and tbname in ('table_2') group by tbname ;" % (self.ts - 10) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from stable_1 where "  % interp_select
        sql += "ts > %s  and tbname in ('table_3') group by tbname ;" % (self.ts - 10) 
        datacheck = self.regular3_checkall_0(sql) 

        sql = "select %s from stable_1 where "  % interp_select
        sql += "ts > %s  group by tbname ;" % (self.ts + 85000) 
        datacheck = self.regular1_checkall_9(sql)  
        sql = "select %s from stable_1 where "  % interp_select
        sql += "ts > %s  and tbname in ('table_2') group by tbname;" % (self.ts + 85000) 
        datacheck = self.regular2_checkall_9(sql)  
        sql = "select %s from stable_1 where "  % interp_select
        sql += "ts > %s  and tbname in ('table_3') group by tbname;" % (self.ts + 85000) 
        datacheck = self.regular3_checkall_9(sql) 

        # range
        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s')  group by tbname;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s')  group by tbname;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') group by tbname ;" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') group by tbname;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') group by tbname;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') group by tbname;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') group by tbname;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') group by tbname;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s')  group by tbname;" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s')  group by tbname;" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s')  group by tbname;" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.result_0(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s')  group by tbname;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s')  group by tbname;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s')  group by tbname;" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # FILL(LINEAR)
        sql = "select %s from stable_1 FILL(LINEAR) group by tbname "  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from stable_1 where tbname in ('table_2') FILL(LINEAR) group by tbname "  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from stable_1 where tbname in ('table_3') FILL(LINEAR) group by tbname "  % interp_select
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR) group by tbname;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') FILL(LINEAR) group by tbname;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') FILL(LINEAR) group by tbname;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR) group by tbname;" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') FILL(LINEAR) group by tbname;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') FILL(LINEAR) group by tbname;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR) group by tbname;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') FILL(LINEAR) group by tbname;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') FILL(LINEAR) group by tbname;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR) group by tbname;" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_LINEAR(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') FILL(LINEAR) group by tbname;" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_LINEAR(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') FILL(LINEAR) group by tbname;" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_LINEAR(sql) 

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(LINEAR) group by tbname;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') FILL(LINEAR) group by tbname;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') FILL(LINEAR) group by tbname;" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # FILL(LINEAR) + EVERY( s)
        sql = "select %s from stable_1  EVERY(1s) FILL(LINEAR) group by tbname;"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(273)
        sql = "select %s from stable_1  where tbname in ('table_2') EVERY(1s) FILL(LINEAR) group by tbname;"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(91) 
        sql = "select %s from stable_1  where tbname in ('table_3') EVERY(1s) FILL(LINEAR) group by tbname;"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(91) 

        sql = "select %s from stable_1  EVERY(5s) FILL(LINEAR) group by tbname;"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(57)
        sql = "select %s from stable_1  where tbname in ('table_2') EVERY(5s) FILL(LINEAR) group by tbname;"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(19) 
        sql = "select %s from stable_1  where tbname in ('table_3') EVERY(5s) FILL(LINEAR) group by tbname;"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(19) 

        # range + FILL(LINEAR) + EVERY( s)
        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(273)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(91)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(91)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname;" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname;" % (self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(3)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname;" % (self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname;" % (self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(273)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(91)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(91)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_9(sql) 
        tdSql.checkRows(273)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_9(sql) 
        tdSql.checkRows(91)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname  ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_9(sql)
        tdSql.checkRows(91)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_LINEAR(sql) 
        tdSql.checkRows(6)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # where + range + FILL(LINEAR) + EVERY( s)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(33)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(11)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(3)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(33)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(11)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(33)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and  ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0_NEXT(sql)
        tdSql.checkRows(11)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_LINEAR(sql) 
        tdSql.checkRows(6)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and  ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and  ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # FILL(NEXT)
        sql = "select %s from stable_1  FILL(NEXT) group by tbname "  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from stable_1  where tbname in ('table_2') FILL(NEXT) group by tbname "  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from stable_1  where tbname in ('table_3') FILL(NEXT) group by tbname "  % interp_select
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') FILL(NEXT) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') FILL(NEXT) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT) group by tbname ;" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') FILL(NEXT) group by tbname ;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') FILL(NEXT) group by tbname ;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular3_checkall_0(sql) 

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') FILL(NEXT) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') FILL(NEXT) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_NEXT(sql) 

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NEXT) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') FILL(NEXT) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') FILL(NEXT) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # FILL(NEXT) + EVERY( s)
        sql = "select %s from stable_1  EVERY(1s) FILL(NEXT) group by tbname ;"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(273)
        sql = "select %s from stable_1  where tbname in ('table_2') EVERY(1s) FILL(NEXT) group by tbname ;"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(91) 
        sql = "select %s from stable_1  where tbname in ('table_3') EVERY(1s) FILL(NEXT) group by tbname ;"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(91) 

        sql = "select %s from stable_1  EVERY(5s) FILL(NEXT) group by tbname ;"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(57)
        sql = "select %s from stable_1  where tbname in ('table_2') EVERY(5s) FILL(NEXT) group by tbname ;"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(19) 
        sql = "select %s from stable_1  where tbname in ('table_3') EVERY(5s) FILL(NEXT) group by tbname ;"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(19) 

        # range + FILL(NEXT) + EVERY( s)
        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(303)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(101)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(101)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(3)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(1)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(3)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(273)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(91)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(91)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT)  group by tbname ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_9(sql) 
        tdSql.checkRows(273)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(NEXT)  group by tbname ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_9(sql) 
        tdSql.checkRows(91)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(NEXT)  group by tbname ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_9(sql)
        tdSql.checkRows(91)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(6)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # where + range + FILL(NEXT) + EVERY( s)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(63)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(21)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(21)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(3)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(1)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(3)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(33)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(11)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)  group by tbname ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(33)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)  group by tbname ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname  ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0_NEXT(sql)
        tdSql.checkRows(11)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(6)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # FILL(PREV)
        sql = "select %s from stable_1  FILL(PREV) group by tbname "  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from stable_1  where tbname in ('table_2') FILL(PREV) group by tbname "  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from stable_1  where tbname in ('table_3') FILL(PREV) group by tbname "  % interp_select
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') FILL(PREV) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') FILL(PREV) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.result_0(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV) group by tbname ;" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') FILL(PREV) group by tbname ;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') FILL(PREV) group by tbname ;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV) group by tbname ;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') FILL(PREV) group by tbname ;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') FILL(PREV) group by tbname ;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') FILL(PREV) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') FILL(PREV) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0(sql) 

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(PREV) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_9(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') FILL(PREV) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_9(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') FILL(PREV) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular3_checkall_9(sql) 

        # FILL(PREV) + EVERY( s)
        sql = "select %s from stable_1  EVERY(1s) FILL(PREV) group by tbname ;"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(273)
        sql = "select %s from stable_1  where tbname in ('table_2') EVERY(1s) FILL(PREV) group by tbname ;"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(91) 
        sql = "select %s from stable_1  where tbname in ('table_3') EVERY(1s) FILL(PREV) group by tbname ;"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(91) 

        sql = "select %s from stable_1  EVERY(5s) FILL(PREV) group by tbname ;"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(57)
        sql = "select %s from stable_1  where tbname in ('table_2') EVERY(5s) FILL(PREV) group by tbname ;"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(19) 
        sql = "select %s from stable_1  where tbname in ('table_3') EVERY(5s) FILL(PREV) group by tbname ;"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(19) 

        # range + FILL(PREV) + EVERY( s)
        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(303)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(101)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(101)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(3)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(603)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV)  group by tbname ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_9(sql) 
        tdSql.checkRows(603)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname  ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_9(sql) 
        tdSql.checkRows(201)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(PREV)  group by tbname ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_9(sql)
        tdSql.checkRows(201)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(6)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(2)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(2)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_9(sql) 
        tdSql.checkRows(153)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_9(sql) 
        tdSql.checkRows(51)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular3_checkall_9(sql) 
        tdSql.checkRows(51)

        # where + range + FILL(PREV) + EVERY( s)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(303)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(101)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(101)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(3)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(603)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)  group by tbname ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(603)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)  group by tbname ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(201)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)  group by tbname ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0_NEXT(sql)
        tdSql.checkRows(201)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(6)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(2)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(2)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(153)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(51)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(51)

        # FILL(VALUE，100)
        sql = "select %s from stable_1  FILL(VALUE, 100) group by tbname "  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from stable_1  where tbname in ('table_2') FILL(VALUE, 100) group by tbname "  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from stable_1  where tbname in ('table_3') FILL(VALUE, 100) group by tbname "  % interp_select
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') FILL(VALUE, 100) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') FILL(VALUE, 100) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100) group by tbname ;" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') FILL(VALUE, 100) group by tbname ;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') FILL(VALUE, 100) group by tbname ;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') FILL(VALUE, 100) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') FILL(VALUE, 100) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_VALUE100(sql) 

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(VALUE, 100) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') FILL(VALUE, 100) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') FILL(VALUE, 100) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_VALUE100(sql)

        # FILL(VALUE，100) + EVERY( s)
        sql = "select %s from stable_1  EVERY(1s) FILL(VALUE, 100) group by tbname ;"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(273)
        sql = "select %s from stable_1  where tbname in ('table_2') EVERY(1s) FILL(VALUE, 100) group by tbname ;"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(91) 
        sql = "select %s from stable_1  where tbname in ('table_3') EVERY(1s) FILL(VALUE, 100) group by tbname ;"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(91) 

        sql = "select %s from stable_1  EVERY(5s) FILL(VALUE, 100) group by tbname ;"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(57)
        sql = "select %s from stable_1  where tbname in ('table_2') EVERY(5s) FILL(VALUE, 100) group by tbname ;"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(19) 
        sql = "select %s from stable_1  where tbname in ('table_3') EVERY(5s) FILL(VALUE, 100) group by tbname ;"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(19) 

        # FILL(NULL) + EVERY( s)
        sql = "select %s from stable_1  EVERY(1s) FILL(NULL) group by tbname ;"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(273)
        sql = "select %s from stable_1  where tbname in ('table_2') EVERY(1s) FILL(NULL) group by tbname ;"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(91) 
        sql = "select %s from stable_1  where tbname in ('table_3') EVERY(1s) FILL(NULL) group by tbname ;"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(91) 

        sql = "select %s from stable_1  EVERY(5s) FILL(NULL) group by tbname ;"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(57)
        sql = "select %s from stable_1  where tbname in ('table_2') EVERY(5s) FILL(NULL) group by tbname ;"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(19) 
        sql = "select %s from stable_1  where tbname in ('table_3') EVERY(5s) FILL(NULL) group by tbname ;"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(19) 

        # range + FILL(VALUE, 100) + EVERY( s)
        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(333)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(3)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(3)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(603)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)  group by tbname ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(603)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)  group by tbname ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(201)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)  group by tbname ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(201)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(6)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(2)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(2)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(153)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(51)

        # where + range + FILL(VALUE, 100) + EVERY( s)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(333)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(3)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(3)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(603)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname  ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(603)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname  ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(201)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname  ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(201)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(6)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(2)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(2)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(153)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)

        # FILL(NULL)
        sql = "select %s from stable_1  FILL(NULL) group by tbname "  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from stable_1  where tbname in ('table_2') FILL(NULL) group by tbname "  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from stable_1  where tbname in ('table_3') FILL(NULL) group by tbname "  % interp_select
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') FILL(NULL) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') FILL(NULL) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL) group by tbname ;" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') FILL(NULL) group by tbname ;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') FILL(NULL) group by tbname ;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL) group by tbname ;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') FILL(NULL) group by tbname ;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') FILL(NULL) group by tbname ;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') FILL(NULL) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_NULL(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') FILL(NULL) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_NULL(sql) 

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') FILL(NULL) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') FILL(NULL) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') FILL(NULL) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_NULL(sql)

        # range + FILL(NULL) + EVERY( s)
        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(333)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(3)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(3)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(603)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL)  group by tbname ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(603)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(NULL)  group by tbname ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(201)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(NULL)  group by tbname ORDER BY ts DESC;" % (self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(201)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(6)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(2)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(2)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(153)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(51)

        # where + range + FILL(NULL) + EVERY( s)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(333)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(3)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(3)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(603)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)  group by tbname ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(603)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)  group by tbname ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(201)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)  group by tbname ORDER BY ts DESC;" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(201)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(6)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(2)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(2)

        sql = "select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(153)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname ;" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)

        # EVERY(1s)
        sql = "select %s from stable_1  EVERY(1s) group by tbname ;"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        datacheck = self.regular1_checkall_20(sql) 
        sql = "select %s from stable_1  where tbname in ('table_2') EVERY(1s) group by tbname ;"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        datacheck = self.regular2_checkall_20(sql) 
        sql = "select %s from stable_1  where tbname in ('table_3') EVERY(1s) group by tbname ;"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        datacheck = self.regular3_checkall_20(sql) 

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        datacheck = self.regular1_checkall_20(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        datacheck = self.regular2_checkall_20(sql) 
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql)
        datacheck = self.regular3_checkall_20(sql) 

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) group by tbname ;" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) group by tbname ;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) group by tbname ;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) group by tbname ;" % (self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(3)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) group by tbname ;" % (self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) group by tbname ;" % (self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) group by tbname ;" % (self.ts + 10000 ,  self.ts + 25000) 
        datacheck = self.regular1_checkall_0_NEXT(sql)
        tdSql.checkRows(21)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) group by tbname ;" % (self.ts + 10000 ,  self.ts + 25000)  
        datacheck = self.regular2_checkall_0_NEXT(sql)
        tdSql.checkRows(7)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) group by tbname ;" % (self.ts + 10000 ,  self.ts + 25000)   
        datacheck = self.regular3_checkall_0_NEXT(sql)
        tdSql.checkRows(7)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(1s) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(1s) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(1s) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # EVERY(2s)
        sql = "select %s from stable_1  EVERY(2s) group by tbname ;"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(42)
        sql = "select %s from stable_1  where tbname in ('table_2') EVERY(2s) group by tbname ;"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(14)
        sql = "select %s from stable_1  where tbname in ('table_3') EVERY(2s) group by tbname ;"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(14)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(42)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(2s) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(14)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(2s) group by tbname ;" % (self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(14)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s) group by tbname ;" % (self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(2s) group by tbname ;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(2s) group by tbname ;" % (self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s) group by tbname ;" % (self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(3)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(2s) group by tbname ;" % (self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(2s) group by tbname ;" % (self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s) group by tbname ;" % (self.ts + 10000 ,  self.ts + 25000) 
        datacheck = self.regular1_checkall_0_NEXT(sql)
        tdSql.checkRows(15)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(2s) group by tbname ;" % (self.ts + 10000 ,  self.ts + 25000)  
        datacheck = self.regular2_checkall_0_NEXT(sql)
        tdSql.checkRows(5)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(2s) group by tbname ;" % (self.ts + 10000 ,  self.ts + 25000)   
        datacheck = self.regular3_checkall_0_NEXT(sql)
        tdSql.checkRows(5)

        sql = "select %s from stable_1  " % interp_select   
        sql += "range('%s' , '%s') EVERY(2s) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(2s) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(2s) group by tbname ;" % (self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # EVERY(5s)
        sql = "select %s from stable_1  EVERY(5s) group by tbname ;"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(33)
        sql = "select %s from stable_1  where tbname in ('table_2') EVERY(5s) group by tbname ;"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select %s from stable_1  where tbname in ('table_3') EVERY(5s) group by tbname ;"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(11)

        # EVERY(100s)
        sql = "select %s from stable_1  EVERY(100s) group by tbname ;"  % interp_select
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(3)
        sql = "select %s from stable_1  where tbname in ('table_2') EVERY(100s) group by tbname ;"  % interp_select
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select %s from stable_1  where tbname in ('table_3') EVERY(100s) group by tbname ;"  % interp_select
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        # error
        tdSql.error("select interp(*) from stable_1 group by tbname ")
        tdSql.error("select interp(*) from stable_1 FILL(NEXT) group by tbname ") 
        sql = "select interp(*) from stable_1 where ts >= '%s' FILL(NULL) group by tbname ;" % self.ts
        tdSql.error(sql)  
        sql = "select interp(*) from stable_1 where ts <= '%s' FILL(LINEAR) group by tbname ;" % self.ts
        tdSql.error(sql)  
        sql = "select interp(*) from stable_1 where ts > '%s' and ts < now every(1s) fill(PREV) group by tbname ;" % self.ts
        tdSql.error(sql)  

        sql = "select %s from stable_1  " % interp_select  
        sql += "range('%s' , '%s') EVERY(1s) FILL (LINEAR) group by tbname ;" % (self.ts + 150000 ,  self.ts + 100000) 
        tdSql.error(sql)
        sql = "select %s from stable_1  " % interp_select  
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(2s) FILL (PREV) group by tbname ;" % (self.ts + 150000 ,  self.ts - 100000) 
        tdSql.error(sql)
        sql = "select %s from stable_1  " % interp_select  
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(5s) FILL (NEXT) group by tbname ;" % (self.ts - 150000 ,  self.ts - 200000) 
        tdSql.error(sql)

        sql = "select %s from stable_1  " % interp_select  
        sql += "range('%s' , '%s') EVERY(10s) FILL (NULL) group by tbname ;" % (self.ts + 150000 ,  self.ts + 100000) 
        tdSql.error(sql)
        sql = "select %s from stable_1  " % interp_select  
        sql += "where tbname in ('table_2') range('%s' , '%s') EVERY(60s) FILL (VALUE，100) group by tbname ;" % (self.ts + 150000 ,  self.ts - 100000) 
        tdSql.error(sql)
        sql = "select %s from stable_1  " % interp_select  
        sql += "where tbname in ('table_3') range('%s' , '%s') EVERY(120s) FILL (LINEAR)  group by tbname ORDER BY ts DESC;" % (self.ts - 150000 ,  self.ts - 200000) 
        tdSql.error(sql)
       
        # Nested Query
        sql = "select * from (select %s from stable_1 group by tbname) ;" % interp_select 
        datacheck = self.regular1_checkall_0(sql) 
        sql = "select * from (select %s from stable_1 where tbname in ('table_2') group by tbname) ;" % interp_select 
        datacheck = self.regular2_checkall_0(sql) 
        sql = "select * from (select %s from stable_1 where tbname in ('table_3') group by tbname) ;" % interp_select 
        datacheck = self.regular3_checkall_0(sql) 

        sql = "select %s from (select * from stable_1) ;" % interp_select 
        tdSql.error(sql)
        sql = "select %s from (select * from stable_1 where tbname in ('table_2') ) ;" % interp_select 
        tdSql.error(sql) 
        sql = "select %s from (select * from stable_1 where tbname in ('table_3') ) ;" % interp_select 
        tdSql.error(sql) 

        # Nested Query + where + range + FILL(LINEAR) + EVERY( s) (1)
        sql = "select * from (select %s from stable_1   " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(33)
        sql = "select * from (select %s from stable_1   " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select * from (select %s from stable_1   " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(11)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname );" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR) group by tbname );" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(3)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and  ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(33)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and  ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(11)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(33)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and  ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_LINEAR(sql) 
        tdSql.checkRows(6)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and  ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_LINEAR(sql) 
        tdSql.checkRows(2)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and  ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)
        tdLog.info(sql)

        # Nested Query + where + range + FILL(LINEAR) + EVERY( s)(2-error)
        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        tdSql.error(sql)
        sql = "select %s from (select * from stable_1  where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        tdSql.error(sql)
        sql = "select %s from (select * from stable_1  where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        tdSql.error(sql)

        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        tdSql.error(sql)
        sql = "select %s from (select * from stable_1  where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        tdSql.error(sql)
        sql = "select %s from (select * from stable_1  where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        tdSql.error(sql)

        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        tdSql.error(sql)
        sql = "select %s from (select * from stable_1  where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        tdSql.error(sql)
        sql = "select %s from (select * from stable_1  where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        tdSql.error(sql)

        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        tdSql.error(sql)
        sql = "select %s from (select * from stable_1  where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        tdSql.error(sql)
        sql = "select %s from (select * from stable_1 where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        tdSql.error(sql)

        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        tdSql.error(sql)
        sql = "select %s from (select * from stable_1  where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        tdSql.error(sql)
        sql = "select %s from (select * from stable_1  where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        tdSql.error(sql)

        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        tdSql.error(sql)
        sql = "select %s from (select * from stable_1  where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        tdSql.error(sql)
        sql = "select %s from (select * from stable_1  where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        tdSql.error(sql)

        # Nested Query + where + range + FILL(LINEAR) + EVERY( s)(3)
        #TD-11096
        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        # sql += "(select %s from stable_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        # datacheck = self.regular1_checkall_0(sql) 
        # tdSql.checkRows(11)

        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        # sql += "(select %s from stable_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        # datacheck = self.result_0(sql)

        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        # sql += "(select %s from stable_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        # datacheck = self.regular3_checkall_0(sql) 
        # tdSql.checkRows(1)

        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        # sql += "(select %s from stable_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        # datacheck = self.regular1_checkall_0(sql) 
        # tdSql.checkRows(11)        

        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        # sql += "(select %s from stable_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)  group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        # datacheck = self.regular2_checkall_0_LINEAR(sql) 
        # tdSql.checkRows(2)   

        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        # sql += "(select %s from stable_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(LINEAR)) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        # datacheck = self.result_0(sql)   

        # Nested Query + where + range + FILL(NEXT) + EVERY( s) (1)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(63)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_2') and ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(21)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_3') and  ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(21)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(3)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_3') and  ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(1)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(3)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_3') and  ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(33)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(11)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_3') and  ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(11)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)  group by tbname ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(33)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)  group by tbname ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_3') and  ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT)  group by tbname ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(11)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(6)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_3') and  ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(2)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.result_0(sql)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_3') and  ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.result_0(sql)

        # Nested Query + where + range + FILL(NEXT) + EVERY( s)(2)--error
        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        tdSql.error(sql)
        sql = "select %s from (select * from stable_1    where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        tdSql.error(sql)
        sql = "select %s from (select * from stable_1    where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        tdSql.error(sql)

        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        tdSql.error(sql)
        sql = "select %s from (select * from stable_1    where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        tdSql.error(sql)
        sql = "select %s from (select * from stable_1    where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        tdSql.error(sql)

        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        tdSql.error(sql)
        sql = "select %s from (select * from stable_1    where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        tdSql.error(sql)
        sql = "select %s from (select * from stable_1    where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        tdSql.error(sql)

        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        tdSql.error(sql)
        sql = "select %s from (select * from stable_1    where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        tdSql.error(sql)

        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        tdSql.error(sql)
        sql = "select %s from (select * from stable_1    where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        tdSql.error(sql)
        sql = "select %s from (select * from stable_1    where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        tdSql.error(sql)

        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        tdSql.error(sql)
        sql = "select %s from (select * from stable_1    where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        tdSql.error(sql)
        sql = "select %s from (select * from stable_1    where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        tdSql.error(sql)

        # Nested Query + where + range + FILL(NEXT) + EVERY( s)(3)
        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        # sql += "(select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        # datacheck = self.regular1_checkall_0(sql) 
        # tdSql.checkRows(21)

        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        # sql += "(select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        # datacheck = self.regular2_checkall_0(sql) 
        # tdSql.checkRows(1)

        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        # sql += "(select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        # datacheck = self.regular3_checkall_0(sql) 
        # tdSql.checkRows(1)

        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        # sql += "(select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        # datacheck = self.regular1_checkall_0(sql) 
        # tdSql.checkRows(11)        

        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        # sql += "(select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        # datacheck = self.regular2_checkall_0_NEXT(sql) 
        # tdSql.checkRows(2)   

        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        # sql += "(select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NEXT) group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        # datacheck = self.result_0(sql)   

        # Nested Query + where + range + FILL(PREV) + EVERY( s) (1)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(303)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(101)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and  ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(101)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.result_0(sql)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and  ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.result_0(sql)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(3)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and  ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(603)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and  ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)  group by tbname ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(603)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)  group by tbname ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(201)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and  ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV)  group by tbname ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(201)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(6)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(2)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and  ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular3_checkall_0(sql) 
        tdSql.checkRows(2)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NEXT(sql) 
        tdSql.checkRows(153)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0_NEXT(sql) 
        tdSql.checkRows(51)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where tbname in ('table_3') and  ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular3_checkall_0_NEXT(sql) 
        tdSql.checkRows(51)

        # Nested Query + where + range + FILL(PREV) + EVERY( s)(2)
        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1  where tbname in ('table_2')  )" % interp_select   
        sql += " where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_3') )" % interp_select   
        sql += " where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = tdSql.error(sql)

        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_2'))" % interp_select   
        sql += " where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_3'))" % interp_select   
        sql += " where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = tdSql.error(sql)

        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_2'))" % interp_select   
        sql += " where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_3'))" % interp_select   
        sql += " where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = tdSql.error(sql)

        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_2'))" % interp_select   
        sql += " where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_3'))" % interp_select   
        sql += " where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = tdSql.error(sql)

        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = tdSql.error(sql)

        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(PREV);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = tdSql.error(sql)

        # Nested Query + where + range + FILL(PREV) + EVERY( s)(3)
        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        # sql += "(select %s from stable_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        # datacheck = self.regular1_checkall_0(sql) 
        # tdSql.checkRows(101)

        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        # sql += "(select %s from stable_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        # datacheck = self.result_0(sql)

        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        # sql += "(select %s from stable_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        # datacheck = self.regular3_checkall_0(sql) 
        # tdSql.checkRows(1)

        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        # sql += "(select %s from stable_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        # datacheck = self.regular1_checkall_0(sql) 
        # tdSql.checkRows(201)        

        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        # sql += "(select %s from stable_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        # datacheck = self.regular2_checkall_0(sql) 
        # tdSql.checkRows(2)   

        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        # sql += "(select %s from stable_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(PREV) group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        # datacheck = self.regular3_checkall_0_NEXT(sql) 
        # tdSql.checkRows(51)  

        # Nested Query + where + range + FILL(VALUE, 100) + EVERY( s) (1)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(333)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_3') and  ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(111)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(3)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_3') and  ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(1)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(3)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_3') and  ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(603)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_3') and  ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)  group by tbname ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(603)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)  group by tbname ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(201)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_3') and  ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100)  group by tbname ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql)
        tdSql.checkRows(201)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(6)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(2)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_3') and  ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(2)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(153)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_3') and  ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_VALUE100(sql) 
        tdSql.checkRows(51)

        # Nested Query + where + range + FILL(VALUE, 100) + EVERY( s)(2)
        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = tdSql.error(sql)

        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = tdSql.error(sql)

        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = tdSql.error(sql)

        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = tdSql.error(sql)

        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = tdSql.error(sql)

        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = tdSql.error(sql)

        # Nested Query + where + range + FILL(VALUE, 100) + EVERY( s)(3)
        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        # sql += "(select %s from stable_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        # datacheck = self.regular1_checkall_0_VALUE100(sql) 
        # tdSql.checkRows(111)

        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        # sql += "(select %s from stable_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        # datacheck = self.regular1_checkall_0_VALUE100(sql) 
        # tdSql.checkRows(1)

        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        # sql += "(select %s from stable_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        # datacheck = self.regular3_checkall_0(sql) 
        # tdSql.checkRows(1)

        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        # sql += "(select %s from stable_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        # datacheck = self.regular1_checkall_0(sql) 
        # tdSql.checkRows(201)        

        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        # sql += "(select %s from stable_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        # datacheck = self.regular1_checkall_0_VALUE100(sql) 
        # tdSql.checkRows(2)    

        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        # sql += "(select %s from stable_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(VALUE, 100) group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        # datacheck = self.regular1_checkall_0_VALUE100(sql) 
        # tdSql.checkRows(51)    

        # Nested Query + where + range + FILL(NULL) + EVERY( s) (1)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(333)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_3') and  ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(111)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(3)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_3') and  ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(1)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(3)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(1)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_3') and  ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(1)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0(sql) 
        tdSql.checkRows(603)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular2_checkall_0(sql) 
        tdSql.checkRows(201)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_3') and  ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular3_checkall_0(sql)
        tdSql.checkRows(201)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)  group by tbname ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(603)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)  group by tbname ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(201)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_3') and  ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL)  group by tbname ORDER BY ts DESC);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql)
        tdSql.checkRows(201)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname );" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(6)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(2)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_3') and  ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(2)

        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(153)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_2') and  ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)
        sql = "select * from (select %s from stable_1  " % interp_select   
        sql += " where tbname in ('table_3') and  ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = self.regular1_checkall_0_NULL(sql) 
        tdSql.checkRows(51)

        # Nested Query + where + range + FILL(NULL) + EVERY( s)(2)
        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000) 
        datacheck = tdSql.error(sql)

        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts  - 9900) 
        datacheck = tdSql.error(sql)

        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s' AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts) 
        datacheck = tdSql.error(sql)

        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000) 
        datacheck = tdSql.error(sql)

        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)  
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)   
        datacheck = tdSql.error(sql)

        sql = "select %s from (select * from stable_1  )" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_2'))" % interp_select   
        sql += "where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000) 
        datacheck = tdSql.error(sql)
        sql = "select %s from (select * from stable_1   where tbname in ('table_3'))" % interp_select   
        sql += "where ts >= '%s'  AND ts <= '%s' range('%s' , '%s') EVERY(1s) FILL(NULL);" % (self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)  
        datacheck = tdSql.error(sql)

        #  TD-10736  Exception use case  for coredump 

        tdSql.execute("create database db_except;")
        tdSql.execute("use db_except;")
        tdSql.execute("create table tb (ts timestamp, c1 int);")
        tdSql.execute("insert into tb values ('2021-10-01 08:00:00.000' ,1);")
        tdSql.execute("insert into tb values ('2021-10-01 08:00:01.000' ,2);")
        tdSql.execute("insert into tb values ('2021-10-01 08:00:02.000' ,3);")
        tdSql.execute("insert into tb values ('2021-10-01 08:00:03.000' ,4);")

        tdSql.execute("create stable stb (ts timestamp, c1 int) tags (id int);")
        tdSql.execute("insert into sub_1 using stb tags (1) values ('2021-10-01 08:00:00.000' ,1);")
        tdSql.execute("insert into sub_1 using stb tags (1) values ('2021-10-01 08:00:01.000' ,2);")

        tdSql.execute("insert into sub_2 using stb tags (1) values ('2021-10-01 08:00:00.000' ,3);")
        tdSql.execute("insert into sub_2 using stb tags (1) values ('2021-10-01 08:00:01.000' ,4);")

        tdSql.execute("insert into sub_3 using stb tags (1) values ('2021-10-01 08:00:01.000' ,1);")
        tdSql.execute("insert into sub_3 using stb tags (1) values ('2021-10-01 08:00:02.000' ,2);")
        tdSql.execute("insert into sub_3 using stb tags (1) values ('2021-10-01 08:00:03.000' ,3);")
        tdSql.execute("insert into sub_3 using stb tags (1) values ('2021-10-01 08:00:04.000' ,4);")

        tdSql.query("select interp(c1) from tb where ts = '2021-10-01 08:00:00.000' every(1s); ")
        tdSql.checkData(0,1,1)
        tdSql.query("select interp(c1) from tb where ts = '2021-10-01 08:00:98.000' every(1s); ")
        tdSql.checkRows(0)

        tdSql.query("select interp(c1) from sub_1 where ts = '2021-10-01 08:00:00.000' every(1s); ")
        tdSql.checkData(0,1,1)
        tdSql.query("select interp(c1) from sub_1 where ts = '2021-10-01 08:00:98.000' every(1s); ")
        tdSql.checkRows(0)

        tdSql.error("select interp(c1) from stb where ts = '2021-10-01 08:00:00.000' every(1s); ")
        tdSql.error("select interp(c1) from stb where ts = '2021-10-01 08:00:98.000' every(1s); ")
        tdSql.query("select interp(c1) from stb where ts = '2021-10-01 08:00:00.000' every(1s) group by tbname; ")
        tdSql.checkRows(2)
        tdSql.query("select interp(c1) from stb where ts = '2021-10-01 08:00:98.000' every(1s) group by tbname; ")
        tdSql.checkRows(0)

        # Nested Query + where + range + FILL(NULL) + EVERY( s)(3)
        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        # sql += "(select %s from stable_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts + 100000)
        # datacheck = self.regular1_checkall_0_NULL(sql) 
        # tdSql.checkRows(111)

        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        # sql += "(select %s from stable_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts - 10000 ,  self.ts - 9900)
        # datacheck = self.regular1_checkall_0_NULL(sql) 
        # tdSql.checkRows(1)

        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        # sql += "(select %s from stable_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts)
        # datacheck = self.regular3_checkall_0(sql) 
        # tdSql.checkRows(1)

        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        # sql += "(select %s from stable_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts ,  self.ts + 200000)
        # datacheck = self.regular1_checkall_0(sql) 
        # tdSql.checkRows(201)        

        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        # sql += "(select %s from stable_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 1500 ,  self.ts + 2500)
        # datacheck = self.regular1_checkall_0_NULL(sql) 
        # tdSql.checkRows(2)    

        # sql = "select * from (select %s from stable_1 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname) z1," % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        # sql += "(select %s from stable_2 where ts BETWEEN '%s' AND '%s' range('%s' , '%s') EVERY(1s) FILL(NULL) group by tbname) z2 where z1.ts=z2.ts ;"  % (interp_select , self.ts ,  self.ts + 10000 , self.ts + 150000 ,  self.ts + 200000)
        # datacheck = self.regular1_checkall_0_NULL(sql) 
        # tdSql.checkRows(51)    

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
