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

import os
import time
import subprocess
from faker import Faker
from new_test_framework.utils import tdLog, tdSql

class TestMaxMinData:
    updatecfgDict = {'maxSQLLength':1048576,'debugFlag': 131 ,"querySmaOptimize":1}
    
    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        # tdSql.init(conn.cursor(), logSql)

        cls.testcasePath = os.path.split(__file__)[0]
        cls.testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf %s/%s.sql" % (cls.testcasePath,cls.testcaseFilename))
        
        cls.db = "max_min"

    def dropandcreateDB_random(self,database,n):
        ts = 1630000000000
        num_random = 5
        fake = Faker('zh_CN')
        tdSql.execute('''drop database if exists %s ;''' %database)
        tdSql.execute('''create database %s keep 36500 ;'''%(database))
        tdSql.execute('''use %s;'''%database)

        tdSql.execute('''create stable %s.stable_1 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp , \
                q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) \
                tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);'''%database)
          
        for i in range(num_random):
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
           
        tdSql.query("select count(*) from %s.stable_1;" %database)
        tdSql.checkData(0,0,num_random*n)
        tdSql.query("select count(*) from %s.stable_1_1;"%database)
        tdSql.checkData(0,0,n)

    def TD_22219_max(self,database):    
        
        sql3 = "select count(*) from (select max(q_int) from %s.stable_1 group by tbname); ;"  %database
        tdSql.query(sql3) 
        sql_value = tdSql.getData(0,0)
        self.value_check(sql_value,5)
        
        sql1 = "select max(q_int) from %s.stable_1_1 ;" %database
        sql2 = "select max(q_int) from %s.stable_1  where tbname = 'stable_1_1' ;"  %database
        self.constant_check(database,sql1,sql2,0)
        
        sql3 = "select count(*) from (select max(q_int) from %s.stable_1 group by tbname); ;"  %database
        tdSql.query(sql3) 
        sql_value = tdSql.getData(0,0)
        self.value_check(sql_value,5)
        
    def TD_22219_min(self,database):    
        
        sql3 = "select count(*) from (select min(q_int) from %s.stable_1 group by tbname); ;"  %database
        tdSql.query(sql3) 
        sql_value = tdSql.getData(0,0)
        self.value_check(sql_value,5)
        
        sql1 = "select min(q_int) from %s.stable_1_1 ;" %database
        sql2 = "select min(q_int) from %s.stable_1  where tbname = 'stable_1_1' ;"  %database
        self.constant_check(database,sql1,sql2,0)
        
        sql3 = "select count(*) from (select min(q_int) from %s.stable_1 group by tbname); ;"  %database
        tdSql.query(sql3) 
        sql_value = tdSql.getData(0,0)
        self.value_check(sql_value,5)
        
    def constant_check(self,database,sql1,sql2,column):   
        #column =0 代表0列， column = n代表n-1列 
        tdLog.info("\n=============sql1:(%s)___sql2:(%s) ====================\n" %(sql1,sql2)) 
 
        tdSql.query(sql1) 
        sql1_value = tdSql.getData(0,column)
        tdSql.query(sql2) 
        sql2_value = tdSql.getData(0,column)
        self.value_check(sql1_value,sql2_value)  
                    
        tdSql.execute(" flush database %s;" %database)  
        
        time.sleep(3)
            
        tdSql.query(sql1) 
        sql1_flush_value = tdSql.getData(0,column)
        tdSql.query(sql2) 
        sql2_flush_value = tdSql.getData(0,column)
        self.value_check(sql1_flush_value,sql2_flush_value)   
        
        self.value_check(sql1_value,sql1_flush_value)  
        self.value_check(sql2_value,sql2_flush_value)   
                      
    def value_check(self,base_value,check_value):
        if base_value==check_value:
            tdLog.info(f"checkEqual success, base_value={base_value},check_value={check_value}") 
        else :
            tdLog.exit(f"checkEqual error, base_value=={base_value},check_value={check_value}") 
                            
    def test_max_min_data(self):
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

        startTime = time.time()  
                  
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename)) 
        
        self.dropandcreateDB_random("%s" %self.db, 2000)
        
        self.TD_22219_max("%s" %self.db)
        
        self.dropandcreateDB_random("%s" %self.db, 2000)
        
        self.TD_22219_min("%s" %self.db)

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))

        #tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
