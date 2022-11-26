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
import subprocess
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql

class TDTestCase:
    updatecfgDict = {'maxSQLLength':1048576,'debugFlag': 143 ,"querySmaOptimize":1}
    
    def init(self, conn, logSql, replicaVar):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.testcasePath = os.path.split(__file__)[0]
        self.testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))
        
        self.db = "insert_select"

    def dropandcreateDB_random(self,database,n):
        ts = 1604298064000

        tdSql.execute('''drop database if exists %s ;''' %database)
        tdSql.execute('''create database %s keep 36500 ;'''%(database))
        tdSql.execute('''use %s;'''%database)

        tdSql.execute('''create table %s.tb (ts timestamp , i tinyint );'''%database)
        tdSql.execute('''create table %s.tb1 (speed timestamp , c1 int , c2 int , c3 int );'''%database)

        sql_before = "insert into %s.tb1 values" %database
        sql_value = ''
        for i in range(n):         
            sql_value = sql_value +"(%d, %d, %d, %d)"  % (ts + i, i, i, i)

        sql = sql_before + sql_value
        tdSql.execute(sql)

        tdSql.query("select count(*) from %s.tb1;"%database)    
        sql_result = tdSql.getData(0,0)
        tdLog.info("result: %s" %(sql_result))
        tdSql.query("reset query cache;")
        tdSql.query("insert into %s.tb1 select * from %s.tb1;"%(database,database))        
        tdSql.query("select count(*) from %s.tb1;"%database)
        sql_result = tdSql.getData(0,0)
        tdLog.info("result: %s" %(sql_result))
        
        
    def users_bug_TD_20592(self,database):    
        tdSql.execute('''drop database if exists %s ;''' %database)
        tdSql.execute('''create database %s keep 36500 ;'''%(database))
        tdSql.execute('''use %s;'''%database)

        tdSql.execute('''create table %s.sav_stb (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 smallint, c6 tinyint, c7 bool, c8 binary(10), c9 nchar(10)) tags(t1 int, t2 int);'''%database)
        tdSql.execute('''create table %s.tb1 using %s.sav_stb tags( 9 , 0);'''%(database,database))
        
        tdSql.error('''insert into %s.tb1 (c8, c9) values(now, 1);'''%(database))

    
                            
    def run(self):      
        
        startTime = time.time()  
                  
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename)) 
        
        self.dropandcreateDB_random("%s" %self.db, random.randint(10000,30000))
            
        #taos -f sql 
        print("taos -f sql start!")
        taos_cmd1 = "taos -f %s/%s.sql" % (self.testcasePath,self.testcaseFilename)
        _ = subprocess.check_output(taos_cmd1, shell=True)
        print("taos -f sql over!")   
        
        self.users_bug_TD_20592("%s" %self.db) 
        
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
