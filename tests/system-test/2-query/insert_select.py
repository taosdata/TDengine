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

    def use_select_sort(self,database):  
        ts = 1604298064000
          
        tdSql.execute('''drop database if exists %s ;''' %database)
        tdSql.execute('''create database %s keep 36500 ;'''%(database))
        tdSql.execute('''use %s;'''%database)

        tdSql.execute('''create stable %s.st (ts timestamp, val int, vt timestamp)  tags (location NCHAR(100));'''%(database))
        tdSql.execute('''create table %s.t1 using %s.st (location) tags ("0001");'''%(database,database))
        tdSql.execute('''create table %s.t2 using %s.st (location) tags ("0002");'''%(database,database))
        tdSql.execute('''create table %s.mt (ts timestamp, val int);'''%(database))
        
        
        tdSql.execute(f'''insert into %s.t1 values({ts}, 1, {ts}) ({ts}, 2, {ts});'''%(database))
        tdSql.query("select ts, val from %s.t1;"%database)
        tdSql.checkData(0,1,2)
        
        ts += 1
        tdSql.execute(f'''insert into %s.t2 values({ts}, 1, {ts}) ({ts}, 5, {ts}) ({ts}, 2, {ts});'''%(database))
        tdSql.query("select ts, val from %s.t2;"%database)
        tdSql.checkData(0,1,2)
        
        tdSql.execute('''delete from %s.t2;'''%(database))
        tdSql.execute('''delete from %s.t1;'''%(database))
        
        ts -= 10
        tdSql.execute(f'''insert into %s.t1 values({ts}, 1, {ts}) %s.t2 values({ts}, 2, {ts});'''%(database,database))
        ts += 11
        tdSql.execute(f'''insert into %s.t1 values({ts}, 1, {ts}) %s.t2 values({ts}, 2, {ts});'''%(database,database))
        ts += 1
        tdSql.execute(f'''insert into %s.t1 values({ts}, 1, {ts}) %s.t2 values({ts}, 2, {ts});'''%(database,database))
        
        tdSql.query("select count(*) from %s.st;"%database)
        tdSql.checkData(0,0,6)
        
        tdSql.query('''select vt, val from %s.st order by vt, val desc;'''%(database))
        tdSql.checkData(0,1,2)
        tdSql.checkData(1,1,1)
        tdSql.checkData(2,1,2)
        tdSql.checkData(3,1,1)
        tdSql.checkData(4,1,2)
        tdSql.checkData(5,1,1)
        
        tdSql.execute('''insert into %s.mt select vt, val from %s.st order by vt, val desc;'''%(database,database))
        tdSql.query("select count(*) from %s.mt;"%database)
        tdSql.checkData(0,0,3)
        
        tdSql.query('''select ts, val from %s.mt order by ts asc;'''%(database))
        tdSql.checkData(0,1,1)
        tdSql.checkData(1,1,1)
        tdSql.checkData(2,1,1)
        
        tdSql.execute('''delete from %s.mt;'''%(database))
        tdSql.query('''select vt, val from %s.st order by vt, val asc;'''%(database))
        tdSql.checkData(0,1,1)
        tdSql.checkData(1,1,2)
        tdSql.checkData(2,1,1)
        tdSql.checkData(3,1,2)
        tdSql.checkData(4,1,1)
        tdSql.checkData(5,1,2)
        
        tdSql.execute('''insert into %s.mt select vt, val from %s.st order by vt, val asc;'''%(database,database))
        tdSql.query("select count(*) from %s.mt;"%database)
        tdSql.checkData(0,0,3)
        
        tdSql.query('''select ts, val from %s.mt order by ts asc;'''%(database))
        tdSql.checkData(0,1,2)
        tdSql.checkData(1,1,2)
        tdSql.checkData(2,1,2) 
        
        tdSql.execute('''delete from %s.mt;'''%(database))
        tdSql.query('''select vt, val from %s.st order by ts, val asc;'''%(database))
        tdSql.checkData(0,1,1)
        tdSql.checkData(1,1,2)
        tdSql.checkData(2,1,1)
        tdSql.checkData(3,1,2)
        tdSql.checkData(4,1,1)
        tdSql.checkData(5,1,2)

        tdSql.execute('''insert into %s.mt select vt, val from %s.st order by ts, val asc;'''%(database,database))
        tdSql.query("select count(*) from %s.mt;"%database)
        tdSql.checkData(0,0,3)

        tdSql.query('''select ts, val from %s.mt order by ts asc;'''%(database))
        tdSql.checkData(0,1,2)
        tdSql.checkData(1,1,2)
        tdSql.checkData(2,1,2)

        tdSql.execute('''delete from %s.mt;'''%(database))
        ts += 1
        tdSql.execute(f'''insert into %s.t1 values({ts}, -1, {ts}) %s.t2 values({ts}, -2, {ts});'''%(database,database))
        tdSql.query('''select vt, val from %s.st order by val asc;'''%(database))
        tdSql.checkData(0,1,-2)
        tdSql.checkData(1,1,-1)
        tdSql.checkData(2,1,1)
        tdSql.checkData(3,1,1)
        tdSql.checkData(4,1,1)
        tdSql.checkData(5,1,2)
        tdSql.checkData(6,1,2)
        tdSql.checkData(7,1,2)

        tdSql.execute('''insert into %s.mt select vt, val from %s.st order by val asc;'''%(database,database))
        tdSql.query("select count(*) from %s.mt;"%database)
        tdSql.checkData(0,0,4)
        
        tdSql.query('''select ts, val from %s.mt order by ts asc;'''%(database))
        tdSql.checkData(0,1,2)
        tdSql.checkData(1,1,2)
        tdSql.checkData(2,1,2)
        tdSql.checkData(3,1,-1)
                            
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
        
        self.use_select_sort("%s" %self.db)

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
