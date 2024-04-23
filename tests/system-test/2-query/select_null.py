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
    updatecfgDict = {'debugflag':0,'stdebugFlag': 143 ,"tqDebugflag":135}

    def init(self, conn, logSql, replicaVar):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.testcasePath = os.path.split(__file__)[0]
        self.testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))
        
        self.db = "sel_null"

    def insert_data(self,database,vgroups):
        num_random = 10
        tdSql.execute('''drop database if exists %s ;''' %database)
        tdSql.execute('''create database %s keep 36500 vgroups %d PRECISION 'us';'''%(database,vgroups))
        tdSql.execute('''use %s;'''%database)

        tdSql.execute('''create stable %s.stb0 (ts timestamp , c0 int , c1 double , c0null int , c1null double ) tags( t0 tinyint , t1 varchar(16) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint , t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);'''%database)

        for i in range(5):
            tdSql.execute('''create table %s.stb0_%d using %s.stb0 tags(%d,'varchar%d',%d,%d, %d, %d,%d,'binary%d','nchar%d',%d,%d,%d ) ;'''%(database,i,database,i,i,i,i,i,i,i,i,i,i,i,i))
            
        # insert data
        for i in range(num_random):   
            for j in range(50):     
                tdSql.execute('''insert into %s.stb0_0  (ts , c1 , c0) values(now, %d, %d) ;''' % (database,j,j))
                tdSql.execute('''insert into %s.stb0_1  (ts , c1 , c0) values(now, %d, %d) ;''' % (database,j,j))
                tdSql.execute('''insert into %s.stb0_2  (ts , c1 , c0) values(now, %d, %d) ;''' % (database,j,j))
                tdSql.execute('''insert into %s.stb0_3  (ts , c1 , c0) values(now, %d, %d) ;''' % (database,j,j))
                tdSql.execute('''insert into %s.stb0_4  (ts , c1 , c0) values(now, %d, %d) ;''' % (database,j,j))

        tdSql.query("select count(*) from %s.stb0;" %database)
        tdSql.checkData(0,0,5*num_random*50)
        tdSql.query("select count(*) from %s.stb0_0;"%database)
        tdSql.checkData(0,0,num_random*50)
        
    def ts_3085(self,database):  
        sql = "select count(c0null) from(select * from %s.stb0 limit 20,4) "%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,0)
        
        offset =  random.randint(10,100)
        for i in range(offset):
            sql = "select count(c0null) from(select * from %s.stb0 limit %d,%d) "%(database,offset,i)
            tdSql.query(sql) 
            tdSql.checkData(0,0,0)
            sql = "select count(c1null) from(select * from %s.stb0 limit %d,%d) "%(database,offset,i)
            tdSql.query(sql) 
            tdSql.checkData(0,0,0)
            sql = "select count(c0) from(select * from %s.stb0 limit %d,%d) "%(database,offset,i)
            tdSql.query(sql) 
            tdSql.checkData(0,0,i)
            sql = "select count(c1) from(select * from %s.stb0 limit %d,%d) "%(database,offset,i)
            tdSql.query(sql) 
            tdSql.checkData(0,0,i)
            sql = "select count(t0) from(select * from %s.stb0 limit %d,%d) "%(database,offset,i)
            tdSql.query(sql) 
            tdSql.checkData(0,0,i)
            sql = "select count(t1) from(select * from %s.stb0 limit %d,%d) "%(database,offset,i)
            tdSql.query(sql) 
            tdSql.checkData(0,0,i)


    def ts_2974_max(self,database):  
        sql = "select max(c0) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,49)
        sql = "select max(c0),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,49)
        sql = "select max(c1) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,49)
        sql = "select max(c1),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,49)
        
        sql = "select max(c0null) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,"None")
        sql = "select max(c0null),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,"None")
        sql = "select max(c1null) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,"None")
        sql = "select max(c1null),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,"None") 
        
        sql = "select max(t0) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,4)
        sql = "select max(t0),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,4)
        sql = "select max(t1) from %s.stb0 where ts<now;"%(database)
        tdSql.error(sql) 
        sql = "select max(t1),ts from %s.stb0 where ts<now;"%(database)
        tdSql.error(sql) 
        sql = "select max(t_bool) from %s.stb0 where ts<now;"%(database)
        tdSql.error(sql) 
        sql = "select max(t_bool),ts from %s.stb0 where ts<now;"%(database)
        tdSql.error(sql) 
        sql = "select max(t_binary) from %s.stb0 where ts<now;"%(database)
        tdSql.error(sql) 
        sql = "select max(t_binary),ts from %s.stb0 where ts<now;"%(database)
        tdSql.error(sql) 
        sql = "select max(t_nchar) from %s.stb0 where ts<now;"%(database)
        tdSql.error(sql) 
        sql = "select max(t_nchar),ts from %s.stb0 where ts<now;"%(database)
        tdSql.error(sql) 
        sql = "select max(t_int) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,4)
        sql = "select max(t_int),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,4)
        sql = "select max(t_int) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,4)
        sql = "select max(t_int),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,4)
        sql = "select max(t_int) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,4)
        sql = "select max(t_int),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,4)
        sql = "select max(t_bigint) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,4)
        sql = "select max(t_bigint),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,4)
        sql = "select max(t_smallint) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,4)
        sql = "select max(t_smallint),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,4)
        sql = "select max(t_tinyint) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,4)
        sql = "select max(t_tinyint),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,4)
        sql = "select max(t_float) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,4)
        sql = "select max(t_float),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,4)
        sql = "select max(t_double) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,4)
        sql = "select max(t_double),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,4)
        
    def ts_2974_min(self,database):  
        sql = "select min(c0) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,0)
        sql = "select min(c0),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,0)
        sql = "select min(c1) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,0)
        sql = "select min(c1),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,0)
        
        sql = "select min(c0null) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,"None")
        sql = "select min(c0null),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,"None")
        sql = "select min(c1null) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,"None")
        sql = "select min(c1null),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,"None") 
        
        sql = "select min(t0) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,0)
        sql = "select min(t0),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,0)
        sql = "select min(t1) from %s.stb0 where ts<now;"%(database)
        tdSql.error(sql) 
        sql = "select min(t1),ts from %s.stb0 where ts<now;"%(database)
        tdSql.error(sql) 
        sql = "select min(t_bool) from %s.stb0 where ts<now;"%(database)
        tdSql.error(sql) 
        sql = "select min(t_bool),ts from %s.stb0 where ts<now;"%(database)
        tdSql.error(sql) 
        sql = "select min(t_binary) from %s.stb0 where ts<now;"%(database)
        tdSql.error(sql) 
        sql = "select min(t_binary),ts from %s.stb0 where ts<now;"%(database)
        tdSql.error(sql) 
        sql = "select min(t_nchar) from %s.stb0 where ts<now;"%(database)
        tdSql.error(sql) 
        sql = "select min(t_nchar),ts from %s.stb0 where ts<now;"%(database)
        tdSql.error(sql) 
        sql = "select min(t_int) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,0)
        sql = "select min(t_int),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,0)
        sql = "select min(t_int) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,0)
        sql = "select min(t_int),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,0)
        sql = "select min(t_int) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,0)
        sql = "select min(t_int),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,0)
        sql = "select min(t_bigint) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,0)
        sql = "select min(t_bigint),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,0)
        sql = "select min(t_smallint) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,0)
        sql = "select min(t_smallint),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,0)
        sql = "select min(t_tinyint) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,0)
        sql = "select min(t_tinyint),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,0)
        sql = "select min(t_float) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,0)
        sql = "select min(t_float),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,0)
        sql = "select min(t_double) from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,0)
        sql = "select min(t_double),ts from %s.stb0 where ts<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,0)
        
    def ts_2601(self,database): 
        
        tdSql.query("alter local 'keepcolumnname' '0';")  
        sql = "select ts,c0 from (select last(*) from %s.stb0 where ts<now);"%(database)
        tdSql.error(sql) 
        sql = "select ts,c0 from (select last(*) from %s.stb0 where ts<now order by ts );"%(database)
        tdSql.error(sql) 
        
        tdSql.query("alter local 'keepcolumnname' '1';")         
        sql = "select ts,c0 from (select last(*) from %s.stb0 where ts<now);"%(database)
        tdSql.query(sql) 
        sql = "select ts,c0 from (select last(*) from %s.stb0 where ts<now order by ts );"%(database)
        tdSql.query(sql) 
        
    def ts_3108(self,database): 
         
        sql = "select count(*) from %s.stb0 where to_unixtimestamp('2023-01-01 00:00:00.000')<now;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,2500)
        sql = "select count(*) from %s.stb0 where to_unixtimestamp('2023-01-01 00:00:00.000')>now;"%(database)
        tdSql.query(sql) 
        tdSql.checkRows(0)
        
        sql = "select count(*) from %s.stb0 where to_unixtimestamp('2024-01-01 00:00:00.000')<now+1y;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,0,2500)
        sql = "select count(*) from %s.stb0 where to_unixtimestamp('2024-01-01 00:00:00.000')>now+1y;"%(database)
        tdSql.query(sql) 
        tdSql.checkRows(0)
        
    def ts_3110(self,database): 
         
        sql1 = "select * from %s.stb0 order by ts desc limit 2;"%(database)
        tdSql.query(sql1) 
        data1_0_0 = tdSql.getData(0,0)
        data1_1_0 = tdSql.getData(1,0)
        
        sql2 = "select * from (select * from %s.stb0 order by ts desc limit 2) order by ts;"%(database)
        tdSql.query(sql2) 
        data2_0_0 = tdSql.getData(0,0)
        data2_1_0 = tdSql.getData(1,0)
        
        if (data1_0_0 == data2_1_0) and (data1_1_0 == data2_0_0):
            tdLog.info("ts_3110: success")
        else:
            tdLog.exit("ts_3110: sql1 result:'%s' not equal sql2 result:'%s'" % (sql1,sql2))
        
    def ts_3036(self,database): 
         
        sql1 = "select ts , c0 , c1 , c0null , c1null from (select ts , c0 , c1 , c0null , c1null from %s.stb0_0 where ts between now -1d and now +1d \
            union all select ts , c0 , c1 , c0null , c1null from %s.stb0_1 where ts between now -1d and now +1d \
            union all select ts , c0 , c1 , c0null , c1null from %s.stb0_2 where ts between now -1d and now +1d ) tt \
            where ts < now order by tt.ts desc limit 2;"%(database,database,database)
        tdSql.query(sql1) 
        data1_0_0 = tdSql.getData(0,0)
        data1_1_0 = tdSql.getData(1,0)
        
        sql2 = "select ts , c0 , c1 , c0null , c1null  from (select tbname as tb, ts , c0 , c1 , c0null , c1null from %s.stb0 where ts > now  \
            union all select tbname as tb, ts , c0 , c1 , c0null , c1null from %s.stb0 where ts = now  \
            union all select tbname as tb, ts , c0 , c1 , c0null , c1null from %s.stb0 where ts < now ) tt \
            where tt.ts between now -1d and now +1d  and tt.tb in ('stb0_0','stb0_1','stb0_2') order by tt.ts desc limit 2;"%(database,database,database)
        tdSql.query(sql2) 
        data2_0_0 = tdSql.getData(0,0)
        data2_1_0 = tdSql.getData(1,0)
        
        sql3 = "select ts , c0 , c1 , c0null , c1null  from %s.stb0  \
            where ts between now -1d and now +1d  and tbname in ('stb0_0','stb0_1','stb0_2') order by ts desc limit 2;"%(database)
        tdSql.query(sql3) 
        data3_0_0 = tdSql.getData(0,0)
        data3_1_0 = tdSql.getData(1,0)
        
        if (data1_0_0 == data2_0_0 == data3_0_0) and (data1_1_0 == data2_1_0 == data3_1_0):
            tdLog.info("ts_3036: success")
        else:
            tdLog.exit("ts_3036: sql1 result:'%s' not equal sql2 result:'%s' or not equal sql3 result:'%s'" % (sql1,sql2,sql3))
        
        
    def ts_23569(self,database): 
        
        tdSql.query("alter local 'keepcolumnname' '0';") 
        sql = "alter table %s.stb0 drop tag t10;"%(database)
        tdSql.error(sql) 
        error_msg = tdSql.error(sql) 
        include_msg = 'Invalid tag name'
        if include_msg in error_msg:
            tdLog.info("ts_23569: success")
        else:
            tdLog.exit("ts_23569: include_msg:'%s' not in error_msg:'%s'" % (include_msg,error_msg))
        
        tdSql.query("alter local 'keepcolumnname' '1';")         
        sql = "alter table %s.stb0 drop tag t10;"%(database)
        tdSql.error(sql) 
        error_msg = tdSql.error(sql) 
        include_msg = 'Invalid tag name'
        if include_msg in error_msg:
            tdLog.info("ts_23569: success")
        else:
            tdLog.exit("ts_23569: include_msg:'%s' not in error_msg:'%s'" % (include_msg,error_msg))
            
    def ts_23505(self,database): 
        
        sql = "create table %s.`12345` (`567` timestamp,num int);"%(database)
        tdSql.execute(sql)
        
        sql = "insert into %s.12345 values (now,1);"%(database)
        tdSql.error(sql) 
        
        sql = "insert into %s.`12345` values (now,1);"%(database)
        tdSql.execute(sql) 
        
        sql = "select * from %s.`12345` order by `567` desc limit 2;"%(database)
        tdSql.query(sql) 
        tdSql.checkData(0,1,1)
        
        sql = "drop table %s.`12345` ;"%(database)
        tdSql.execute(sql) 
        sql = "select * from %s.`12345` order by `567` desc limit 2;"%(database)
        tdSql.error(sql) 

    def td_27939(self,database): 
        sql = "create table %s.`test1eq2` (`ts` timestamp, id int);"%(database)
        tdSql.execute(sql)

        sql = "insert into %s.test1eq2 values (now,1);"%(database)
        tdSql.execute(sql) 
        
        sql = "insert into %s.`test1eq2` values (now,2);"%(database)
        tdSql.execute(sql) 

        sql = "select * from %s.`test1eq2` where 1=2;"%(database)
        tdSql.query(sql) 
        tdSql.checkRows(0)

        sql = "select * from (select * from %s.`test1eq2` where 1=2);"%(database)
        tdSql.query(sql) 
        tdSql.checkRows(0)

        sql = "drop table %s.`test1eq2` ;"%(database)
        tdSql.execute(sql)


    def run(self):    
        startTime = time.time()  
                  
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename)) 
        
        self.insert_data("%s" %self.db,2)
        
        self.ts_3085("%s" %self.db)
        self.ts_2974_max("%s" %self.db)
        self.ts_2974_min("%s" %self.db)
        self.ts_2601("%s" %self.db)
        self.ts_23569("%s" %self.db)
        self.ts_3108("%s" %self.db)
        self.ts_3110("%s" %self.db)
        self.ts_23505("%s" %self.db)
        self.ts_3036("%s" %self.db)

        self.td_27939("%s" %self.db)
        
        tdSql.query("flush database %s" %self.db) 
        
        self.ts_2974_max("%s" %self.db)
        self.ts_2974_min("%s" %self.db)
        self.ts_3085("%s" %self.db)
        self.ts_2601("%s" %self.db)
        self.ts_23569("%s" %self.db)
        self.ts_3108("%s" %self.db)
        self.ts_3110("%s" %self.db)
        self.ts_23505("%s" %self.db)
        self.ts_3036("%s" %self.db)

        self.td_27939("%s" %self.db)

        self.test_select_as_chinese_characters();
        endTime = time.time()
        print("total time %ds" % (endTime - startTime))
    
    def test_select_as_chinese_characters(self):
        tdSql.execute("use sel_null")
        tdSql.query("select ts as 时间戳, c0 as c第一列, t0 标签1 from sel_null.stb0_0 limit 10", queryTimes=1)
        tdSql.checkRows(10)
        tdSql.query("select 时间戳 from (select ts as 时间戳, c0 as c第一列, t0 标签1 from sel_null.stb0_0) where 时间戳 > '2023-1-1' and c第一列 != 0 and 标签1 == 0 limit 10", queryTimes=1)
        tdSql.checkRows(10)
        tdSql.query("select count(*) as 计数 from sel_null.stb0_0 partition by c0 as 分组列", queryTimes=1)

        tdSql.error("create database 数据库")
        tdSql.error("create table sel_null.中文库 (ts timestamp, c2 int)")
        tdSql.error("create table sel_null.table1(ts timestamp, 列2 int)")
        tdSql.execute("create stable sel_null.stable1(ts timestamp, `值` int) tags(`标签1` int, `标签2` int)")
        tdSql.execute('insert into sel_null.ct1 using sel_null.stable1 tags(1, 1) values(now, 1)', queryTimes=1)
        tdSql.execute('insert into sel_null.ct1 using sel_null.stable1 tags(2, 2) values(now, 2)', queryTimes=1)
        tdSql.execute('insert into sel_null.ct1 using sel_null.stable1 tags(2, 2) values(now, 3)', queryTimes=1)
        tdSql.query('select 值 , 标签1 from sel_null.stable1', queryTimes=1)
        tdSql.query('select case 值 when 标签1 then 标签1 else 标签2 end from sel_null.stable1', queryTimes=1)
        tdSql.query('select count(*) from sel_null.stable1 group by 值 having sum(标签1) > 0', queryTimes=1)
        tdSql.query('show table tags `标签1` 标签n  from sel_null.stable1', queryTimes=1)
        tdSql.query('create sma index a on sel_null.stable1 FUNCTION (sum(值)) interval(1s)', queryTimes=1)
        tdSql.query('select count(值) from sel_null.stable1', queryTimes=1)
        tdSql.query('select stable1.值 from sel_null.stable1', queryTimes=1)
        tdSql.query('select stable1.值 from sel_null.stable1 order by 值', queryTimes=1)
        tdSql.execute('create stable sel_null.join_stable(`时间戳` timestamp, c1 int) tags(`标签1` int)', queryTimes=1)
        tdSql.query('select a.值 from sel_null.stable1 a join sel_null.join_stable b on a.ts = 时间戳;', queryTimes=1)
        tdSql.query('select a.值 from sel_null.stable1 a join sel_null.join_stable b on a.ts = b.时间戳;', queryTimes=1)
        tdSql.execute('create user user1 pass "asd"', queryTimes=1)
        tdSql.execute('grant write on sel_null.stable1 with 标签1 = 1 to user1',queryTimes=1)
        tdSql.execute('select count(*) from sel_null.stable1 state_window(值)', queryTimes=1)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
