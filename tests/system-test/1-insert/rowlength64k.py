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
import string
from faker import Faker
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
from util.dnodes import tdDnodes
from util.dnodes import *

class TDTestCase:
    updatecfgDict = {'maxSQLLength':1048576,'debugFlag': 143 ,"querySmaOptimize":1}
    
    def init(self, conn, logSql, replicaVar):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.testcasePath = os.path.split(__file__)[0]
        self.testcasePath = self.testcasePath.replace('\\', '//')
        self.testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))
        
        now = time.time()
        self.ts = int(round(now * 1000))
        self.num = 100

    def get_random_string(self, length):
        letters = string.ascii_lowercase
        result_str = ''.join(random.choice(letters) for i in range(length))
        return result_str
    
    def ins_query(self):
        sql = 'select * from information_schema.ins_tables where table_name match "table"'    
        tdSql.query(sql)
        
        self.stable_query()
    
    def stable_query(self):
        # select * from stable_1 where loc match '[a-z]';
        show_sql = "show db.stables;"
        tdSql.query(show_sql) 
        queryRows = len(tdSql.queryResult)  
        for i in range(queryRows):
            show_sql = "show db.stables;"
            tdSql.query(show_sql) 
            stable_name = tdSql.queryResult[i][0]
            
            stable_sql = "select * from db.%s where loc match '[a-z]'" %stable_name
            tdSql.query(stable_sql) 

    def run_8(self):  

        print("==============step8,stable table , mix data type==============")
        sql = "create stable db.stable_16(ts timestamp, "
        sql += "col4090 int ,"  
        sql += "col4091 binary(65517))"  
        sql += " tags (loc binary(16370),tag_1 int,tag_2 int,tag_3 int) " 
        tdLog.info(len(sql))      
        tdSql.execute(sql)
        sql = '''create table db.table_160 using db.stable_16 
                    tags('%s' , '1' , '2' , '3' );'''% self.get_random_string(16370)
        tdSql.execute(sql)

        for i in range(self.num):
            sql = "insert into db.table_160 values(%d, "
            sql += "'%d'," % i
            sql += "'%s')" % self.get_random_string(65517)
            tdSql.execute(sql % (self.ts + i))
        time.sleep(1)
        tdSql.query("select count(*) from db.table_160")
        tdSql.checkData(0, 0, self.num)
        tdSql.query("select * from db.table_160")
        tdSql.checkRows(self.num)
        tdSql.checkCols(3)

        self.ins_query()
        
        #insert null value 
        tdLog.info('test insert null value') 
        sql = '''create table db.table_161 using db.stable_16 
                    tags('table_61' , '1' , '2' , '3' );'''
        tdSql.execute(sql)

        for i in range(self.num):
            sql = "insert into db.table_161(ts) values(%d) "
            tdSql.execute(sql % (self.ts + i))
        time.sleep(1)
        tdSql.query("select count(*) from db.table_161")
        tdSql.checkData(0, 0, self.num)
        tdSql.query("select * from db.table_161")
        tdSql.checkRows(self.num)
        tdSql.checkCols(3)

        #define TSDB_MAX_BYTES_PER_ROW 65531   TSDB_MAX_TAGS_LEN 16384 
        #ts:8\int:4\smallint:2\bigint:8\bool:1\float:4\tinyint:1\nchar:4*（）+2[offset]\binary:1*（）+2[offset]
        tdLog.info('test super table max bytes per row 65531') 
        sql = "create table db.stable_17(ts timestamp, "  
        sql += "col4090 int,"  
        sql += "col4091 binary(65517))"    
        sql += " tags (loc binary(16370),tag_1 int,tag_2 int,tag_3 int) " #4*3+16370+2
        tdSql.execute(sql)
        sql = '''create table db.table_170 using db.stable_17 
                    tags('%s' , '1' , '2' , '3' );'''% self.get_random_string(16370)
        tdSql.execute(sql)
        tdSql.query("select * from db.table_170")
        tdSql.checkCols(3)
        tdSql.query("describe db.table_170")
        tdSql.checkRows(7)

        self.ins_query()

        tdLog.info('test super table drop and add column or tag') 
        sql = "alter stable db.stable_17 drop column col4091; "
        tdSql.execute(sql)
        sql = "select * from db.stable_17; "
        tdSql.query(sql)
        tdSql.checkCols(6)
        sql = "alter table db.stable_17 add column col4091 binary(65518); "
        tdSql.error(sql) 
        sql = "alter table db.stable_17 add column col4091 binary(65517); "
        tdSql.execute(sql)
        sql = "select * from db.stable_17; "
        tdSql.query(sql)
        tdSql.checkCols(7) 

        self.ins_query()
        
        sql = "alter stable db.stable_17 drop tag loc; "
        tdSql.execute(sql)
        sql = "select * from db.stable_17; "
        tdSql.query(sql)
        tdSql.checkCols(6)
        sql = "alter table db.stable_17 add tag loc binary(16371); "
        tdSql.error(sql) 
        sql = "alter table db.stable_17 add tag loc binary(16370); "
        tdSql.execute(sql)
        sql = "select * from db.stable_17; "
        tdSql.query(sql)
        tdSql.checkCols(7) 

        sql = "alter stable db.stable_17 drop tag tag_1; "
        tdSql.execute(sql)
        sql = "select * from db.stable_17; "
        tdSql.query(sql)
        tdSql.checkCols(6)
        sql = "alter table db.stable_17 add tag tag_1 int; "
        tdSql.execute(sql)
        sql = "select * from db.stable_17; "
        tdSql.query(sql)
        tdSql.checkCols(7) 
        sql = "alter table db.stable_17 add tag loc1 nchar(10); "
        tdSql.error(sql) 

        tdLog.info('test super table max bytes per row 65531') 
        sql = "create table db.stable_18(ts timestamp, "
        sql += "col4091 binary(65518))"  
        sql += " tags (loc nchar(10),tag_1 int,tag_2 int,tag_3 int) " 
        tdSql.error(sql)

        tdLog.info('test super table max bytes per row tag 16384') 
        sql = "create table db.stable_18(ts timestamp, "
        sql += "col4091 binary(65517))"  
        sql += " tags (loc binary(16371),tag_1 int,tag_2 int,tag_3 int) " 
        tdSql.error(sql)

        self.ins_query()
        
    def run_9(self):  
        
        print("==============step9,stable table , mix data type==============")
        sql = "create stable db.stable_26(ts timestamp, "
        sql += "col4090 int ,"  
        sql += "col4091 binary(65517))"  
        sql += " tags (loc nchar(4092),tag_1 int,tag_2 int,tag_3 int) " 
        tdLog.info(len(sql))      
        tdSql.execute(sql)
        sql = '''create table db.table_260 using db.stable_26 
                    tags('%s' , '1' , '2' , '3' );'''% self.get_random_string(4092)
        tdSql.execute(sql)

        for i in range(self.num):
            sql = "insert into db.table_260 values(%d, "
            sql += "'%d'," % i
            sql += "'%s')" % self.get_random_string(65517)
            tdSql.execute(sql % (self.ts + i))
        time.sleep(1)
        tdSql.query("select count(*) from db.table_260")
        tdSql.checkData(0, 0, self.num)
        tdSql.query("select * from db.table_260")
        tdSql.checkRows(self.num)
        tdSql.checkCols(3)

        self.ins_query()
        
        #insert null value 
        tdLog.info('test insert null value') 
        sql = '''create table db.table_261 using db.stable_26 
                    tags('table_261' , '1' , '2' , '3' );'''
        tdSql.execute(sql)

        for i in range(self.num):
            sql = "insert into db.table_261(ts) values(%d) "
            tdSql.execute(sql % (self.ts + i))
        time.sleep(1)
        tdSql.query("select count(*) from db.table_261")
        tdSql.checkData(0, 0, self.num)
        tdSql.query("select * from db.table_261")
        tdSql.checkRows(self.num)
        tdSql.checkCols(3)

        #define TSDB_MAX_BYTES_PER_ROW 65531   TSDB_MAX_TAGS_LEN 16384 
        #ts:8\int:4\smallint:2\bigint:8\bool:1\float:4\tinyint:1\nchar:4*（）+2[offset]\binary:1*（）+2[offset]
        tdLog.info('test super table max bytes per row 65531') 
        sql = "create table db.stable_27(ts timestamp, "  
        sql += "col4090 int,"  
        sql += "col4091 binary(65517))"    
        sql += " tags (loc nchar(4092),tag_1 int,tag_2 int,tag_3 int) " #4*3+16370+2
        tdSql.execute(sql)
        sql = '''create table db.table_270 using db.stable_27 
                    tags('%s' , '1' , '2' , '3' );'''% self.get_random_string(4092)
        tdSql.execute(sql)
        tdSql.query("select * from db.table_270")
        tdSql.checkCols(3)
        tdSql.query("describe db.table_270")
        tdSql.checkRows(7)

        self.ins_query()

        tdLog.info('test super table drop and add column or tag') 
        sql = "alter stable db.stable_27 drop column col4091; "
        tdSql.execute(sql)
        sql = "select * from db.stable_27; "
        tdSql.query(sql)
        tdSql.checkCols(6)
        sql = "alter table db.stable_27 add column col4091 binary(65518); "
        tdSql.error(sql) 
        sql = "alter table db.stable_27 add column col4091 binary(65517); "
        tdSql.execute(sql)
        sql = "select * from db.stable_27; "
        tdSql.query(sql)
        tdSql.checkCols(7) 

        self.ins_query()
        
        sql = "alter stable db.stable_27 drop tag loc; "
        tdSql.execute(sql)
        sql = "select * from db.stable_27; "
        tdSql.query(sql)
        tdSql.checkCols(6)
        sql = "alter table db.stable_27 add tag loc binary(16371); "
        tdSql.error(sql) 
        sql = "alter table db.stable_27 add tag loc binary(16370); "
        tdSql.execute(sql)
        sql = "select * from db.stable_27; "
        tdSql.query(sql)
        tdSql.checkCols(7) 

        sql = "alter stable db.stable_27 drop tag tag_1; "
        tdSql.execute(sql)
        sql = "select * from db.stable_27; "
        tdSql.query(sql)
        tdSql.checkCols(6)
        sql = "alter table db.stable_27 add tag tag_1 int; "
        tdSql.execute(sql)
        sql = "select * from db.stable_27; "
        tdSql.query(sql)
        tdSql.checkCols(7) 
        sql = "alter table db.stable_27 add tag loc1 nchar(10); "
        tdSql.error(sql) 

        tdLog.info('test super table max bytes per row 65531') 
        sql = "create table db.stable_28(ts timestamp, "
        sql += "col4091 binary(65518))"  
        sql += " tags (loc nchar(10),tag_1 int,tag_2 int,tag_3 int) " 
        tdSql.error(sql)

        tdLog.info('test super table max bytes per row tag 16384') 
        sql = "create table db.stable_28(ts timestamp, "
        sql += "col4091 binary(65517))"  
        sql += " tags (loc binary(16371),tag_1 int,tag_2 int,tag_3 int) " 
        tdSql.error(sql)

        self.ins_query()
        
    def run_1(self):  
 

        print("==============step1, regular table, 1 ts + 4094 cols + 1 binary==============")
        startTime = time.time() 
        sql = "create table db.regular_table_1(ts timestamp, "
        for i in range(4094):
            sql += "col%d int, " % (i + 1)
        sql += "col4095 binary(22))"  
        tdLog.info(len(sql))      
        tdSql.execute(sql)

        for i in range(self.num):
            sql = "insert into db.regular_table_1 values(%d, "
            for j in range(4094):
                str = "'%s', " % random.randint(0,1000)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i))
        time.sleep(1)
        tdSql.query("select count(*) from db.regular_table_1")
        tdSql.checkData(0, 0, self.num)
        tdSql.query("select * from db.regular_table_1")
        tdSql.checkRows(self.num)
        tdSql.checkCols(4096)

        self.ins_query()

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))

        #insert in order
        tdLog.info('test insert in order') 
        for i in range(self.num):
            sql = "insert into db.regular_table_1 (ts,col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col4095) values(%d, "
            for j in range(10):
                str = "'%s', " % random.randint(0,1000)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i + 1000))
        time.sleep(1)
        tdSql.query("select count(*) from db.regular_table_1")
        tdSql.checkData(0, 0, 2*self.num)
        tdSql.query("select * from db.regular_table_1")
        tdSql.checkRows(2*self.num)
        tdSql.checkCols(4096)

        #insert out of order
        tdLog.info('test insert out of order') 
        for i in range(self.num):
            sql = "insert into db.regular_table_1 (ts,col123,col2213,col331,col41,col523,col236,col71,col813,col912,col1320,col4095) values(%d, "
            for j in range(10):
                str = "'%s', " % random.randint(0,1000)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i + 2000))
        time.sleep(1)
        tdSql.query("select count(*) from db.regular_table_1")
        tdSql.checkData(0, 0, 3*self.num)
        tdSql.query("select * from db.regular_table_1")
        tdSql.checkRows(3*self.num)
        tdSql.checkCols(4096)

        self.ins_query()
        
    def run_2(self):  

        print("==============step2,regular table error col or value==============")
        tdLog.info('test regular table exceeds row num') 
        # column > 4096
        sql = "create table db.regular_table_2(ts timestamp, "
        for i in range(4095):
            sql += "col%d int, " % (i + 1)
        sql += "col4096 binary(22))"  
        tdLog.info(len(sql))      
        tdSql.error(sql)

        self.ins_query()
        
        # column > 4096
        sql = "insert into db.regular_table_1 values(%d, "
        for j in range(4095):
            str = "'%s', " % random.randint(0,1000)                
            sql += str
        sql += "'%s')" % self.get_random_string(22)
        tdSql.error(sql)

        # insert column < 4096
        sql = "insert into db.regular_table_1 values(%d, "
        for j in range(4092):
            str = "'%s', " % random.randint(0,1000)                
            sql += str
        sql += "'%s')" % self.get_random_string(22)
        tdSql.error(sql)

        # alter column > 4096
        sql = "alter table db.regular_table_1 add column max int; "
        tdSql.error(sql)

        self.ins_query()
        
    def run_3(self):  
 

        print("==============step3,regular table , mix data type==============")
        startTime = time.time() 
        sql = "create table db.regular_table_3(ts timestamp, "
        for i in range(2000):
            sql += "col%d int, " % (i + 1)
        for i in range(2000,4094):
            sql += "col%d bigint, " % (i + 1)
        sql += "col4095 binary(22))"  
        tdLog.info(len(sql))      
        tdSql.execute(sql)

        for i in range(self.num):
            sql = "insert into db.regular_table_3 values(%d, "
            for j in range(4094):
                str = "'%s', " % random.randint(0,1000)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i))
        time.sleep(1)
        tdSql.query("select count(*) from db.regular_table_3")
        tdSql.checkData(0, 0, self.num)
        tdSql.query("select * from db.regular_table_3")
        tdSql.checkRows(self.num)
        tdSql.checkCols(4096)

        self.ins_query()

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))

        sql = "create table db.regular_table_4(ts timestamp, "
        for i in range(500):
            sql += "int_%d int, " % (i + 1)
        for i in range(500,1000):
            sql += "smallint_%d smallint, " % (i + 1)
        for i in range(1000,1500):
            sql += "tinyint_%d tinyint, " % (i + 1)
        for i in range(1500,2000):
            sql += "double_%d double, " % (i + 1)
        for i in range(2000,2500):
            sql += "float_%d float, " % (i + 1)
        for i in range(2500,3000):
            sql += "bool_%d bool, " % (i + 1)
        for i in range(3000,3500):
            sql += "bigint_%d bigint, " % (i + 1)
        for i in range(3500,3800):
            sql += "nchar_%d nchar(4), " % (i + 1)
        for i in range(3800,4090):
            sql += "binary_%d binary(10), " % (i + 1)
        for i in range(4090,4094):
            sql += "timestamp_%d timestamp, " % (i + 1)
        sql += "col4095 binary(22))"  
        tdLog.info(len(sql))      
        tdSql.execute(sql)

        self.ins_query()

        for i in range(self.num):
            sql = "insert into db.regular_table_4 values(%d, "
            for j in range(500):
                str = "'%s', " % random.randint(-2147483647,2147483647)                
                sql += str
            for j in range(500,1000):
                str = "'%s', " % random.randint(-32767,32767 )                
                sql += str
            for j in range(1000,1500):
                str = "'%s', " % random.randint(-127,127)                
                sql += str
            for j in range(1500,2000):
                str = "'%s', " % random.randint(-922337203685477580700,922337203685477580700)                
                sql += str
            for j in range(2000,2500):
                str = "'%s', " % random.randint(-92233720368547758070,92233720368547758070)                
                sql += str
            for j in range(2500,3000):
                str = "'%s', " % random.choice(['true','false'])               
                sql += str
            for j in range(3000,3500):
                str = "'%s', " % random.randint(-9223372036854775807,9223372036854775807)                
                sql += str
            for j in range(3500,3800):
                str = "'%s', " % self.get_random_string(4)                
                sql += str
            for j in range(3800,4090):
                str = "'%s', " % self.get_random_string(10)                
                sql += str
            for j in range(4090,4094):
                str = "%s, " % (self.ts + j)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i))
        time.sleep(1)
        tdSql.query("select count(*) from db.regular_table_4")
        tdSql.checkData(0, 0, self.num)
        tdSql.query("select * from db.regular_table_4")
        tdSql.checkRows(self.num)
        tdSql.checkCols(4096)
        tdLog.info("end ,now new one")

        self.ins_query()

        #insert null value 
        tdLog.info('test insert null value') 
        for i in range(self.num):
            sql = "insert into db.regular_table_4 values(%d, "
            for j in range(2500):
                str = "'%s', " % random.choice(['NULL' ,'NULL' ,'NULL' ,1 , 10 ,100 ,-100 ,-10, 88 ,66 ,'NULL' ,'NULL' ,'NULL' ])               
                sql += str
            for j in range(2500,3000):
                str = "'%s', " % random.choice(['true' ,'false'])               
                sql += str
            for j in range(3000,3500):
                str = "'%s', " % random.randint(-9223372036854775807,9223372036854775807)                
                sql += str
            for j in range(3500,3800):
                str = "'%s', " % self.get_random_string(4)                
                sql += str
            for j in range(3800,4090):
                str = "'%s', " % self.get_random_string(10)                
                sql += str
            for j in range(4090,4094):
                str = "%s, " % (self.ts + j)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i + 10000))
        time.sleep(1)
        tdSql.query("select count(*) from db.regular_table_4")
        tdSql.checkData(0, 0, 2*self.num)
        tdSql.query("select * from db.regular_table_4")
        tdSql.checkRows(2*self.num)
        tdSql.checkCols(4096)

        #insert in order
        tdLog.info('test insert in order') 
        for i in range(self.num):
            sql = "insert into db.regular_table_4 (ts,int_2,int_22,int_169,smallint_537,smallint_607,tinyint_1030,tinyint_1491,double_1629,double_1808,float_2075,col4095) values(%d, "
            for j in range(10):
                str = "'%s', " % random.randint(0,100)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i + 1000))
        time.sleep(1)
        tdSql.query("select count(*) from db.regular_table_4")
        tdSql.checkData(0, 0, 3*self.num)
        tdSql.query("select * from db.regular_table_4")
        tdSql.checkRows(3*self.num)
        tdSql.checkCols(4096)

        self.ins_query()

        #insert out of order
        tdLog.info('test insert out of order') 
        for i in range(self.num):
            sql = "insert into db.regular_table_4 (ts,int_169,float_2075,int_369,tinyint_1491,tinyint_1030,float_2360,smallint_537,double_1808,double_1608,double_1629,col4095) values(%d, "
            for j in range(10):
                str = "'%s', " % random.randint(0,100)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i + 2000))
        time.sleep(1)
        tdSql.query("select count(*) from db.regular_table_4")
        tdSql.checkData(0, 0, 4*self.num)
        tdSql.query("select * from db.regular_table_4")
        tdSql.checkRows(4*self.num)
        tdSql.checkCols(4096)

        #define TSDB_MAX_BYTES_PER_ROW 49151[old:1024 && 16384]
        # 3.0 old: tag binary max is 16384, col+ts binary max  49151
        # 3.0 new: tag binary max is 16384-2, col+ts binary max 65531
        #ts:8\int:4\smallint:2\bigint:8\bool:1\float:4\tinyint:1\nchar:4*（）+2[offset]\binary:1*（）+2[offset]
        tdLog.info('test regular_table max bytes per row 65531') 
        sql = "create table db.regular_table_5(ts timestamp, " #1*8 sum=8
        for i in range(500):
            sql += "int_%d int, " % (i + 1)   #500*4=2000 sum=2008
        for i in range(500,1000):
            sql += "smallint_%d smallint, " % (i + 1) #500*2=1000 sum=3008
        for i in range(1000,1500):
            sql += "tinyint_%d tinyint, " % (i + 1)  #500*1=500 sum=3508
        for i in range(1500,2000):
            sql += "double_%d double, " % (i + 1) #500*8=4000 sum=7508
        for i in range(2000,2500):
            sql += "float_%d float, " % (i + 1) #500*4=2000 sum=9508
        for i in range(2500,3000):
            sql += "bool_%d bool, " % (i + 1) #500*1=500 sum=10008
        for i in range(3000,3500):
            sql += "bigint_%d bigint, " % (i + 1) #500*8=4000 sum=14008
        for i in range(3500,3800):
            sql += "nchar_%d nchar(32), " % (i + 1) #300*(32*4+2)=39000 sum=53008
        for i in range(3800,4090):
            sql += "binary_%d binary(40), " % (i + 1) #290*(40+2)=12180 sum=65188
        for i in range(4090,4094):
            sql += "timestamp_%d timestamp, " % (i + 1) #4*8=32   sum=65220
        sql += "col4095 binary(309))"       #309+2=311 sum=65531
        tdSql.execute(sql)
        tdSql.query("select * from db.regular_table_5")
        tdSql.checkCols(4096)
        
        sql = "alter table db.regular_table_5 modify column col4095 binary(310); "
        tdSql.error(sql)    

        self.ins_query() 
        
        # drop and add
        sql = "alter table db.regular_table_5 drop column col4095; "
        tdSql.execute(sql)
        sql = "select * from db.regular_table_5; "
        tdSql.query(sql)
        tdSql.checkCols(4095)
        sql = "alter table db.regular_table_5 add column col4095 binary(310); "
        tdSql.error(sql) 
        sql = "alter table db.regular_table_5 add column col4095 binary(309); "
        tdSql.execute(sql)
        sql = "select * from db.regular_table_5; "
        tdSql.query(sql)
        tdSql.checkCols(4096)  

        #out TSDB_MAX_BYTES_PER_ROW 65531
        tdLog.info('test regular_table max bytes per row out 65531') 
        sql = "create table db.regular_table_6(ts timestamp, "
        for i in range(500):
            sql += "int_%d int, " % (i + 1)
        for i in range(500,1000):
            sql += "smallint_%d smallint, " % (i + 1)
        for i in range(1000,1500):
            sql += "tinyint_%d tinyint, " % (i + 1)
        for i in range(1500,2000):
            sql += "double_%d double, " % (i + 1)
        for i in range(2000,2500):
            sql += "float_%d float, " % (i + 1)
        for i in range(2500,3000):
            sql += "bool_%d bool, " % (i + 1)
        for i in range(3000,3500):
            sql += "bigint_%d bigint, " % (i + 1)
        for i in range(3500,3800):
            sql += "nchar_%d nchar(32), " % (i + 1)
        for i in range(3800,4090):
            sql += "binary_%d binary(40), " % (i + 1)
        for i in range(4090,4094):
            sql += "timestamp_%d timestamp, " % (i + 1)
        sql += "col4095 binary(310))"  
        tdLog.info(len(sql))      
        tdSql.error(sql)

        self.ins_query()
        
    def run_4(self):  
 
        print("==============step4, super table , 1 ts + 4090 cols + 4 tags ==============")
        startTime = time.time() 
        sql = "create stable db.stable_1(ts timestamp, "
        for i in range(4090):
            sql += "col%d int, " % (i + 1)
        sql += "col4091 binary(22))"  
        sql += " tags (loc nchar(10),tag_1 int,tag_2 int,tag_3 int) " 
        tdLog.info(len(sql))      
        tdSql.execute(sql)
        sql = '''create table db.table_0 using db.stable_1 
                    tags('%s' , '1' , '2' , '3' );'''% self.get_random_string(10)
        tdSql.execute(sql)

        for i in range(self.num):
            sql = "insert into db.table_0 values(%d, "
            for j in range(4090):
                str = "'%s', " % random.randint(0,1000)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i))
        time.sleep(1)
        tdSql.query("select count(*) from db.table_0")
        tdSql.checkData(0, 0, self.num)
        tdSql.query("select * from db.table_0")
        tdSql.checkRows(self.num)
        tdSql.checkCols(4092)

        sql = '''create table db.table_1 using db.stable_1 
                    tags('%s' , '1' , '2' , '3' );'''% self.get_random_string(10)
        tdSql.execute(sql)

        self.ins_query()

        for i in range(self.num):
            sql = "insert into db.table_1 values(%d, "
            for j in range(2080):                
                sql += "'%d', " % random.randint(0,1000)
            for j in range(2080,4080):                
                sql += "'%s', " % 'NULL'
            for j in range(4080,4090):                
                sql += "'%s', " % random.randint(0,10000)
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i))
        time.sleep(1)
        tdSql.query("select count(*) from db.table_1")
        tdSql.checkData(0, 0, self.num)
        tdSql.query("select * from db.table_1")
        tdSql.checkRows(self.num)
        tdSql.checkCols(4092)

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))

        #insert in order
        tdLog.info('test insert in order')
        for i in range(self.num):
            sql = "insert into db.table_1 (ts,col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col4091) values(%d, "
            for j in range(10):
                str = "'%s', " % random.randint(0,1000)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i + 1000))
        time.sleep(1)
        tdSql.query("select count(*) from db.table_1")
        tdSql.checkData(0, 0, 2*self.num)
        tdSql.query("select * from db.table_1")
        tdSql.checkRows(2*self.num)
        tdSql.checkCols(4092)

        #insert out of order
        tdLog.info('test insert out of order') 
        for i in range(self.num):
            sql = "insert into db.table_1 (ts,col123,col2213,col331,col41,col523,col236,col71,col813,col912,col1320,col4091) values(%d, "
            for j in range(10):
                str = "'%s', " % random.randint(0,1000)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i + 2000))
        time.sleep(1)
        tdSql.query("select count(*) from db.table_1")
        tdSql.checkData(0, 0, 3*self.num)
        tdSql.query("select * from db.table_1")
        tdSql.checkRows(3*self.num)
        tdSql.checkCols(4092)

        self.ins_query()
        
    def run_5(self):  
 
        print("==============step5,stable table , mix data type==============")
        sql = "create stable db.stable_3(ts timestamp, "
        for i in range(500):
            sql += "int_%d int, " % (i + 1)
        for i in range(500,1000):
            sql += "smallint_%d smallint, " % (i + 1)
        for i in range(1000,1500):
            sql += "tinyint_%d tinyint, " % (i + 1)
        for i in range(1500,2000):
            sql += "double_%d double, " % (i + 1)
        for i in range(2000,2500):
            sql += "float_%d float, " % (i + 1)
        for i in range(2500,3000):
            sql += "bool_%d bool, " % (i + 1)
        for i in range(3000,3500):
            sql += "bigint_%d bigint, " % (i + 1)
        for i in range(3500,3800):
            sql += "nchar_%d nchar(4), " % (i + 1)
        for i in range(3800,4090):
            sql += "binary_%d binary(10), " % (i + 1)
        sql += "col4091 binary(22))"  
        sql += " tags (loc binary(16370),tag_1 int,tag_2 int,tag_3 int) " 
        tdLog.info(len(sql))      
        tdSql.execute(sql)
        sql = '''create table db.table_30 using db.stable_3 
                    tags('%s' , '1' , '2' , '3' );'''%self.get_random_string(16370)
        tdSql.execute(sql)

        self.ins_query()

        for i in range(self.num):
            sql = "insert into db.table_30 values(%d, "
            for j in range(500):
                str = "'%s', " % random.randint(-2147483647,2147483647)                
                sql += str
            for j in range(500,1000):
                str = "'%s', " % random.randint(-32767,32767 )                
                sql += str
            for j in range(1000,1500):
                str = "'%s', " % random.randint(-127,127)                
                sql += str
            for j in range(1500,2000):
                str = "'%s', " % random.randint(-922337203685477580700,922337203685477580700)                
                sql += str
            for j in range(2000,2500):
                str = "'%s', " % random.randint(-92233720368547758070,92233720368547758070)                
                sql += str
            for j in range(2500,3000):
                str = "'%s', " % random.choice(['true','false'])               
                sql += str
            for j in range(3000,3500):
                str = "'%s', " % random.randint(-9223372036854775807,9223372036854775807)                
                sql += str
            for j in range(3500,3800):
                str = "'%s', " % self.get_random_string(4)                
                sql += str
            for j in range(3800,4090):
                str = "'%s', " % self.get_random_string(10)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i))
        time.sleep(1)
        tdSql.query("select count(*) from db.table_30")
        tdSql.checkData(0, 0, self.num)
        tdSql.query("select * from db.table_30")
        tdSql.checkRows(self.num)
        tdSql.checkCols(4092)

        #insert null value 
        tdLog.info('test insert null value') 
        sql = '''create table db.table_31 using db.stable_3 
                    tags('%s' , '1' , '2' , '3' );'''%self.get_random_string(16370)
        tdSql.execute(sql)

        for i in range(self.num):
            sql = "insert into db.table_31 values(%d, "
            for j in range(2500):
                str = "'%s', " % random.choice(['NULL' ,'NULL' ,'NULL' ,1 , 10 ,100 ,-100 ,-10, 88 ,66 ,'NULL' ,'NULL' ,'NULL' ])               
                sql += str
            for j in range(2500,3000):
                str = "'%s', " % random.choice(['true' ,'false'])               
                sql += str
            for j in range(3000,3500):
                str = "'%s', " % random.randint(-9223372036854775807,9223372036854775807)                
                sql += str
            for j in range(3500,3800):
                str = "'%s', " % self.get_random_string(4)                
                sql += str
            for j in range(3800,4090):
                str = "'%s', " % self.get_random_string(10)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i))
        time.sleep(1)
        tdSql.query("select count(*) from db.table_31")
        tdSql.checkData(0, 0, self.num)
        tdSql.query("select * from db.table_31")
        tdSql.checkRows(self.num)
        tdSql.checkCols(4092)

        self.ins_query()

        #insert in order
        tdLog.info('test insert in order') 
        for i in range(self.num):
            sql = "insert into db.table_31 (ts,int_2,int_22,int_169,smallint_537,smallint_607,tinyint_1030,tinyint_1491,double_1629,double_1808,float_2075,col4091) values(%d, "
            for j in range(10):
                str = "'%s', " % random.randint(0,100)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i + 1000))
        time.sleep(1)
        tdSql.query("select count(*) from db.table_31")
        tdSql.checkData(0, 0, 2*self.num)
        tdSql.query("select * from db.table_31")
        tdSql.checkRows(2*self.num)
        tdSql.checkCols(4092)

        #insert out of order
        tdLog.info('test insert out of order') 
        for i in range(self.num):
            sql = "insert into db.table_31 (ts,int_169,float_2075,int_369,tinyint_1491,tinyint_1030,float_2360,smallint_537,double_1808,double_1608,double_1629,col4091) values(%d, "
            for j in range(10):
                str = "'%s', " % random.randint(0,100)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i + 2000))
        time.sleep(1)
        tdSql.query("select count(*) from db.table_31")
        tdSql.checkData(0, 0, 3*self.num)
        tdSql.query("select * from db.table_31")
        tdSql.checkRows(3*self.num)
        tdSql.checkCols(4092)

        #define TSDB_MAX_BYTES_PER_ROW 65531   TSDB_MAX_TAGS_LEN 16384 
        #ts:8\int:4\smallint:2\bigint:8\bool:1\float:4\tinyint:1\nchar:4*（）+2[offset]\binary:1*（）+2[offset]
        tdLog.info('test super table max bytes per row 65531') 
        sql = "create table db.stable_4(ts timestamp, "  #1*8 sum=8
        for i in range(500):
            sql += "int_%d int, " % (i + 1)   #500*4=2000 sum=2008
        for i in range(500,1000):
            sql += "smallint_%d smallint, " % (i + 1) #500*2=1000 sum=3008
        for i in range(1000,1500):
            sql += "tinyint_%d tinyint, " % (i + 1)  #500*1=500 sum=3508
        for i in range(1500,2000):
            sql += "double_%d double, " % (i + 1) #500*8=4000 sum=7508
        for i in range(2000,2500):
            sql += "float_%d float, " % (i + 1) #500*4=2000 sum=9508
        for i in range(2500,3000):
            sql += "bool_%d bool, " % (i + 1) #500*1=500 sum=10008
        for i in range(3000,3500):
            sql += "bigint_%d bigint, " % (i + 1) #500*8=4000 sum=14008
        for i in range(3500,3800):
            sql += "nchar_%d nchar(32), " % (i + 1) #300*(32*4+2)=39000 sum=53008
        for i in range(3800,4090):
            sql += "binary_%d binary(40), " % (i + 1) #290*(40+2)=12180 sum=65188
        sql += "col4091 binary(341))"    #341+2=343 sum=65531
        sql += " tags (loc binary(16370),tag_1 int,tag_2 int,tag_3 int) " 
        tdSql.execute(sql)
        sql = '''create table db.table_40 using db.stable_4 
                    tags('%s' , '1' , '2' , '3' );'''%self.get_random_string(16370)
        tdSql.execute(sql)
        tdSql.query("select * from db.table_40")
        tdSql.checkCols(4092)
        tdSql.query("describe db.table_40")
        tdSql.checkRows(4096)

        tdLog.info('test super table drop and add column or tag') 
        sql = "alter stable db.stable_4 drop column col4091; "
        tdSql.execute(sql)
        sql = "select * from db.stable_4; "
        tdSql.query(sql)
        tdSql.checkCols(4095)
        sql = "alter table db.stable_4 add column col4091 binary(342); "
        tdSql.error(sql) 
        sql = "alter table db.stable_4 add column col4091 binary(341); "
        tdSql.execute(sql)
        sql = "select * from db.stable_4; "
        tdSql.query(sql)
        tdSql.checkCols(4096) 

        self.ins_query()

        sql = "alter stable db.stable_4 drop tag tag_1; "
        tdSql.execute(sql)
        sql = "select * from db.stable_4; "
        tdSql.query(sql)
        tdSql.checkCols(4095)
        sql = "alter table db.stable_4 add tag tag_1 int; "
        tdSql.execute(sql)
        sql = "select * from db.stable_4; "
        tdSql.query(sql)
        tdSql.checkCols(4096) 
        sql = "alter table db.stable_4 add tag loc1 nchar(10); "
        tdSql.error(sql) 

        tdLog.info('test super table max bytes per row 65531') 
        sql = "create table db.stable_5(ts timestamp, "
        for i in range(500):
            sql += "int_%d int, " % (i + 1)
        for i in range(500,1000):
            sql += "smallint_%d smallint, " % (i + 1)
        for i in range(1000,1500):
            sql += "tinyint_%d tinyint, " % (i + 1)
        for i in range(1500,2000):
            sql += "double_%d double, " % (i + 1)
        for i in range(2000,2500):
            sql += "float_%d float, " % (i + 1)
        for i in range(2500,3000):
            sql += "bool_%d bool, " % (i + 1)
        for i in range(3000,3500):
            sql += "bigint_%d bigint, " % (i + 1)
        for i in range(3500,3800):
            sql += "nchar_%d nchar(32), " % (i + 1)
        for i in range(3800,4090):
            sql += "binary_%d binary(40), " % (i + 1)
        sql += "col4091 binary(342))"  
        sql += " tags (loc nchar(10),tag_1 int,tag_2 int,tag_3 int) " 
        tdSql.error(sql)

        self.ins_query()
        
    def run_6(self):  
 
        
        print("==============step6,stable table , mix data type==============")
        sql = "create stable db.stable_6(ts timestamp, "
        for i in range(500):
            sql += "int_%d int, " % (i + 1)
        for i in range(500,1000):
            sql += "smallint_%d smallint, " % (i + 1)
        for i in range(1000,1500):
            sql += "tinyint_%d tinyint, " % (i + 1)
        for i in range(1500,2000):
            sql += "double_%d double, " % (i + 1)
        for i in range(2000,2500):
            sql += "float_%d float, " % (i + 1)
        for i in range(2500,3000):
            sql += "bool_%d bool, " % (i + 1)
        for i in range(3000,3500):
            sql += "bigint_%d bigint, " % (i + 1)
        for i in range(3500,3800):
            sql += "nchar_%d nchar(4), " % (i + 1)
        for i in range(3800,4090):
            sql += "binary_%d binary(10), " % (i + 1)
        sql += "col4091 binary(22))"  
        sql += " tags (loc binary(16370),tag_1 int,tag_2 int,tag_3 int) " 
        tdLog.info(len(sql))      
        tdSql.execute(sql)
        sql = '''create table db.table_60 using db.stable_6 
                    tags('%s' , '1' , '2' , '3' );'''%self.get_random_string(16370)
        tdSql.execute(sql)

        self.ins_query()

        for i in range(self.num):
            sql = "insert into db.table_60 values(%d, "
            for j in range(500):
                str = "'%s', " % random.randint(-2147483647,2147483647)                
                sql += str
            for j in range(500,1000):
                str = "'%s', " % random.randint(-32767,32767 )                
                sql += str
            for j in range(1000,1500):
                str = "'%s', " % random.randint(-127,127)                
                sql += str
            for j in range(1500,2000):
                str = "'%s', " % random.randint(-922337203685477580700,922337203685477580700)                
                sql += str
            for j in range(2000,2500):
                str = "'%s', " % random.randint(-92233720368547758070,92233720368547758070)                
                sql += str
            for j in range(2500,3000):
                str = "'%s', " % random.choice(['true','false'])               
                sql += str
            for j in range(3000,3500):
                str = "'%s', " % random.randint(-9223372036854775807,9223372036854775807)                
                sql += str
            for j in range(3500,3800):
                str = "'%s', " % self.get_random_string(4)                
                sql += str
            for j in range(3800,4090):
                str = "'%s', " % self.get_random_string(10)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i))
        time.sleep(1)
        tdSql.query("select count(*) from db.table_60")
        tdSql.checkData(0, 0, self.num)
        tdSql.query("select * from db.table_60")
        tdSql.checkRows(self.num)
        tdSql.checkCols(4092)

        #insert null value 
        tdLog.info('test insert null value') 
        sql = '''create table db.table_61 using db.stable_6 
                    tags('%s' , '1' , '2' , '3' );'''%self.get_random_string(16370)
        tdSql.execute(sql)

        self.ins_query()

        for i in range(self.num):
            sql = "insert into db.table_61 values(%d, "
            for j in range(2500):
                str = "'%s', " % random.choice(['NULL' ,'NULL' ,'NULL' ,1 , 10 ,100 ,-100 ,-10, 88 ,66 ,'NULL' ,'NULL' ,'NULL' ])               
                sql += str
            for j in range(2500,3000):
                str = "'%s', " % random.choice(['true' ,'false'])               
                sql += str
            for j in range(3000,3500):
                str = "'%s', " % random.randint(-9223372036854775807,9223372036854775807)                
                sql += str
            for j in range(3500,3800):
                str = "'%s', " % self.get_random_string(4)                
                sql += str
            for j in range(3800,4090):
                str = "'%s', " % self.get_random_string(10)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i))
        time.sleep(1)
        tdSql.query("select count(*) from db.table_61")
        tdSql.checkData(0, 0, self.num)
        tdSql.query("select * from db.table_61")
        tdSql.checkRows(self.num)
        tdSql.checkCols(4092)

        #insert in order
        tdLog.info('test insert in order') 
        for i in range(self.num):
            sql = "insert into db.table_61 (ts,int_2,int_22,int_169,smallint_537,smallint_607,tinyint_1030,tinyint_1491,double_1629,double_1808,float_2075,col4091) values(%d, "
            for j in range(10):
                str = "'%s', " % random.randint(0,100)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i + 1000))
        time.sleep(1)
        tdSql.query("select count(*) from db.table_61")
        tdSql.checkData(0, 0, 2*self.num)
        tdSql.query("select * from db.table_61")
        tdSql.checkRows(2*self.num)
        tdSql.checkCols(4092)

        #insert out of order
        tdLog.info('test insert out of order') 
        for i in range(self.num):
            sql = "insert into db.table_61 (ts,int_169,float_2075,int_369,tinyint_1491,tinyint_1030,float_2360,smallint_537,double_1808,double_1608,double_1629,col4091) values(%d, "
            for j in range(10):
                str = "'%s', " % random.randint(0,100)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i + 2000))
        time.sleep(1)
        tdSql.query("select count(*) from db.table_61")
        tdSql.checkData(0, 0, 3*self.num)
        tdSql.query("select * from db.table_61")
        tdSql.checkRows(3*self.num)
        tdSql.checkCols(4092)

        self.ins_query()

        #define TSDB_MAX_BYTES_PER_ROW 65531   TSDB_MAX_TAGS_LEN 16384 
        #ts:8\int:4\smallint:2\bigint:8\bool:1\float:4\tinyint:1\nchar:4*（）+2[offset]\binary:1*（）+2[offset]
        tdLog.info('test super table max bytes per row 65531') 
        sql = "create table db.stable_7(ts timestamp, "  #1*8 sum=8
        for i in range(500):
            sql += "int_%d int, " % (i + 1)   #500*4=2000 sum=2008
        for i in range(500,1000):
            sql += "smallint_%d smallint, " % (i + 1) #500*2=1000 sum=3008
        for i in range(1000,1500):
            sql += "tinyint_%d tinyint, " % (i + 1)  #500*1=500 sum=3508
        for i in range(1500,2000):
            sql += "double_%d double, " % (i + 1) #500*8=4000 sum=7508
        for i in range(2000,2500):
            sql += "float_%d float, " % (i + 1) #500*4=2000 sum=9508
        for i in range(2500,3000):
            sql += "bool_%d bool, " % (i + 1) #500*1=500 sum=10008
        for i in range(3000,3500):
            sql += "bigint_%d bigint, " % (i + 1) #500*8=4000 sum=14008
        for i in range(3500,3800):
            sql += "nchar_%d nchar(32), " % (i + 1) #300*(32*4+2)=39000 sum=53008
        for i in range(3800,4090):
            sql += "binary_%d binary(40), " % (i + 1) #290*(40+2)=12180 sum=65188
        sql += "col4091 binary(341))"    #341+2=343 sum=65531
        sql += " tags (loc binary(16370),tag_1 int,tag_2 int,tag_3 int) " #4*3+16370+2
        tdSql.execute(sql)
        sql = '''create table db.table_70 using db.stable_7 
                    tags('%s' , '1' , '2' , '3' );'''%self.get_random_string(16370)
        tdSql.execute(sql)
        tdSql.query("select * from db.table_70")
        tdSql.checkCols(4092)
        tdSql.query("describe db.table_70")
        tdSql.checkRows(4096)

        tdLog.info('test super table drop and add column or tag') 
        sql = "alter stable db.stable_7 drop column col4091; "
        tdSql.execute(sql)
        sql = "select * from db.stable_7; "
        tdSql.query(sql)
        tdSql.checkCols(4095)
        sql = "alter table db.stable_7 add column col4091 binary(342); "
        tdSql.error(sql) 
        sql = "alter table db.stable_7 add column col4091 binary(341); "
        tdSql.execute(sql)
        sql = "select * from db.stable_7; "
        tdSql.query(sql)
        tdSql.checkCols(4096) 
        
        sql = "alter stable db.stable_7 drop tag loc; "
        tdSql.execute(sql)
        sql = "select * from db.stable_7; "
        tdSql.query(sql)
        tdSql.checkCols(4095)
        sql = "alter table db.stable_7 add tag loc binary(16371); "
        tdSql.error(sql) 
        sql = "alter table db.stable_7 add tag loc binary(16370); "
        tdSql.execute(sql)
        sql = "select * from db.stable_7; "
        tdSql.query(sql)
        tdSql.checkCols(4096) 

        sql = "alter stable db.stable_7 drop tag tag_1; "
        tdSql.execute(sql)
        sql = "select * from db.stable_7; "
        tdSql.query(sql)
        tdSql.checkCols(4095)
        sql = "alter table db.stable_7 add tag tag_1 int; "
        tdSql.execute(sql)
        sql = "select * from db.stable_7; "
        tdSql.query(sql)
        tdSql.checkCols(4096) 
        sql = "alter table db.stable_7 add tag loc1 nchar(10); "
        tdSql.error(sql) 

        self.ins_query()

        tdLog.info('test super table max bytes per row 65531') 
        sql = "create table db.stable_8(ts timestamp, "
        for i in range(500):
            sql += "int_%d int, " % (i + 1)
        for i in range(500,1000):
            sql += "smallint_%d smallint, " % (i + 1)
        for i in range(1000,1500):
            sql += "tinyint_%d tinyint, " % (i + 1)
        for i in range(1500,2000):
            sql += "double_%d double, " % (i + 1)
        for i in range(2000,2500):
            sql += "float_%d float, " % (i + 1)
        for i in range(2500,3000):
            sql += "bool_%d bool, " % (i + 1)
        for i in range(3000,3500):
            sql += "bigint_%d bigint, " % (i + 1)
        for i in range(3500,3800):
            sql += "nchar_%d nchar(32), " % (i + 1)
        for i in range(3800,4090):
            sql += "binary_%d binary(40), " % (i + 1)
        sql += "col4091 binary(342))"  
        sql += " tags (loc nchar(10),tag_1 int,tag_2 int,tag_3 int) " 
        tdSql.error(sql)

        tdLog.info('test super table max bytes per row tag 16384') 
        sql = "create table db.stable_8(ts timestamp, "
        for i in range(500):
            sql += "int_%d int, " % (i + 1)
        for i in range(500,1000):
            sql += "smallint_%d smallint, " % (i + 1)
        for i in range(1000,1500):
            sql += "tinyint_%d tinyint, " % (i + 1)
        for i in range(1500,2000):
            sql += "double_%d double, " % (i + 1)
        for i in range(2000,2500):
            sql += "float_%d float, " % (i + 1)
        for i in range(2500,3000):
            sql += "bool_%d bool, " % (i + 1)
        for i in range(3000,3500):
            sql += "bigint_%d bigint, " % (i + 1)
        for i in range(3500,3800):
            sql += "nchar_%d nchar(32), " % (i + 1)
        for i in range(3800,4090):
            sql += "binary_%d binary(40), " % (i + 1)
        sql += "col4091 binary(341))"  
        sql += " tags (loc binary(16371),tag_1 int,tag_2 int,tag_3 int) " 
        tdSql.error(sql)
        
    def run_7(self):  
 

        print("==============step7, super table error col ==============")
        tdLog.info('test exceeds row num') 
        # column + tag > 4096 
        sql = "create stable db.stable_2(ts timestamp, "
        for i in range(4091):
            sql += "col%d int, " % (i + 1)
        sql += "col4092 binary(22))"  
        sql += " tags (loc nchar(10),tag_1 int,tag_2 int,tag_3 int) " 
        tdLog.info(len(sql))      
        tdSql.error(sql)

        self.ins_query()
        
        # column + tag > 4096
        sql = "create stable db.stable_2(ts timestamp, "
        for i in range(4090):
            sql += "col%d int, " % (i + 1)
        sql += "col4091 binary(22))"  
        sql += " tags (loc nchar(10),tag_1 int,tag_2 int,tag_3 int,tag_4 int) " 
        tdLog.info(len(sql))  
        tdSql.error(sql)

        # alter column + tag > 4096
        sql = "alter table db.stable_1 add column max int; "
        tdSql.error(sql)
        
        sql = "alter table db.stable_1 add tag max int; "
        tdSql.error(sql)
        
        sql = "alter table db.stable_4 modify column col4091 binary(102); "
        tdSql.error(sql)
        sql = "alter table db.stable_4 modify tag loc nchar(20); "
        tdSql.query("select * from db.table_70")
        tdSql.checkCols(4092)
        tdSql.query("describe db.table_70")
        tdSql.checkRows(4096)

        self.ins_query()

    def run(self):
        tdSql.prepare()
        
        startTime_all = time.time() 
        self.run_8() 
        self.run_9() 
        self.run_1() 
        self.run_2() 
        # self.run_3() 
        # self.run_4() 
        # self.run_5() 
        # self.run_6() 
        # self.run_7() 
        
        endTime_all = time.time()
        print("total time %ds" % (endTime_all - startTime_all))        


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
        

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())