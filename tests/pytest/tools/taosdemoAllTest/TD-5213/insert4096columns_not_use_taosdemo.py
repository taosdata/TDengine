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
import time
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
from util.dnodes import tdDnodes

class TDTestCase:
    updatecfgDict={'maxSQLLength':1048576}
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        self.ts = 1538548685000
        self.num = 100

    def get_random_string(self, length):
        letters = string.ascii_lowercase
        result_str = ''.join(random.choice(letters) for i in range(length))
        return result_str

    def run(self):
        tdSql.prepare()
        # test case for https://jira.taosdata.com:18080/browse/TD-5213

        print("==============step1, regular table, 1 ts + 4094 cols + 1 binary==============")
        startTime = time.time() 
        sql = "create table regular_table_1(ts timestamp, "
        for i in range(4094):
            sql += "col%d int, " % (i + 1)
        sql += "col4095 binary(22))"  
        tdLog.info(len(sql))      
        tdSql.execute(sql)

        for i in range(self.num):
            sql = "insert into regular_table_1 values(%d, "
            for j in range(4094):
                str = "'%s', " % random.randint(0,1000)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i))
        time.sleep(1)
        tdSql.query("select count(*) from regular_table_1")
        tdSql.checkData(0, 0, self.num)
        tdSql.query("select * from regular_table_1")
        tdSql.checkRows(self.num)
        tdSql.checkCols(4096)

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))

        #insert in order
        tdLog.info('test insert in order') 
        for i in range(self.num):
            sql = "insert into regular_table_1 (ts,col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col4095) values(%d, "
            for j in range(10):
                str = "'%s', " % random.randint(0,1000)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i + 1000))
        time.sleep(1)
        tdSql.query("select count(*) from regular_table_1")
        tdSql.checkData(0, 0, 2*self.num)
        tdSql.query("select * from regular_table_1")
        tdSql.checkRows(2*self.num)
        tdSql.checkCols(4096)

        #insert out of order
        tdLog.info('test insert out of order') 
        for i in range(self.num):
            sql = "insert into regular_table_1 (ts,col123,col2213,col331,col41,col523,col236,col71,col813,col912,col1320,col4095) values(%d, "
            for j in range(10):
                str = "'%s', " % random.randint(0,1000)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i + 2000))
        time.sleep(1)
        tdSql.query("select count(*) from regular_table_1")
        tdSql.checkData(0, 0, 3*self.num)
        tdSql.query("select * from regular_table_1")
        tdSql.checkRows(3*self.num)
        tdSql.checkCols(4096)


        print("==============step2,regular table error col or value==============")
        tdLog.info('test regular table exceeds row num') 
        # column > 4096
        sql = "create table regular_table_2(ts timestamp, "
        for i in range(4095):
            sql += "col%d int, " % (i + 1)
        sql += "col4096 binary(22))"  
        tdLog.info(len(sql))      
        tdSql.error(sql)

        # column > 4096
        sql = "insert into regular_table_1 values(%d, "
        for j in range(4095):
            str = "'%s', " % random.randint(0,1000)                
            sql += str
        sql += "'%s')" % self.get_random_string(22)
        tdSql.error(sql)

        # insert column < 4096
        sql = "insert into regular_table_1 values(%d, "
        for j in range(4092):
            str = "'%s', " % random.randint(0,1000)                
            sql += str
        sql += "'%s')" % self.get_random_string(22)
        tdSql.error(sql)

        # alter column > 4096
        sql = "alter table regular_table_1 add column max int; "
        tdSql.error(sql)

        print("==============step3,regular table , mix data type==============")
        startTime = time.time() 
        sql = "create table regular_table_3(ts timestamp, "
        for i in range(2000):
            sql += "col%d int, " % (i + 1)
        for i in range(2000,4094):
            sql += "col%d bigint, " % (i + 1)
        sql += "col4095 binary(22))"  
        tdLog.info(len(sql))      
        tdSql.execute(sql)

        for i in range(self.num):
            sql = "insert into regular_table_3 values(%d, "
            for j in range(4094):
                str = "'%s', " % random.randint(0,1000)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i))
        time.sleep(1)
        tdSql.query("select count(*) from regular_table_3")
        tdSql.checkData(0, 0, self.num)
        tdSql.query("select * from regular_table_3")
        tdSql.checkRows(self.num)
        tdSql.checkCols(4096)

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))

        sql = "create table regular_table_4(ts timestamp, "
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

        for i in range(self.num):
            sql = "insert into regular_table_4 values(%d, "
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
        tdSql.query("select count(*) from regular_table_4")
        tdSql.checkData(0, 0, self.num)
        tdSql.query("select * from regular_table_4")
        tdSql.checkRows(self.num)
        tdSql.checkCols(4096)
        tdLog.info("end ,now new one")

        #insert null value 
        tdLog.info('test insert null value') 
        for i in range(self.num):
            sql = "insert into regular_table_4 values(%d, "
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
        tdSql.query("select count(*) from regular_table_4")
        tdSql.checkData(0, 0, 2*self.num)
        tdSql.query("select * from regular_table_4")
        tdSql.checkRows(2*self.num)
        tdSql.checkCols(4096)

        #insert in order
        tdLog.info('test insert in order') 
        for i in range(self.num):
            sql = "insert into regular_table_4 (ts,int_2,int_22,int_169,smallint_537,smallint_607,tinyint_1030,tinyint_1491,double_1629,double_1808,float_2075,col4095) values(%d, "
            for j in range(10):
                str = "'%s', " % random.randint(0,100)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i + 1000))
        time.sleep(1)
        tdSql.query("select count(*) from regular_table_4")
        tdSql.checkData(0, 0, 3*self.num)
        tdSql.query("select * from regular_table_4")
        tdSql.checkRows(3*self.num)
        tdSql.checkCols(4096)

        #insert out of order
        tdLog.info('test insert out of order') 
        for i in range(self.num):
            sql = "insert into regular_table_4 (ts,int_169,float_2075,int_369,tinyint_1491,tinyint_1030,float_2360,smallint_537,double_1808,double_1608,double_1629,col4095) values(%d, "
            for j in range(10):
                str = "'%s', " % random.randint(0,100)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i + 2000))
        time.sleep(1)
        tdSql.query("select count(*) from regular_table_4")
        tdSql.checkData(0, 0, 4*self.num)
        tdSql.query("select * from regular_table_4")
        tdSql.checkRows(4*self.num)
        tdSql.checkCols(4096)

        #define TSDB_MAX_BYTES_PER_ROW 49151[old:1024 && 16384]
        #ts:8\int:4\smallint:2\bigint:8\bool:1\float:4\tinyint:1\nchar:4*（）+2[offset]\binary:1*（）+2[offset]
        tdLog.info('test regular_table max bytes per row 49151') 
        sql = "create table regular_table_5(ts timestamp, "
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
            sql += "nchar_%d nchar(20), " % (i + 1)
        for i in range(3800,4090):
            sql += "binary_%d binary(34), " % (i + 1)
        for i in range(4090,4094):
            sql += "timestamp_%d timestamp, " % (i + 1)
        sql += "col4095 binary(69))"       
        tdSql.execute(sql)
        tdSql.query("select * from regular_table_5")
        tdSql.checkCols(4096)
        # TD-5324
        sql = "alter table regular_table_5 modify column col4095 binary(70); "
        tdSql.error(sql)     
        
        # drop and add
        sql = "alter table regular_table_5 drop column col4095; "
        tdSql.execute(sql)
        sql = "select * from regular_table_5; "
        tdSql.query(sql)
        tdSql.checkCols(4095)
        sql = "alter table regular_table_5 add column col4095 binary(70); "
        tdSql.error(sql) 
        sql = "alter table regular_table_5 add column col4095 binary(69); "
        tdSql.execute(sql)
        sql = "select * from regular_table_5; "
        tdSql.query(sql)
        tdSql.checkCols(4096)  

        #out TSDB_MAX_BYTES_PER_ROW 49151
        tdLog.info('test regular_table max bytes per row out 49151') 
        sql = "create table regular_table_6(ts timestamp, "
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
            sql += "nchar_%d nchar(20), " % (i + 1)
        for i in range(3800,4090):
            sql += "binary_%d binary(34), " % (i + 1)
        for i in range(4090,4094):
            sql += "timestamp_%d timestamp, " % (i + 1)
        sql += "col4095 binary(70))"  
        tdLog.info(len(sql))      
        tdSql.error(sql)


        print("==============step4, super table , 1 ts + 4090 cols + 4 tags ==============")
        startTime = time.time() 
        sql = "create stable stable_1(ts timestamp, "
        for i in range(4090):
            sql += "col%d int, " % (i + 1)
        sql += "col4091 binary(22))"  
        sql += " tags (loc nchar(10),tag_1 int,tag_2 int,tag_3 int) " 
        tdLog.info(len(sql))      
        tdSql.execute(sql)
        sql = '''create table table_0 using stable_1 
                    tags('table_0' , '1' , '2' , '3' );'''
        tdSql.execute(sql)

        for i in range(self.num):
            sql = "insert into table_0 values(%d, "
            for j in range(4090):
                str = "'%s', " % random.randint(0,1000)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i))
        time.sleep(1)
        tdSql.query("select count(*) from table_0")
        tdSql.checkData(0, 0, self.num)
        tdSql.query("select * from table_0")
        tdSql.checkRows(self.num)
        tdSql.checkCols(4092)

        sql = '''create table table_1 using stable_1 
                    tags('table_1' , '1' , '2' , '3' );'''
        tdSql.execute(sql)

        for i in range(self.num):
            sql = "insert into table_1 values(%d, "
            for j in range(2080):                
                sql += "'%d', " % random.randint(0,1000)
            for j in range(2080,4080):                
                sql += "'%s', " % 'NULL'
            for j in range(4080,4090):                
                sql += "'%s', " % random.randint(0,10000)
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i))
        time.sleep(1)
        tdSql.query("select count(*) from table_1")
        tdSql.checkData(0, 0, self.num)
        tdSql.query("select * from table_1")
        tdSql.checkRows(self.num)
        tdSql.checkCols(4092)

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))

        #insert in order
        tdLog.info('test insert in order')
        for i in range(self.num):
            sql = "insert into table_1 (ts,col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col4091) values(%d, "
            for j in range(10):
                str = "'%s', " % random.randint(0,1000)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i + 1000))
        time.sleep(1)
        tdSql.query("select count(*) from table_1")
        tdSql.checkData(0, 0, 2*self.num)
        tdSql.query("select * from table_1")
        tdSql.checkRows(2*self.num)
        tdSql.checkCols(4092)

        #insert out of order
        tdLog.info('test insert out of order') 
        for i in range(self.num):
            sql = "insert into table_1 (ts,col123,col2213,col331,col41,col523,col236,col71,col813,col912,col1320,col4091) values(%d, "
            for j in range(10):
                str = "'%s', " % random.randint(0,1000)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i + 2000))
        time.sleep(1)
        tdSql.query("select count(*) from table_1")
        tdSql.checkData(0, 0, 3*self.num)
        tdSql.query("select * from table_1")
        tdSql.checkRows(3*self.num)
        tdSql.checkCols(4092)

        print("==============step5,stable table , mix data type==============")
        sql = "create stable stable_3(ts timestamp, "
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
        sql += " tags (loc nchar(10),tag_1 int,tag_2 int,tag_3 int) " 
        tdLog.info(len(sql))      
        tdSql.execute(sql)
        sql = '''create table table_30 using stable_3 
                    tags('table_30' , '1' , '2' , '3' );'''
        tdSql.execute(sql)

        for i in range(self.num):
            sql = "insert into table_30 values(%d, "
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
        tdSql.query("select count(*) from table_30")
        tdSql.checkData(0, 0, self.num)
        tdSql.query("select * from table_30")
        tdSql.checkRows(self.num)
        tdSql.checkCols(4092)

        #insert null value 
        tdLog.info('test insert null value') 
        sql = '''create table table_31 using stable_3 
                    tags('table_31' , '1' , '2' , '3' );'''
        tdSql.execute(sql)

        for i in range(self.num):
            sql = "insert into table_31 values(%d, "
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
        tdSql.query("select count(*) from table_31")
        tdSql.checkData(0, 0, self.num)
        tdSql.query("select * from table_31")
        tdSql.checkRows(self.num)
        tdSql.checkCols(4092)

        #insert in order
        tdLog.info('test insert in order') 
        for i in range(self.num):
            sql = "insert into table_31 (ts,int_2,int_22,int_169,smallint_537,smallint_607,tinyint_1030,tinyint_1491,double_1629,double_1808,float_2075,col4091) values(%d, "
            for j in range(10):
                str = "'%s', " % random.randint(0,100)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i + 1000))
        time.sleep(1)
        tdSql.query("select count(*) from table_31")
        tdSql.checkData(0, 0, 2*self.num)
        tdSql.query("select * from table_31")
        tdSql.checkRows(2*self.num)
        tdSql.checkCols(4092)

        #insert out of order
        tdLog.info('test insert out of order') 
        for i in range(self.num):
            sql = "insert into table_31 (ts,int_169,float_2075,int_369,tinyint_1491,tinyint_1030,float_2360,smallint_537,double_1808,double_1608,double_1629,col4091) values(%d, "
            for j in range(10):
                str = "'%s', " % random.randint(0,100)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i + 2000))
        time.sleep(1)
        tdSql.query("select count(*) from table_31")
        tdSql.checkData(0, 0, 3*self.num)
        tdSql.query("select * from table_31")
        tdSql.checkRows(3*self.num)
        tdSql.checkCols(4092)

        #define TSDB_MAX_BYTES_PER_ROW 49151   TSDB_MAX_TAGS_LEN 16384 
        #ts:8\int:4\smallint:2\bigint:8\bool:1\float:4\tinyint:1\nchar:4*（）+2[offset]\binary:1*（）+2[offset]
        tdLog.info('test super table max bytes per row 49151') 
        sql = "create table stable_4(ts timestamp, "
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
            sql += "nchar_%d nchar(20), " % (i + 1)
        for i in range(3800,4090):
            sql += "binary_%d binary(34), " % (i + 1)
        sql += "col4091 binary(101))"  
        sql += " tags (loc nchar(10),tag_1 int,tag_2 int,tag_3 int) " 
        tdSql.execute(sql)
        sql = '''create table table_40 using stable_4 
                    tags('table_40' , '1' , '2' , '3' );'''
        tdSql.execute(sql)
        tdSql.query("select * from table_40")
        tdSql.checkCols(4092)
        tdSql.query("describe table_40")
        tdSql.checkRows(4096)

        tdLog.info('test super table drop and add column or tag') 
        sql = "alter stable stable_4 drop column col4091; "
        tdSql.execute(sql)
        sql = "select * from stable_4; "
        tdSql.query(sql)
        tdSql.checkCols(4095)
        sql = "alter table stable_4 add column col4091 binary(102); "
        tdSql.error(sql) 
        sql = "alter table stable_4 add column col4091 binary(101); "
        tdSql.execute(sql)
        sql = "select * from stable_4; "
        tdSql.query(sql)
        tdSql.checkCols(4096) 

        sql = "alter stable stable_4 drop tag tag_1; "
        tdSql.execute(sql)
        sql = "select * from stable_4; "
        tdSql.query(sql)
        tdSql.checkCols(4095)
        sql = "alter table stable_4 add tag tag_1 int; "
        tdSql.execute(sql)
        sql = "select * from stable_4; "
        tdSql.query(sql)
        tdSql.checkCols(4096) 
        sql = "alter table stable_4 add tag loc1 nchar(10); "
        tdSql.error(sql) 

        tdLog.info('test super table max bytes per row 49151') 
        sql = "create table stable_5(ts timestamp, "
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
            sql += "nchar_%d nchar(20), " % (i + 1)
        for i in range(3800,4090):
            sql += "binary_%d binary(34), " % (i + 1)
        sql += "col4091 binary(102))"  
        sql += " tags (loc nchar(10),tag_1 int,tag_2 int,tag_3 int) " 
        tdSql.error(sql)

        print("==============step6, super table error col ==============")
        tdLog.info('test exceeds row num') 
        # column + tag > 4096 
        sql = "create stable stable_2(ts timestamp, "
        for i in range(4091):
            sql += "col%d int, " % (i + 1)
        sql += "col4092 binary(22))"  
        sql += " tags (loc nchar(10),tag_1 int,tag_2 int,tag_3 int) " 
        tdLog.info(len(sql))      
        tdSql.error(sql)
        
        # column + tag > 4096
        sql = "create stable stable_2(ts timestamp, "
        for i in range(4090):
            sql += "col%d int, " % (i + 1)
        sql += "col4091 binary(22))"  
        sql += " tags (loc nchar(10),tag_1 int,tag_2 int,tag_3 int,tag_4 int) " 
        tdLog.info(len(sql))  
        tdSql.error(sql)

        # alter column + tag > 4096
        sql = "alter table stable_1 add column max int; "
        tdSql.error(sql)
        # TD-5322
        sql = "alter table stable_1 add tag max int; "
        tdSql.error(sql)
        # TD-5324
        sql = "alter table stable_4 modify column col4091 binary(102); "
        tdSql.error(sql)
        sql = "alter table stable_4 modify tag loc nchar(20); "
        tdSql.query("select * from table_40")
        tdSql.checkCols(4092)
        tdSql.query("describe table_40")
        tdSql.checkRows(4096)

        os.system("rm -rf tools/taosdemoAllTest/TD-5213/insert4096columns_not_use_taosdemo.py.sql") 


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
