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
from util.log import *
from util.cases import *
from util.sql import *
import numpy as np


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.rowNum = 100
        self.ts = 1537146000000
        self.ts1 = 1537146000000000
        self.ts2 = 1597146000000

        
    def run(self):
        # db precison ms
        tdSql.prepare()
        tdSql.execute('''create table test(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double, 
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned) tags(loc nchar(20), tag1 int)''')
        tdSql.execute("create table test1 using test tags('beijing', 10)")
        tdSql.execute("create table test2 using test tags('tianjing', 20)")
        tdSql.execute("create table test3 using test tags('shanghai', 20)")    
        tdSql.execute("create table gtest1 (ts timestamp, col1 float)")
        tdSql.execute("create table gtest2 (ts timestamp, col1 tinyint)")
        tdSql.execute("create table gtest3 (ts timestamp, col1 tinyint)")
        tdSql.execute("create table gtest4 (ts timestamp, col1 tinyint)")
        tdSql.execute("create table gtest5 (ts timestamp, col1 tinyint)")
        tdSql.execute("create table gtest6 (ts timestamp, col1 tinyint)")
        tdSql.execute("create table gtest7 (ts timestamp, col1 tinyint)")
        tdSql.execute("create table gtest8 (ts timestamp, col1 tinyint)")


        for i in range(self.rowNum):
            tdSql.execute("insert into test1 values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)" 
                        % (self.ts + i*1000, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))
            tdSql.execute("insert into test2 values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)" 
                        % (self.ts2 + i*1000, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))
            tdSql.execute("insert into test3 values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)" 
                        % (self.ts2 + i*1000, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))
                    
        tdSql.execute("insert into gtest1 values(1537146000000,0);")
        tdSql.execute("insert into gtest1 values(1537146001100,1.2);")
        tdSql.execute("insert into gtest2 values(1537146001001,1);")
        tdSql.execute("insert into gtest2 values(1537146001101,2);")
        tdSql.execute("insert into gtest3 values(1537146001101,2);")
        tdSql.execute("insert into gtest4(ts) values(1537146001101);")
        tdSql.execute("insert into gtest5 values(1537146001002,4);")
        tdSql.execute("insert into gtest5 values(1537146002202,4);")
        tdSql.execute("insert into gtest6 values(1537146000000,5);")
        tdSql.execute("insert into gtest6 values(1537146001000,2);")
        tdSql.execute("insert into gtest7 values(1537146001000,1);")
        tdSql.execute("insert into gtest7 values(1537146008000,2);")
        tdSql.execute("insert into gtest7 values(1537146009000,6);")
        tdSql.execute("insert into gtest7 values(1537146012000,3);")
        tdSql.execute("insert into gtest7 values(1537146015000,3);")
        tdSql.execute("insert into gtest7 values(1537146017000,1);")
        tdSql.execute("insert into gtest7 values(1537146019000,3);")
        tdSql.execute("insert into gtest8 values(1537146000002,4);")
        tdSql.execute("insert into gtest8 values(1537146002202,4);")  
        
        # irate verifacation --child table'query
        tdSql.query("select irate(col1) from test1;")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select irate(col1) from test1 interval(10s);")
        tdSql.checkData(0, 1, 1)
        tdSql.query("select irate(col1) from test1;")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select irate(col2) from test1;")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select irate(col3) from test1;")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select irate(col4) from test1;")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select irate(col5) from test1;")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select irate(col6) from test1;")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select irate(col11) from test1;")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select irate(col12) from test1;")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select irate(col13) from test1;")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select irate(col14) from test1;")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select irate(col2) from test1;")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select irate(col2) from test1;")
        tdSql.checkData(0, 0, 1)

        # irate verifacation --super table'query
        tdSql.query("select irate(col1) from test group by tbname,loc,tag1;")
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 1, "test2")
        tdSql.checkData(2, 2, "shanghai")    
        
        # add function testcase of twa: query from super table
        tdSql.query("select twa(col1) from test group by tbname,loc,tag1;")
        tdSql.checkData(0, 0, 50.5)
        tdSql.checkData(1, 1, "test2")
        tdSql.checkData(2, 2, "shanghai")        

        # error: function of irate and twa has invalid operation
        tdSql.error("select irate(col7) from test group by tbname,loc,tag1;")
        tdSql.error("select irate(col7) from test group by tbname;")
        tdSql.error("select irate(col1) from test group by loc,tbname,tag1;")
        # tdSql.error("select irate(col1) from test group by tbname,col7;")
        tdSql.error("select irate(col1) from test group by col7,tbname;")
        tdSql.error("select twa(col7) from test group by tbname,loc,tag1;")
        tdSql.error("select twa(col7) from test group by tbname;")
        tdSql.error("select twa(col1) from test group by loc,tbname,tag1;")
        # tdSql.error("select twa(col1) from test group by tbname,col7;")
        tdSql.error("select twa(col1) from test group by col7,tbname;")
                

        # general table'query 
        tdSql.query("select irate(col1) from gtest1;")
        tdSql.checkData(0, 0, 1.2/1.1)
        tdSql.query("select irate(col1) from gtest2;")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select irate(col1) from gtest3;")
        tdSql.checkData(0, 0, 0)
        tdSql.query("select irate(col1) from gtest4;")
        tdSql.checkRows(0)
        tdSql.query("select irate(col1) from gtest5;")
        tdSql.checkData(0, 0, 0)
        tdSql.query("select irate(col1) from gtest6;")
        tdSql.checkData(0, 0, 2)
        tdSql.query("select irate(col1) from gtest7;")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select irate(col1) from gtest7 interval(5s) order by ts asc;")
        tdSql.checkData(1, 1, 4)
        tdSql.checkData(2, 1, 0)
        tdSql.checkData(3, 1, 1)        
        tdSql.query("select irate(col1) from gtest7 interval(5s) order by ts desc ;")
        tdSql.checkData(1, 1, 0)
        tdSql.checkData(2, 1, 4)
        tdSql.checkData(3, 1, 0)    

        #error 
        tdSql.error("select irate(col1) from test")
        tdSql.error("select irate(ts) from test1")
        tdSql.error("select irate(col7) from test1")        
        tdSql.error("select irate(col8) from test1")
        tdSql.error("select irate(col9) from test1")
        tdSql.error("select irate(loc) from test1")
        tdSql.error("select irate(tag1) from test1")

        # use db1 precision us
        tdSql.execute("create database db1 precision 'us' keep 3650 UPDATE 1")        
        tdSql.execute("use db1 ")      
        tdSql.execute('''create table test(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double, 
                    col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned) tags(loc nchar(20))''')
        tdSql.execute("create table test1 using test tags('beijing')")
        tdSql.execute("create table gtest1 (ts timestamp, col1 float)")
        tdSql.execute("create table gtest2 (ts timestamp, col1 tinyint)")
        tdSql.execute("create table gtest3 (ts timestamp, col1 tinyint)")
        tdSql.execute("create table gtest4 (ts timestamp, col1 tinyint)")
        tdSql.execute("create table gtest5 (ts timestamp, col1 tinyint)")
        tdSql.execute("create table gtest6 (ts timestamp, col1 tinyint)")
        tdSql.execute("create table gtest7 (ts timestamp, col1 tinyint)")
        tdSql.execute("create table gtest8 (ts timestamp, col1 tinyint)")
        tdSql.execute("create table gtest9 (ts timestamp, col1 tinyint)")

        for i in range(self.rowNum):
            tdSql.execute("insert into test1 values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)" 
                        % (self.ts1 + i*1000000, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))
                    
        tdSql.execute("insert into gtest1 values(1537146000000000,0);")
        tdSql.execute("insert into gtest1 values(1537146001100000,1.2);")
        tdSql.execute("insert into gtest2 values(1537146001001000,1);")
        tdSql.execute("insert into gtest2 values(1537146001101000,2);")
        tdSql.execute("insert into gtest3 values(1537146001101000,2);")
        tdSql.execute("insert into gtest4(ts) values(1537146001101000);")
        tdSql.execute("insert into gtest5 values(1537146001002000,4);")
        tdSql.execute("insert into gtest5 values(1537146002202000,4);")
        tdSql.execute("insert into gtest6 values(1537146000000000,5);")
        tdSql.execute("insert into gtest6 values(1537146001000000,2);")
        tdSql.execute("insert into gtest7 values(1537146001000000,1);")
        tdSql.execute("insert into gtest7 values(1537146008000000,2);")
        tdSql.execute("insert into gtest7 values(1537146009000000,6);")
        tdSql.execute("insert into gtest7 values(1537146012000000,3);")
        tdSql.execute("insert into gtest7 values(1537146015000000,3);")
        tdSql.execute("insert into gtest7 values(1537146017000000,1);")
        tdSql.execute("insert into gtest7 values(1537146019000000,3);")
        tdSql.execute("insert into gtest8 values(1537146000002000,3);")
        tdSql.execute("insert into gtest8 values(1537146001003000,4);")  
        tdSql.execute("insert into gtest9 values(1537146000000000,4);")
        tdSql.execute("insert into gtest9 values(1537146000000001,5);")  


        # irate verifacation 
        tdSql.query("select irate(col1) from test1;")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select irate(col1) from test1 interval(10s);")
        tdSql.checkData(0, 1, 1)
        tdSql.query("select irate(col1) from test1;")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select irate(col1) from gtest1;")
        tdSql.checkData(0, 0, 1.2/1.1)
        tdSql.query("select irate(col1) from gtest2;")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select irate(col1) from gtest3;")
        tdSql.checkData(0, 0, 0)
        tdSql.query("select irate(col1) from gtest4;")
        tdSql.checkRows(0)
        tdSql.query("select irate(col1) from gtest5;")
        tdSql.checkData(0, 0, 0)
        tdSql.query("select irate(col1) from gtest6;")
        tdSql.checkData(0, 0, 2)
        tdSql.query("select irate(col1) from gtest7;")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select irate(col1) from gtest7 interval(5s) order by ts asc;")
        tdSql.checkData(1, 1, 4)
        tdSql.checkData(2, 1, 0)
        tdSql.checkData(3, 1, 1)        
        tdSql.query("select irate(col1) from gtest7 interval(5s) order by ts desc ;")
        tdSql.checkData(1, 1, 0)
        tdSql.checkData(2, 1, 4)
        tdSql.checkData(3, 1, 0)
        tdSql.query("select irate(col1) from gtest8;")
        tdSql.checkData(0, 0, 1/1.001)    
        tdSql.query("select irate(col1) from gtest9;")
        tdSql.checkData(0, 0, 1000000)

        #error 
        tdSql.error("select irate(col1) from test")
        tdSql.error("select irate(ts) from test1")
        tdSql.error("select irate(col7) from test1")        
        tdSql.error("select irate(col8) from test1")
        tdSql.error("select irate(col9) from test1")
        tdSql.error("select irate(loc) from test1")
        tdSql.error("select irate(tag1) from test1")




    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
