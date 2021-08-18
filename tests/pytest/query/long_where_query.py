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

        print("==============step1, regular table==============")
        startTime = time.time() 
        sql = "create table regular_table_1(ts timestamp, "
        for i in range(4094):
            sql += "col00000111112222233333444445555566666777778888899999000000l%d int, " % (i + 1)
        sql += "col4095 binary(22))"  
        tdLog.info(len(sql))      
        tdSql.execute(sql)

        tdLog.info("========== test1.1 : test regular table in ( ) ==========")
        sql = '''insert into regular_table_1(ts,col00000111112222233333444445555566666777778888899999000000l1) values(now,1);'''
        tdSql.execute(sql)
        sql = ''' select * from regular_table_1  where  col00000111112222233333444445555566666777778888899999000000l1 in (1); '''
        tdSql.query(sql)  
        tdSql.checkData(0, 1, 1)

        for i in range(self.num):
            sql = "insert into regular_table_1 values(%d, "
            for j in range(4094):
                str = "'%s', " % random.randint(0,1000)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i))
        time.sleep(1)
        tdSql.query("select count(*) from regular_table_1")
        tdSql.checkData(0, 0, self.num+1)
        tdSql.query("select * from regular_table_1")
        tdSql.checkRows(self.num+1)
        tdSql.checkCols(4096)

        #maxSQLLength 1048576
        sql = "select * from regular_table_1 where col00000111112222233333444445555566666777778888899999000000l1 in ("
        for i in range(2,128840):
            sql += "%d , " % (i + 1)
        sql += "1 ,12345) order by ts desc;"  
        #tdLog.info(sql)
        tdLog.info(len(sql)) 
        tdSql.query(sql)  
        tdSql.checkData(0, 1, 1)
        tdSql.checkCols(4096)

        #maxSQLLength 1048577--error
        sql = "select * from regular_table_1 where col00000111112222233333444445555566666777778888899999000000l1 in ("
        for i in range(2,128840):
            sql += "%d , " % (i + 1)
        sql += "1 ,123456) order by ts desc;"  
        #tdLog.info(sql)
        tdLog.info(len(sql))  
        tdSql.error(sql)  

        tdLog.info("========== test1.2 : test regular table in (' ') ==========")
        sql = '''insert into regular_table_1(ts,col4095) values(now,1);'''
        tdSql.execute(sql)
        sql = ''' select * from regular_table_1  where  col4095 in ('1',"1"); '''
        tdSql.query(sql)  
        tdSql.checkData(0, 4095, 1)

        #maxSQLLength 1048576
        sql = " select * from regular_table_1 where col4095 in ("
        for i in range(96328):
            sql += " '%d' , " % (i + 1)
        sql += " '1' ) order by ts desc;" 
        #tdLog.info(sql)
        tdLog.info(len(sql)) 
        tdSql.query(sql)  
        tdSql.checkData(0, 4095, 1)
        tdSql.checkCols(4096)
        
        #maxSQLLength 1048577--error
        sql = " select * from regular_table_1 where col4095 in ("
        for i in range(96328):
            sql += " '%d' , " % (i + 1)
        sql += " '123' ) order by ts desc;" 
        #tdLog.info(sql)
        tdLog.info(len(sql))   
        tdSql.error(sql)  
        
        endTime = time.time()
        print("total time %ds" % (endTime - startTime))
 



        print("==============step2, super table ==============")
        startTime = time.time() 
        sql = "create stable stable_1(ts timestamp, "
        for i in range(4090):
            sql += "col00000111112222233333444445555566666777778888899999000000l%d int, " % (i + 1)
        sql += "col4091 binary(22))"  
        sql += " tags (loc nchar(10),tag_1 int,tag_2 int,tag_3 int) " 
        tdLog.info(len(sql))      
        tdSql.execute(sql)
        sql = '''create table table_1 using stable_1 
                    tags('table_1' , '1' , '2' , '3' );'''
        tdSql.execute(sql)

        tdLog.info("========== test2.1 : test super table in ( ) ==========")
        sql = '''insert into table_1(ts,col00000111112222233333444445555566666777778888899999000000l1) values(now,1);'''
        tdSql.execute(sql)
        sql = ''' select * from stable_1  where  col00000111112222233333444445555566666777778888899999000000l1 in (1); '''
        tdSql.query(sql)  
        tdSql.checkData(0, 1, 1)
        sql = ''' select * from table_1  where  col00000111112222233333444445555566666777778888899999000000l1 in (1); '''
        tdSql.query(sql)  
        tdSql.checkData(0, 1, 1)

        for i in range(self.num):
            sql = "insert into table_1 values(%d, "
            for j in range(4090):
                str = "'%s', " % random.randint(0,1000)                
                sql += str
            sql += "'%s')" % self.get_random_string(22)
            tdSql.execute(sql % (self.ts + i))
        time.sleep(1)

        tdSql.query("select count(*) from table_1")
        tdSql.checkData(0, 0, self.num+1)
        tdSql.query("select * from table_1")
        tdSql.checkRows(self.num+1)
        tdSql.checkCols(4092)

        tdSql.query("select count(*) from stable_1")
        tdSql.checkData(0, 0, self.num+1)
        tdSql.query("select * from stable_1")
        tdSql.checkRows(self.num+1)
        tdSql.checkCols(4096)

        #maxSQLLength 1048576
        sql = "select * from table_1 where col00000111112222233333444445555566666777778888899999000000l1 in ("
        for i in range(128840):
            sql += "%d , " % (i + 1)
        sql += "1 ,12345) order by ts desc;"  
        #tdLog.info(sql)
        tdLog.info(len(sql)) 
        tdSql.query(sql)  
        tdSql.checkData(0, 1, 1)
        tdSql.checkCols(4092)  

        sql = "select * from stable_1 where col00000111112222233333444445555566666777778888899999000000l1 in ("
        for i in range(128840):
            sql += "%d , " % (i + 1)
        sql += "1 ,1234) order by ts desc;"  
        #tdLog.info(sql)
        tdLog.info(len(sql)) 
        tdSql.query(sql)  
        tdSql.checkData(0, 1, 1)
        tdSql.checkCols(4096)      

        #TD-5640
        sql = "select * from stable_1 where tag_1 in ("
        for i in range(128847):
            sql += "%d , " % (i + 1)
        sql += "1)order by ts desc;"  
        #tdLog.info(sql)
        tdLog.info(len(sql)) 
        tdSql.query(sql)  
        tdSql.checkData(0, 1, 1)
        tdSql.checkCols(4096)    

        #maxSQLLength 1048577--error
        sql = "select * from table_1 where col00000111112222233333444445555566666777778888899999000000l1 in ("
        for i in range(128840):
            sql += "%d , " % (i + 1)
        sql += "1 ,123456) order by ts desc;"  
        #tdLog.info(sql)
        tdLog.info(len(sql)) 
        tdSql.error(sql)    

        sql = "select * from stable_1 where col00000111112222233333444445555566666777778888899999000000l1 in ("
        for i in range(128840):
            sql += "%d , " % (i + 1)
        sql += "1 ,12345) order by ts desc;"  
        #tdLog.info(sql)
        tdLog.info(len(sql)) 
        tdSql.error(sql)    

        sql = "select * from stable_1 where tag_1 in ("
        for i in range(128847):
            sql += "%d , " % (i + 1)
        sql += "1) order by ts desc;"  
        #tdLog.info(sql)
        tdLog.info(len(sql)) 
        tdSql.error(sql)     


        tdLog.info("========== tests2.2 : test super table in (' ') ==========")
        sql = '''insert into table_1(ts,col4091) values(now,1);'''
        tdSql.execute(sql)
        sql = ''' select * from table_1  where  col4091 in ('1',"1"); '''
        tdSql.query(sql)  
        tdSql.checkData(0, 4091, 1)

        #maxSQLLength 1048576
        sql = " select * from table_1 where col4091 in ("
        for i in range(96328):
            sql += " '%d' , " % (i + 1)
        sql += " '1','123456' ) order by ts desc;" 
        #tdLog.info(sql)
        tdLog.info(len(sql)) 
        tdSql.query(sql)  
        tdSql.checkData(0, 4091, 1)
        tdSql.checkCols(4092)

        sql = " select * from stable_1 where col4091 in ("
        for i in range(96328):
            sql += " '%d' , " % (i + 1)
        sql += " '1','12345' ) order by ts desc;" 
        #tdLog.info(sql)
        tdLog.info(len(sql)) 
        tdSql.query(sql)  
        tdSql.checkData(0, 4091, 1)
        tdSql.checkCols(4096)

        #TD-5650
        sql = " select * from stable_1 where loc in ("
        for i in range(96328):
            sql += " '%d' , " % (i + 1)
        sql += " '123','table_1' ) order by ts desc;" 
        #tdLog.info(sql)
        tdLog.info(len(sql)) 
        tdSql.query(sql)  
        tdSql.checkData(0, 4092, 'table_1')
        tdSql.checkCols(4096)

        #maxSQLLength 1048577--error
        sql = " select * from table_1 where col4091 in ("
        for i in range(96328):
            sql += " '%d' , " % (i + 1)
        sql += " '1','1234567' ) order by ts desc;" 
        #tdLog.info(sql)
        tdLog.info(len(sql)) 
        tdSql.error(sql) 

        sql = " select * from stable_1 where col4091 in ("
        for i in range(96328):
            sql += " '%d' , " % (i + 1)
        sql += " '1','123456' ) order by ts desc;" 
        #tdLog.info(sql)
        tdLog.info(len(sql)) 
        tdSql.error(sql) 

        sql = " select * from stable_1 where loc in ("
        for i in range(96328):
            sql += " '%d' , " % (i + 1)
        sql += " '1','1234567890' ) order by ts desc;" 
        #tdLog.info(sql)
        tdLog.info(len(sql)) 
        tdSql.error(sql) 

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))
        #os.system("rm -rf query/long_where_query.py.sql")


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
