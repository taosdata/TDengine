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
        self.rowNum = 100000
        self.ts = 1537146000000
        
    def run(self):
        tdSql.prepare()

        print("==============step1")
        tdSql.execute(
            "create table if not exists st (ts timestamp, t1 int, t2 timestamp, t3 bigint, t4 float, t5 double, t6 binary(10), t7 smallint, t8 tinyint, t9 bool, t10 nchar(10), t11 int unsigned, t12 bigint unsigned, t13 smallint unsigned, t14 tinyint unsigned ,t15 int) tags(dev nchar(50), tag2 binary(16))")
        tdSql.execute(
            'CREATE TABLE if not exists dev_001 using st tags("dev_01", "tag_01")')
        tdSql.execute(
            'CREATE TABLE if not exists dev_002 using st tags("dev_02", "tag_02")') 

        print("==============step2")

        tdSql.execute(
            "INSERT INTO dev_001 VALUES('2020-05-13 10:00:00.000', 1, '2020-05-13 10:00:00.000', 10, 3.1, 3.14, 'test', -10, -126, true, '测试', 15, 10, 65534, 254, 1)('2020-05-13 10:00:01.000', 1, '2020-05-13 10:00:01.000', 10, 3.1, 3.14, 'test', -10, -126, true, '测试', 15, 10, 65534, 253, 5)('2020-05-13 10:00:02.000', 10, '2020-05-13 10:00:00.000', 11, 3.1, 3.14, 'test', 10, -127, false, '测试', 15, 10, 65534, 253, 10)('2020-05-13 10:00:03.000', 1, '2020-05-13 10:00:00.000', 11, 3.1, 3.14, 'test', -10, -126, true, '测试', 14, 12, 65532, 254, 15)")

        
        for i in range(10):
            sql = "insert into dev_002 (ts,t1) values "            
            for j in range(int(self.rowNum / 1000)):
                sql += "(%d, %d,)" % (self.ts + i * 1000 + j, i * 1000 + j + 1)
                tdSql.execute(sql)            

        tdSql.query("select count(ts) from dev_001 state_window(t1)")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2)
        tdSql.query("select count(ts) from dev_001 state_window(t3)")
        tdSql.checkRows(2)
        tdSql.checkData(1, 0, 2)
        tdSql.query("select count(ts) from dev_001 state_window(t7)")
        tdSql.checkRows(3)
        tdSql.checkData(1, 0, 1)
        tdSql.query("select count(ts) from dev_001 state_window(t8)")
        tdSql.checkRows(3)
        tdSql.checkData(2, 0, 1)
        tdSql.query("select count(ts) from dev_001 state_window(t11)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 3)     
        tdSql.query("select count(ts) from dev_001 state_window(t12)")
        tdSql.checkRows(2)
        tdSql.checkData(1, 0, 1)     
        tdSql.query("select count(ts) from dev_001 state_window(t13)")
        tdSql.checkRows(2)
        tdSql.checkData(1, 0, 1)         
        tdSql.query("select count(ts) from dev_001 state_window(t14)")
        tdSql.checkRows(3)
        tdSql.checkData(1, 0, 2)
        tdSql.query("select count(ts) from dev_002 state_window(t1)")
        tdSql.checkRows(1000)
        
        # with all aggregate function
        tdSql.query("select count(*),sum(t1),avg(t1),twa(t1),stddev(t15),leastsquares(t15,1,1),first(t15),last(t15),spread(t15),percentile(t15,90),t9 from dev_001 state_window(t9);")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 1, 10)
        tdSql.checkData(0, 2, 1)             
        # tdSql.checkData(0, 3, 1) 
        tdSql.checkData(0, 4, np.std([1,5])) 
        # tdSql.checkData(0, 5, 1) 
        tdSql.checkData(0, 6, 1)         
        tdSql.checkData(0, 7, 5)         
        tdSql.checkData(0, 8, 4)         
        tdSql.checkData(0, 9, 4.6)         
        tdSql.checkData(0, 10, 'True')         

        # with where
        tdSql.query("select avg(t15),t9 from dev_001 where  t9='true' state_window(t9);")
        tdSql.checkData(0, 0, 7)  
        tdSql.checkData(0, 1, 'True')  

        # error      
        tdSql.error("select count(*) from dev_001 state_window(t2)")
        tdSql.error("select count(*) from st state_window(t3)")
        tdSql.error("select count(*) from dev_001 state_window(t4)")
        tdSql.error("select count(*) from dev_001 state_window(t5)")
        tdSql.error("select count(*) from dev_001 state_window(t6)")
        tdSql.error("select count(*) from dev_001 state_window(t10)")
        tdSql.error("select count(*) from dev_001 state_window(tag2)")
        
        # TD-15164, TD-15226, TD-15227, TD-15186
        tdSql.execute("drop database if exists dd")
        tdSql.execute("create database dd keep 36500")
        tdSql.execute("use dd")
        tdSql.execute("create table table_1(ts timestamp , q_int int,q_bool bool)")
        tdSql.execute("insert into table_1 (ts , q_int,q_bool) values(1630000000000, 1,0)")
        tdSql.execute("insert into table_1 (ts , q_int,q_bool) values(1630000010000, 2,0)")
        tdSql.execute("insert into table_1 (ts , q_int,q_bool) values(1630000020000, 3,0)")
        tdSql.execute("insert into table_1 (ts , q_int,q_bool) values(1630000100000, 3,0)")
        tdSql.execute("insert into table_1 (ts , q_int,q_bool) values(1630000110000, 2,0)")
        tdSql.execute("insert into table_1 (ts , q_int,q_bool) values(1630000120000, 1,0)")
        
        tdSql.query("select STDDEV(q_int) from table_1 STATE_WINDOW(q_bool) order by ts ")
        tdSql.checkData(0, 0, 0.8164965)
        
        tdSql.query("select STDDEV(q_int) from table_1 STATE_WINDOW(q_bool) order by ts desc")
        tdSql.checkData(0, 0, 0.8164965)
        
        tdSql.query("select MAX(q_int) from table_1 STATE_WINDOW(q_bool) order by ts;")
        tdSql.checkData(0, 0, 3)
        
        tdSql.query("select MAX(q_int) from table_1 STATE_WINDOW(q_bool) order by ts desc;")
        tdSql.checkData(0, 0, 3)
        
        tdSql.query("select MAX(q_int) from table_1 where q_bool in ( true , false) STATE_WINDOW(q_bool) order by ts desc")
        tdSql.checkData(0, 0, 3)
        
        tdSql.query("select TOP(q_int,3) from table_1 STATE_WINDOW(q_bool) order by ts")
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(2, 1, 3)
        
        tdSql.query("select TOP(q_int,3) from table_1 STATE_WINDOW(q_bool) order by ts desc")
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(2, 1, 2)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
