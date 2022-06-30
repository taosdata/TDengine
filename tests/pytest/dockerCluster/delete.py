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

from numpy.lib.function_base import insert
import taos
from util.log import *
from util.cases import *
from util.sql import *
import numpy as np

# constant define
WAITS = 10 # wait seconds

class TDTestCase:
    
    updatecfgDict = {'numOfNodes': 3, '1':{'replica': 3}, '2':{'replica': 3}, '3':{'replica': 3}}
    
    # init
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        tdSql.prepare()
        self.create_tables()
        self.ts = 1500000000000


    # run case  
    def run(self):
        # insert data
        for i in range(10):
            tbname = "t%d"%i
            self.insert_data(tbname, self.ts, (i+1)*10000, 20000);

        tdLog.debug(" INSERT data 10 tables ....... [OK]")  

        # test base case
        self.test_case1()
        tdLog.debug(" DELETE test_case1 ............ [OK]")
        # test advance case
        self.test_case2()
        tdLog.debug(" DELETE test_case2 ............ [OK]")
        # test advance case
        self.test_case3()
        tdLog.debug(" DELETE test_case3 ............ [OK]")


    # stop 
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

    #
    # --------------- case  -------------------
    #

    # create table
    def create_tables(self):
        # super table
        tdSql.execute("create table st(ts timestamp, i1 int) tags(area int)")
        # child table
        for i in range(10):
          sql = "create table t%d using st tags(%d)"%(i, i)
          tdSql.execute(sql)
        
        return 

    # insert data1
    def insert_data(self, tbname, ts_start, count, batch_num):
        pre_insert = "insert into %s values"%tbname
        sql = pre_insert
        tdLog.debug("doing insert table %s rows=%d ..."%(tbname, count))
        for i in range(count):
            sql += " (%d,%d)"%(ts_start + i*1000, i)
            if i >0 and i%batch_num == 0:
                tdSql.execute(sql)
                sql = pre_insert
        # end sql        
        if sql != pre_insert:
            tdSql.execute(sql)

        return

    # test case1 base 
    def test_case1(self):
        # 
        # delete from single table
        #
        
        # where <
        sql = "select count(*) from t0 where ts < 1500000120000"
        tdSql.query(sql)
        tdSql.checkData(0, 0, 120)
        
        sql = "delete from t0 where ts < 1500000120000"
        tdSql.execute(sql)
        tdSql.checkAffectedRows(120)

        sql = "select count(*) from t0"
        tdSql.query(sql)
        tdSql.checkData(0, 0, 10000 - 120 )

        sql = "select * from t0 limit 1"
        tdSql.query(sql)
        tdSql.checkData(0, 1, 120)

        # where > and <
        sql = "delete from t0 where ts > 1500000240000 and ts <= 1500000300000"
        tdSql.execute(sql)
        tdSql.checkAffectedRows(60)
        sql = "select count(*) from t0"
        tdSql.query(sql)
        tdSql.checkData(0, 0, 10000 - 120 - 60)

        sql = "select * from t0 limit 2 offset 120"
        tdSql.query(sql)
        tdSql.checkData(0, 1, 240)
        tdSql.checkData(1, 1, 301)

        
        # where >  delete 1000 rows from end
        sql = "delete from t0 where ts >= 1500009000000; "
        tdSql.execute(sql)
        tdSql.checkAffectedRows(1000)
        sql = "select count(*) from t0"
        tdSql.query(sql)
        tdSql.checkData(0, 0, 10000 - 120 - 60 - 1000)
        
        sql = "select last_row(*) from t0; "
        tdSql.query(sql)
        tdSql.checkData(0, 1, 8999)

        sql = "select last(*) from t0"
        tdSql.query(sql)
        tdSql.checkData(0, 1, 8999)

        # insert last_row
        sql = "insert into t0 values(1500009999000,9999); "
        tdSql.execute(sql)

        sql = "select last_row(*) from t0; "
        tdSql.query(sql)
        tdSql.checkData(0, 1, 9999)

        sql = "select last(*) from t0"
        tdSql.query(sql)
        tdSql.checkData(0, 1, 9999)

        # insert last 
        sql = "insert into t0 values(1500010000000,10000); "
        tdSql.execute(sql)
        sql = "insert into t0 values(1500010002000,NULL); "
        tdSql.execute(sql)
        sql = "insert into t0 values(1500010001000,10001); "
        tdSql.execute(sql)
        sql = "delete from t0 where ts = 1500010001000;  "
        tdSql.execute(sql)

        sql = "select last_row(i1) from t0; "
        tdSql.query(sql)
        tdSql.checkData(0, 0, None)

        sql = "select last(i1) from t0; "
        tdSql.query(sql)
        tdSql.checkData(0, 0, 10000)

        # delete whole
        sql = "delete from t0;"
        tdSql.execute(sql)
        tdSql.checkAffectedRows(8823)
        
        return 

    # test advance 
    def test_case2(self):
        # 
        # delete from super table
        #
        
        # where <
        sql = "select count(*) from st where ts < 1500000120000;"
        tdSql.query(sql)
        tdSql.checkData(0, 0, 9*120) #1080
        
        sql = "delete from st where ts < 1500000120000;"
        tdSql.execute(sql)
        tdSql.checkAffectedRows(9*120) #1080

        sql = "select count(*) from st;"
        tdSql.query(sql)
        tdSql.checkData(0, 0, 540000 - 9*120 )

        sql = "select * from st limit 1;"
        tdSql.query(sql)
        tdSql.checkData(0, 1, 120)

        # where > and <
        sql = "delete from st where ts > 1500000240000 and ts <= 1500000300000;"
        tdSql.execute(sql)
        tdSql.checkAffectedRows(9*60)
        sql = "select count(*) from st;"
        tdSql.query(sql)
        tdSql.checkData(0, 0, 540000 - 9*120 - 9*60)

        sql = "select * from st limit 2 offset 120"
        tdSql.query(sql)
        tdSql.checkData(0, 1, 240)
        tdSql.checkData(1, 1, 301)

        
        # where >  delete 1000 rows from end
        sql = "delete from st where ts >= 1500009000000; "
        tdSql.execute(sql)
        tdSql.checkAffectedRows(459000)
        sql = "select count(*) from st;"
        tdSql.query(sql)
        tdSql.checkData(0, 0, 79380)
        
        sql = "select last_row(*) from st; "
        tdSql.query(sql)
        tdSql.checkData(0, 1, 8999)

        sql = "select last(*) from st"
        tdSql.query(sql)
        tdSql.checkData(0, 1, 8999)

        # insert last_row
        sql = "insert into t0 values(1500009999000,9999); "
        tdSql.execute(sql)

        sql = "select last_row(*) from st; "
        tdSql.query(sql)
        tdSql.checkData(0, 1, 9999)

        sql = "select last(*) from st"
        tdSql.query(sql)
        tdSql.checkData(0, 1, 9999)

        # insert last 
        sql = "insert into t0 values(1500010000000,10000); "
        tdSql.execute(sql)
        sql = "insert into t0 values(1500010002000,NULL); "
        tdSql.execute(sql)
        sql = "insert into t0 values(1500010001000,10001); "
        tdSql.execute(sql)
        sql = "delete from t0 where ts = 1500010001000;  "
        tdSql.execute(sql)

        sql = "select last_row(i1) from st; "
        tdSql.query(sql)
        tdSql.checkData(0, 0, None)

        sql = "select last(i1) from st; "
        tdSql.query(sql)
        tdSql.checkData(0, 0, 10000)

        # delete whole
        sql = "delete from st;"
        tdSql.execute(sql)
        tdSql.checkAffectedRows(79383)
        
        return 
    
    # verify function results after delete
    def test_case3(self):
        tdSql.execute("create database test")
        tdSql.execute("use test")
        
        self.create_tables()
        # insert data
        for i in range(10):
            tbname = "t%d"%i
            self.insert_data(tbname, self.ts, (i+1)*10000, 20000);
                        
        tdSql.query("select count(*) from st")
        tdSql.checkData(0, 0, 550000)
        count = tdSql.getData(0, 0)        
        
        tdSql.query("select sum(i1) from st")
        sum = tdSql.getData(0, 0)
        
        tdSql.query("select avg(i1) from st")
        avg = tdSql.getData(0, 0)
        
        tdSql.query("select count(*) from t0")
        count1 = tdSql.getData(0, 0)
        
        tdSql.query("select sum(i1) from t0")
        sum1 = tdSql.getData(0, 0)
        
        tdSql.query("select avg(i1) from t0")
        avg1 = tdSql.getData(0, 0)
        
        tdSql.execute("delete from st where tbname='t0'")
        tdSql.checkAffectedRows(count1)
        
        tdSql.query("select count(*) from st")        
        tdSql.checkData(0, 0, count - count1)
        
        tdSql.query("select sum(i1) from st")
        tdSql.checkData(0, 0, sum - sum1)
        
        tdSql.query("select avg(i1) from st")
        tdSql.checkData(0, 0, (sum - sum1) / (count - count1))        

#
# add case with filename
#
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())