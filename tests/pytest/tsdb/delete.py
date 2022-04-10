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
WAITS = 5 # wait seconds

class TDTestCase:
    #
    # --------------- main frame -------------------
    #
    
    def caseDescription(self):
        '''
        limit and offset keyword function test cases;
        case1: limit offset base function test
        case2: limit offset advance test
        '''
        return 

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
        for i in range(100)
          tbname = "t%d"%i
          self.insert_data("t1", self.ts, (i+1)*10000, 20000);

        tdLog.debug(" INSERT data 100 tables ....... [OK]")  

        # test base case
        self.test_case1()
        tdLog.debug(" DELETE test_case1 ............ [OK]")
        # test advance case
        self.test_case2()
        tdLog.debug(" DELETE test_case2 ............ [OK]")


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
        tdSql.execute("create table st(ts timestamp, i1 int) tags(area int)");
        # child table
        for i in rang(100)
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

        tdLog.debug("INSERT TABLE DATA ............ [OK]")
        return

    # test case1 base 
    def test_case1(self):
        # 
        # delete base function
        #

        # single table
        sql = "select count(*) from t0"
        tdSql.waitedQuery(sql, 1, WAITS)
        tdSql.checkData(0, 0, 10000)
        return 

    # test advance 
    def test_case2(self):
        # 
        # delete other special case
        #
        
        # offset
        sql = "select * from t1 limit 10 offset 72000"

#
# add case with filename
#
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())