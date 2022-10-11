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

    # init
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        tdSql.prepare()        
        self.ts = 1500000000000
        self.rows = 100


    # run case  
    def run(self):
        self.case3()
        
    def case1(self):
        tdSql.execute("create table tb1(ts timestamp, c1 int, c2 int, c3 bool)")
        sql = "insert into tb1 values "
        for i in range(self.rows):
            sql += "(%d, %d, 1, %d)" % (self.ts + i, i % 10, i % 2)
        tdSql.execute(sql)
        
        operators = ['&', '|', '^', '<<', '>>']
        
        for operator in operators:
            tdSql.query("select c1 %s c2 from tb1" % operator)
            tdSql.checkRows(100)
            
            tdSql.query("select * from tb1 where c1 %s c2 = 0" % operator)
            if operator == '|' or operator == '>>':
                tdSql.checkRows(0)
            elif operator == '&':
                tdSql.checkRows(50)
            elif operator == '^':
                tdSql.checkRows(10)
            elif operator == '<<':
                tdSql.checkRows(52)
        
        tdSql.query("select ~c1 from tb1")
        tdSql.checkRows(52)
    
    def case2(self):
        tdSql.execute("create table tb2(ts timestamp, c1 tinyint, c2 tinyint unsigned, c3 smallint, c4 smallint unsigned, c5 int, c6 int unsigned, c7 bigint, c8 bigint unsigned, c9 float, c10 double, c11 bool, c12 binary(10), c13 nchar(10))")
        sql = "insert into tb2 values "
        for i in range(self.rows):
            sql += "(%d, 1, 1, 2, 2, 3, 3, 4, 4, 1.1, 2.5, True, 'test', 'test')" % (self.ts + i)
        tdSql.execute(sql)
        
        operators = ['&', '|', '^', '<<', '>>']
        
        for operator in operators:
            for i in range(13):                
                if i < 8:
                    tdSql.query("select 1 %s c%d from tb2" % (operator, i + 1))
                    tdSql.checkRows(self.rows)
                else:
                    tdSql.error("select 1 %s c%d from tb2" % (operator, i + 1))
                    tdSql.error("select c%d %s c%d from tb2" % (operator, i + 1, i + 1))
                
        for i in range(13):                    
            if i < 8:
                tdSql.execute("select  %sc%d from tb2" % (operator, i + 1))
                tdSql.checkRows(self.rows)
            else:
                tdSql.error("select %sc%d from tb2" % (operator, i + 1))
        
    def case3(self):
        tdSql.execute("create table st1(ts timestamp, c1 int, c2 int, c3 binary(20)) tags(t1 int, t2 int, t3 nchar(20))")
        for i in range(self.rows):
            tdSql.execute("create table t%d using st1 tags(%d, 1, 'test')" % (i, i))
            sql = "insert into t%d values " % i       
            for i in range(self.rows):
                sql += "(%d, %d, 1, %d)" % (self.ts + i, i % 10, i % 2)
            tdSql.execute(sql)
        
        operators = ['&', '|', '^', '<<', '>>']
        
        for operator in operators:
            tdSql.query("select c1 %s c2 from st1" % operator)
            tdSql.checkRows(10000)
            
            tdSql.query("select * from st1 where c1 %s c2 = 0" % operator)
            if operator == '|' or operator == '>>':
                tdSql.checkRows(0)
            elif operator == '&':
                tdSql.checkRows(5000)
            elif operator == '^':
                tdSql.checkRows(1000)
            elif operator == '<<':
                tdSql.checkRows(52)
        
        tdSql.query("select ~c1 from st1")
        tdSql.checkRows(10000)
                

    # stop 
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())