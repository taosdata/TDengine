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
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def caseDescription(self):
        '''
        case1<pxiao>: [TS-854] normal table batch insert with binding same table, different number of columns and timestamp in ascending order
        case2<pxiao>: [TS-854] normal table batch insert with binding same table, different number of columns and timestamp in descending order
        case3<pxiao>: [TS-854] normal table batch insert with binding same table, different number of columns and timestamp out of order
        case4<pxiao>: [TS-854] normal table batch insert with binding same table, different number of columns and timestamp overriding

        case5<pxiao>: [TS-854] normal table batch insert with binding different tables, different number of columns and timestamp in ascending order
        case6<pxiao>: [TS-854] normal table batch insert with binding different tables, different number of columns and timestamp in descending order
        case7<pxiao>: [TS-854] normal table batch insert with binding different tables, different number of columns and timestamp out of order
        case8<pxiao>: [TS-854] normal table batch insert with binding different tables, different number of columns and timestamp overriding

        case9<pxiao>: [TS-854] sub table batch insert with binding same table, different number of columns and timestamp in ascending order
        case10<pxiao>: [TS-854] sub table batch insert with binding same table, different number of columns and timestamp in descending order
        case11<pxiao>: [TS-854] sub table batch insert with binding same table, different number of columns and timestamp out of order
        case12<pxiao>: [TS-854] sub table batch insert with binding same table, different number of columns and timestamp overriding
        
        case13<pxiao>: [TS-854] sub table batch insert with binding different tables, different number of columns and timestamp in ascending order
        case14<pxiao>: [TS-854] sub table batch insert with binding different tables, different number of columns and timestamp in descending order
        case15<pxiao>: [TS-854] sub table batch insert with binding different tables, different number of columns and timestamp out of order
        case16<pxiao>: [TS-854] sub table batch insert with binding different tables, different number of columns and timestamp overriding
        ''' 
        return
    
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self.ts = 1607817000000

    def run(self):
        tdSql.prepare()        

        tdSql.execute("create table tb1(ts timestamp, c1 int, c2 int, c3 int, c4 int)")

        args = [(self.ts + 1000, self.ts + 3000, self.ts + 5000, self.ts + 7000),
                (self.ts + 8000, self.ts + 6000, self.ts + 4000, self.ts + 2000),
                (self.ts - 1000 , self.ts - 5000, self.ts - 3000, self.ts - 8000),
                (self.ts, self.ts, self.ts, self.ts)]

        # case 1, 2, 3, 4
        tdLog.info("test case for case 1, 2, 3, 4")        
        sql = "insert into tb1(ts, c1) values(%d, 0) tb1(ts, c1) values(%d, 0) tb1(ts, c1) values(%d, 0) tb1(ts, c1, c2, c3, c4) values(%d, 0, 0, 0, 0)"
        i = 1
        rows = 0
        for arg in args:                       
            tdLog.info("test case for case %d" % i)
            tdLog.info(sql % arg)
            tdSql.execute(sql % arg)

            if i == 4:
                rows = rows + 1                
            else:
                rows = rows + 4

            tdSql.query("select * from tb1")
            tdSql.checkRows(rows)
            i = i + 1
        
        # case 5, 6, 7, 8
        tdSql.execute("create table tb2(ts timestamp, c1 int, c2 int, c3 int, c4 int)")
        tdSql.execute("create table tb3(ts timestamp, c1 double, c2 double, c3 double, c4 double)")        
        tdLog.info("test case for case 5, 6, 7, 8")        
        sql = "insert into tb2(ts, c1) values(%d, 0) tb2(ts, c1, c2, c3, c4) values(%d, 0, 0, 0, 0) tb3(ts, c2) values(%d, 0.0) tb3(ts, c1, c2, c3, c4) values(%d, 0.0, 0.0, 0.0, 0.0)"        
        rows = 0
        for arg in args:
            tdLog.info("test case for case %d" % i)
            tdLog.info(sql % arg)
            tdSql.execute(sql % arg)
            tdSql.query("select * from tb2")
            if i == 8:
                rows = rows + 1                
            else:
                rows = rows + 2
            tdSql.query("select * from tb2")
            tdSql.checkRows(rows)               
            tdSql.query("select * from tb3")
            tdSql.checkRows(rows)            
            i = i + 1

        # case 9, 10, 11, 12
        tdSql.execute("create table stb(ts timestamp, c1 int, c2 int, c3 int, c4 int) tags(t1 nchar(20))")           
        tdLog.info("test case for case 9, 10, 11, 12")        
        sql = "insert into t1(ts, c1) using stb tags('tag1') values(%d, 0) t1(ts, c1) using stb tags('tag1') values(%d, 0) t1(ts, c1) using stb tags('tag1') values(%d, 0) t1(ts, c1, c2, c3, c4) using stb tags('tag1') values(%d, 0, 0, 0, 0)"        
        rows = 0
        for arg in args:
            tdLog.info("test case for case %d" % i)
            tdLog.info(sql % arg)
            tdSql.execute(sql % arg)            
            if i == 12:
                rows = rows + 1                
            else:
                rows = rows + 4
            tdSql.query("select * from stb")
            tdSql.checkRows(rows)                          
            i = i + 1
        
        # case 13, 14, 15, 16
        tdSql.execute("create table stb2(ts timestamp, c1 int, c2 int, c3 int, c4 int) tags(t1 nchar(20))")
        tdSql.execute("create table stb3(ts timestamp, c1 double, c2 double, c3 double, c4 double) tags(t1 binary(20))")        
        tdLog.info("test case for case 5, 6, 7, 8")        
        sql = "insert into t2(ts, c1) using stb2 tags('tag2') values(%d, 0) t2(ts, c1, c2, c3, c4) using stb2 tags('tag2') values(%d, 0, 0, 0, 0) t3(ts, c2) using stb3 tags('tag3') values(%d, 0.0) t3(ts, c1, c2, c3, c4) using stb3 tags('tag3') values(%d, 0.0, 0.0, 0.0, 0.0)"        
        rows = 0
        for arg in args:
            tdLog.info("test case for case %d" % i)
            tdLog.info(sql % arg)
            tdSql.execute(sql % arg)            
            if i == 8:
                rows = rows + 1                
            else:
                rows = rows + 2
            tdSql.query("select * from stb2")
            tdSql.checkRows(rows)               
            tdSql.query("select * from stb3")
            tdSql.checkRows(rows)            
            i = i + 1
    
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
