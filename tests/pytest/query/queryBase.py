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
#
#     query base function test case
#

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
        Query moudle base api or keyword test case:
        case1: api first() last() 
        case2: none
        '''
        return 

    # init
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        tdSql.prepare()
        self.create_tables();
        self.ts = 1500000000000


    # run case  
    def run(self):
        # insert data
        self.insert_data("t1", self.ts, 1*10000, 30000, 0);
        self.insert_data("t2", self.ts, 2*10000, 30000, 100000);
        self.insert_data("t3", self.ts, 3*10000, 30000, 200000);
        # test base case
        self.case_first()
        tdLog.debug(" QUERYBASE first() api ............ [OK]")
        # test advance case
        self.case_last()
        tdLog.debug(" QUERYBASE last() api  ............ [OK]")

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
        tdSql.execute("create table t1 using st tags(1)");
        tdSql.execute("create table t2 using st tags(2)");
        tdSql.execute("create table t3 using st tags(3)");
        return 

    # insert data1
    def insert_data(self, tbname, ts_start, count, batch_num, base):
        pre_insert = "insert into %s values"%tbname
        sql = pre_insert
        tdLog.debug("doing insert table %s rows=%d ..."%(tbname, count))
        for i in range(count):
            sql += " (%d,%d)"%(ts_start + i*1000, base + i)
            if i >0 and i%batch_num == 0:
                tdSql.execute(sql)
                sql = pre_insert
        # end sql        
        if sql != pre_insert:
            tdSql.execute(sql)

        tdLog.debug("INSERT TABLE DATA ............ [OK]")
        return

    # first case base 
    def case_first(self):
        # 
        # last base function
        #

        # base t1 table
        sql = "select first(*) from t1 where ts>='2017-07-14 12:40:00' order by ts asc;"
        tdSql.waitedQuery(sql, 1, WAITS)
        tdSql.checkData(0, 1, 7200)
        sql = "select first(*) from t1 where ts>='2017-07-14 12:40:00' order by ts desc;" # desc
        tdSql.waitedQuery(sql, 1, WAITS)
        tdSql.checkData(0, 1, 7200)
        # super table st
        sql = "select first(*)  from st where ts>='2017-07-14 11:40:00' and ts<='2017-07-14 12:40:00' and tbname in('t1') order by ts;"
        tdSql.waitedQuery(sql, 1, WAITS)
        tdSql.checkData(0, 1, 3600)
        sql = "select first(*)  from st where ts>='2017-07-14 11:40:00' and ts<='2017-07-14 12:40:00' and tbname in('t1') order by ts desc;" # desc
        tdSql.waitedQuery(sql, 1, WAITS)
        tdSql.checkData(0, 1, 3600)
        # sub query
        sql = "select first(*) from ( select sum(i1)  from st where ts>='2017-07-14 11:40:00' and ts<'2017-07-14 12:40:00' interval(10m) order by ts asc );"
        tdSql.waitedQuery(sql, 1, WAITS)
        tdSql.checkData(0, 1, 187019100)
        sql = "select first(*) from ( select sum(i1)  from st where ts>='2017-07-14 11:40:00' and ts<'2017-07-14 12:40:00' interval(10m) order by ts desc );" # desc
        tdSql.waitedQuery(sql, 1, WAITS)
        tdSql.checkData(0, 1, 187019100)
        return 

    # last case
    def case_last(self):
        # 
        #  last base test
        #
        
        # base t1 table
        sql = "select last(*) from t1 where ts<='2017-07-14 12:40:00' order by ts asc;"
        tdSql.waitedQuery(sql, 1, WAITS)
        tdSql.checkData(0, 1, 7200)
        sql = "select last(*) from t1 where ts<='2017-07-14 12:40:00' order by ts desc;" # desc
        tdSql.waitedQuery(sql, 1, WAITS)
        tdSql.checkData(0, 1, 7200)        
        # super table st
        sql = "select last(*)  from st where ts>='2017-07-14 11:40:00' and ts<='2017-07-14 12:40:00' and tbname in('t1') order by ts;"
        tdSql.waitedQuery(sql, 1, WAITS)
        tdSql.checkData(0, 1, 7200)
        sql = "select last(*)  from st where ts>='2017-07-14 11:40:00' and ts<='2017-07-14 12:40:00' and tbname in('t1') order by ts desc;" # desc
        tdSql.waitedQuery(sql, 1, WAITS)
        tdSql.checkData(0, 1, 7200)
        
        # sub query
        sql = "select last(*) from ( select sum(i1)  from st where ts>='2017-07-14 11:40:00' and ts<'2017-07-14 12:40:00'  interval(10m) order by ts asc );"
        tdSql.waitedQuery(sql, 1, WAITS)
        tdSql.checkData(0, 1, 192419100)
        sql = "select last(*) from ( select sum(i1)  from st where ts>='2017-07-14 11:40:00' and ts<'2017-07-14 12:40:00'  interval(10m) order by ts desc );" # desc
        tdSql.waitedQuery(sql, 1, WAITS)
        tdSql.checkData(0, 1, 192419100)

        # add parent query order by
        # first 
        sql = "select first(*) from (select first(i1) from st interval(10m) order by ts asc) order by ts desc;"
        tdSql.waitedQuery(sql, 1, WAITS)
        tdSql.checkData(0, 1, 0)
        sql = "select first(*) from (select first(i1) from st interval(10m) order by ts desc) order by ts asc;"
        tdSql.waitedQuery(sql, 1, WAITS)
        tdSql.checkData(0, 1, 0)
        # last
        sql = "select last(*) from (select first(i1) from st interval(10m) order by ts asc) order by ts desc;"
        tdSql.waitedQuery(sql, 1, WAITS)
        tdSql.checkData(0, 1, 229400)
        sql = "select last(*) from (select first(i1) from st interval(10m) order by ts desc) order by ts asc;"
        tdSql.waitedQuery(sql, 1, WAITS)
        tdSql.checkData(0, 1, 229400)

#
# add case with filename
#
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())