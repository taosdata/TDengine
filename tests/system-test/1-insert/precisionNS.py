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
import random
import time

import taos
from util.log import *
from util.cases import *
from util.sql import *

class TDTestCase:

    # get col value and total max min ...
    def getColsValue(self, i, j):
        # c1 value
        if random.randint(1, 10) == 5:
            c1 = None
        else:
            c1 = 1

        # c2 value
        if j % 3200 == 0:
            c2 = 8764231
        elif random.randint(1, 10) == 5:
            c2 = None
        else:
            c2 = random.randint(-87654297, 98765321)    


        value = f"({self.ts}, "

        # c1
        if c1 is None:
            value += "null,"
        else:
            self.c1Cnt += 1
            value += f"{c1},"
        # c2
        if c2 is None:
            value += "null,"
        else:
            value += f"{c2},"
            # total count
            self.c2Cnt += 1
            # max
            if self.c2Max is None:
                self.c2Max = c2
            else:
                if c2 > self.c2Max:
                    self.c2Max = c2
            # min
            if self.c2Min is None:
                self.c2Min = c2
            else:
                if c2 < self.c2Min:
                    self.c2Min = c2
            # sum
            if self.c2Sum is None:
                self.c2Sum = c2
            else:
                self.c2Sum += c2

        # c3 same with ts
        value += f"{self.ts})"
        
        # move next
        self.ts += 1

        return value

    # insert data
    def insertData(self):
        tdLog.info("insert data ....")
        sqls = ""
        for i in range(self.childCnt):
            # insert child table
            values = ""
            pre_insert = f"insert into t{i} values "
            for j in range(self.childRow):
                if values == "":
                    values = self.getColsValue(i, j)
                else:
                    values += "," + self.getColsValue(i, j)

                # batch insert    
                if j % self.batchSize == 0  and values != "":
                    sql = pre_insert + values
                    tdSql.execute(sql)
                    values = ""
            # append last
            if values != "":
                sql = pre_insert + values
                tdSql.execute(sql)
                values = ""

        sql = "flush database db;"
        tdLog.info(sql)
        tdSql.execute(sql)
        # insert finished
        tdLog.info(f"insert data successfully.\n"
        f"                            inserted child table = {self.childCnt}\n"
        f"                            inserted child rows  = {self.childRow}\n"
        f"                            total inserted rows  = {self.childCnt*self.childRow}\n")
        return


    # prepareEnv
    def prepareEnv(self):
        # init                
        self.ts = 1680000000000*1000*1000
        self.childCnt = 5
        self.childRow = 10000
        self.batchSize = 5000
        
        # total
        self.c1Cnt = 0
        self.c2Cnt = 0
        self.c2Max = None
        self.c2Min = None
        self.c2Sum = None

        # create database  db
        sql = f"create database db vgroups 2 precision 'ns' "
        tdLog.info(sql)
        tdSql.execute(sql)
        sql = f"use db"
        tdSql.execute(sql)

        # create super talbe st
        sql = f"create table st(ts timestamp, c1 int, c2 bigint, ts1 timestamp) tags(area int)"
        tdLog.info(sql)
        tdSql.execute(sql)

        # create child table
        for i in range(self.childCnt):
            sql = f"create table t{i} using st tags({i}) "
            tdSql.execute(sql)

        # create stream
        sql = "create stream ma into sta as select count(ts) from st interval(100b)"
        tdLog.info(sql)
        tdSql.execute(sql)

        # insert data
        self.insertData()

    # check data correct
    def checkExpect(self, sql, expectVal):
        tdSql.query(sql)
        rowCnt = tdSql.getRows()
        for i in range(rowCnt):
            val = tdSql.getData(i,0)
            if val != expectVal:
                tdLog.exit(f"Not expect . query={val} expect={expectVal} i={i} sql={sql}")
                return False

        tdLog.info(f"check expect ok. sql={sql} expect ={expectVal} rowCnt={rowCnt}")
        return True
    

        

    # check time macro
    def checkTimeMacro(self):
        # 2 week
        val = 2
        nsval = val*7*24*60*60*1000*1000*1000
        expectVal = self.childCnt * self.childRow
        sql = f"select count(ts) from st where timediff(ts - {val}w, ts1) = {nsval} "
        self.checkExpect(sql, expectVal)

        # 20 day
        val = 20
        nsval = val*24*60*60*1000*1000*1000
        uint = "d"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {nsval} "
        self.checkExpect(sql, expectVal)

        # 30 hour
        val = 30
        nsval = val*60*60*1000*1000*1000
        uint = "h"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {nsval} "
        self.checkExpect(sql, expectVal)

        # 90 minutes
        val = 90
        nsval = val*60*1000*1000*1000
        uint = "m"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {nsval} "
        self.checkExpect(sql, expectVal)
        # 2s
        val = 2
        nsval = val*1000*1000*1000
        uint = "s"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {nsval} "
        self.checkExpect(sql, expectVal)
        # 20a
        val = 5
        nsval = val*1000*1000
        uint = "a"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {nsval} "
        self.checkExpect(sql, expectVal)
        # 300u
        val = 300
        nsval = val*1000
        uint = "u"
        sql = f"select count(ts) from st where timediff(ts - {val}{uint}, ts1) = {nsval} "
        self.checkExpect(sql, expectVal)
        # 8b
        val = 8
        sql = f"select timediff(ts - {val}b, ts1) from st "
        self.checkExpect(sql, val)

        # timetruncate check
        sql = '''select ts,timetruncate(ts,1u),
                          timetruncate(ts,1b),
                          timetruncate(ts,1m),
                          timetruncate(ts,1h),
                          timetruncate(ts,1w)
               from t0 order by ts desc limit 1;'''
        tdSql.query(sql)
        tdSql.checkData(0,1, "2023-03-28 18:40:00.000009000")
        tdSql.checkData(0,2, "2023-03-28 18:40:00.000009999")
        tdSql.checkData(0,3, "2023-03-28 18:40:00.000000000")
        tdSql.checkData(0,4, "2023-03-28 18:00:00.000000000")
        tdSql.checkData(0,5, "2023-03-23 00:00:00.000000000")

        # timediff
        sql = '''select ts,timediff(ts,ts+1b,1b),
                          timediff(ts,ts+1u,1u),
                          timediff(ts,ts+1a,1a),
                          timediff(ts,ts+1s,1s),
                          timediff(ts,ts+1m,1m),
                          timediff(ts,ts+1h,1h),
                          timediff(ts,ts+1d,1d),
                          timediff(ts,ts+1w,1w)
               from t0 order by ts desc limit 1;'''
        tdSql.query(sql)
        tdSql.checkData(0,1, 1)
        tdSql.checkData(0,2, 1)
        tdSql.checkData(0,3, 1)
        tdSql.checkData(0,4, 1)
        tdSql.checkData(0,5, 1)
        tdSql.checkData(0,6, 1)
        tdSql.checkData(0,7, 1)
        tdSql.checkData(0,8, 1)

    # init
    def init(self, conn, logSql, replicaVar=1):
        seed = time.time() % 10000 
        random.seed(seed)
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    # where
    def checkWhere(self):
        cnt = 300
        start = self.ts - cnt
        sql = f"select count(ts) from st where ts >= {start} and ts <= {self.ts}"
        self.checkExpect(sql, cnt)

        for i in range(50):
            cnt =  random.randint(1,40000)
            base = 2000
            start = self.ts - cnt - base
            end   = self.ts - base 
            sql = f"select count(ts) from st where ts >= {start} and ts < {end}"
            self.checkExpect(sql, cnt)

    # stream
    def checkStream(self):
        allRows = self.childCnt * self.childRow
        # ensure write data is expected
        sql = "select count(*) from (select diff(ts) as a from (select ts from st order by ts asc)) where a=1;"
        self.checkExpect(sql, allRows - 1)

        # stream count is ok
        sql =f"select count(*) from sta"
        cnt = int(allRows / 100) - 1 # last window is not close, so need reduce one
        self.checkExpect(sql, cnt)

        # check fields
        sql =f"select count(*) from sta where `count(ts)` != 100"
        self.checkExpect(sql, 0)

        # check timestamp
        sql =f"select count(*) from (select diff(`_wstart`) from sta)"
        self.checkExpect(sql, cnt - 1)
        sql =f"select count(*) from (select diff(`_wstart`) as a from sta) where a != 100"
        self.checkExpect(sql, 0)

    # run
    def run(self):
        # prepare env
        self.prepareEnv()

        # time macro like 1w 1d 1h 1m 1s 1a 1u 1b 
        self.checkTimeMacro()

        # check where
        self.checkWhere()

        # check stream
        self.checkStream()

    # stop
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
