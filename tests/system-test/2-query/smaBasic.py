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
        self.ts += 1

        # c1
        if c1 is None:
            value += "null,"
        else:
            self.c1Cnt += 1
            value += f"{c1},"
        # c2
        if c2 is None:
            value += "null)"
        else:
            value += f"{c2})"
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
        self.ts = 1600000000000
        self.childCnt = 5
        self.childRow = 1000000
        self.batchSize = 5000
        
        # total
        self.c1Cnt = 0
        self.c2Cnt = 0
        self.c2Max = None
        self.c2Min = None
        self.c2Sum = None

        # create database  db
        sql = f"create database db vgroups 5 replica 3 stt_trigger 1"
        tdLog.info(sql)
        tdSql.execute(sql)
        sql = f"use db"
        tdSql.execute(sql)

        # create super talbe st
        sql = f"create table st(ts timestamp, c1 int, c2 bigint) tags(area int)"
        tdLog.info(sql)
        tdSql.execute(sql)

        # create child table
        for i in range(self.childCnt):
            sql = f"create table t{i} using st tags({i}) "
            tdSql.execute(sql)

        # insert data
        self.insertData()

    # query sql value
    def queryValue(self, sql):
        tdSql.query(sql)
        return tdSql.getData(0, 0)
    
    # sum
    def checkCorrentSum(self):
        # query count
        sql = "select sum(c1) from st"
        val = self.queryValue(sql)
        # c1Sum is equal c1Cnt
        if  val != self.c1Cnt:
            tdLog.exit(f"Sum Not Expect. expect={self.c1Cnt} query={val} sql:{sql}")
            return 
        
        # not
        sql1 = "select sum(c1) from st where c2 = 8764231"
        val1 = self.queryValue(sql1)
        sql2 = "select sum(c1) from st where c2 != 8764231"
        val2 = self.queryValue(sql2)
        sql3 = "select sum(c1) from st where c2 is null"
        val3 = self.queryValue(sql3)
        if  val != val1 + val2 + val3:
            tdLog.exit(f"Sum Not Equal. val != val1 + val2 + val3. val={val} val1={val1} val2={val2} val2={val3} sql1={sql1} sql2={sql2} sql2={sql3}")
            return 
        
        # over than
        sql1 = "select sum(c1) from st where c2 > 8000"
        val1 = self.queryValue(sql1)
        sql2 = "select sum(c1) from st where c2 <= 8000"
        val2 = self.queryValue(sql2)
        sql3 = "select sum(c1) from st where c2 is null"
        val3 = self.queryValue(sql3)
        if  val != val1 + val2 + val3:
            tdLog.exit(f"Sum Not Equal. val != val1 + val2 + val3. val={val} val1={val1} val2={val2} val2={val3} sql1={sql1} sql2={sql2} sql2={sql3}")
            return 
        
        tdLog.info(f"check correct sum on c1 successfully.")

    # check result
    def checkResult(self, fun, val, val1, val2, sql1, sql2):
        if fun == "count":
            if  val != val1 + val2:
                tdLog.exit(f"{fun} NOT SAME. val != val1 + val2. val={val} val1={val1} val2={val2} sql1={sql1} sql2={sql2}")
                return
        elif fun == "max":
            if val != max([val1, val2]):
                tdLog.exit(f"{fun} NOT SAME . val != max(val1 ,val2) val={val} val1={val1} val2={val2} sql1={sql1} sql2={sql2}")
                return
        elif fun == "min":
            if val != min([val1, val2]):
                tdLog.exit(f"{fun} NOT SAME . val != min(val1 ,val2) val={val} val1={val1} val2={val2} sql1={sql1} sql2={sql2}")
                return

    # sum
    def checkCorrentFun(self, fun, expectVal):
        # query
        sql = f"select {fun}(c2) from st"
        val = self.queryValue(sql)
        if  val != expectVal:
            tdLog.exit(f"{fun} Not Expect. expect={expectVal} query={val} sql:{sql}")
            return 
        
        # not
        sql1 = f"select {fun}(c2) from st where c2 = 8764231"
        val1 = self.queryValue(sql1)
        sql2 = f"select {fun}(c2) from st where c2 != 8764231"
        val2 = self.queryValue(sql2)
        self.checkResult(fun, val, val1, val2, sql1, sql2)
        
        # over than
        sql1 = f"select {fun}(c2) from st where c2 > 8000"
        val1 = self.queryValue(sql1)
        sql2 = f"select {fun}(c2) from st where c2 <= 8000"
        val2 = self.queryValue(sql2)
        self.checkResult(fun, val, val1, val2, sql1, sql2)

        # successful
        tdLog.info(f"check correct {fun} on c2 successfully.")

    # check query corrent
    def checkCorrect(self):
        # count
        self.checkCorrentFun("count", self.c2Cnt)
        # max
        self.checkCorrentFun("max", self.c2Max)
        # min
        self.checkCorrentFun("min", self.c2Min)
        # sum
        self.checkCorrentSum()

        # c2 sum
        sql = "select sum(c2) from st"
        val = self.queryValue(sql)
        # c1Sum is equal c1Cnt
        if  val != self.c2Sum:
            tdLog.exit(f"c2 Sum Not Expect. expect={self.c2Sum} query={val} sql:{sql}")
            return

    def checkPerformance(self):
        # have sma caculate
        sql1 = "select count(*) from st"
        stime = time.time()
        tdSql.execute(sql1, 1)
        spend1 = time.time() - stime
        

        # no sma caculate
        sql2 = "select count(*) from st where c2 != 8764231 or c2 is null"
        stime = time.time()
        tdSql.execute(sql2, 1)
        spend2 = time.time() - stime

        time1 = "%.2f"%(spend1*1000)
        time2 = "%.2f"%(spend2*1000)
        if spend2 < spend1 * 8:
            tdLog.exit(f"performance not passed! sma spend1={time1}ms no sma spend2= {time2}ms sql1={sql1} sql2= {sql2}")
            return 
        tdLog.info(f"performance passed! sma spend1={time1}ms no sma spend2= {time2}ms sql1={sql1} sql2= {sql2}")


    # init
    def init(self, conn, logSql, replicaVar=1):
        seed = time.time() % 10000
        random.seed(seed)
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    # run
    def run(self):
        # prepare env
        self.prepareEnv()

        # query 
        self.checkCorrect()

        # performance
        self.checkPerformance()

    # stop
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
