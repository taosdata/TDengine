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
import copy

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

        # c3 is order 
        c3 = i * self.childRow + j

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

        # c3
        value += f"{c3},"
        # ts1 same with ts
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
        self.ts = 1680000000000*1000
        self.childCnt = 10
        self.childRow = 100000
        self.batchSize = 5000
        
        # total
        self.c1Cnt = 0
        self.c2Cnt = 0
        self.c2Max = None
        self.c2Min = None
        self.c2Sum = None

        # create database  db
        sql = f"create database db vgroups 2 precision 'us' "
        tdLog.info(sql)
        tdSql.execute(sql)
        sql = f"use db"
        tdSql.execute(sql)

        # alter config
        sql = "alter local 'querySmaOptimize 1';"
        tdLog.info(sql)
        tdSql.execute(sql)

        # create super talbe st
        sql = f"create table st(ts timestamp, c1 int, c2 bigint, c3 bigint, ts1 timestamp) tags(area int)"
        tdLog.info(sql)
        tdSql.execute(sql)

        # create child table
        for i in range(self.childCnt):
            sql = f"create table t{i} using st tags({i}) "
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

    # check query
    def queryResultSame(self, sql1, sql2):
        # sql
        tdLog.info(sql1)
        start1 = time.time()
        rows1 = tdSql.query(sql1)
        spend1 = time.time() - start1
        res1 = copy.copy(tdSql.queryResult)

        tdLog.info(sql2)
        start2 = time.time()
        tdSql.query(sql2)
        spend2 = time.time() - start2
        res2 = tdSql.queryResult

        rowlen1 = len(res1)
        rowlen2 = len(res2)

        if rowlen1 != rowlen2:
            tdLog.exit(f"rowlen1={rowlen1} rowlen2={rowlen2} both not equal.")
            return False
        
        for i in range(rowlen1):
            row1 = res1[i]
            row2 = res2[i]
            collen1 = len(row1)
            collen2 = len(row2)
            if collen1 != collen2:
                tdLog.exit(f"collen1={collen1} collen2={collen2} both not equal.")
                return False
            for j in range(collen1):
                if row1[j] != row2[j]:
                    tdLog.exit(f"col={j} col1={row1[j]} col2={row2[j]} both col not equal.")
                    return False

        # warning performance
        diff = (spend2 - spend1)*100/spend1
        tdLog.info("spend1=%.6fs spend2=%.6fs diff=%.1f%%"%(spend1, spend2, diff))
        if spend2 > spend1 and diff > 50:
            tdLog.info("warning: the diff for performance after spliting is over 20%")

        return True


    # init
    def init(self, conn, logSql, replicaVar=1):
        seed = time.time() % 10000
        random.seed(seed)
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    # check time macro
    def queryBasic(self):
        # check count
        expectVal = self.childCnt * self.childRow
        sql = f"select count(ts) from st "
        self.checkExpect(sql, expectVal)
        
        # check diff
        sql = f"select count(*) from (select diff(ts) as dif from st order by ts)"
        self.checkExpect(sql, expectVal - 1)

        # check ts order count
        sql = f"select count(*) from (select diff(ts) as dif from st order by ts) where dif!=1"
        self.checkExpect(sql, 0)

        # check ts1 order count
        sql = f"select count(*) from (select diff(ts1) as dif from st order by ts1) where dif!=1"
        self.checkExpect(sql, 0)

        # check c3 order asc
        sql = f"select count(*) from (select diff(c3) as dif from st order by c3) where dif!=1"
        self.checkExpect(sql, 0)

        # check c3 order desc todo FIX
        #sql = f"select count(*) from (select diff(c3) as dif from st order by c3 desc) where dif!=-1"
        #self.checkExpect(sql, 0)


    # advance
    def queryAdvance(self):
        # interval order todo FIX
        #sql = f"select _wstart,count(ts),max(c2),min(c2) from st interval(100u) sliding(50u) order by _wstart limit 10"
        #tdSql.query(sql)
        #tdSql.checkRows(10)
        
        # simulate crash sql
        sql = f"select _wstart,count(ts),max(c2),min(c2) from st interval(100a) sliding(10a) order by _wstart limit 10"
        tdSql.query(sql)
        tdSql.checkRows(10)

        # extent
        sql = f"select _wstart,count(ts),max(c2),min(c2) from st interval(100a) sliding(10a) order by _wstart desc limit 5"
        tdSql.query(sql)
        tdSql.checkRows(5)

        # data correct checked
        sql1 = "select sum(a),sum(b), max(c), min(d),sum(e) from (select _wstart,count(ts) as a,count(c2) as b ,max(c2) as c, min(c2) as d, sum(c2) as e from st interval(100a) sliding(100a) order by _wstart desc);"
        sql2 = "select count(*) as a, count(c2) as b, max(c2) as c, min(c2) as d, sum(c2) as e  from st;"
        self.queryResultSame(sql1, sql2)

    # run
    def run(self):
        # prepare env
        self.prepareEnv()

        # basic
        self.queryBasic()

        # advance
        self.queryAdvance()


    # stop
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
