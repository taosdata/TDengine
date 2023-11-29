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
import string

import taos
from util.log import *
from util.cases import *
from util.sql import *

class TDTestCase:
    updatecfgDict = {'vdebugFlag': 143, 'qdebugflag':135, 'tqdebugflag':135, 'udebugflag':135, 'rpcdebugflag':135,
                     'asynclog': 0, 'stdebugflag':135}
    # random string
    def random_string(self, count):
        letters = string.ascii_letters
        return ''.join(random.choice(letters) for i in range(count))

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
        
        # move next 1s interval
        self.ts += 1

        return value

    # insert data
    def insertData(self):
        tdLog.info("insert data ....")
        sqls = ""
        for i in range(self.childCnt):
            # insert child table
            values = ""
            pre_insert = f"insert into @db_name.t{i} values "
            for j in range(self.childRow):
                if values == "":
                    values = self.getColsValue(i, j)
                else:
                    values += "," + self.getColsValue(i, j)

                # batch insert    
                if j % self.batchSize == 0  and values != "":
                    sql = pre_insert + values
                    self.exeDouble(sql)
                    values = ""
            # append last
            if values != "":
                sql = pre_insert + values
                self.exeDouble(sql)
                values = ""

        # insert finished
        tdLog.info(f"insert data successfully.\n"
        f"                            inserted child table = {self.childCnt}\n"
        f"                            inserted child rows  = {self.childRow}\n"
        f"                            total inserted rows  = {self.childCnt*self.childRow}\n")
        return
    
    def exeDouble(self, sql):
        # dbname replace
        sql1 = sql.replace("@db_name", self.db1)

        if len(sql1) > 100:
            tdLog.info(sql1[:100])
        else:
            tdLog.info(sql1)
        tdSql.execute(sql1)

        sql2 = sql.replace("@db_name", self.db2)
        if len(sql2) > 100:
            tdLog.info(sql2[:100])
        else:
            tdLog.info(sql2)
        tdSql.execute(sql2)
        

    # prepareEnv
    def prepareEnv(self):
        # init                
        self.ts = 1680000000000
        self.childCnt = 2
        self.childRow = 100000
        self.batchSize = 5000
        self.vgroups1  = 4
        self.vgroups2  = 4
        self.db1 = "db1" # no sma
        self.db2 = "db2" # have sma
        self.smaClause = "interval(10s)"
        
        # total
        self.c1Cnt = 0
        self.c2Cnt = 0
        self.c2Max = None
        self.c2Min = None
        self.c2Sum = None

        # alter local optimization to treu
        sql = "alter local 'querysmaoptimize 1'"
        tdSql.execute(sql, 5, True)

        # check forbid mulit-replic on create sma index
        sql = f"create database db vgroups {self.vgroups1} replica 3"
        tdSql.execute(sql, 5, True)
        sql = f"create table db.st(ts timestamp, c1 int, c2 bigint, ts1 timestamp) tags(area int)"
        tdSql.execute(sql, 5, True)

        sql = f"create sma index sma_test on db.st function(max(c1),max(c2),min(c1),min(c2)) {self.smaClause};"
        tdLog.info(sql)
        tdSql.error(sql)


        # create database  db
        sql = f"create database @db_name vgroups {self.vgroups1} replica 1"
        self.exeDouble(sql)

        # create super talbe st
        sql = f"create table @db_name.st(ts timestamp, c1 int, c2 bigint, ts1 timestamp) tags(area int)"
        self.exeDouble(sql)

        # create child table
        for i in range(self.childCnt):
            sql = f"create table @db_name.t{i} using @db_name.st tags({i}) "
            self.exeDouble(sql)

        # create sma index on db2
        sql = f"use {self.db2}"
        tdSql.execute(sql)
        sql = f"create sma index sma_index_maxmin on {self.db2}.st function(max(c1),max(c2),min(c1),min(c2)) {self.smaClause};"
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

    # init
    def init(self, conn, logSql, replicaVar=1):
        seed = time.time() % 10000
        random.seed(seed)
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    # check query result same
    def queryDoubleImpl(self, sql):
        # sql
        sql1 = sql.replace('@db_name', self.db1)
        tdLog.info(sql1)
        start1 = time.time()
        rows1 = tdSql.query(sql1)
        spend1 = time.time() - start1
        res1 = copy.copy(tdSql.queryResult)

        sql2 = sql.replace('@db_name', self.db2)
        tdLog.info(sql2)
        start2 = time.time()
        tdSql.query(sql2)
        spend2 = time.time() - start2
        res2 = tdSql.queryResult

        rowlen1 = len(res1)
        rowlen2 = len(res2)

        if rowlen1 != rowlen2:
            tdLog.info(f"check error. rowlen1={rowlen1} rowlen2={rowlen2} both not equal.")
            return False
        
        for i in range(rowlen1):
            row1 = res1[i]
            row2 = res2[i]
            collen1 = len(row1)
            collen2 = len(row2)
            if collen1 != collen2:
                tdLog.info(f"checkerror. collen1={collen1} collen2={collen2} both not equal.")
                return False
            for j in range(collen1):
                if row1[j] != row2[j]:
                    tdLog.exit(f"col={j} col1={row1[j]} col2={row2[j]} both col not equal.")
                    return False

        # warning performance
        multiple = spend1/spend2
        tdLog.info("spend1=%.6fs spend2=%.6fs multiple=%.1f"%(spend1, spend2, multiple))
        if spend2 > spend1 and multiple < 4:
            tdLog.info(f"performace not reached: multiple(spend1/spend)={multiple} require is >=4 ")
            return False

        return True

    # check query result same
    def queryDouble(self, sql, tryCount=60, gap=1):
        for i in range(tryCount):
            if self.queryDoubleImpl(sql):
                return True
            # error
            tdLog.info(f"queryDouble return false, try loop={i}")
            time.sleep(gap)

        tdLog.exit(f"queryDouble try {tryCount} times, but all failed.")
        return False

    # check result
    def checkResult(self):

        # max
        sql = f"select max(c1) from @db_name.st {self.smaClause}"
        self.queryDouble(sql)

        # min
        sql = f"select max(c2) from @db_name.st {self.smaClause}"
        self.queryDouble(sql)

        # mix
        sql = f"select max(c1),max(c2),min(c1),min(c2) from @db_name.st {self.smaClause}"
        self.queryDouble(sql)


    # run
    def run(self):
        # prepare env
        self.prepareEnv()

        # check two db query result same
        tdLog.info(f"check have sma(db1) and no sma(db2) performace...")
        self.checkResult()        

    # stop
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
