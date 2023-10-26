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

        # insert nomal talbe
        for i in range(20):
            self.ts += 1000
            name = self.random_string(20)
            sql = f"insert into @db_name.ta values({self.ts}, {i}, {self.ts%100000}, '{name}', false)"
            self.exeDouble(sql)

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
        self.childCnt = 10
        self.childRow = 10000
        self.batchSize = 5000
        self.vgroups1  = 2
        self.vgroups2  = 2
        self.db1 = "db1"
        self.db2 = "db2"
        
        # total
        self.c1Cnt = 0
        self.c2Cnt = 0
        self.c2Max = None
        self.c2Min = None
        self.c2Sum = None

        # create database  db
        sql = f"create database @db_name vgroups {self.vgroups1} replica 3"
        self.exeDouble(sql)

        # create super talbe st
        sql = f"create table @db_name.st(ts timestamp, c1 int, c2 bigint, ts1 timestamp) tags(area int)"
        self.exeDouble(sql)

        # create child table
        for i in range(self.childCnt):
            sql = f"create table @db_name.t{i} using @db_name.st tags({i}) "
            self.exeDouble(sql)

        # create normal table
        sql = f"create table @db_name.ta(ts timestamp, c1 int, c2 bigint, c3 binary(32), c4 bool)"
        self.exeDouble(sql)

        # insert data
        self.insertData()

        # update
        self.ts = 1680000000000 + 10000
        self.childRow = 2000

        # delete data
        sql = "delete from @db_name.st where ts > 1680000001900 and ts < 1680000012000"
        self.exeDouble(sql)
        sql = "delete from @db_name.st where ts > 1680000029000 and ts < 1680000048000"
        self.exeDouble(sql)

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
    def queryDouble(self, sql):
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
        errCnt = 0

        if rowlen1 != rowlen2:
            tdLog.exit(f"both row count not equal. rowlen1={rowlen1} rowlen2={rowlen2} ")
            return False
        
        for i in range(rowlen1):
            row1 = res1[i]
            row2 = res2[i]
            collen1 = len(row1)
            collen2 = len(row2)
            if collen1 != collen2:
                tdLog.exit(f"both col count not equal. collen1={collen1} collen2={collen2}")
                return False
            for j in range(collen1):
                if row1[j] != row2[j]:
                    tdLog.info(f"error both column value not equal. row={i} col={j} col1={row1[j]} col2={row2[j]} .")
                    errCnt += 1

        if errCnt > 0:
            tdLog.exit(f" db2 column value  different with db2. different count ={errCnt} ")

        # warning performance
        diff = (spend2 - spend1)*100/spend1
        tdLog.info("spend1=%.6fs spend2=%.6fs diff=%.1f%%"%(spend1, spend2, diff))
        if spend2 > spend1 and diff > 20:
            tdLog.info("warning: the diff for performance after spliting is over 20%")

        return True


    # check result
    def checkResult(self):
        # check vgroupid
        sql = f"select vgroup_id from information_schema.ins_vgroups where db_name='{self.db2}'"
        tdSql.query(sql)
        tdSql.checkRows(self.vgroups2)

        # check child table count same
        sql = "select table_name from information_schema.ins_tables where db_name='@db_name' order by table_name"
        self.queryDouble(sql)

        # check row value is ok
        sql = "select * from @db_name.st order by ts"
        self.queryDouble(sql)
        
        # where
        sql = "select *,tbname from @db_name.st where c1 < 1000 order by ts"
        self.queryDouble(sql)

        # max
        sql = "select max(c1) from @db_name.st"
        self.queryDouble(sql)

        # min
        sql = "select min(c2) from @db_name.st"
        self.queryDouble(sql)

        # sum
        sql = "select sum(c1) from @db_name.st"
        self.queryDouble(sql)

        # normal table

        # count
        sql = "select count(*) from @db_name.ta"
        self.queryDouble(sql)

        # all rows
        sql = "select * from @db_name.ta"
        self.queryDouble(sql)

        # sum
        sql = "select sum(c1) from @db_name.ta"
        self.queryDouble(sql)


    # get vgroup list
    def getVGroup(self, db_name):
        vgidList = []
        sql = f"select vgroup_id from information_schema.ins_vgroups where db_name='{db_name}'"
        res = tdSql.getResult(sql)
        rows = len(res)
        for i in range(rows):
            vgidList.append(res[i][0])

        return vgidList;        

    # split vgroup on db2
    def splitVGroup(self, db_name):
        vgids = self.getVGroup(db_name)
        selid = random.choice(vgids)
        sql = f"split vgroup {selid}"
        tdLog.info(sql)
        tdSql.execute(sql)

        # wait end
        seconds = 300
        for i in range(seconds):
            sql ="show transactions;"
            rows = tdSql.query(sql)
            if rows == 0:
                tdLog.info("split vgroup finished.")
                return True
            #tdLog.info(f"i={i} wait split vgroup ...")
            time.sleep(1)

        tdLog.exit(f"split vgroup transaction is not finished after executing {seconds}s")
        return False

    # split error 
    def expectSplitError(self, dbName):
        vgids = self.getVGroup(dbName)
        selid = random.choice(vgids)
        sql = f"split vgroup {selid}"
        tdLog.info(sql)
        tdSql.error(sql)    

    # expect split ok
    def expectSplitOk(self, dbName):
        # split vgroup
        vgList1 = self.getVGroup(dbName)
        self.splitVGroup(dbName)
        vgList2 = self.getVGroup(dbName)
        vgNum1 = len(vgList1) + 1
        vgNum2 = len(vgList2)
        if vgNum1 != vgNum2:
            tdLog.exit(f" vglist len={vgNum1} is not same for expect {vgNum2}")
            return

    # split empty database
    def splitEmptyDB(self):        
        dbName = "emptydb"
        vgNum = 2
        # create database
        sql = f"create database {dbName} vgroups {vgNum} replica 3"
        tdLog.info(sql)
        tdSql.execute(sql)

        # split vgroup
        self.expectSplitOk(dbName)


    # forbid
    def checkForbid(self):
        # stream
        tdLog.info("check forbid split having stream...")
        tdSql.execute("create database streamdb;")
        tdSql.execute("use streamdb;")
        tdSql.execute("create table ta(ts timestamp, age int);")
        tdSql.execute("create stream ma into sta as select count(*) from ta interval(1s);")
        self.expectSplitError("streamdb")
        tdSql.execute("drop stream ma;")
        self.expectSplitOk("streamdb")

        # topic
        tdLog.info("check forbid split having topic...")
        tdSql.execute("create database topicdb wal_retention_period 10;")
        tdSql.execute("use topicdb;")
        tdSql.execute("create table ta(ts timestamp, age int);")
        tdSql.execute("create topic toa as select * from ta;")
        #self.expectSplitError("topicdb")
        tdSql.execute("drop topic toa;")
        self.expectSplitOk("topicdb")
   
    # compact and check db2
    def compactAndCheck(self):
        tdLog.info("compact db2 and check result ...")
        # compact
        tdSql.execute(f"compact database {self.db2};")
        # check result
        self.checkResult()

    # run
    def run(self):
        # prepare env
        self.prepareEnv()

        for i in range(2):
            # split vgroup on db2
            start = time.time()
            self.splitVGroup(self.db2)
            end = time.time()
            self.vgroups2 += 1
            
            # check two db query result same
            self.checkResult()
            spend = "%.3f"%(end-start)
            tdLog.info(f"split vgroup i={i} passed. spend = {spend}s")

        # split empty db
        self.splitEmptyDB()

        # check topic and stream forib
        self.checkForbid()

        # compact database
        self.compactAndCheck()

    # stop
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
