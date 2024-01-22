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
import os
import time
import datetime
import random
import copy

from frame.log import *
from frame.sql import *

# test case base
class TBase:

#
#   frame call 
#          

    # init
    def init(self, conn, logSql, replicaVar=1, db="db", stb="stb", checkColName="ic"):
        # save param
        self.replicaVar = int(replicaVar)
        tdSql.init(conn.cursor(), True)

        # record server information
        self.dnodeNum = 0
        self.mnodeNum = 0
        self.mLevel = 0
        self.mLevelDisk = 0

        # test case information
        self.db     = db
        self.stb    = stb

        # sql 
        self.sqlSum = f"select sum({checkColName}) from {self.stb}"
        self.sqlMax = f"select max({checkColName}) from {self.stb}"
        self.sqlMin = f"select min({checkColName}) from {self.stb}"
        self.sqlAvg = f"select avg({checkColName}) from {self.stb}"
        self.sqlFirst = f"select first(ts) from {self.stb}"
        self.sqlLast  = f"select last(ts) from {self.stb}"

    # stop
    def stop(self):
        tdSql.close()


#
#   db action
#         

    def trimDb(self, show = False):
        tdSql.execute(f"trim database {self.db}", show = show)

    def compactDb(self, show = False):
        tdSql.execute(f"compact database {self.db}", show = show)

    def flushDb(self, show = False):
        tdSql.execute(f"flush database {self.db}", show = show)

    def dropDb(self, show = False):
        tdSql.execute(f"drop database {self.db}", show = show)

    def dropStream(self, sname, show = False):
        tdSql.execute(f"drop stream {sname}", show = show)

    def splitVGroups(self):
        vgids = self.getVGroup(self.db)
        selid = random.choice(vgids)
        sql = f"split vgroup {selid}"
        tdSql.execute(sql, show=True)
        if self.waitTransactionZero() is False:
            tdLog.exit(f"{sql} transaction not finished")
            return False
        return True
    

    def alterReplica(self, replica):
        sql = f"alter database {self.db} replica {replica}"
        tdSql.execute(sql, show=True)
        if self.waitTransactionZero() is False:
            tdLog.exit(f"{sql} transaction not finished")
            return False
        return True

    def balanceVGroup(self):
        sql = f"balance vgroup"
        tdSql.execute(sql, show=True)
        if self.waitTransactionZero() is False:
            tdLog.exit(f"{sql} transaction not finished")
            return False
        return True
    
    def balanceVGroupLeader(self):
        sql = f"balance vgroup leader"
        tdSql.execute(sql, show=True)
        if self.waitTransactionZero() is False:
            tdLog.exit(f"{sql} transaction not finished")
            return False
        return True


    def balanceVGroupLeaderOn(self, vgId):
        sql = f"balance vgroup leader on {vgId}"
        tdSql.execute(sql, show=True)
        if self.waitTransactionZero() is False:
            tdLog.exit(f"{sql} transaction not finished")
            return False
        return True


#
#  check db correct
#                

    # basic
    def checkInsertCorrect(self):
        # check count
        sql = f"select count(*) from {self.stb}"
        tdSql.checkAgg(sql, self.childtable_count * self.insert_rows)

        # check child table count
        sql = f" select count(*) from (select count(*) as cnt , tbname from {self.stb} group by tbname) where cnt = {self.insert_rows} "
        tdSql.checkAgg(sql, self.childtable_count)

        # check step
        sql = f"select * from (select diff(ts) as dif from {self.stb} partition by tbname order by ts desc) where dif != {self.timestamp_step}"
        tdSql.query(sql)
        tdSql.checkRows(0)

    # save agg result
    def snapshotAgg(self):        
        self.sum =  tdSql.getFirstValue(self.sqlSum)
        self.avg =  tdSql.getFirstValue(self.sqlAvg)
        self.min =  tdSql.getFirstValue(self.sqlMin)
        self.max =  tdSql.getFirstValue(self.sqlMax)
        self.first = tdSql.getFirstValue(self.sqlFirst)
        self.last  = tdSql.getFirstValue(self.sqlLast)

    # check agg 
    def checkAggCorrect(self):
        tdSql.checkAgg(self.sqlSum, self.sum)
        tdSql.checkAgg(self.sqlAvg, self.avg)
        tdSql.checkAgg(self.sqlMin, self.min)
        tdSql.checkAgg(self.sqlMax, self.max)
        tdSql.checkAgg(self.sqlFirst, self.first)
        tdSql.checkAgg(self.sqlLast,  self.last)

    # self check 
    def checkConsistency(self, col):
        # top with max
        sql = f"select max({col}) from {self.stb}"
        expect = tdSql.getFirstValue(sql)
        sql = f"select top({col}, 5) from {self.stb}"
        tdSql.checkFirstValue(sql, expect)

        #bottom with min
        sql = f"select min({col}) from {self.stb}"
        expect = tdSql.getFirstValue(sql)
        sql = f"select bottom({col}, 5) from {self.stb}"
        tdSql.checkFirstValue(sql, expect)

        # order by asc limit 1 with first
        sql = f"select last({col}) from {self.stb}"
        expect = tdSql.getFirstValue(sql)
        sql = f"select {col} from {self.stb} order by _c0 desc limit 1"
        tdSql.checkFirstValue(sql, expect)

        # order by desc limit 1 with last
        sql = f"select first({col}) from {self.stb}"
        expect = tdSql.getFirstValue(sql)
        sql = f"select {col} from {self.stb} order by _c0 asc limit 1"
        tdSql.checkFirstValue(sql, expect)


    # check sql1 is same result with sql2
    def checkSameResult(self, sql1, sql2):
        tdLog.info(f"sql1={sql1}")
        tdLog.info(f"sql2={sql2}")
        tdLog.info("compare sql1 same with sql2 ...")

        # sql
        rows1 = tdSql.query(sql1,queryTimes=2)
        res1 = copy.deepcopy(tdSql.res)

        tdSql.query(sql2,queryTimes=2)
        res2 = tdSql.res

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
            tdLog.exit(f"sql2 column value different with sql1. different count ={errCnt} ")

        tdLog.info("sql1 same result with sql2.")

#
#   get db information
#

    # get vgroups
    def getVGroup(self, dbName):
        vgidList = []
        sql = f"select vgroup_id from information_schema.ins_vgroups where db_name='{dbName}'"
        res = tdSql.getResult(sql)
        rows = len(res)
        for i in range(rows):
            vgidList.append(res[i][0])

        return vgidList
    
    # get distributed rows
    def getDistributed(self, tbName):
        sql = f"show table distributed {tbName}"
        tdSql.query(sql)
        dics = {}
        i = 0
        for i in range(tdSql.getRows()):
            row = tdSql.getData(i, 0)
            #print(row)
            row = row.replace('[', '').replace(']', '')
            #print(row)
            items = row.split(' ')
            #print(items)
            for item in items:
                #print(item)
                v = item.split('=')
                #print(v)
                if len(v) == 2:
                    dics[v[0]] = v[1]
            if i > 5:
                break
        print(dics)
        return dics


#
#   util 
#
    
    # wait transactions count to zero , return False is translation not finished
    def waitTransactionZero(self, seconds = 300, interval = 1):
        # wait end
        for i in range(seconds):
            sql ="show transactions;"
            rows = tdSql.query(sql)
            if rows == 0:
                tdLog.info("transaction count became zero.")
                return True
            #tdLog.info(f"i={i} wait ...")
            time.sleep(interval)
        
        return False    

    # check file exist
    def checkFileExist(self, pathFile):
        if os.path.exists(pathFile) == False:
            tdLog.exit(f"file not exist {pathFile}")

    # check list not exist
    def checkListNotEmpty(self, lists, tips=""):
        if len(lists) == 0:
            tdLog.exit(f"list is empty {tips}")


#
#  str util
#
    # covert list to sql format string
    def listSql(self, lists, sepa = ","):
        strs = ""
        for ls in lists:
            if strs != "":
                strs += sepa
            strs += f"'{ls}'"
        return strs