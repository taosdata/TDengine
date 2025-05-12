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
import json
import tempfile
import uuid

import frame.eos
import frame.etool
import frame.eutil
from frame.log import *
from frame.sql import *
from frame     import *
import frame

# test case base
class TBase:

#
#   frame call 
#          

    # init
    def init(self, conn, logSql, replicaVar=1, db="db", stb="stb", checkColName="ic"):
        
        # init
        self.childtable_count = 0
        self.insert_rows      = 0
        self.timestamp_step   = 0

        # save param
        self.replicaVar = int(replicaVar)
        tdSql.init(conn.cursor(), True)
        self.tmpdir = "tmp"

        # record server information
        self.dnodeNum = 0
        self.mnodeNum = 0
        self.mLevel = 0
        self.mLevelDisk = 0

        # test case information
        self.db     = db
        self.stb    = stb

        # sql 
        self.sqlSum = f"select sum({checkColName}) from {db}.{self.stb}"
        self.sqlMax = f"select max({checkColName}) from {db}.{self.stb}"
        self.sqlMin = f"select min({checkColName}) from {db}.{self.stb}"
        self.sqlAvg = f"select avg({checkColName}) from {db}.{self.stb}"
        self.sqlFirst = f"select first(ts) from {db}.{self.stb}"
        self.sqlLast  = f"select last(ts) from {db}.{self.stb}"

    # stop
    def stop(self):
        tdSql.close()

    def createDb(self, options=""):
        sql = f"create database {self.db} {options}"
        tdSql.execute(sql, show=True)

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
    def checkInsertCorrect(self, difCnt = 0):
        # check count
        sql = f"select count(*) from {self.db}.{self.stb}"
        tdSql.checkAgg(sql, self.childtable_count * self.insert_rows)

        # check child table count
        sql = f" select count(*) from (select count(*) as cnt , tbname from {self.db}.{self.stb} group by tbname) where cnt = {self.insert_rows} "
        tdSql.checkAgg(sql, self.childtable_count)

        # check step
        sql = f"select count(*) from (select diff(ts) as dif from {self.db}.{self.stb} partition by tbname order by ts desc) where dif != {self.timestamp_step}"
        tdSql.checkAgg(sql, difCnt)

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
        sql = f"select max({col}) from {self.db}.{self.stb}"
        expect = tdSql.getFirstValue(sql)
        sql = f"select top({col}, 5) from {self.db}.{self.stb}"
        tdSql.checkFirstValue(sql, expect)

        #bottom with min
        sql = f"select min({col}) from {self.db}.{self.stb}"
        expect = tdSql.getFirstValue(sql)
        sql = f"select bottom({col}, 5) from {self.db}.{self.stb}"
        tdSql.checkFirstValue(sql, expect)

        # order by asc limit 1 with first
        sql = f"select last({col}) from {self.db}.{self.stb}"
        expect = tdSql.getFirstValue(sql)
        sql = f"select {col} from {self.db}.{self.stb} order by _c0 desc limit 1"
        tdSql.checkFirstValue(sql, expect)

        # order by desc limit 1 with last
        sql = f"select first({col}) from {self.db}.{self.stb}"
        expect = tdSql.getFirstValue(sql)
        sql = f"select {col} from {self.db}.{self.stb} order by _c0 asc limit 1"
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

    # check same value
    def checkSame(self, real, expect, show = True):
        if real == expect:
            if show:
                tdLog.info(f"check same succ. real={real} expect={expect}.")
        else:
            tdLog.exit(f"check same failed. real={real} expect={expect}.")

    # check except
    def checkExcept(self, command):
        try:
            code = frame.eos.exe(command, show = True)
            if code == 0:
                tdLog.exit(f"Failed, not report error cmd:{command}")
            else:
                tdLog.info(f"Passed, report error code={code} is expect, cmd:{command}")
        except:
            tdLog.info(f"Passed, catch expect report error for command {command}")

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
#  run bin file
#
    # taos
    def taos(self, command, show = True, checkRun = False):
        return frame.etool.runBinFile("taos", command, show, checkRun)

    def taosdump(self, command, show = True, checkRun = True, retFail = True):
        return frame.etool.runBinFile("taosdump", command, show, checkRun, retFail)

    def benchmark(self, command, show = True, checkRun = True, retFail = True):
        return frame.etool.runBinFile("taosBenchmark", command, show, checkRun, retFail)
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
    def waitCompactsZero(self, seconds = 300, interval = 1):
        # wait end
        for i in range(seconds):
            sql ="show compacts;"
            rows = tdSql.query(sql)
            if rows == 0:
                tdLog.info("compacts count became zero.")
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


    # check list have str
    def checkListString(self, rlist, s):
        if s is None:
            return 
        for i in range(len(rlist)):
            if rlist[i].find(s) != -1:
                # found
                tdLog.info(f'found "{s}" on index {i} , line={rlist[i]}')
                return 

        # not found
        
        i = 1
        for x in rlist:
            print(f"{i} {x}")
            i += 1
        tdLog.exit(f'faild, not found "{s}" on above')
    
    # check many string
    def checkManyString(self, rlist, manys):
        for s in manys:
            self.checkListString(rlist, s)

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

#
#  taosBenchmark 
#
    
    # insert
    def insertBenchJson(self, jsonFile, options="", checkStep=False):
        # exe insert 
        cmd = f"{options} -f {jsonFile}"        
        frame.etool.benchMark(command = cmd)

        #
        # check insert result
        #
        with open(jsonFile, "r") as file:
            data = json.load(file)
        
        db  = data["databases"][0]["dbinfo"]["name"]        
        stb = data["databases"][0]["super_tables"][0]["name"]
        child_count = data["databases"][0]["super_tables"][0]["childtable_count"]
        insert_rows = data["databases"][0]["super_tables"][0]["insert_rows"]
        timestamp_step = data["databases"][0]["super_tables"][0]["timestamp_step"]
        
        # drop
        try:
            drop = data["databases"][0]["dbinfo"]["drop"]
        except:
            drop = "yes"

        # command is first
        if options.find("-Q") != -1:
            drop = "no"

        # cachemodel
        try:
            cachemode = data["databases"][0]["dbinfo"]["cachemodel"]
        except:
            cachemode = None

        # vgropus
        try:
            vgroups   = data["databases"][0]["dbinfo"]["vgroups"]
        except:
            vgroups = None

        tdLog.info(f"get json info: db={db} stb={stb} child_count={child_count} insert_rows={insert_rows} \n")
        
        # all count insert_rows * child_table_count
        sql = f"select * from {db}.{stb}"
        tdSql.query(sql)
        tdSql.checkRows(child_count * insert_rows)

        # timestamp step
        if checkStep:
            sql = f"select * from (select diff(ts) as dif from {db}.{stb} partition by tbname) where dif != {timestamp_step};"
            tdSql.query(sql)
            tdSql.checkRows(0)

        if drop.lower() == "yes":
            # check database optins 
            sql = f"select `vgroups`,`cachemodel` from information_schema.ins_databases where name='{db}';"
            tdSql.query(sql)

            if cachemode != None:
                
                value = frame.eutil.removeQuota(cachemode)                
                tdLog.info(f" deal both origin={cachemode} after={value}")
                tdSql.checkData(0, 1, value)

            if vgroups != None:
                tdSql.checkData(0, 0, vgroups)

        return db, stb, child_count, insert_rows
    
    # insert & check
    def benchInsert(self, jsonFile, options = "", results = None):
        # exe insert 
        benchmark = frame.etool.benchMarkFile()
        cmd   = f"{benchmark} {options} -f {jsonFile}"
        rlist = frame.eos.runRetList(cmd, True, True, True)
        if results != None:
            for result in results:
                self.checkListString(rlist, result)
        
        # open json
        with open(jsonFile, "r") as file:
            data = json.load(file)
        
        # read json
        dbs = data["databases"]
        for db in dbs:
            dbName = db["dbinfo"]["name"]        
            stbs   = db["super_tables"]
            for stb in stbs:
                stbName        = stb["name"]
                child_count    = stb["childtable_count"]
                insert_rows    = stb["insert_rows"]
                timestamp_step = stb["timestamp_step"]

                # check result

                # count
                sql = f"select count(*) from {dbName}.{stbName}"
                tdSql.checkAgg(sql, child_count * insert_rows)
                # diff
                sql = f"select * from (select diff(ts) as dif from {dbName}.{stbName} partition by tbname) where dif != {timestamp_step};"
                tdSql.query(sql)
                tdSql.checkRows(0)
                # show 
                tdLog.info(f"insert check passed. db:{dbName} stb:{stbName} child_count:{child_count} insert_rows:{insert_rows}\n")

    # tmq
    def tmqBenchJson(self, jsonFile, options="", checkStep=False):
        # exe insert 
        command = f"{options} -f {jsonFile}"
        rlist = frame.etool.runBinFile("taosBenchmark", command, checkRun = True)

        #
        # check insert result
        #
        print(rlist)

        return rlist

    # cmd
    def benchmarkCmd(self, options, childCnt, insertRows, timeStep, results):
        # set
        self.childtable_count = childCnt
        self.insert_rows      = insertRows
        self.timestamp_step   = timeStep

        # run
        cmd = f"{options} -t {childCnt} -n {insertRows} -S {timeStep} -y"
        rlist = self.benchmark(cmd)
        for result in results:
            self.checkListString(rlist, result)

        # check correct
        self.checkInsertCorrect()    


    # generate new json file
    def genNewJson(self, jsonFile, modifyFunc=None):
        try:
            with open(jsonFile, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except FileNotFoundError:
            tdLog.info(f"the specified json file '{jsonFile}' was not found.")
            return None
        except Exception as e:
            tdLog.info(f"error reading the json file: {e}")
            return None
        
        if callable(modifyFunc):
            modifyFunc(data)
        
        tempDir = os.path.join(tempfile.gettempdir(), 'json_templates')
        try:
            os.makedirs(tempDir, exist_ok=True)
        except PermissionError:
            tdLog.info(f"no sufficient permissions to create directory at '{tempDir}'.")
            return None
        except Exception as e:
            tdLog.info(f"error creating temporary directory: {e}")
            return None
        
        tempPath = os.path.join(tempDir, f"temp_{uuid.uuid4().hex}.json")

        try:
            with open(tempPath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            tdLog.info(f"error writing to temporary json file: {e}")
            return None

        tdLog.info(f"create temporary json file successfully, file: {tempPath}")
        return tempPath

    # delete file
    def deleteFile(self, filename):
        try:
            if os.path.exists(filename):
                os.remove(filename)
        except Exception as err:
            raise Exception(err)

    # read file to list
    def readFileToList(self, filePath):
        try:
            with open(filePath, 'r', encoding='utf-8') as file:
                lines = file.readlines()
            # Strip trailing newline characters
            return [line.rstrip('\n') for line in lines]
        except FileNotFoundError:
            tdLog.info(f"Error: File not found {filePath}")
            return []
        except Exception as e:
            tdLog.info(f"Error reading file: {e}")
            return []
