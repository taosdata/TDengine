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
import math
from asyncore import loop
from collections import defaultdict
import subprocess
import random
import string
import threading
import requests
import time
# import socketfrom
import json
import toml

import taos
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *

# class actionType(Enum):
#     CREATE_DATABASE = 0
#     CREATE_STABLE   = 1
#     CREATE_CTABLE   = 2
#     INSERT_DATA     = 3

class TMQCom:
    def __init__(self):
        self.g_end_insert_flag = 0
    
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdSql.init(conn.cursor())
        # tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def initConsumerTable(self,cdbName='cdb', replicaVar=1):
        tdLog.info("create consume database, and consume info table, and consume result table")
        tdSql.query("create database if not exists %s vgroups 1 replica %d"%(cdbName,replicaVar))
        tdSql.query("drop table if exists %s.consumeinfo "%(cdbName))
        tdSql.query("drop table if exists %s.consumeresult "%(cdbName))
        tdSql.query("drop table if exists %s.notifyinfo "%(cdbName))

        tdSql.query("create table %s.consumeinfo (ts timestamp, consumerid int, topiclist binary(1024), keylist binary(1024), expectmsgcnt bigint, ifcheckdata int, ifmanualcommit int)"%cdbName)
        tdSql.query("create table %s.consumeresult (ts timestamp, consumerid int, consummsgcnt bigint, consumrowcnt bigint, checkresult int)"%cdbName)
        tdSql.query("create table %s.notifyinfo (ts timestamp, cmdid int, consumerid int)"%cdbName)

    def initConsumerInfoTable(self,cdbName='cdb'):
        tdLog.info("drop consumeinfo table")
        tdSql.query("drop table if exists %s.consumeinfo "%(cdbName))
        tdSql.query("create table %s.consumeinfo (ts timestamp, consumerid int, topiclist binary(1024), keylist binary(1024), expectmsgcnt bigint, ifcheckdata int, ifmanualcommit int)"%cdbName)

    def insertConsumerInfo(self,consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifmanualcommit,cdbName='cdb'):
        sql = "insert into %s.consumeinfo values "%cdbName
        sql += "(now + %ds, %d, '%s', '%s', %d, %d, %d)"%(consumerId, consumerId, topicList, keyList, expectrowcnt, ifcheckdata, ifmanualcommit)
        tdLog.info("consume info sql: %s"%sql)
        tdSql.query(sql)

    def selectConsumeResult(self,expectRows,cdbName='cdb'):
        resultList=[]
        while 1:
            tdSql.query("select * from %s.consumeresult"%cdbName)
            #tdLog.info("row: %d, %l64d, %l64d"%(tdSql.getData(0, 1),tdSql.getData(0, 2),tdSql.getData(0, 3))
            if tdSql.getRows() == expectRows:
                break
            else:
                time.sleep(0.5)

        for i in range(expectRows):
            tdLog.info ("consume id: %d, consume msgs: %d, consume rows: %d"%(tdSql.getData(i , 1), tdSql.getData(i , 2), tdSql.getData(i , 3)))
            resultList.append(tdSql.getData(i , 3))

        return resultList

    def selectConsumeMsgResult(self,expectRows,cdbName='cdb'):
        resultList=[]
        while 1:
            tdSql.query("select * from %s.consumeresult"%cdbName)
            #tdLog.info("row: %d, %l64d, %l64d"%(tdSql.getData(0, 1),tdSql.getData(0, 2),tdSql.getData(0, 3))
            if tdSql.getRows() == expectRows:
                break
            else:
                time.sleep(5)

        for i in range(expectRows):
            tdLog.info ("consume id: %d, consume msgs: %d, consume rows: %d"%(tdSql.getData(i , 1), tdSql.getData(i , 2), tdSql.getData(i , 3)))
            resultList.append(tdSql.getData(i , 2))

        return resultList

    def startTmqSimProcess(self,pollDelay,dbName,showMsg=1,showRow=1,cdbName='cdb',valgrind=0,alias=0,snapshot=0):
        buildPath = tdCom.getBuildPath()
        cfgPath = tdCom.getClientCfgPath()
        if valgrind == 1:
            logFile = cfgPath + '/../log/valgrind-tmq.log'
            shellCmd = 'nohup valgrind --log-file=' + logFile
            shellCmd += '--tool=memcheck --leak-check=full --show-reachable=no --track-origins=yes --show-leak-kinds=all --num-callers=20 -v --workaround-gcc296-bugs=yes '

        if (platform.system().lower() == 'windows'):
            processorName = buildPath + '\\build\\bin\\tmq_sim.exe'
            if alias != 0:
                processorNameNew = buildPath + '\\build\\bin\\tmq_sim_new.exe'
                shellCmd = 'cp %s %s'%(processorName, processorNameNew)
                os.system(shellCmd)
                processorName = processorNameNew
            shellCmd = 'mintty -h never ' + processorName + ' -c ' + cfgPath
            shellCmd += " -y %d -d %s -g %d -r %d -w %s -e %d "%(pollDelay, dbName, showMsg, showRow, cdbName, snapshot)
            shellCmd += "> nul 2>&1 &"
        else:
            processorName = buildPath + '/build/bin/tmq_sim'
            if alias != 0:
                processorNameNew = buildPath + '/build/bin/tmq_sim_new'
                shellCmd = 'cp %s %s'%(processorName, processorNameNew)
                os.system(shellCmd)
                processorName = processorNameNew
            shellCmd = 'nohup ' + processorName + ' -c ' + cfgPath
            shellCmd += " -y %d -d %s -g %d -r %d -w %s -e %d "%(pollDelay, dbName, showMsg, showRow, cdbName, snapshot)
            shellCmd += "> /dev/null 2>&1 &"
        tdLog.info(shellCmd)
        os.system(shellCmd)

    def stopTmqSimProcess(self, processorName):
        psCmd = "unset LD_PRELOAD; ps -ef|grep -w %s|grep -v grep | awk '{print $2}'"%(processorName)
        if platform.system().lower() == 'windows':
            psCmd = "ps -ef|grep -w %s|grep -v grep | awk '{print $2}'"%(processorName)
        processID = subprocess.check_output(psCmd, shell=True).decode("utf-8")
        onlyKillOnceWindows = 0
        while(processID):
            if not platform.system().lower() == 'windows' or (onlyKillOnceWindows == 0 and platform.system().lower() == 'windows'):
                if platform.system().lower() == 'windows':
                    killCmd = "kill -INT %s > /dev/nul 2>&1" % processID
                else:
                    killCmd = "unset LD_PRELOAD; kill -INT %s > /dev/null 2>&1" % processID
                os.system(killCmd)
                onlyKillOnceWindows = 1
            time.sleep(0.2)
            processID = subprocess.check_output(psCmd, shell=True).decode("utf-8")
        tdLog.debug("%s is stopped by kill -INT" % (processorName))

    def getStartConsumeNotifyFromTmqsim(self,cdbName='cdb',rows=1):
        loopFlag = 1
        while loopFlag:
            tdSql.query("select * from %s.notifyinfo where cmdid = 0"%cdbName)
            actRows = tdSql.getRows()
            tdLog.info("row: %d"%(actRows))
            if (actRows >= rows):
                    loopFlag = 0
            time.sleep(0.5)
        return

    def getStartCommitNotifyFromTmqsim(self,cdbName='cdb',rows=1):
        loopFlag = 1
        while loopFlag:
            tdSql.query("select * from %s.notifyinfo where cmdid = 1"%cdbName)
            actRows = tdSql.getRows()
            tdLog.info("row: %d"%(actRows))
            if (actRows >= rows):
                loopFlag = 0
            time.sleep(0.5)
        return

    def create_database(self,tsql, dbName,dropFlag=1,vgroups=4,replica=1):
        if dropFlag == 1:
            tsql.execute("drop database if exists %s"%(dbName))

        tsql.execute("create database if not exists %s vgroups %d replica %d"%(dbName, vgroups, replica))
        tdLog.debug("complete to create database %s"%(dbName))
        return

    # self.create_stable() and self.create_ctable() and self.insert_data_interlaceByMultiTbl() : The three functions are matched
    # schema: (ts timestamp, c1 int, c2 bigint, c3 double, c4 binary(32), c5 nchar(32), c6 timestamp) tags (t1 int, t2 bigint, t3 double, t4 binary(32), t5 nchar(32))
    def create_stable(self,tsql, dbName,stbName):
        schemaString = "(ts timestamp, c1 int, c2 bigint, c3 double, c4 binary(32), c5 nchar(32), c6 timestamp) tags (t1 int, t2 bigint, t3 double, t4 binary(32), t5 nchar(32))"
        tsql.execute("create table if not exists %s.%s %s"%(dbName, stbName, schemaString))
        tdLog.debug("complete to create %s.%s" %(dbName, stbName))
        return

    def create_ctable(self,tsql=None, dbName='dbx',stbName='stb',ctbPrefix='ctb',ctbNum=1,ctbStartIdx=0):
        # tsql.execute("use %s" %dbName)
        pre_create = "create table"
        sql = pre_create
        #tdLog.debug("doing create one  stable %s and %d  child table in %s  ..." %(stbname, count ,dbname))
        batchNum = 10
        tblBatched  = 0
        for i in range(ctbNum):
            tagBinaryValue = 'beijing'
            if (i % 2 == 0):
                tagBinaryValue = 'shanghai'
            elif (i % 3 == 0):
                tagBinaryValue = 'changsha'

            sql += " %s.%s%d using %s.%s tags(%d, %d, %d, '%s', '%s')"%(dbName,ctbPrefix,i+ctbStartIdx,dbName,stbName,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx,tagBinaryValue,tagBinaryValue)
            tblBatched += 1
            if (i == ctbNum-1 ) or (tblBatched == batchNum):
                tsql.execute(sql)
                tblBatched = 0
                sql = pre_create

        if sql != pre_create:
            tsql.execute(sql)

        tdLog.debug("complete to create %d child tables by %s.%s" %(ctbNum, dbName, stbName))
        return

    def drop_ctable(self, tsql, dbname=None, count=1, default_ctbname_prefix="ctb",ctbStartIdx=0):
        for _ in range(count):
            create_ctable_sql = f'drop table if exists {dbname}.{default_ctbname_prefix}{ctbStartIdx};'
            ctbStartIdx += 1
            tdLog.info("drop ctb sql: %s"%create_ctable_sql)
            tsql.execute(create_ctable_sql)

    # schema: (ts timestamp, c1 int, c2 binary(16))
    def insert_data(self,tsql,dbName,stbName,ctbNum,rowsPerTbl,batchNum,startTs=None):
        tdLog.debug("start to insert data ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        if startTs is None:
            t = time.time()
            startTs = int(round(t * 1000))
        #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        for i in range(ctbNum):
            rowsBatched = 0
            sql += " %s.%s%d values "%(dbName, stbName, i)
            for j in range(rowsPerTbl):
                sql += "(%d, %d, 'tmqrow_%d') "%(startTs + j, j, j)
                rowsBatched += 1
                if ((rowsBatched == batchNum) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    rowsBatched = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s.%s%d values " %(dbName, stbName,i)
                    else:
                        sql = "insert into "
        #end sql
        if sql != pre_insert:
            #print("insert sql:%s"%sql)
            tsql.execute(sql)
        tdLog.debug("insert data ............ [OK]")
        return

    # schema: (ts timestamp, c1 int, c2 int, c3 binary(16))
    def insert_data_1(self,tsql,dbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs):
        tdLog.debug("start to insert data ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        t = time.time()
        startTs = int(round(t * 1000))
        #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        for i in range(ctbNum):
            rowsBatched = 0
            sql += " %s.%s%d values "%(dbName, ctbPrefix,i)
            for j in range(rowsPerTbl):
                if (j % 2 == 0):
                    sql += "(%d, %d, %d, 'tmqrow_%d') "%(startTs + j, j, j, j)
                else:
                    sql += "(%d, %d, %d, 'tmqrow_%d') "%(startTs + j, j, -j, j)
                rowsBatched += 1
                if ((rowsBatched == batchNum) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    rowsBatched = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s.%s%d values " %(dbName, ctbPrefix, i)
                    else:
                        sql = "insert into "
        #end sql
        if sql != pre_insert:
            #print("insert sql:%s"%sql)
            tsql.execute(sql)
        tdLog.debug("insert data ............ [OK]")
        return

    # schema: (ts timestamp, c1 int, c2 int, c3 binary(16), c4 timestamp)
    def insert_data_2(self,tsql,dbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs,ctbStartIdx=0):
        tdLog.debug("start to insert data ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        t = time.time()
        startTs = int(round(t * 1000))
        #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        for i in range(ctbNum):
            rowsBatched = 0
            sql += " %s.%s%d values "%(dbName, ctbPrefix, i+ctbStartIdx)
            for j in range(rowsPerTbl):
                if (j % 2 == 0):
                    sql += "(%d, %d, %d, 'tmqrow_%d', now) "%(startTs + j, j, j, j)
                else:
                    sql += "(%d, %d, %d, 'tmqrow_%d', now) "%(startTs + j, j, -j, j)
                rowsBatched += 1
                if (rowsBatched == batchNum) or (j == rowsPerTbl - 1):
                    tsql.execute(sql)
                    rowsBatched = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s.%s%d values " %(dbName, ctbPrefix, i+ctbStartIdx)
                    else:
                        sql = "insert into "
        #end sql
        if sql != pre_insert:
            #print("insert sql:%s"%sql)
            tsql.execute(sql)
        tdLog.debug("insert data ............ [OK]")
        return

    # schema: (ts timestamp, c1 int, c2 bigint, c3 double, c4 binary(32), c5 nchar(32), c6 timestamp) tags (t1 int, t2 bigint, t3 double, t4 binary(32), t5 nchar(32))
    def insert_data_interlaceByMultiTbl(self,tsql,dbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs=0,ctbStartIdx=0):
        tdLog.debug("start to insert data ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        if startTs == 0:
            t = time.time()
            startTs = int(round(t * 1000))

        ctbDict = {}
        for i in range(ctbNum):
            ctbDict[i] = 0

        #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        rowsOfCtb = 0        
        while rowsOfCtb < rowsPerTbl:
            if (0 != self.g_end_insert_flag):
                tdLog.debug("get signal to stop insert data")
                break
            for i in range(ctbNum):
                sql += " %s.%s%d values "%(dbName,ctbPrefix,i+ctbStartIdx)
                rowsBatched = 0
                for k in range(batchNum):
                    if (k % 2 == 0):
                        sql += "(%d, %d, %d, %d, 'binary_%d', 'nchar_%d', now) "%(startTs+ctbDict[i], ctbDict[i],ctbDict[i], ctbDict[i],i+ctbStartIdx,k)
                    else:
                        sql += "(%d, %d, %d, %d, 'binary_%d', 'nchar_%d', now) "%(startTs+ctbDict[i],-ctbDict[i],ctbDict[i],-ctbDict[i],i+ctbStartIdx,k)

                    rowsBatched += 1
                    ctbDict[i] += 1
                    if (rowsBatched == batchNum) or (ctbDict[i] == rowsPerTbl):
                        tsql.execute(sql)
                        rowsBatched = 0
                        sql = "insert into "
                        break
            rowsOfCtb = ctbDict[0]

        tdLog.debug("insert data ............ [OK]")
        return

    def threadFunctionForInsertByInterlace(self, **paraDict):
        # create new connector for new tdSql instance in my thread
        newTdSql = tdCom.newTdSql()
        self.insert_data_interlaceByMultiTbl(newTdSql,paraDict["dbName"],paraDict["ctbPrefix"],paraDict["ctbNum"],paraDict["rowsPerTbl"],paraDict["batchNum"],paraDict["startTs"],paraDict["ctbStartIdx"])
        return

    def asyncInsertDataByInterlace(self, paraDict):
        pThread = threading.Thread(target=self.threadFunctionForInsertByInterlace, kwargs=paraDict)
        pThread.start()
        return pThread

    def insert_data_with_autoCreateTbl(self,tsql,dbName,stbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs=0,ctbStartIdx=0):
        tdLog.debug("start to insert data with auto create child table ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        if startTs == 0:
            t = time.time()
            startTs = int(round(t * 1000))

        #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        rowsBatched = 0
        for i in range(ctbNum):
            tagBinaryValue = 'beijing'
            if (i % 2 == 0):
                tagBinaryValue = 'shanghai'
            elif (i % 3 == 0):
                tagBinaryValue = 'changsha'

            sql += " %s.%s%d using %s.%s tags (%d, %d, %d, '%s', '%s') values "%(dbName,ctbPrefix,i+ctbStartIdx,dbName,stbName,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx,tagBinaryValue,tagBinaryValue)
            for j in range(rowsPerTbl):
                sql += "(%d, %d, %d, %d, 'binary_%d', 'nchar_%d', now) "%(startTs+j, j,j, j,i+ctbStartIdx,rowsBatched)
                rowsBatched += 1
                if ((rowsBatched == batchNum) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    rowsBatched = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s.%s%d using %s.%s tags (%d, %d, %d, '%s', '%s') values " %(dbName,ctbPrefix,i+ctbStartIdx,dbName,stbName,i+ctbStartIdx,i+ctbStartIdx,i+ctbStartIdx,tagBinaryValue,tagBinaryValue)
                    else:
                        sql = "insert into "
        #end sql
        if sql != pre_insert:
            #print("insert sql:%s"%sql)
            tsql.execute(sql)
        tdLog.debug("insert data ............ [OK]")
        return

    def syncCreateDbStbCtbInsertData(self, tsql, paraDict):
        tdCom.create_database(tsql, paraDict["dbName"],paraDict["dropFlag"])
        tdCom.create_stable(tsql, dbname=paraDict["dbName"],stbname=paraDict["stbName"], column_elm_list=paraDict['colSchema'], tag_elm_list=paraDict['tagSchema'])
        tdCom.create_ctable(tsql, dbname=paraDict["dbName"],stbname=paraDict["stbName"],tag_elm_list=paraDict['tagSchema'],count=paraDict["ctbNum"], default_ctbname_prefix=paraDict['ctbPrefix'])
        if "event" in paraDict and type(paraDict['event']) == type(threading.Event()):
            paraDict["event"].set()

        ctbPrefix = paraDict['ctbPrefix']
        ctbNum = paraDict["ctbNum"]
        for i in range(ctbNum):
            tbName = '%s%s'%(ctbPrefix,i)
            tdCom.insert_rows(tsql,dbname=paraDict["dbName"],tbname=tbName,start_ts_value=paraDict['startTs'],count=paraDict['rowsPerTbl'])
        return

    def threadFunction(self, **paraDict):
        # create new connector for new tdSql instance in my thread
        newTdSql = tdCom.newTdSql()
        self.syncCreateDbStbCtbInsertData(self, newTdSql, paraDict)
        return

    def asyncCreateDbStbCtbInsertData(self, paraDict):
        pThread = threading.Thread(target=self.threadFunction, kwargs=paraDict)
        pThread.start()
        return pThread

    def threadFunctionForInsert(self, **paraDict):
        # create new connector for new tdSql instance in my thread
        newTdSql = tdCom.newTdSql()
        if 'ctbStartIdx' in paraDict.keys():
            self.insert_data_2(newTdSql,paraDict["dbName"],paraDict["ctbPrefix"],paraDict["ctbNum"],paraDict["rowsPerTbl"],paraDict["batchNum"],paraDict["startTs"],paraDict["ctbStartIdx"])
        else:
            self.insert_data_2(newTdSql,paraDict["dbName"],paraDict["ctbPrefix"],paraDict["ctbNum"],paraDict["rowsPerTbl"],paraDict["batchNum"],paraDict["startTs"])
        return

    def asyncInsertData(self, paraDict):
        pThread = threading.Thread(target=self.threadFunctionForInsert, kwargs=paraDict)
        pThread.start()
        return pThread

    def checkFileContent(self, consumerId, queryString, skipRowsOfCons=0):
        buildPath = tdCom.getBuildPath()
        cfgPath = tdCom.getClientCfgPath()
        dstFile = '%s/../log/dstrows_%d.txt'%(cfgPath, consumerId)
        cmdStr = '%s/build/bin/taos -c %s -s "%s >> %s"'%(buildPath, cfgPath, queryString, dstFile)
        tdLog.info(cmdStr)
        os.system(cmdStr)

        consumeRowsFile = '%s/../log/consumerid_%d.txt'%(cfgPath, consumerId)
        tdLog.info("rows file: %s, %s"%(consumeRowsFile, dstFile))

        consumeFile = open(consumeRowsFile, mode='r')
        queryFile = open(dstFile, mode='r')

        # skip first line for it is schema
        queryFile.readline()

        # skip offset for consumer
        for i in range(0,skipRowsOfCons):
            consumeFile.readline()

        while True:
            dst = queryFile.readline()
            src = consumeFile.readline()
            dstSplit = dst.split(',')
            srcSplit = src.split(',')

            if not dst or not src:
                break
            if len(dstSplit) != len(srcSplit):
                tdLog.exit("consumerId %d consume rows len is not match the rows by direct query,len(dstSplit):%d != len(srcSplit):%d, dst:%s, src:%s"
                           %(consumerId, len(dstSplit), len(srcSplit), dst, src))

            for i in range(len(dstSplit)):
                if srcSplit[i] != dstSplit[i]:
                    srcFloat = float(srcSplit[i])
                    dstFloat = float(dstSplit[i])
                    if not math.isclose(srcFloat, dstFloat, abs_tol=1e-9):
                        tdLog.exit("consumerId %d consume rows is not match the rows by direct query"%consumerId)
        return

    def getResultFileByTaosShell(self, consumerId, queryString):
        buildPath = tdCom.getBuildPath()
        cfgPath = tdCom.getClientCfgPath()
        dstFile = '%s/../log/dstrows_%d.txt'%(cfgPath, consumerId)
        cmdStr = '%s/build/bin/taos -c %s -s "%s >> %s"'%(buildPath, cfgPath, queryString, dstFile)
        tdLog.info(cmdStr)
        os.system(cmdStr)
        return dstFile

    def checkTmqConsumeFileContent(self, consumerId, dstFile):
        cfgPath = tdCom.getClientCfgPath()
        consumeRowsFile = '%s/../log/consumerid_%d.txt'%(cfgPath, consumerId)
        tdLog.info("rows file: %s, %s"%(consumeRowsFile, dstFile))

        consumeFile = open(consumeRowsFile, mode='r')
        queryFile = open(dstFile, mode='r')

        # skip first line for it is schema
        queryFile.readline()
        lines = 0
        while True:
            dst = queryFile.readline()
            src = consumeFile.readline()
            lines += 1
            if dst:
                if dst != src:
                    tdLog.info("src row: %s"%src)
                    tdLog.info("dst row: %s"%dst)
                    tdLog.exit("consumerId %d consume rows[%d] is not match the rows by direct query"%(consumerId, lines))
            else:
                break
        return

    def create_ntable(self, tsql, dbname=None, tbname_prefix="ntb", tbname_index_start_num = 1, column_elm_list=None, colPrefix='c', tblNum=1, **kwargs):
        tb_params = ""
        if len(kwargs) > 0:
            for param, value in kwargs.items():
                tb_params += f'{param} "{value}" '
        column_type_str = tdCom.gen_column_type_str(colPrefix, column_elm_list)

        for _ in range(tblNum):
            create_table_sql = f'create table {dbname}.{tbname_prefix}{tbname_index_start_num} ({column_type_str}) {tb_params};'
            tbname_index_start_num += 1
            tsql.execute(create_table_sql)

    def insert_rows_into_ntbl(self, tsql, dbname=None, tbname_prefix="ntb", tbname_index_start_num = 1, column_ele_list=None, startTs=None, tblNum=1, rows=1):
        if startTs is None:
            startTs = tdCom.genTs()[0]

        for tblIdx in range(tblNum):
            for rowIdx in range(rows):
                column_value_list = tdCom.gen_column_value_list(column_ele_list, f'{startTs}+{rowIdx}s')
                column_value_str = ''
                idx = 0
                for column_value in column_value_list:
                    if isinstance(column_value, str) and idx != 0:
                        column_value_str += f'"{column_value}", '
                    else:
                        column_value_str += f'{column_value}, '
                        idx += 1
                column_value_str = column_value_str.rstrip()[:-1]
                insert_sql = f'insert into {dbname}.{tbname_prefix}{tblIdx+tbname_index_start_num} values ({column_value_str});'
                tsql.execute(insert_sql)

    def waitSubscriptionExit(self, tsql, topicName):
        wait_cnt = 0
        while True:
            exit_flag = 1
            tsql.query("show subscriptions")
            rows = tsql.getRows()
            for idx in range (rows):
                if tsql.getData(idx, 0) != topicName:
                    continue

                if tsql.getData(idx, 3) == None:
                    continue
                else:
                    time.sleep(0.5)
                    wait_cnt += 1
                    exit_flag = 0
                    break

            if exit_flag == 1:
                break

        tsql.query("show subscriptions")
        tdLog.info("show subscriptions:")
        tdLog.info(tsql.queryResult)
        tdLog.info("wait subscriptions exit for %d s"%wait_cnt)

    def killProcesser(self, processerName):
        if platform.system().lower() == 'windows':
            killCmd = ("wmic process where name=\"%s.exe\" call terminate > NUL 2>&1" % processerName)
            psCmd = ("wmic process where name=\"%s.exe\" | findstr \"%s.exe\"" % (processerName, processerName))
        else:
            killCmd = (
                "ps -ef|grep -w %s| grep -v grep | awk '{print $2}' | xargs kill -TERM > /dev/null 2>&1"
                % processerName
            )
            psCmd = ("ps -ef|grep -w %s| grep -v grep | awk '{print $2}'" % processerName)

        processID = ""
        
        try: 
            processID = subprocess.check_output(psCmd, shell=True)
        except Exception as err:
            processID = ""
            print('**** warn: ', err)        
        
        while processID:
            os.system(killCmd)
            time.sleep(1)
            try: 
                processID = subprocess.check_output(psCmd, shell=True)
            except Exception as err:
                processID = ""
                print('**** warn: ', err)
                
    def startProcess(self, processName, param):
        if platform.system().lower() == 'windows':
            cmd = f"mintty -h never %s %s > NUL 2>&1" % (processName, param)
        else:
            cmd = f"nohup %s %s > /dev/null 2>&1 &" % (processName, param)
        tdLog.info("%s"%(cmd))
        os.system(cmd)                

    def close(self):
        self.cursor.close()

tmqCom = TMQCom()
