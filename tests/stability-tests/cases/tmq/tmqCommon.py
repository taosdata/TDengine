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


from itertools import product
from itertools import combinations
from faker import Faker
import operator
import numpy as np
import pandas as pd
import time, datetime
import threading
# from taostest import TDSql
from taostest import TDCase
import subprocess
import os
import taos
from taostest.util.remote import Remote

class TMQCom():
    def __init__(self, tdSql, logger):
        self.tdSql =  tdSql
        self.logger = logger
        
        self._remote: Remote = Remote(self.logger)
        self._remote._logger.info("********")
        
        # self._run_log_dir, self._log_dir_name = self._get_run_log_dir()

    def desc(self) -> str:
        case_description = '''
        case1<lihui>: consume data from wal 
        ''' 
        return case_description

    def tags(self) :
		
        return ""

    def author(self) -> str:

        return "lihui"

    def initConsumerTable(self,cdbName='cdb'):        
        self.logger.info("create consume database, and consume info table, and consume result table")
        self.tdSql.query("create database if not exists %s vgroups 1"%(cdbName))
        self.tdSql.query("drop table if exists %s.consumeinfo "%(cdbName))
        self.tdSql.query("drop table if exists %s.consumeresult "%(cdbName))
        self.tdSql.query("drop table if exists %s.notifyinfo "%(cdbName))      

        self.tdSql.query("create table %s.consumeinfo (ts timestamp, consumerid int, topiclist binary(1024), keylist binary(1024), expectmsgcnt bigint, ifcheckdata int, ifmanualcommit int)"%cdbName)
        self.tdSql.query("create table %s.consumeresult (ts timestamp, consumerid int, consummsgcnt bigint, consumrowcnt bigint, checkresult int)"%cdbName)
        self.tdSql.query("create table %s.notifyinfo (ts timestamp, cmdid int, consumerid int)"%cdbName)

    def initConsumerInfoTable(self,cdbName='cdb'):        
        self.logger.info("drop consumeinfo table")
        self.tdSql.query("drop table if exists %s.consumeinfo "%(cdbName))
        self.tdSql.query("create table %s.consumeinfo (ts timestamp, consumerid int, topiclist binary(1024), keylist binary(1024), expectmsgcnt bigint, ifcheckdata int, ifmanualcommit int)"%cdbName)

    def insertConsumerInfo(self,consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifmanualcommit,cdbName='cdb'):    
        sql = "insert into %s.consumeinfo values "%cdbName
        sql += "(now, %d, '%s', '%s', %d, %d, %d)"%(consumerId, topicList, keyList, expectrowcnt, ifcheckdata, ifmanualcommit)
        self.logger.info("consume info sql: %s"%sql)
        self.tdSql.query(sql)

    def selectConsumeResult(self,expectRows,cdbName='cdb'):
        resultList=[]
        while 1:
            self.tdSql.query("select * from %s.consumeresult"%cdbName)
            #self.logger.info("row: %d, %l64d, %l64d"%(self.tdSql.getData(0, 1),self.tdSql.getData(0, 2),self.tdSql.getData(0, 3))
            if self.tdSql.query_row == expectRows:
                break
            else:
                time.sleep(5)
        
        for i in range(expectRows):
            self.logger.info ("consume id: %d, consume msgs: %d, consume rows: %d"%(self.tdSql.getData(i , 1), self.tdSql.getData(i , 2), self.tdSql.getData(i , 3)))
            resultList.append(self.tdSql.getData(i , 3))
        
        return resultList
    
    def selectConsumeMsgResult(self,expectRows,cdbName='cdb'):
        resultList=[]
        while 1:
            self.tdSql.query("select * from %s.consumeresult"%cdbName)
            #self.logger.info("row: %d, %l64d, %l64d"%(self.tdSql.getData(0, 1),self.tdSql.getData(0, 2),self.tdSql.getData(0, 3))
            if self.tdSql.query_row == expectRows:
                break
            else:
                time.sleep(5)
        
        for i in range(expectRows):
            self.logger.info ("consume id: %d, consume msgs: %d, consume rows: %d"%(self.tdSql.getData(i , 1), self.tdSql.getData(i , 2), self.tdSql.getData(i , 3)))
            resultList.append(self.tdSql.getData(i , 2))
        
        return resultList

    def startTmqSimProcess(self,paraDict):
        cmd = 'nohup tmq_sim -c '%(paraDict['cfgPath'])
        cmd += " -y %d -d %s -g %d -r %d -w %s -e %d "%(paraDict["pollDelay"], paraDict["dbName"], paraDict["showMsg"], paraDict["showRow"], "cdb", paraDict["snapshot"]) 
        cmd += "> /dev/null 2>&1 &"

        self.logger.info(cmd)
        os.system(cmd) 
        result = self._remote.cmd2(paraDict["host"], cmd)
        if result.failed:
            self.logger.error("cmd [{}] failed on [{}]".format(cmd, paraDict["host"]))
            return False
        else:
            self.logger.info("cmd [{}] succeed on [{}]".format(cmd, paraDict["host"]))

    def stopTmqSimProcess(self, processorName):
        psCmd = "ps -ef|grep -w %s|grep -v grep | awk '{print $2}'"%(processorName)
        processID = subprocess.check_output(psCmd, shell=True).decode("utf-8")
        while(processID):
            killCmd = "kill -INT %s > /dev/null 2>&1" % processID
            os.system(killCmd)
            time.sleep(0.2)
            processID = subprocess.check_output(psCmd, shell=True).decode("utf-8")
        self.logger.debug("%s is stopped by kill -INT" % (processorName))
    
    def runTmqSim(self, **paraDict):
        cmd = "tmq_sim -c %s "%paraDict['cfgPath']
        cmd += " -y %d -d %s -g %d -r %d -w %s -e %d "%(paraDict["pollDelay"], paraDict["dbName"], paraDict["showMsg"], paraDict["showRow"], "cdb", paraDict["snapshot"]) 
        # self.logger.info("==================cmd: %s"%cmd)
        result = self._remote.cmd2(paraDict["host"], cmd)
        if result.failed:
            self.logger.error("cmd [{}] failed on [{}]".format(cmd, paraDict["host"]))
            return False
        else:
            self.logger.info("cmd [{}] succeed on [{}]".format(cmd, paraDict["host"]))

    def runTaosBenchmark(self, **paraDict):
        # cmd = ["ulimit -n 1048576", "nohup taosBenchmark -f /tmp/%s > /dev/null 2>&1 &"%(paraDict['json_template_filename'])]
        cmd = ["ulimit -n 1048576", "taosBenchmark -f /tmp/%s"%(paraDict['json_template_filename'])]
        self.logger.debug("===============host:%s"%paraDict["host"])
        result = self._remote.cmd2(paraDict["host"], cmd)
        if result.failed:
            self.logger.error("cmd [{}] failed on [{}]".format(cmd, paraDict["host"]))
            return False
        else:
            self.logger.info("cmd [{}] succeed on [{}]".format(cmd, paraDict["host"]))

    def startOtherProcessor(self, paraDict):
        if paraDict['processorName'] == "tmq_sim":
            pThread = threading.Thread(target=self.runTmqSim, kwargs=paraDict)
            pThread.start()
        elif  paraDict['processorName'] == "taosBenchmark":
            pThread = threading.Thread(target=self.runTaosBenchmark, kwargs=paraDict)
            pThread.start()
        return pThread

    def getStartConsumeNotifyFromTmqsim(self,cdbName='cdb',rows=1):
        loopFlag = 1
        while loopFlag:
            self.tdSql.query("select * from %s.notifyinfo"%cdbName)
            #self.logger.info("row: %d, %l64d, %l64d"%(self.tdSql.getData(0, 1),self.tdSql.getData(0, 2),self.tdSql.getData(0, 3))
            actRows = self.tdSql.query_row
            if (actRows >= rows):
                for i in range(actRows):
                    if self.tdSql.getData(i, 1) == 0:
                        loopFlag = 0
                        break            
            time.sleep(0.1)
        return

    def getStartCommitNotifyFromTmqsim(self,cdbName='cdb',rows=2):
        loopFlag = 1
        while loopFlag:
            self.tdSql.query("select * from %s.notifyinfo"%cdbName)
            #self.logger.info("row: %d, %l64d, %l64d"%(self.tdSql.getData(0, 1),self.tdSql.getData(0, 2),self.tdSql.getData(0, 3))
            actRows = self.tdSql.query_row
            if (actRows >= rows):
                for i in range(actRows):
                    if self.tdSql.getData(i, 1) == 1:
                        loopFlag = 0
                        break            
            time.sleep(0.1)
        return

    def create_database(self,tsql, dbName,dropFlag=1,vgroups=4,replica=1):
        if dropFlag == 1:
            tsql.execute("drop database if exists %s"%(dbName))

        tsql.execute("create database if not exists %s vgroups %d replica %d"%(dbName, vgroups, replica))
        self.logger.debug("complete to create database %s"%(dbName))
        return

    # self.create_stable() and self.create_ctable() and self.insert_data_interlaceByMultiTbl() : The three functions are matched
    # schema: (ts timestamp, c1 int, c2 bigint, c3 double, c4 binary(32), c5 nchar(32), c6 timestamp) tags (t1 int, t2 bigint, t3 double, t4 binary(32), t5 nchar(32))
    def create_stable(self,tsql, dbName,stbName):
        schemaString = "(ts timestamp, c1 int, c2 bigint, c3 double, c4 binary(32), c5 nchar(32), c6 timestamp) tags (t1 int, t2 bigint, t3 double, t4 binary(32), t5 nchar(32))"
        tsql.execute("create table if not exists %s.%s %s"%(dbName, stbName, schemaString))
        self.logger.debug("complete to create %s.%s" %(dbName, stbName))
        return

    def create_ctable(self,tsql=None, dbName='dbx',stbName='stb',ctbPrefix='ctb',ctbNum=1,ctbStartIdx=0):
        # tsql.execute("use %s" %dbName)
        pre_create = "create table"
        sql = pre_create
        #self.logger.debug("doing create one  stable %s and %d  child table in %s  ..." %(stbname, count ,dbname))
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
        
        self.logger.debug("complete to create %d child tables by %s.%s" %(ctbNum, dbName, stbName))
        return    

    # schema: (ts timestamp, c1 int, c2 binary(16))
    def insert_data(self,tsql,dbName,stbName,ctbNum,rowsPerTbl,batchNum,startTs=None):
        self.logger.debug("start to insert data ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        if startTs is None:
            t = time.time()
            startTs = int(round(t * 1000))
        #self.logger.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        for i in range(ctbNum):
            rowsBatched = 0
            sql += " %s%d values "%(stbName,i)
            for j in range(rowsPerTbl):
                sql += "(%d, %d, 'tmqrow_%d') "%(startTs + j, j, j)
                rowsBatched += 1
                if ((rowsBatched == batchNum) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    rowsBatched = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s%d values " %(stbName,i)
                    else:
                        sql = "insert into "
        #end sql
        if sql != pre_insert:
            #print("insert sql:%s"%sql)
            tsql.execute(sql)
        self.logger.debug("insert data ............ [OK]")
        return        

    # schema: (ts timestamp, c1 int, c2 int, c3 binary(16))
    def insert_data_1(self,tsql,dbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs):
        self.logger.debug("start to insert data ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        t = time.time()
        startTs = int(round(t * 1000))
        #self.logger.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        for i in range(ctbNum):
            rowsBatched = 0
            sql += " %s%d values "%(ctbPrefix,i)
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
                        sql = "insert into %s%d values " %(ctbPrefix,i)
                    else:
                        sql = "insert into "
        #end sql
        if sql != pre_insert:
            #print("insert sql:%s"%sql)
            tsql.execute(sql)
        self.logger.debug("insert data ............ [OK]")
        return

    # schema: (ts timestamp, c1 int, c2 int, c3 binary(16), c4 timestamp)
    def insert_data_2(self,tsql,dbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs,ctbStartIdx=0):
        self.logger.debug("start to insert data ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        t = time.time()
        startTs = int(round(t * 1000))
        #self.logger.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        for i in range(ctbNum):
            rowsBatched = 0
            sql += " %s%d values "%(ctbPrefix,i+ctbStartIdx)
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
                        sql = "insert into %s%d values " %(ctbPrefix,i+ctbStartIdx)
                    else:
                        sql = "insert into "
        #end sql
        if sql != pre_insert:
            #print("insert sql:%s"%sql)
            tsql.execute(sql)
        self.logger.debug("insert data ............ [OK]")
        return

    # schema: (ts timestamp, c1 int, c2 bigint, c3 double, c4 binary(32), c5 nchar(32), c6 timestamp) tags (t1 int, t2 bigint, t3 double, t4 binary(32), t5 nchar(32))
    def insert_data_interlaceByMultiTbl(self,tsql,dbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs=0,ctbStartIdx=0):
        self.logger.debug("start to insert data ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        if startTs == 0:
            t = time.time()
            startTs = int(round(t * 1000))

        ctbDict = {}
        for i in range(ctbNum):
            ctbDict[i] = 0

        #self.logger.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        rowsOfCtb = 0
        while rowsOfCtb < rowsPerTbl:
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

        self.logger.debug("insert data ............ [OK]")
        return

    def threadFunctionForInsertByInterlace(self, **paraDict):
        # create new connector for new self.tdSql instance in my thread
        # newTdSql = TDSql(logger=self.logger, run_log_dir=self.run_log_dir, set_error_msg=None)
        newTdSql = self.tdSql
        self.insert_data_interlaceByMultiTbl(newTdSql,paraDict["dbName"],paraDict["ctbPrefix"],paraDict["ctbNum"],paraDict["rowsPerTbl"],paraDict["batchNum"],paraDict["startTs"],paraDict["ctbStartIdx"])
        return

    def asyncInsertDataByInterlace(self, paraDict):
        pThread = threading.Thread(target=self.threadFunctionForInsertByInterlace, kwargs=paraDict)
        pThread.start()
        return pThread

    def insert_data_with_autoCreateTbl(self,tsql,dbName,stbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs=0,ctbStartIdx=0):
        self.logger.debug("start to insert data wiht auto create child table ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        if startTs == 0:
            t = time.time()
            startTs = int(round(t * 1000))

        #self.logger.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        rowsBatched = 0        
        for i in range(ctbNum):
            sql += " %s.%s_%d using %s.%s tags (%d) values "%(dbName,ctbPrefix,i+ctbStartIdx,dbName,stbName,i)
            for j in range(rowsPerTbl):
                sql += "(%d, %d, 'tmqrow_%d') "%(startTs + j, j, j)
                rowsBatched += 1
                if ((rowsBatched == batchNum) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    rowsBatched = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s.%s_%d using %s.%s tags (%d) values " %(dbName,ctbPrefix,i+ctbStartIdx,dbName,stbName,i)
                    else:
                        sql = "insert into "
        #end sql
        if sql != pre_insert:
            #print("insert sql:%s"%sql)
            tsql.execute(sql)
        self.logger.debug("insert data ............ [OK]")
        return

    def threadFunctionForInsert(self, **paraDict):
        # create new connector for new tdSql instance in my thread
        # newTdSql = TDSql()
        newTdSql = self.tdSql
        if 'ctbStartIdx' in paraDict.keys():
            self.insert_data_2(newTdSql,paraDict["dbName"],paraDict["ctbPrefix"],paraDict["ctbNum"],paraDict["rowsPerTbl"],paraDict["batchNum"],paraDict["startTs"],paraDict["ctbStartIdx"])
        else:
            self.insert_data_2(newTdSql,paraDict["dbName"],paraDict["ctbPrefix"],paraDict["ctbNum"],paraDict["rowsPerTbl"],paraDict["batchNum"],paraDict["startTs"])
        return

    def asyncInsertData(self, paraDict):
        pThread = threading.Thread(target=self.threadFunctionForInsert, kwargs=paraDict)
        pThread.start()
        return pThread

    def checkFileContent(self, cfgPath, consumerId, queryString):
        dstFile = '/tmp/dstrows_%d.txt'%(consumerId)
        cmdStr = 'taos -c %s -s "%s >> %s"'%(cfgPath, queryString, dstFile)
        self.logger.info(cmdStr)
        os.system(cmdStr)
        
        consumeRowsFile = '/tmp/consumerid_%d.txt'%(consumerId)
        self.logger.info("rows file: %s, %s"%(consumeRowsFile, dstFile))

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
                    self.logger.info("src row: %s"%src)
                    self.logger.info("dst row: %s"%dst)
                    self.logger.exit("consumerId %d consume rows[%d] is not match the rows by direct query"%(consumerId, lines))
            else:
                break
        return 

    def getResultFileByTaosShell(self, cfgPath, consumerId, queryString):
        dstFile = '/tmp/dstrows_%d.txt'%(consumerId)
        cmdStr = 'taos -c %s -s "%s >> %s"'%(cfgPath, queryString, dstFile)
        self.logger.info(cmdStr)
        os.system(cmdStr)
        return dstFile
    
    def checkTmqConsumeFileContent(self, cfgPath, consumerId, dstFile):   
        consumeRowsFile = '/tmp/consumerid_%d.txt'%(consumerId)
        self.logger.info("rows file: %s, %s"%(consumeRowsFile, dstFile))

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
                    self.logger.info("src row: %s"%src)
                    self.logger.info("dst row: %s"%dst)
                    self.logger.exit("consumerId %d consume rows[%d] is not match the rows by direct query"%(consumerId, lines))
            else:
                break
        return 

# tmqCom = TMQCom()
