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

from collections import defaultdict
import random
import string
import threading
import requests
import time
# import socketfrom

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

class ClusterComCreate:
    def init(self, conn, logSql, replicaVar=1):
        tdSql.init(conn.cursor())
        # tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def initConsumerTable(self,cdbName='cdb'):
        tdLog.info("create consume database, and consume info table, and consume result table")
        tdSql.query("create database if not exists %s vgroups 1"%(cdbName))
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
        sql += "(now, %d, '%s', '%s', %d, %d, %d)"%(consumerId, topicList, keyList, expectrowcnt, ifcheckdata, ifmanualcommit)
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
                time.sleep(5)

        for i in range(expectRows):
            tdLog.info ("consume id: %d, consume msgs: %d, consume rows: %d"%(tdSql.getData(i , 1), tdSql.getData(i , 2), tdSql.getData(i , 3)))
            resultList.append(tdSql.getData(i , 3))

        return resultList

    def startTmqSimProcess(self,pollDelay,dbName,showMsg=1,showRow=1,cdbName='cdb',valgrind=0):
        buildPath = tdCom.getBuildPath()
        cfgPath = tdCom.getClientCfgPath()
        if valgrind == 1:
            logFile = cfgPath + '/../log/valgrind-tmq.log'
            shellCmd = 'nohup valgrind --log-file=' + logFile
            shellCmd += '--tool=memcheck --leak-check=full --show-reachable=no --track-origins=yes --show-leak-kinds=all --num-callers=20 -v --workaround-gcc296-bugs=yes '

        if (platform.system().lower() == 'windows'):
            shellCmd = 'mintty -h never -w hide ' + buildPath + '\\build\\bin\\tmq_sim.exe -c ' + cfgPath
            shellCmd += " -y %d -d %s -g %d -r %d -w %s "%(pollDelay, dbName, showMsg, showRow, cdbName)
            shellCmd += "> nul 2>&1 &"
        else:
            shellCmd = 'nohup ' + buildPath + '/build/bin/tmq_sim -c ' + cfgPath
            shellCmd += " -y %d -d %s -g %d -r %d -w %s "%(pollDelay, dbName, showMsg, showRow, cdbName)
            shellCmd += "> /dev/null 2>&1 &"
        tdLog.info(shellCmd)
        os.system(shellCmd)

    # def getStartConsumeNotifyFromTmqsim(self,cdbName='cdb'):
    #     while 1:
    #         tdSql.query("select * from %s.notifyinfo"%cdbName)
    #         #tdLog.info("row: %d, %l64d, %l64d"%(tdSql.getData(0, 1),tdSql.getData(0, 2),tdSql.getData(0, 3))
    #         if (tdSql.getRows() == 1) and (tdSql.getData(0, 1) == 0):
    #             break
    #         else:
    #             time.sleep(0.1)
    #     return
    #
    # def getStartCommitNotifyFromTmqsim(self,cdbName='cdb'):
    #     while 1:
    #         tdSql.query("select * from %s.notifyinfo"%cdbName)
    #         #tdLog.info("row: %d, %l64d, %l64d"%(tdSql.getData(0, 1),tdSql.getData(0, 2),tdSql.getData(0, 3))
    #         if tdSql.getRows() == 2 :
    #             print(tdSql.getData(0, 1), tdSql.getData(1, 1))
    #             if tdSql.getData(1, 1) == 1:
    #                 break
    #         time.sleep(0.1)
    #     return

    def create_database(self,tsql, dbName,dropFlag=1,vgroups=4,replica=1):
        if dropFlag == 1:
            tsql.execute("drop database if exists %s"%(dbName))

        tsql.execute("create database if not exists %s vgroups %d replica %d"%(dbName, vgroups, replica))
        tdLog.debug("complete to create database %s"%(dbName))
        return

    def create_databases(self,tsql,dbNameIndex,dbNumbers,dropFlag=1,vgroups=4,replica=1):
        for i in range(dbNumbers):
            if dropFlag == 1:
                tsql.execute("drop database if exists %s_%d"%(dbNameIndex,i))
            tdLog.debug("create database if not exists %s_%d vgroups %d replica %d"%(dbNameIndex,i, vgroups, replica))
            tsql.execute("create database if not exists %s_%d vgroups %d replica %d"%(dbNameIndex,i, vgroups, replica))
            tdLog.debug("complete to create database %s_%d"%(dbNameIndex,i))
        return

    def createDeltedatabases(self,tsql,dbNameIndex,dbNumbers,dropFlag=1,vgroups=4,replica=1):

        # for i in range(dbNumbers):
        for i in range(dbNumbers):
            if dropFlag == 1:
                tsql.execute("drop database if exists %s_%d"%(dbNameIndex,i))
            tdLog.debug("create database if not exists %s_%d vgroups %d replica %d"%(dbNameIndex,i, vgroups, replica))
            tsql.execute("create database if not exists %s_%d vgroups %d replica %d"%(dbNameIndex,i, vgroups, replica))
            tdLog.debug("complete to create database %s_%d"%(dbNameIndex,i))
    
    def createUser(self,tsql,user,password):
        tdLog.info(f"create new user f{user}")   
        tsql.execute(f"CREATE USER {user} PASS '{password}';")

    def alterUser(self,tsql,user,password):
        tdLog.info(f"alter user {user} pass '{password}'")   
        tsql.execute(f"alter USER {user}  pass '{password}' ;")

    def deleteUser(self,tsql,user):
        tdLog.info(f"drop user f{user}")   
        tsql.execute(f"DROP USER {user} ;")

        
    def create_stable(self,tsql, dbName,stbName):
        tsql.execute("create table if not exists %s.%s (ts timestamp, c1 int, c2 int, c3 binary(16)) tags(t1 int, t2 binary(32))"%(dbName, stbName))
        tdLog.debug("complete to create %s.%s" %(dbName, stbName))
        return

    def create_stables(self,tsql,dbNameIndex,stbNameIndex,stbNumbers):
        for i in range(stbNumbers):
            tdLog.debug("create table if not exists %s.%s_%d (ts timestamp, c1 int, c2 int, c3 binary(16)) tags(t1 int, t2 binary(32))"%(dbNameIndex, stbNameIndex,i))
            tsql.execute("create table if not exists %s.%s_%d (ts timestamp, c1 int, c2 int, c3 binary(16)) tags(t1 int, t2 binary(32))"%(dbNameIndex, stbNameIndex,i))
            tdLog.debug("complete to create %s.%s_%d" %(dbNameIndex, stbNameIndex,i))
        return

    def create_ctable(self,tsql=None, dbName='dbx',stbName='stb',ctbPrefix='ctb',ctbNum=1):
        tsql.execute("use %s" %dbName)
        pre_create = "create table"
        sql = pre_create
        #tdLog.debug("doing create one  stable %s and %d  child table in %s  ..." %(stbname, count ,dbname))
        for i in range(ctbNum):
            tagValue = 'beijing'
            if (i % 2 == 0):
                tagValue = 'shanghai'

            sql += " %s_%d using %s tags(%d, '%s')"%(ctbPrefix,i,stbName,i+1, tagValue)
            if (i > 0) and (i%100 == 0):
                tsql.execute(sql)
                sql = pre_create
        if sql != pre_create:
            tsql.execute(sql)

        tdLog.debug("complete to create %d child tables in %s.%s" %(ctbNum, dbName, stbName))
        return

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
            sql += " %s_%d values "%(stbName,i)
            for j in range(rowsPerTbl):
                sql += "(%d, %d,  %d, 'mnode_%d') "%(startTs + j, j, j,j)
                if (j > 0) and ((j%batchNum == 0) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s_%d values " %(stbName,i)
                    else:
                        sql = "insert into "
        #end sql
        if sql != pre_insert:
            #print("insert sql:%s"%sql)
            tsql.execute(sql)
        tdLog.debug("insert data ............ [OK]")
        return

    def alterStbMetaData(self,tsql,dbName,stbName,ctbNum,rowsPerTbl,batchNum,startTs=None):
        
        tdLog.debug("alter Stb column ............")
        tdLog.debug(f"describe  {dbName}.{stbName} ")     
        tsql.execute(f"describe  {dbName}.{stbName} ;")           
        tdLog.debug(f"ALTER STABLE {dbName}.{stbName} MODIFY COLUMN c3 binary(20);")
        tsql.execute(f" ALTER STABLE {dbName}.{stbName} MODIFY COLUMN c3 binary(20);")
        tdLog.debug(f"ALTER STABLE {dbName}.{stbName} ADD COLUMN c4 DOUBLE;")
        tsql.execute(f" ALTER STABLE {dbName}.{stbName} ADD COLUMN c4 DOUBLE;")
        tdLog.debug(f"ALTER STABLE {dbName}.{stbName} DROP COLUMN c2;")
        tsql.execute(f" ALTER STABLE {dbName}.{stbName} DROP COLUMN c2;")
        tdLog.debug(f"ALTER STABLE {dbName}.{stbName} RENAME TAG t1 t1r;")
        tsql.execute(f" ALTER STABLE {dbName}.{stbName} RENAME TAG t1 t1r;")
        tdLog.debug(f"ALTER STABLE {dbName}.{stbName} DROP TAG t2;")
        tsql.execute(f" ALTER STABLE {dbName}.{stbName} DROP TAG t2;")
        tdLog.debug(f"ALTER STABLE {dbName}.{stbName} ADD TAG t2 binary(32) ;")
        tsql.execute(f" ALTER STABLE {dbName}.{stbName} ADD TAG t2 binary(32);")        
        tdLog.debug(f"ALTER STABLE {dbName}.{stbName} MODIFY TAG t2 binary(34) ;")
        tsql.execute(f" ALTER STABLE {dbName}.{stbName}  MODIFY TAG t2 binary(34);")    
        tdLog.debug(f"ALTER STABLE {dbName}.{stbName} ADD TAG t3 double ;")
        tsql.execute(f" ALTER STABLE {dbName}.{stbName} ADD TAG t3 double;")  
        tsql.error(f" ALTER STABLE {dbName}.{stbName} ADD TAG t2 double;")  

        tdLog.debug("start to insert data ............")
        # tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        if startTs is None:
            t = time.time()
            startTs = int(round(t * 1000))
        #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        for i in range(ctbNum):
            sql += " %s.%s_%d values "%(dbName,stbName,i)
            for j in range(rowsPerTbl):
                sql += "(%d, %d,'mnode_%d',  %d ) "%(startTs + j, j, j,j)
                if (j > 0) and ((j%batchNum == 0) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s.%s_%d values " %(dbName,stbName,i)
                    else:
                        sql = "insert into "
        #end sql
        if sql != pre_insert:
            #print("insert sql:%s"%sql)
            tsql.execute(sql)
        tdLog.debug("insert data ............ [OK]")
        return

    def insert_data_1(self,tsql,dbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs):
        tdLog.debug("start to insert data ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        t = time.time()
        startTs = int(round(t * 1000))
        #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        for i in range(ctbNum):
            sql += " %s%d values "%(ctbPrefix,i)
            for j in range(rowsPerTbl):
                if (j % 2 == 0):
                    sql += "(%d, %d, %d, 'tmqrow_%d') "%(startTs + j, j, j, j)
                else:
                    sql += "(%d, %d, %d, 'tmqrow_%d') "%(startTs + j, j, -j, j)
                if (j > 0) and ((j%batchNum == 0) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s%d values " %(ctbPrefix,i)
                    else:
                        sql = "insert into "
        #end sql
        if sql != pre_insert:
            #print("insert sql:%s"%sql)
            tsql.execute(sql)
        tdLog.debug("insert data ............ [OK]")
        return

    def insert_data_interlaceByMultiTbl(self,tsql,dbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs=0):
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
            for i in range(ctbNum):
                sql += " %s.%s_%d values "%(dbName,ctbPrefix,i)
                for k in range(batchNum):
                    sql += "(%d, %d, 'tmqrow_%d') "%(startTs + ctbDict[i], ctbDict[i], ctbDict[i])
                    ctbDict[i] += 1
                    if (0 == ctbDict[i]%batchNum) or (ctbDict[i] == rowsPerTbl):
                        tsql.execute(sql)
                        sql = "insert into "
                        break
            rowsOfCtb = ctbDict[0]

        tdLog.debug("insert data ............ [OK]")
        return

    def insert_data_with_autoCreateTbl(self,tsql,dbName,stbName,ctbPrefix,ctbNum,rowsPerTbl,batchNum,startTs=0):
        tdLog.debug("start to insert data wiht auto create child table ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        if startTs == 0:
            t = time.time()
            startTs = int(round(t * 1000))

        #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        rowsOfSql = 0
        for i in range(ctbNum):
            sql += " %s.%s_%d using %s.%s tags (%d) values "%(dbName,ctbPrefix,i,dbName,stbName,i)
            for j in range(rowsPerTbl):
                sql += "(%d, %d, 'tmqrow_%d') "%(startTs + j, j, j)
                rowsOfSql += 1
                if (j > 0) and ((rowsOfSql == batchNum) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    rowsOfSql = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s.%s_%d using %s.%s tags (%d) values " %(dbName,ctbPrefix,i,dbName,stbName,i)
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


    def close(self):
        self.cursor.close()

clusterComCreate = ClusterComCreate()
