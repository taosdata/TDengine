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

class TMQCom:
    def init(self, conn, logSql):
        tdSql.init(conn.cursor())
        # tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def initConsumerTable(self,cdbName='cdb'):        
        tdLog.info("create consume database, and consume info table, and consume result table")
        tdSql.query("create database if not exists %s vgroups 1"%(cdbName))
        tdSql.query("drop table if exists %s.consumeinfo "%(cdbName))
        tdSql.query("drop table if exists %s.consumeresult "%(cdbName))

        tdSql.query("create table %s.consumeinfo (ts timestamp, consumerid int, topiclist binary(1024), keylist binary(1024), expectmsgcnt bigint, ifcheckdata int, ifmanualcommit int)"%cdbName)
        tdSql.query("create table %s.consumeresult (ts timestamp, consumerid int, consummsgcnt bigint, consumrowcnt bigint, checkresult int)"%cdbName)

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

    def syncCreateDbStbCtbInsertData(self, tsql, paraDict):
        tdCom.create_database(tsql, paraDict["dbName"],paraDict["dropFlag"], paraDict['precision'])
        tdCom.create_stable(tsql, paraDict["dbName"],paraDict["stbName"], paraDict["columnDict"], paraDict["tagDict"])
        tdCom.create_ctables(tsql, paraDict["dbName"],paraDict["stbName"],paraDict["ctbNum"],paraDict["tagDict"])
        if "event" in paraDict and type(paraDict['event']) == type(threading.Event()):
            paraDict["event"].set()
        tdCom.insert_data(tsql,paraDict["dbName"],paraDict["stbName"],paraDict["ctbNum"],paraDict["rowsPerTbl"],paraDict["batchNum"],paraDict["startTs"])
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

tmqCom = TMQCom()
