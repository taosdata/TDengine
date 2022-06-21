import sys
import time
import socket
import os
import threading

import taos
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
sys.path.append("./7-tmq")
from tmqCommon import *

class TDTestCase:
    paraDict = {'dbName': 'db12',
                'dropFlag':1,
                'vgroups': 4,
                'precision': 'ms',
                'stbName': 'stb0',
                'ctbNum': 10,
                'rowsPerTbl': 10000,
                'batchNum': 10,
                'startTs': 0, # 1640966400000 ----> 2022-01-01 00:00:00.000
                'event':'',
                'columnDict': {'int':2},
                'tagDict': {'int':1}
                }

    cdbName = 'cdb'
    # some parameter to consumer processor
    consumerId = 0
    expectrowcnt = 0
    topicList = ''
    ifcheckdata = 0
    ifManualCommit = 1
    groupId = 'group.id:cgrp1'
    autoCommit = 'enable.auto.commit:false'
    autoCommitInterval = 'auto.commit.interval.ms:1000'
    autoOffset = 'auto.offset.reset:earliest'

    pollDelay = 20
    showMsg   = 1
    showRow   = 1  

    hostname = socket.gethostname()

    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        logSql = False
        tdSql.init(conn.cursor(), logSql)

    def tmqCase12(self):
        tdLog.printNoPrefix("======== test case 12: ")
        tdLog.info("step 1: create database, stb, ctb and insert data")
        
        tmqCom.initConsumerTable(self.cdbName)

        tdCom.create_database(tdSql,self.paraDict["dbName"],self.paraDict["dropFlag"], self.paraDict['precision'])

        self.paraDict["stbName"] = 'stb1'
        tdCom.create_stable(tdSql,self.paraDict["dbName"],self.paraDict["stbName"],self.paraDict["columnDict"],self.paraDict["tagDict"])
        tdCom.create_ctables(tdSql,self.paraDict["dbName"],self.paraDict["stbName"],self.paraDict["ctbNum"],self.paraDict["tagDict"])
        tdCom.insert_data(tdSql,self.paraDict["dbName"],self.paraDict["stbName"],self.paraDict["ctbNum"],self.paraDict["rowsPerTbl"],self.paraDict["batchNum"])

        self.paraDict["stbName"] = 'stb2'
        tdCom.create_stable(tdSql,self.paraDict["dbName"],self.paraDict["stbName"],self.paraDict["columnDict"],self.paraDict["tagDict"])
        tdCom.create_ctables(tdSql,self.paraDict["dbName"],self.paraDict["stbName"],self.paraDict["ctbNum"],self.paraDict["tagDict"])
        tdCom.insert_data(tdSql,self.paraDict["dbName"],self.paraDict["stbName"],self.paraDict["ctbNum"],self.paraDict["rowsPerTbl"],self.paraDict["batchNum"])

        tdLog.info("create topics from db")
        topicName1 = 'topic_%s'%(self.paraDict['dbName'])        
        tdSql.execute("create topic %s as database %s" %(topicName1, self.paraDict['dbName']))
        
        topicList = topicName1
        keyList = '%s,%s,%s,%s'%(self.groupId,self.autoCommit,self.autoCommitInterval,self.autoOffset)
        self.expectrowcnt = self.paraDict["rowsPerTbl"] * self.paraDict["ctbNum"] * 2
        tmqCom.insertConsumerInfo(self.consumerId, self.expectrowcnt,topicList,keyList,self.ifcheckdata,self.ifManualCommit)
        
        tdLog.info("start consume processor")  
        tmqCom.startTmqSimProcess(self.pollDelay,self.paraDict["dbName"],self.showMsg, self.showRow,self.cdbName)

        tdLog.info("After waiting for a period of time, drop one stable")
        time.sleep(10)              
        tdSql.execute("drop table %s.%s" %(self.paraDict['dbName'], self.paraDict['stbName']))        

        tdLog.info("wait result from consumer, then check it")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)

        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]
        
        if not (totalConsumeRows >= self.expectrowcnt/2 and totalConsumeRows <= self.expectrowcnt):
            tdLog.info("act consume rows: %d, expect consume rows: between %d and %d"%(totalConsumeRows, self.expectrowcnt/2, self.expectrowcnt))
            tdLog.exit("tmq consume rows error!")

        time.sleep(15)
        tdSql.query("drop topic %s"%topicName1)

        tdLog.printNoPrefix("======== test case 12 end ...... ")

    def run(self):
        tdSql.prepare()
        self.tmqCase12()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
