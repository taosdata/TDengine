
import taos
import sys
import time
import socket
import os
import threading
from enum import Enum

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
sys.path.append("./7-tmq")
from tmqCommon import *

class TDTestCase:
    def __init__(self):
        self.snapshot   = 0
        self.vgroups    = 2
        self.ctbNum     = 100
        self.rowsPerTbl = 1000

    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    def prepareTestEnv(self):
        tdLog.printNoPrefix("======== prepare test env include database, stable, ctables, and insert data: ")
        paraDict = {'dbName':     'dbt',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    3,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1},{'type': 'TIMESTAMP', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbStartIdx': 0,
                    'ctbNum':     100,
                    'rowsPerTbl': 1000,
                    'batchNum':   100,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  30,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0}

        paraDict['snapshot'] = self.snapshot
        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        tmqCom.initConsumerTable()
        tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], vgroups=paraDict["vgroups"],replica=1,wal_retention_size=-1, wal_retention_period=-1)
        tdSql.execute("alter database %s wal_retention_period 3600" % (paraDict['dbName']))
        tdLog.info("create stb")
        tmqCom.create_stable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"])
        tdLog.info("create ctb")
        tmqCom.create_ctable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"],ctbPrefix=paraDict['ctbPrefix'],
                             ctbNum=paraDict["ctbNum"],ctbStartIdx=paraDict['ctbStartIdx'])
        tdLog.info("insert data")
        tmqCom.insert_data_interlaceByMultiTbl(tsql=tdSql,dbName=paraDict["dbName"],ctbPrefix=paraDict["ctbPrefix"],
                                               ctbNum=paraDict["ctbNum"],rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],
                                               startTs=paraDict["startTs"],ctbStartIdx=paraDict['ctbStartIdx'])

        tdLog.info("flush database to ensure that the data falls into the disk")
        # tdDnodes.stop(1)
        # tdDnodes.start(1)
        tdSql.query("flush database %s"%(paraDict['dbName']))
        return

    def tmqCase1(self):
        tdLog.printNoPrefix("======== test case 1: ")
        paraDict = {'dbName':     'dbt',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1},{'type': 'TIMESTAMP', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbStartIdx': 0,
                    'ctbNum':     1000,
                    'rowsPerTbl': 1000,
                    'batchNum':   100,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  30,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0}

        paraDict['snapshot'] = self.snapshot
        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl
   
        tdLog.info("create topics from stb")
        topicFromStb = 'topic_stb'
        queryString = "select * from %s.%s"%(paraDict['dbName'], paraDict['stbName'])
        sqlString = "create topic %s as stable %s.%s" %(topicFromStb, paraDict['dbName'], paraDict['stbName'])
        #sqlString = "create topic %s as %s" %(topicFromStb, queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)

        consumerId     = 0
        expectrowcnt   = paraDict["rowsPerTbl"] * paraDict["ctbNum"] 
        topicList      = topicFromStb
        ifcheckdata    = 0
        ifManualCommit = 0
        keyList        = 'group.id:cgrp1,\
                        enable.auto.commit:true,\
                        auto.commit.interval.ms:200,\
                        auto.offset.reset:latest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        tmqCom.startTmqSimProcess(pollDelay=paraDict['pollDelay'],dbName=paraDict["dbName"],showMsg=paraDict['showMsg'], showRow=paraDict['showRow'],snapshot=paraDict['snapshot'])

        # time.sleep(3)
        tmqCom.getStartCommitNotifyFromTmqsim()

        tdLog.info("create some new child table and insert data for latest mode")
        paraDict["batchNum"] = 100
        paraDict["ctbPrefix"] = 'newCtb'
        paraDict["ctbNum"]     = 10
        paraDict["rowsPerTbl"] = 10
        tmqCom.insert_data_with_autoCreateTbl(tdSql,paraDict["dbName"],paraDict["stbName"],paraDict["ctbPrefix"],paraDict["ctbNum"],paraDict["rowsPerTbl"],paraDict["batchNum"])

        tdLog.info("================= restart dnode ===========================")
        tdDnodes.stoptaosd(1)
        tdDnodes.starttaosd(1)
        # time.sleep(3)

        tdLog.info(" restart taosd end and wait to check consume result")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        totalRowsFromQury = paraDict["ctbNum"] * paraDict["rowsPerTbl"]

        tdLog.info("act consume rows: %d, act query rows: %d"%(totalConsumeRows, totalRowsFromQury))
        if (totalConsumeRows < totalRowsFromQury):
            tdLog.exit("tmq consume rows error!")

        tmqCom.waitSubscriptionExit(tdSql, topicFromStb)
        tdSql.query("drop topic %s"%topicFromStb)
        
        tmqCom.stopTmqSimProcess(processorName="tmq_sim")

        tdLog.printNoPrefix("======== test case 1 end ...... ")

    def tmqCase2(self):
        tdLog.printNoPrefix("======== test case 2: ")
        paraDict = {'dbName':     'dbt',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1},{'type': 'TIMESTAMP', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbStartIdx': 0,
                    'ctbNum':     1000,
                    'rowsPerTbl': 1000,
                    'batchNum':   100,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  30,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0}

        paraDict['snapshot'] = self.snapshot
        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl
        
        tmqCom.initConsumerTable()
   
        tdLog.info("create topics from stb")
        topicFromDb = 'topic_db'
        queryString = "select * from %s.%s"%(paraDict['dbName'], paraDict['stbName'])
        sqlString = "create topic %s as database %s" %(topicFromDb, paraDict['dbName'])
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)

        consumerId     = 0
        expectrowcnt   = paraDict["rowsPerTbl"] * paraDict["ctbNum"] 
        topicList      = topicFromDb
        ifcheckdata    = 0
        ifManualCommit = 0
        keyList        = 'group.id:cgrp1,\
                        enable.auto.commit:true,\
                        auto.commit.interval.ms:200,\
                        auto.offset.reset:latest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        tmqCom.startTmqSimProcess(pollDelay=paraDict['pollDelay'],dbName=paraDict["dbName"],showMsg=paraDict['showMsg'], showRow=paraDict['showRow'],snapshot=paraDict['snapshot'])

        tmqCom.getStartCommitNotifyFromTmqsim()

        tdLog.info("create some new child table and insert data for latest mode")
        paraDict["batchNum"] = 10
        paraDict["ctbPrefix"] = 'newCtb'
        paraDict["ctbNum"]     = 100
        paraDict["rowsPerTbl"] = 100
        tmqCom.insert_data_with_autoCreateTbl(tdSql,paraDict["dbName"],paraDict["stbName"],paraDict["ctbPrefix"],paraDict["ctbNum"],paraDict["rowsPerTbl"],paraDict["batchNum"])

        tdLog.info("================= restart dnode ===========================")
        tdDnodes.stoptaosd(1)
        tdDnodes.starttaosd(1)

        tdLog.info(" restart taosd end and wait to check consume result")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        totalRowsFromQury = paraDict["ctbNum"] * paraDict["rowsPerTbl"]

        tdLog.info("act consume rows: %d, act query rows: %d"%(totalConsumeRows, totalRowsFromQury))
        if (totalConsumeRows < totalRowsFromQury):
            tdLog.exit("tmq consume rows error!")

        tmqCom.waitSubscriptionExit(tdSql, topicFromDb)
        tdSql.query("drop topic %s"%topicFromDb)

        tdLog.printNoPrefix("======== test case 2 end ...... ")

    def run(self):
        # tdSql.prepare()
        self.prepareTestEnv()
        self.tmqCase1()
        self.tmqCase2()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
