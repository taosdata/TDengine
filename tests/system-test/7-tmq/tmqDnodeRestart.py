
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
        self.ctbNum     = 1000
        self.rowsPerTbl = 1000
        
    def init(self, conn, logSql):
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), False)

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
                    'ctbNum':     1000,
                    'rowsPerTbl': 1000,
                    'batchNum':   100,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  3,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0}

        paraDict['snapshot'] = self.snapshot
        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl
        
        tmqCom.initConsumerTable()
        tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], vgroups=paraDict["vgroups"],replica=1)
        tdLog.info("create stb")
        tmqCom.create_stable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"])
        tdLog.info("create ctb")
        tmqCom.create_ctable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"],ctbPrefix=paraDict['ctbPrefix'],
                             ctbNum=paraDict["ctbNum"],ctbStartIdx=paraDict['ctbStartIdx'])
        tdLog.info("insert data")
        tmqCom.insert_data_interlaceByMultiTbl(tsql=tdSql,dbName=paraDict["dbName"],ctbPrefix=paraDict["ctbPrefix"],
                                               ctbNum=paraDict["ctbNum"],rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],
                                               startTs=paraDict["startTs"],ctbStartIdx=paraDict['ctbStartIdx'])
        
        tdLog.info("restart taosd to ensure that the data falls into the disk")
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
                    'pollDelay':  5,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0}

        paraDict['snapshot'] = self.snapshot
        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl
        
        # tmqCom.initConsumerTable()
        # tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], vgroups=paraDict["vgroups"],replica=1)
        # tdLog.info("create stb")
        # tmqCom.create_stable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"])
        # tdLog.info("create ctb")
        # tmqCom.create_ctable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"],ctbPrefix=paraDict['ctbPrefix'],
        #                      ctbNum=paraDict["ctbNum"],ctbStartIdx=paraDict['ctbStartIdx'])
        # tdLog.info("insert data")
        # tmqCom.insert_data_interlaceByMultiTbl(tsql=tdSql,dbName=paraDict["dbName"],ctbPrefix=paraDict["ctbPrefix"],
        #                                        ctbNum=paraDict["ctbNum"],rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],
        #                                        startTs=paraDict["startTs"],ctbStartIdx=paraDict['ctbStartIdx'])

        tdLog.info("create topics from stb1")
        topicFromStb1 = 'topic_stb1'                
        queryString = "select ts, c1, c2 from %s.%s"%(paraDict['dbName'], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicFromStb1, queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)
        
        consumerId     = 0
        expectrowcnt   = paraDict["rowsPerTbl"] * paraDict["ctbNum"] * 2
        topicList      = topicFromStb1
        ifcheckdata    = 0
        ifManualCommit = 0
        keyList        = 'group.id:cgrp1,\
                        enable.auto.commit:true,\
                        auto.commit.interval.ms:3000,\
                        auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        tmqCom.startTmqSimProcess(pollDelay=paraDict['pollDelay'],dbName=paraDict["dbName"],showMsg=paraDict['showMsg'], showRow=paraDict['showRow'],snapshot=paraDict['snapshot'])
        
        # time.sleep(3)
        tmqCom.getStartCommitNotifyFromTmqsim()
        tdLog.info("================= restart dnode ===========================")
        tdDnodes.stop(1)
        tdDnodes.start(1)
        time.sleep(3)

        tdLog.info("insert process end, and start to check consume result")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        tdSql.query(queryString)
        totalRowsFromQury = tdSql.getRows()
        
        tdLog.info("act consume rows: %d, act query rows: %d"%(totalConsumeRows, totalRowsFromQury))
        if not (totalConsumeRows == totalRowsFromQury):
            tdLog.exit("tmq consume rows error!")




        # tdLog.info("****************************************************************************")
        # tmqCom.initConsumerTable()
        # consumerId     = 1
        # expectrowcnt   = paraDict["rowsPerTbl"] * paraDict["ctbNum"] * 2
        # topicList      = topicFromStb1
        # ifcheckdata    = 0
        # ifManualCommit = 0
        # keyList        = 'group.id:cgrp2,\
        #                 enable.auto.commit:true,\
        #                 auto.commit.interval.ms:3000,\
        #                 auto.offset.reset:earliest'
        # tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        # tdLog.info("start consume processor")
        # tmqCom.startTmqSimProcess(pollDelay=paraDict['pollDelay'],dbName=paraDict["dbName"],showMsg=paraDict['showMsg'], showRow=paraDict['showRow'],snapshot=paraDict['snapshot'])
        
        # expectRows = 1
        # resultList = tmqCom.selectConsumeResult(expectRows)
        # totalConsumeRows = 0
        # for i in range(expectRows):
        #     totalConsumeRows += resultList[i]

        # tdSql.query(queryString)
        # totalRowsFromQury = tdSql.getRows()
        
        # tdLog.info("act consume rows: %d, act query rows: %d"%(totalConsumeRows, totalRowsFromQury))
        # if not (totalConsumeRows == totalRowsFromQury):
        #     tdLog.exit("tmq consume rows error!")

        
        # tdLog.info("****************************************************************************")

        tmqCom.waitSubscriptionExit(tdSql)
        tdSql.query("drop topic %s"%topicFromStb1)

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
                    'batchNum':   3000,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  5,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0}

        paraDict['snapshot'] = self.snapshot
        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl
        
        tmqCom.initConsumerTable()
        # tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], vgroups=paraDict["vgroups"],replica=1)
        # tdLog.info("create stb")
        # tmqCom.create_stable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"])
        # tdLog.info("create ctb")
        # tmqCom.create_ctable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"],ctbPrefix=paraDict['ctbPrefix'],
        #                      ctbNum=paraDict["ctbNum"],ctbStartIdx=paraDict['ctbStartIdx'])
        # tdLog.info("insert data")
        # tmqCom.insert_data_interlaceByMultiTbl(tsql=tdSql,dbName=paraDict["dbName"],ctbPrefix=paraDict["ctbPrefix"],
        #                                        ctbNum=paraDict["ctbNum"],rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],
        #                                        startTs=paraDict["startTs"],ctbStartIdx=paraDict['ctbStartIdx'])
        tdLog.info("create topics from stb1")
        topicFromStb1 = 'topic_stb1'                
        queryString = "select ts, c1, c2 from %s.%s"%(paraDict['dbName'], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicFromStb1, queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)
        
        consumerId     = 1
        expectrowcnt   = paraDict["rowsPerTbl"] * paraDict["ctbNum"] * 2
        topicList      = topicFromStb1
        ifcheckdata    = 0
        ifManualCommit = 0
        keyList        = 'group.id:cgrp1,\
                        enable.auto.commit:true,\
                        auto.commit.interval.ms:3000,\
                        auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        tmqCom.startTmqSimProcess(pollDelay=paraDict['pollDelay'],dbName=paraDict["dbName"],showMsg=paraDict['showMsg'], showRow=paraDict['showRow'],snapshot=paraDict['snapshot'])

        tmqCom.getStartCommitNotifyFromTmqsim()
        tdLog.info("================= restart dnode ===========================")
        tdDnodes.stop(1)
        tdDnodes.start(1)
        time.sleep(3)

        # tdLog.info("create some new child table and insert data ")
        # paraDict["batchNum"] = 1000
        # paraDict["ctbPrefix"] = 'newCtb'
        # tmqCom.insert_data_with_autoCreateTbl(tdSql,paraDict["dbName"],paraDict["stbName"],paraDict["ctbPrefix"],paraDict["ctbNum"],paraDict["rowsPerTbl"],paraDict["batchNum"])
        
        tdLog.info("insert process end, and start to check consume result")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        tdSql.query(queryString)
        totalRowsFromQuery = tdSql.getRows()
        
        tdLog.info("act consume rows: %d, expect consume rows: %d"%(totalConsumeRows, totalRowsFromQuery))
        if totalConsumeRows != totalRowsFromQuery:
            tdLog.exit("tmq consume rows error!")

        tdSql.query("drop topic %s"%topicFromStb1)

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
