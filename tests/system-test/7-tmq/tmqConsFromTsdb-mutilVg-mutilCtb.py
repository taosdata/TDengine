
import taos
import sys
import time
import socket
import os
import threading
import math

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
sys.path.append("./7-tmq")
from tmqCommon import *

class TDTestCase:
    def __init__(self):
        self.vgroups    = 4
        self.ctbNum     = 3000
        self.rowsPerTbl = 70

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    def prepareTestEnv(self):
        tdLog.printNoPrefix("======== prepare test env include database, stable, ctables, and insert data: ")
        paraDict = {'dbName':     'dbt',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    1,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1},{'type': 'TIMESTAMP', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbStartIdx': 0,
                    'ctbNum':     10,
                    'rowsPerTbl': 10000,
                    'batchNum':   100,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  3,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   1}

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
        # # tdDnodes.start(1)
        # tdDnodes.starttaosd(1)
        tdSql.query("flush database %s"%(paraDict['dbName']))
        return

    def tmqCase1(self):
        tdLog.printNoPrefix("======== test case 1: ")
        paraDict = {'dbName':     'dbt',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    1,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1},{'type': 'TIMESTAMP', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbStartIdx': 0,
                    'ctbNum':     10,
                    'rowsPerTbl': 10000,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  3,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   1}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        topicNameList = ['topic1']
        expectRowsList = []
        tmqCom.initConsumerTable()

        tdLog.info("create topics from stb with filter")
        queryString = "select * from %s.%s"%(paraDict['dbName'], paraDict['stbName'])
        # sqlString = "create topic %s as stable %s" %(topicNameList[0], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicNameList[0], queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)
        tdSql.query(queryString)
        expectRowsList.append(tdSql.getRows())

        # init consume info, and start tmq_sim, then check consume result
        tdLog.info("insert consume info to consume processor")
        consumerId   = 0
        expectrowcnt = paraDict["rowsPerTbl"] * paraDict["ctbNum"]
        topicList    = topicNameList[0]
        ifcheckdata  = 1
        ifManualCommit = 1
        keyList      = 'group.id:cgrp1, enable.auto.commit:true, auto.commit.interval.ms:1000, auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        tmqCom.startTmqSimProcess(pollDelay=paraDict['pollDelay'],dbName=paraDict["dbName"],showMsg=paraDict['showMsg'], showRow=paraDict['showRow'],snapshot=paraDict['snapshot'])
        tdLog.info("wait the consume result")

        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)

        if expectRowsList[0] != resultList[0]:
            tdLog.info("expect consume rows: %d, act consume rows: %d"%(expectRowsList[0], resultList[0]))
            tdLog.exit("%d tmq consume rows error!"%consumerId)

        # tmqCom.checkFileContent(consumerId, queryString)

        time.sleep(10)
        for i in range(len(topicNameList)):
            tdSql.query("drop topic %s"%topicNameList[i])

        tdLog.printNoPrefix("======== test case 1 end ...... ")

    def tmqCase2(self):
        tdLog.printNoPrefix("======== test case 2: ")
        paraDict = {'dbName':     'dbt',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    1,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1},{'type': 'TIMESTAMP', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1},{'type': 'BIGINT', 'count':1},{'type': 'DOUBLE', 'count':1},{'type': 'BINARY', 'len':32, 'count':1},{'type': 'NCHAR', 'len':32, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbStartIdx': 0,
                    'ctbNum':     10,
                    'rowsPerTbl': 10000,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  3,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   1}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        topicNameList = ['topic1']
        expectRowsList = []
        tmqCom.initConsumerTable()

        tdLog.info("create topics from stb with filter")
        queryString = "select * from %s.%s"%(paraDict['dbName'], paraDict['stbName'])
        # sqlString = "create topic %s as stable %s" %(topicNameList[0], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicNameList[0], queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)
        tdSql.query(queryString)
        expectRowsList.append(tdSql.getRows())
        totalRowsInserted = expectRowsList[0]

        # init consume info, and start tmq_sim, then check consume result
        tdLog.info("insert consume info to consume processor")
        consumerId   = 1
        expectrowcnt = math.ceil(paraDict["rowsPerTbl"] * paraDict["ctbNum"] / 3)
        topicList    = topicNameList[0]
        ifcheckdata  = 1
        ifManualCommit = 1
        keyList      = 'group.id:cgrp1, enable.auto.commit:true, auto.commit.interval.ms:1000, auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor 0")
        tmqCom.startTmqSimProcess(pollDelay=paraDict['pollDelay'],dbName=paraDict["dbName"],showMsg=paraDict['showMsg'], showRow=paraDict['showRow'],snapshot=paraDict['snapshot'])
        tdLog.info("wait the consume result")

        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)

        if not (expectrowcnt <= resultList[0] and totalRowsInserted >= resultList[0]):
            tdLog.info("act consume rows: %d, expect consume rows between %d and %d"%(resultList[0], expectrowcnt, totalRowsInserted))
            tdLog.exit("%d tmq consume rows error!"%consumerId)

        firstConsumeRows = resultList[0]

        # reinit consume info, and start tmq_sim, then check consume result
        tmqCom.initConsumerTable()
        consumerId   = 2
        expectrowcnt = math.ceil(paraDict["rowsPerTbl"] * paraDict["ctbNum"] * 2/3)
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor 1")
        tmqCom.startTmqSimProcess(pollDelay=paraDict['pollDelay'],dbName=paraDict["dbName"],showMsg=paraDict['showMsg'], showRow=paraDict['showRow'],snapshot=paraDict['snapshot'])
        tdLog.info("wait the consume result")

        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)

        actConsumeTotalRows = firstConsumeRows + resultList[0]

        if not (expectrowcnt >= resultList[0] and totalRowsInserted == actConsumeTotalRows):
            tdLog.info("act consume rows, first: %d, second: %d "%(firstConsumeRows, resultList[0]))
            tdLog.info("and sum of two consume rows: %d should be equal to total inserted rows: %d"%(actConsumeTotalRows, totalRowsInserted))
            tdLog.exit("%d tmq consume rows error!"%consumerId)

        time.sleep(10)
        for i in range(len(topicNameList)):
            tdSql.query("drop topic %s"%topicNameList[i])

        tdLog.printNoPrefix("======== test case 2 end ...... ")

    def run(self):
        tdSql.prepare()
        self.prepareTestEnv()
        self.tmqCase1()
        self.tmqCase2()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
