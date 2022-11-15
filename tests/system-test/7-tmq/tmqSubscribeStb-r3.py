import os
import platform
import socket
import subprocess
import sys
import threading
import time
from distutils.log import error

import taos
from util.cases import *
from util.cluster import *
from util.common import *
from util.dnodes import *
from util.dnodes import TDDnode, TDDnodes
from util.log import *
from util.sql import *

sys.path.append("./6-cluster")
sys.path.append("./7-tmq")
from clusterCommonCheck import clusterComCheck
from clusterCommonCreate import *
from tmqCommon import *


class TDTestCase:
    def __init__(self):
        self.snapshot   = 0
        self.replica    = 3
        self.vgroups    = 4
        self.ctbNum     = 1000
        self.rowsPerTbl = 100
        self.dnodeNumbers = 5

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        #tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def checkFileContent(self, consumerId, queryString):
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

        while True:
            dst = queryFile.readline()
            src = consumeFile.readline()

            if dst:
                if dst != src:
                    tdLog.exit("consumerId %d consume rows is not match the rows by direct query"%consumerId)
            else:
                break
        return

    def prepareTestEnv(self):
        tdLog.printNoPrefix("======== prepare test env include database, stable, ctables, and insert data: ")
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
                    'ctbNum':     100,
                    'rowsPerTbl': 1000,
                    'batchNum':   100,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  3,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        tmqCom.initConsumerTable()
        tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], vgroups=paraDict["vgroups"],replica=self.replica)
        tdLog.info("create stb")
        tmqCom.create_stable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"])
        tdLog.info("create ctb")
        tmqCom.create_ctable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"],ctbPrefix=paraDict['ctbPrefix'],
                             ctbNum=paraDict["ctbNum"],ctbStartIdx=paraDict['ctbStartIdx'])
        tdLog.info("insert data")
        # tmqCom.insert_data_interlaceByMultiTbl(tsql=tdSql,dbName=paraDict["dbName"],ctbPrefix=paraDict["ctbPrefix"],
        #                                        ctbNum=paraDict["ctbNum"],rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],
        #                                        startTs=paraDict["startTs"],ctbStartIdx=paraDict['ctbStartIdx'])
        # tmqCom.insert_data_with_autoCreateTbl(tsql=tdSql,dbName=paraDict["dbName"],stbName=paraDict["stbName"],ctbPrefix="ctbx",
        #                                       ctbNum=paraDict["ctbNum"],rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],
        #                                       startTs=paraDict["startTs"],ctbStartIdx=paraDict['ctbStartIdx'])
        pThread = tmqCom.asyncInsertDataByInterlace(paraDict)

        tdLog.info("wait some data inserted")
        exitFlag = 1
        while exitFlag:
            queryString = "select count(*) from %s.%s"%(paraDict["dbName"],paraDict["stbName"])
            tdSql.query(queryString)
            if tdSql.getRows() > 0:
                rowsInserted = tdSql.getData(0,0)
                if (rowsInserted > ((self.ctbNum * self.rowsPerTbl)/5)):
                    exitFlag = 0
            time.sleep(0.1)

        tdLog.info("inserted rows: %d"%tdSql.getData(0,0))
        # tdDnodes=cluster.dnodes
        tdLog.info("================= restart dnode 2===========================")
        cluster.dnodes[1].stoptaosd()
        cluster.dnodes[1].starttaosd()
        clusterComCheck.checkDnodes(self.dnodeNumbers)
        tdLog.info("================= restart dnode 3===========================")
        cluster.dnodes[2].stoptaosd()
        cluster.dnodes[2].starttaosd()
        clusterComCheck.checkDnodes(self.dnodeNumbers)
        tdLog.info("================= restart dnode 4===========================")
        cluster.dnodes[3].stoptaosd()
        cluster.dnodes[3].starttaosd()
        clusterComCheck.checkDnodes(self.dnodeNumbers)
        tdLog.info("================= restart dnode 5===========================")
        cluster.dnodes[4].stoptaosd()
        cluster.dnodes[4].starttaosd()
        clusterComCheck.checkDnodes(self.dnodeNumbers)

        pThread.join()
        # tdLog.info("restart taosd to ensure that the data falls into the disk")
        # tdSql.query("flush database %s"%(paraDict['dbName']))
        return

    def tmqCase1(self):
        tdLog.printNoPrefix("======== test case 1: ")

        # create and start thread
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
                    'ctbNum':     100,
                    'rowsPerTbl': 1000,
                    'batchNum':   100,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  15,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        tdLog.info("create topics from stb1")
        topicFromStb1 = 'topic_stb1'
        queryString = "select ts, c1, c2 from %s.%s  where t4 == 'beijing' or t4 == 'changsha' "%(paraDict['dbName'], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicFromStb1, queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)

        consumerId     = 0
        expectrowcnt   = paraDict["rowsPerTbl"] * paraDict["ctbNum"]
        topicList      = topicFromStb1
        ifcheckdata    = 0
        ifManualCommit = 0
        keyList        = 'group.id:cgrp1,\
                        enable.auto.commit:false,\
                        auto.commit.interval.ms:6000,\
                        auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        tmqCom.startTmqSimProcess(pollDelay=paraDict['pollDelay'],dbName=paraDict["dbName"],showMsg=paraDict['showMsg'], showRow=paraDict['showRow'],snapshot=paraDict['snapshot'])

        tdLog.info("start to check consume result")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        tdSql.query(queryString)
        totalRowsFromQuery = tdSql.getRows()

        tdLog.info("act consume rows: %d, act query rows: %d "%(totalConsumeRows, totalRowsFromQuery))

        if totalConsumeRows < totalRowsFromQuery:
            tdLog.exit("tmq consume rows error!")

        # tmqCom.checkFileContent(consumerId, queryString)

        tmqCom.waitSubscriptionExit(tdSql, topicFromStb1)
        tdSql.query("drop topic %s"%topicFromStb1)

        tdLog.printNoPrefix("======== test case 1 end ...... ")

    def tmqCase2(self):
        tdLog.printNoPrefix("======== test case 2: ")

        # create and start thread
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
                    'ctbNum':     100,
                    'rowsPerTbl': 1000,
                    'batchNum':   100,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  30,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   1}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        tdLog.info("create topics from stb1")
        topicFromStb1 = 'topic_stb1'
        queryString = "select ts, c1, c2 from %s.%s  where t4 == 'beijing' or t4 == 'changsha' "%(paraDict['dbName'], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicFromStb1, queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)        

        tdSql.query(queryString)
        totalRowsFromQuery = tdSql.getRows()

        consumerId     = 0
        expectrowcnt   = paraDict["rowsPerTbl"] * paraDict["ctbNum"]
        topicList      = topicFromStb1
        ifcheckdata    = 0
        ifManualCommit = 0
        keyList        = 'group.id:cgrp1,\
                        enable.auto.commit:false,\
                        auto.commit.interval.ms:6000,\
                        auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        tmqCom.startTmqSimProcess(pollDelay=paraDict['pollDelay'],dbName=paraDict["dbName"],showMsg=paraDict['showMsg'], showRow=paraDict['showRow'],snapshot=paraDict['snapshot'])

        tmqCom.getStartConsumeNotifyFromTmqsim()
        tdLog.info("================= restart dnode 2===========================")
        cluster.dnodes[1].stoptaosd()
        cluster.dnodes[1].starttaosd()
        clusterComCheck.checkDnodes(self.dnodeNumbers)
        tdLog.info("================= restart dnode 3===========================")
        cluster.dnodes[2].stoptaosd()
        cluster.dnodes[2].starttaosd()
        clusterComCheck.checkDnodes(self.dnodeNumbers)
        tdLog.info("================= restart dnode 4===========================")
        cluster.dnodes[3].stoptaosd()
        cluster.dnodes[3].starttaosd()
        clusterComCheck.checkDnodes(self.dnodeNumbers)
        tdLog.info("================= restart dnode 5===========================")
        cluster.dnodes[4].stoptaosd()
        cluster.dnodes[4].starttaosd()
        clusterComCheck.checkDnodes(self.dnodeNumbers)

        tdLog.info("start to check consume result")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        tdLog.info("act consume rows: %d, act query rows: %d "%(totalConsumeRows, totalRowsFromQuery))

        if totalConsumeRows < totalRowsFromQuery:
            tdLog.exit("tmq consume rows error!")

        # tmqCom.checkFileContent(consumerId, queryString)

        tmqCom.waitSubscriptionExit(tdSql, topicFromStb1)
        tdSql.query("drop topic %s"%topicFromStb1)

        tdLog.printNoPrefix("======== test case 2 end ...... ")

    def run(self):
        #self.prepareTestEnv()
        #self.tmqCase1()
        self.prepareTestEnv()
        self.tmqCase2()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
