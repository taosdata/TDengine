
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
        self.vgroups    = 4
        self.ctbNum     = 100
        self.rowsPerTbl = 1000

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

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
                    'batchNum':   1000,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  3,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        tmqCom.initConsumerTable()
        tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], vgroups=paraDict["vgroups"],replica=self.replicaVar,wal_retention_size=-1, wal_retention_period=-1)
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
        # tmqCom.insert_data_with_autoCreateTbl(tsql=tdSql,dbName=paraDict["dbName"],stbName=paraDict["stbName"],ctbPrefix="ctbx",
        #                                       ctbNum=paraDict["ctbNum"],rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],
        #                                       startTs=paraDict["startTs"],ctbStartIdx=paraDict['ctbStartIdx'])

        # tdLog.info("restart taosd to ensure that the data falls into the disk")
        # tdSql.query("flush database %s"%(paraDict['dbName']))
        return

    def delData(self,tsql,dbName,ctbPrefix,ctbNum,startTs=0,endTs=0,ctbStartIdx=0):
        tdLog.debug("start to del data ............")
        for i in range(ctbNum):
            sql = "delete from %s.%s%d where _c0 >= %d and _c0 <= %d "%(dbName,ctbPrefix,i+ctbStartIdx,startTs,endTs)
            tsql.execute(sql)

        tdLog.debug("del data ............ [OK]")
        return

    def threadFunctionForDeletaData(self, **paraDict):
        # create new connector for new tdSql instance in my thread
        newTdSql = tdCom.newTdSql()
        self.delData(newTdSql,paraDict["dbName"],paraDict["ctbPrefix"],paraDict["ctbNum"],paraDict["startTs"],paraDict["endTs"],paraDict["ctbStartIdx"])
        return

    def asyncDeleteData(self, paraDict):
        pThread = threading.Thread(target=self.threadFunctionForDeletaData, kwargs=paraDict)
        pThread.start()
        return pThread

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
                    'ctbNum':     100,
                    'rowsPerTbl': 1000,
                    'batchNum':   1000,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'endTs': 0,
                    'pollDelay':  5,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0}
        paraDict['snapshot'] = self.snapshot
        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        # del some data
        rowsOfDelete = int(paraDict["rowsPerTbl"] / 4)
        paraDict["endTs"] = paraDict["startTs"] + rowsOfDelete - 1
        self.delData(tsql=tdSql,dbName=paraDict["dbName"],ctbPrefix=paraDict["ctbPrefix"],ctbNum=paraDict["ctbNum"],
                     startTs=paraDict["startTs"], endTs=paraDict["endTs"],ctbStartIdx=paraDict['ctbStartIdx'])

        tdLog.info("create topics from stb1")
        topicFromStb1 = 'topic_stb1'
        queryString = "select ts, c1, c2 from %s.%s"%(paraDict['dbName'], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicFromStb1, queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)

        if self.snapshot == 0:
            consumerId     = 0
        elif self.snapshot == 1:
            consumerId     = 1
            rowsOfDelete = 0

        # paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        expectrowcnt   = int(paraDict["rowsPerTbl"] * paraDict["ctbNum"])
        topicList      = topicFromStb1
        ifcheckdata    = 1
        ifManualCommit = 1
        keyList        = 'group.id:cgrp1,\
                        enable.auto.commit:true,\
                        auto.commit.interval.ms:200,\
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

        tdLog.info("act consume rows: %d, expect consume rows: %d, act query rows: %d"%(totalConsumeRows, expectrowcnt, totalRowsFromQuery))

        if self.snapshot == 0:
            if totalConsumeRows != expectrowcnt:
                tdLog.exit("tmq consume rows error with snapshot = 0!")
        elif self.snapshot == 1:
            if totalConsumeRows != totalRowsFromQuery:
                tdLog.exit("tmq consume rows error with snapshot = 1!")

        # tmqCom.checkFileContent(consumerId=consumerId, queryString=queryString, skipRowsOfCons=rowsOfDelete)

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
                    'ctbNum':     100,
                    'rowsPerTbl': 1000,
                    'batchNum':   1000,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  5,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0}

        paraDict['snapshot'] = self.snapshot
        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        tdLog.info("restart taosd to ensure that the data falls into the disk")
        tdSql.query("flush database %s"%(paraDict['dbName']))

        # update to 1/4 rows and insert 3/4 new rows
        paraDict['startTs'] = paraDict['startTs'] + int(self.rowsPerTbl * 3 / 4)
        # paraDict['rowsPerTbl'] = self.rowsPerTbl
        tmqCom.insert_data_with_autoCreateTbl(tsql=tdSql,dbName=paraDict["dbName"],stbName=paraDict["stbName"],ctbPrefix=paraDict["ctbPrefix"],
                                              ctbNum=paraDict["ctbNum"],rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],
                                              startTs=paraDict["startTs"],ctbStartIdx=paraDict['ctbStartIdx'])
        # tmqCom.insert_data_interlaceByMultiTbl(tsql=tdSql,dbName=paraDict["dbName"],ctbPrefix=paraDict["ctbPrefix"],
        #                                        ctbNum=paraDict["ctbNum"],rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],
        #                                        startTs=paraDict["startTs"],ctbStartIdx=paraDict['ctbStartIdx'])

        # del some data
        rowsOfDelete = int(self.rowsPerTbl / 4 )
        paraDict["endTs"] = paraDict["startTs"] + rowsOfDelete - 1
        self.delData(tsql=tdSql,dbName=paraDict["dbName"],ctbPrefix=paraDict["ctbPrefix"],ctbNum=paraDict["ctbNum"],
                     startTs=paraDict["startTs"], endTs=paraDict["endTs"],ctbStartIdx=paraDict['ctbStartIdx'])

        tmqCom.initConsumerTable()
        tdLog.info("create topics from stb1")
        topicFromStb1 = 'topic_stb1'
        queryString = "select ts, c1, c2 from %s.%s"%(paraDict['dbName'], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicFromStb1, queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)

        # paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl
        consumerId     = 1

        if self.snapshot == 0:
            consumerId     = 2
            expectrowcnt   = int(paraDict["rowsPerTbl"] * paraDict["ctbNum"] * (1 + 1/4 + 3/4))
        elif self.snapshot == 1:
            consumerId     = 3
            expectrowcnt   = int(paraDict["rowsPerTbl"] * paraDict["ctbNum"] * (1 - 1/4 + 1/4 + 3/4))

        topicList      = topicFromStb1
        ifcheckdata    = 1
        ifManualCommit = 1
        keyList        = 'group.id:cgrp1,\
                        enable.auto.commit:true,\
                        auto.commit.interval.ms:200,\
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

        tdLog.info("act consume rows: %d, act query rows: %d, expect consume rows: %d, "%(totalConsumeRows, totalRowsFromQuery, expectrowcnt))

        if self.snapshot == 0:
            if totalConsumeRows != expectrowcnt:
                tdLog.exit("tmq consume rows error with snapshot = 0!")
        elif self.snapshot == 1:
            if totalConsumeRows != totalRowsFromQuery:
                tdLog.exit("tmq consume rows error with snapshot = 1!")

        # tmqCom.checkFileContent(consumerId, queryString)

        tdSql.query("drop topic %s"%topicFromStb1)

        tdLog.printNoPrefix("======== test case 2 end ...... ")

    def tmqCase3(self):
        tdLog.printNoPrefix("======== test case 3: ")
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
                    'batchNum':   1000,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  5,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0}

        paraDict['snapshot'] = self.snapshot
        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        tdLog.info("restart taosd to ensure that the data falls into the disk")
        tdSql.query("flush database %s"%(paraDict['dbName']))

        tmqCom.initConsumerTable()
        tdLog.info("create topics from stb1")
        topicFromStb1 = 'topic_stb1'
        queryString = "select ts, c1, c2 from %s.%s"%(paraDict['dbName'], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicFromStb1, queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)

        # paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl
        consumerId     = 1

        if self.snapshot == 0:
            consumerId     = 4
            expectrowcnt   = int(paraDict["rowsPerTbl"] * paraDict["ctbNum"] * (1 + 1/4 + 3/4))
        elif self.snapshot == 1:
            consumerId     = 5
            # expectrowcnt   = int(paraDict["rowsPerTbl"] * paraDict["ctbNum"] * (1 - 1/4 + 1/4 + 3/4))
            # fix case: sometimes only 200 rows are deleted
            expectrowcnt   = int(paraDict["rowsPerTbl"] * paraDict["ctbNum"] * (1 + 1/4 + 3/4))

        topicList      = topicFromStb1
        ifcheckdata    = 1
        ifManualCommit = 1
        keyList        = 'group.id:cgrp1,\
                        enable.auto.commit:true,\
                        auto.commit.interval.ms:200,\
                        auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        # del some data
        rowsOfDelete = int(self.rowsPerTbl / 4 )
        paraDict["endTs"] = paraDict["startTs"] + rowsOfDelete - 1
        pDeleteThread = self.asyncDeleteData(paraDict)

        tdLog.info("start consume processor")
        tmqCom.startTmqSimProcess(pollDelay=paraDict['pollDelay'],dbName=paraDict["dbName"],showMsg=paraDict['showMsg'], showRow=paraDict['showRow'],snapshot=paraDict['snapshot'])

        # update to 1/4 rows and insert 3/4 new rows
        paraDict['startTs'] = paraDict['startTs'] + int(self.rowsPerTbl * 3 / 4)
        # paraDict['rowsPerTbl'] = self.rowsPerTbl
        # tmqCom.insert_data_with_autoCreateTbl(tsql=tdSql,dbName=paraDict["dbName"],stbName=paraDict["stbName"],ctbPrefix=paraDict["ctbPrefix"],
        #                                       ctbNum=paraDict["ctbNum"],rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],
        #                                       startTs=paraDict["startTs"],ctbStartIdx=paraDict['ctbStartIdx'])
        pInsertThread = tmqCom.asyncInsertDataByInterlace(paraDict)

        pInsertThread.join()

        tdLog.info("start to check consume result")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        tdSql.query(queryString)
        totalRowsFromQuery = tdSql.getRows()

        tdLog.info("act consume rows: %d, act query rows: %d, expect consume rows: %d, "%(totalConsumeRows, totalRowsFromQuery, expectrowcnt))

        if self.snapshot == 0:
            if totalConsumeRows < expectrowcnt:
                tdLog.exit("tmq consume rows error with snapshot = 0!")
        elif self.snapshot == 1:
            if not ((totalConsumeRows >= totalRowsFromQuery) and (totalConsumeRows <= expectrowcnt)):
                tdLog.exit("tmq consume rows error with snapshot = 1!")

        # tmqCom.checkFileContent(consumerId, queryString)

        tdSql.query("drop topic %s"%topicFromStb1)

        tdLog.printNoPrefix("======== test case 3 end ...... ")


    def run(self):
        tdLog.printNoPrefix("=============================================")
        tdLog.printNoPrefix("======== snapshot is 0: only consume from wal")
        self.snapshot = 0
        self.prepareTestEnv()
        self.tmqCase1()
        self.tmqCase2()

        tdLog.printNoPrefix("====================================================================")
        tdLog.printNoPrefix("======== snapshot is 1: firstly consume from tsbs, and then from wal")
        self.snapshot = 1
        self.prepareTestEnv()
        self.tmqCase1()
        self.tmqCase2()

        tdLog.printNoPrefix("=============================================")
        tdLog.printNoPrefix("======== snapshot is 0: only consume from wal")
        self.snapshot = 0
        self.prepareTestEnv()
        self.tmqCase3()
        tdLog.printNoPrefix("====================================================================")
        tdLog.printNoPrefix("======== snapshot is 1:  firstly consume from tsbs, and then from wal")
        self.snapshot = 1
        self.prepareTestEnv()
        self.tmqCase3()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
