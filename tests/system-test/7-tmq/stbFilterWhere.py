
import taos
import sys
import time
import socket
import os
import threading

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
sys.path.append("./7-tmq")
from tmqCommon import *

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        #tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def prepareTestEnv(self):
        tdLog.printNoPrefix("======== prepare test env include database, stable, ctables, and insert data: ")
        paraDict = {'dbName':     'dbt',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'replica':    1,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':2}, {'type': 'binary', 'len':20, 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbStartIdx': 0,
                    'ctbNum':     10,
                    'rowsPerTbl': 10000,
                    'batchNum':   100,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  2,
                    'showMsg':    1,
                    'showRow':    1}

        tmqCom.initConsumerTable()
        tmqCom.create_database(tsql=tdSql, dbName=paraDict["dbName"],dropFlag=paraDict["dropFlag"], vgroups=paraDict['vgroups'],replica=paraDict['replica'])
        tdSql.execute("alter database %s wal_retention_period 3600"%(paraDict["dbName"]))
        tdLog.info("create stb")
        tmqCom.create_stable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"])
        tdLog.info("create ctb")
        tmqCom.create_ctable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"],ctbPrefix=paraDict['ctbPrefix'], ctbNum=paraDict['ctbNum'])
        tdLog.info("insert data")
        tmqCom.insert_data_interlaceByMultiTbl(tsql=tdSql,dbName=paraDict["dbName"],ctbPrefix=paraDict["ctbPrefix"],
                                               ctbNum=paraDict["ctbNum"],rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],
                                               startTs=paraDict["startTs"],ctbStartIdx=paraDict['ctbStartIdx'])
        return

    def tmqCaseError(self, topicName, condition):
        tdLog.printNoPrefix("======== test case error: ")
        paraDict = {'dbName':     'dbt',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'replica':    1,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':2}, {'type': 'binary', 'len':20, 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbStartIdx': 0,
                    'ctbNum':     10,
                    'rowsPerTbl': 10000,
                    'batchNum':   100,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  2,
                    'showMsg':    1,
                    'showRow':    1}

        tdLog.info("create topics from stb with column filter")
        topicString = "create topic %s as stable %s.%s where %s" %(topicName, paraDict['dbName'], paraDict['stbName'], condition)
        tdLog.info("create topic sql: %s"%topicString)
        tdSql.error(topicString)

    def tmqCase(self, topicName, condition):
        tdLog.printNoPrefix("======== test case: ")
        paraDict = {'dbName':     'dbt',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'replica':    1,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':2}, {'type': 'binary', 'len':20, 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbStartIdx': 0,
                    'ctbNum':     10,
                    'rowsPerTbl': 10000,
                    'batchNum':   100,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  2,
                    'showMsg':    1,
                    'showRow':    1}

        expectRowsList = []
        tmqCom.initConsumerTable()

        tdLog.info("create topics from stb with tag filter")
        topicString = "create topic %s as stable %s.%s where %s" %(topicName, paraDict['dbName'], paraDict['stbName'], condition)
        tdLog.info("create topic sql: %s"%topicString)
        tdSql.execute(topicString)

        queryString = "select * from %s.%s where %s" %(paraDict['dbName'], paraDict['stbName'], condition)
        tdSql.query(queryString)
        expectRowsList.append(tdSql.getRows())

        # init consume info, and start tmq_sim, then check consume result
        tdLog.info("insert consume info to consume processor")
        consumerId   = 0
        expectrowcnt = paraDict["rowsPerTbl"] * paraDict["ctbNum"]
        topicList    = topicName
        ifcheckdata  = 0
        ifManualCommit = 1
        keyList      = 'group.id:cgrp1, enable.auto.commit:false, auto.commit.interval.ms:6000, auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        tmqCom.startTmqSimProcess(paraDict['pollDelay'],paraDict["dbName"],paraDict['showMsg'], paraDict['showRow'])

        tdLog.info("wait the consume result")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)

        if expectRowsList[0] != resultList[0]:
            tdLog.info("expect consume rows: %d, act consume rows: %d"%(expectRowsList[0], resultList[0]))
            tdLog.exit("0 tmq consume rows error!")

        tdLog.printNoPrefix("======== test case end ...... ")

    def run(self):
        tdSql.prepare()
        self.prepareTestEnv()
        self.tmqCaseError("t1", "c1 = 4 and t1 = 3")
        self.tmqCase("t2", "2 > 1")
        self.tmqCase("t3", "t4 = 'beijing'")
        self.tmqCase("t4", "t4 > t3")
        self.tmqCase("t5", "t3 = t4")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
