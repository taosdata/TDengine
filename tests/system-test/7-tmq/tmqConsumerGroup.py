
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

    def tmqCase1(self):
        tdLog.printNoPrefix("======== test case 1: ")
        paraDict = {'dbName':     'db1',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':2}, {'type': 'binary', 'len':20, 'count':1},{'type': 'TIMESTAMP', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbNum':     10,
                    'rowsPerTbl': 1000,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  20,
                    'showMsg':    1,
                    'showRow':    1}

        topicNameList = ['topic1', 'topic2']
        queryRowsList = []
        tmqCom.initConsumerTable()
        tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], vgroups=4,replica=1)
        tdLog.info("create stb")
        tdCom.create_stable(tdSql, dbname=paraDict["dbName"],stbname=paraDict["stbName"], column_elm_list=paraDict['colSchema'], tag_elm_list=paraDict['tagSchema'])
        tdLog.info("create ctb")
        tdCom.create_ctable(tdSql, dbname=paraDict["dbName"],stbname=paraDict["stbName"],tag_elm_list=paraDict['tagSchema'],count=paraDict["ctbNum"], default_ctbname_prefix=paraDict['ctbPrefix'])
        tdLog.info("insert data")
        tmqCom.insert_data_2(tdSql,paraDict["dbName"],paraDict["ctbPrefix"],paraDict["ctbNum"],paraDict["rowsPerTbl"],paraDict["batchNum"],paraDict["startTs"])

        tdLog.info("create topics from stb with filter")
        # queryString = "select ts, log(c1), ceil(pow(c1,3)) from %s.%s where c1 %% 7 == 0" %(paraDict['dbName'], paraDict['stbName'])
        queryString = "select ts, log(c1), ceil(pow(c1,3)) from %s.%s" %(paraDict['dbName'], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicNameList[0], queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)
        tdSql.query(queryString)
        queryRowsList.append(tdSql.getRows())

        # create one stb2
        paraDict["stbName"] = 'stb2'
        paraDict["ctbPrefix"] = 'ctbx'
        paraDict["rowsPerTbl"] = 5000
        tdLog.info("create stb2")
        tdCom.create_stable(tdSql, dbname=paraDict["dbName"],stbname=paraDict["stbName"], column_elm_list=paraDict['colSchema'], tag_elm_list=paraDict['tagSchema'])
        tdLog.info("create ctb2")
        tdCom.create_ctable(tdSql, dbname=paraDict["dbName"],stbname=paraDict["stbName"],tag_elm_list=paraDict['tagSchema'],count=paraDict["ctbNum"], default_ctbname_prefix=paraDict['ctbPrefix'])

        # queryString = "select ts, sin(c1), abs(pow(c1,3)) from %s.%s where c1 %% 7 == 0" %(paraDict['dbName'], paraDict['stbName'])
        queryString = "select ts, sin(c1), abs(pow(c1,3)) from %s.%s" %(paraDict['dbName'], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicNameList[1], queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)
        # tdSql.query(queryString)
        # queryRowsList.append(tdSql.getRows())

        # init consume info, and start tmq_sim, then check consume result
        tdLog.info("insert consume info to consume processor")
        consumerId   = 0
        expectrowcnt = paraDict["rowsPerTbl"] * paraDict["ctbNum"] * 2
        topicList    = "%s,%s"%(topicNameList[0],topicNameList[1])
        ifcheckdata  = 1
        ifManualCommit = 1
        keyList      = 'group.id:cgrp1, enable.auto.commit:true, auto.commit.interval.ms:3000, auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor 1")
        tmqCom.startTmqSimProcess(paraDict['pollDelay'],paraDict["dbName"],paraDict['showMsg'], paraDict['showRow'])

        tdLog.info("start consume processor 2")
        tmqCom.startTmqSimProcess(paraDict['pollDelay'],paraDict["dbName"],paraDict['showMsg'], paraDict['showRow'],'cdb',0,1)

        tdLog.info("async insert data")
        pThread = tmqCom.asyncInsertData(paraDict)

        tdLog.info("wait consumer commit notify")
        # tmqCom.getStartCommitNotifyFromTmqsim(rows=4)
        tmqCom.getStartConsumeNotifyFromTmqsim(rows=2)

        tdLog.info("pkill one consume processor")
        tmqCom.stopTmqSimProcess('tmq_sim_new')

        pThread.join()

        tdLog.info("wait the consume result")
        expectRows = 2
        resultList = tmqCom.selectConsumeResult(expectRows)
        actConsumTotalRows = 0
        for i in range(len(resultList)):
            actConsumTotalRows += resultList[i]
            
        tdLog.info("act consumer1 rows: %d, consumer2 rows: %d"%(resultList[0], resultList[1]))

        tdSql.query(queryString)
        queryRowsList.append(tdSql.getRows())
        queryTotalRows = 0
        for i in range(len(queryRowsList)):
            queryTotalRows += queryRowsList[i]

        tdLog.info("act consume rows: %d, query consume rows: %d"%(actConsumTotalRows, queryTotalRows))
        if actConsumTotalRows < queryTotalRows:
            tdLog.info("act consume rows: %d should >= query consume rows: %d"%(actConsumTotalRows, queryTotalRows))
            tdLog.exit("0 tmq consume rows error!")

        # time.sleep(10)
        # for i in range(len(topicNameList)):
        #     tdSql.query("drop topic %s"%topicNameList[i])

        tdLog.printNoPrefix("======== test case 1 end ...... ")


    def tmqCase2(self):
        tdLog.printNoPrefix("======== test case 2: ")
        paraDict = {'dbName':     'db1',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':2}, {'type': 'binary', 'len':20, 'count':1},{'type': 'TIMESTAMP', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbNum':     10,
                    'rowsPerTbl': 1000,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  20,
                    'showMsg':    1,
                    'showRow':    1}

        topicNameList = ['topic3', 'topic4']
        queryRowsList = []
        tmqCom.initConsumerTable()       

        tdLog.info("create topics from stb with filter")
        # queryString = "select ts, log(c1), ceil(pow(c1,3)) from %s.%s where c1 %% 7 == 0" %(paraDict['dbName'], paraDict['stbName'])
        queryString = "select ts, log(c1), ceil(pow(c1,3)) from %s.%s" %(paraDict['dbName'], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicNameList[0], queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)
        tdSql.query(queryString)
        queryRowsList.append(tdSql.getRows())

        # create one stb2
        paraDict["stbName"] = 'stb2'
        # queryString = "select ts, sin(c1), abs(pow(c1,3)) from %s.%s where c1 %% 7 == 0" %(paraDict['dbName'], paraDict['stbName'])
        queryString = "select ts, sin(c1), abs(pow(c1,3)) from %s.%s" %(paraDict['dbName'], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicNameList[1], queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)
        tdSql.query(queryString)
        queryRowsList.append(tdSql.getRows())

        # init consume info, and start tmq_sim, then check consume result
        tdLog.info("insert consume info to consume processor")
        consumerId   = 0
        paraDict["rowsPerTbl"] = 5000
        expectrowcnt = paraDict["rowsPerTbl"] * paraDict["ctbNum"] * 2
        topicList    = "%s,%s"%(topicNameList[0],topicNameList[1])
        ifcheckdata  = 1
        ifManualCommit = 1
        keyList      = 'group.id:cgrp1, enable.auto.commit:true, auto.commit.interval.ms:3000, auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor 1")
        tmqCom.startTmqSimProcess(paraDict['pollDelay'],paraDict["dbName"],paraDict['showMsg'], paraDict['showRow'])

        tdLog.info("start consume processor 2")
        tmqCom.startTmqSimProcess(paraDict['pollDelay'],paraDict["dbName"],paraDict['showMsg'], paraDict['showRow'],'cdb',0,1)

        tdLog.info("wait the consume result")
        expectRows = 2
        resultList = tmqCom.selectConsumeResult(expectRows)
        actConsumTotalRows = 0
        for i in range(len(resultList)):
            actConsumTotalRows += resultList[i]
            
        tdLog.info("act consumer1 rows: %d, consumer2 rows: %d"%(resultList[0], resultList[1]))

        queryTotalRows = 0
        for i in range(len(queryRowsList)):
            queryTotalRows += queryRowsList[i]

        tdLog.info("act consume rows: %d, query consume rows: %d"%(actConsumTotalRows, queryTotalRows))
        if actConsumTotalRows < queryTotalRows:
            tdLog.info("act consume rows: %d should >= query consume rows: %d"%(actConsumTotalRows, queryTotalRows))
            tdLog.exit("0 tmq consume rows error!")

        # time.sleep(10)
        # for i in range(len(topicNameList)):
        #     tdSql.query("drop topic %s"%topicNameList[i])

        tdLog.printNoPrefix("======== test case 2 end ...... ")

    def run(self):
        tdSql.prepare()
        self.tmqCase1()
        self.tmqCase2()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
