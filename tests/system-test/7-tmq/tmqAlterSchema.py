
from ntpath import join
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
from util.cluster import *

sys.path.append("./7-tmq")
from tmqCommon import *

class TDTestCase:
    def __init__(self):
        self.dnodes = 5
        self.mnodes = 3
        self.idIndex = 0
        self.roleIndex = 2
        self.mnodeStatusIndex = 3
        self.mnodeEpIndex = 1
        self.dnodeStatusIndex = 4
        self.mnodeCheckCnt    = 10
        self.host = socket.gethostname()
        self.startPort = 6030
        self.portStep = 100
        self.dnodeOfLeader = 0

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        #tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def tmqCase1(self):
        tdLog.printNoPrefix("======== test case 1: topic: select * from stb, while consume, add column int-A/bianry-B/float-C, and then modify B, drop C")
        tdLog.printNoPrefix("add tag int-A/bianry-B/float-C, and then rename A, modify B, drop C, set t2")
        paraDict = {'dbName':     'db1',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':2}, {'type': 'binary', 'len':20, 'count':1}, {'type': 'TIMESTAMP', 'count':1}],
                    'tagSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbStartIdx': 0,
                    'ctbNum':     1,
                    'rowsPerTbl': 10000,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  10,
                    'showMsg':    1,
                    'showRow':    1}

        topicNameList = ['topic1']
        expectRowsList = []
        queryStringList = []
        tmqCom.initConsumerTable()
        tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], vgroups=4,replica=1)
        tdLog.info("create stb")
        tdCom.create_stable(tdSql, dbname=paraDict["dbName"],stbname=paraDict["stbName"], column_elm_list=paraDict['colSchema'], tag_elm_list=paraDict['tagSchema'])
        tdLog.info("create ctb")
        tdCom.create_ctable(tdSql, dbname=paraDict["dbName"],stbname=paraDict["stbName"],tag_elm_list=paraDict['tagSchema'],count=paraDict["ctbNum"], default_ctbname_prefix=paraDict['ctbPrefix'])
        # tdLog.info("async insert data")
        # pThread = tmqCom.asyncInsertData(paraDict)
        tmqCom.insert_data_2(tdSql,paraDict["dbName"],paraDict["ctbPrefix"],paraDict["ctbNum"],paraDict["rowsPerTbl"],paraDict["batchNum"],paraDict["startTs"],paraDict["ctbStartIdx"])

        tdLog.info("create topics from stb with filter")
        queryStringList.append("select * from %s.%s" %(paraDict['dbName'], paraDict['stbName']))
        sqlString = "create topic %s as %s" %(topicNameList[0], queryStringList[0])
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)
        tdSql.query(queryStringList[0])
        expectRowsList.append(tdSql.getRows())

        # init consume info, and start tmq_sim, then check consume result
        tdLog.info("insert consume info to consume processor")
        consumerId   = 0
        expectrowcnt = paraDict["rowsPerTbl"] * paraDict["ctbNum"]
        topicList    = topicNameList[0]
        ifcheckdata  = 1
        ifManualCommit = 1
        keyList      = 'group.id:cgrp1, enable.auto.commit:true, auto.commit.interval.ms:6000, auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        dstFile = tmqCom.getResultFileByTaosShell(consumerId, queryStringList[0])

        tdLog.info("start consume processor")
        tmqCom.startTmqSimProcess(paraDict['pollDelay'],paraDict["dbName"],paraDict['showMsg'], paraDict['showRow'])

        tdLog.info("wait the notify info of start consume, then alter schema")
        tmqCom.getStartConsumeNotifyFromTmqsim()

        # add column double-A/bianry-B/double-C, and then modify B, drop C
        sqlString = "alter table %s.%s add column newc1 double"%(paraDict["dbName"],paraDict['stbName'])
        tdSql.execute(sqlString)
        sqlString = "alter table %s.%s add column newc2 binary(16)"%(paraDict["dbName"],paraDict['stbName'])
        tdSql.execute(sqlString)
        sqlString = "alter table %s.%s add column newc3 double"%(paraDict["dbName"],paraDict['stbName'])
        tdSql.execute(sqlString)
        sqlString = "alter table %s.%s modify column newc2 binary(32)"%(paraDict["dbName"],paraDict['stbName'])
        tdSql.execute(sqlString)
        sqlString = "alter table %s.%s drop column newc3"%(paraDict["dbName"],paraDict['stbName'])
        tdSql.execute(sqlString)
        # add tag double-A/bianry-B/double-C, and then rename A, modify B, drop C, set t1
        sqlString = "alter table %s.%s add tag newt1 double"%(paraDict["dbName"],paraDict['stbName'])
        tdSql.execute(sqlString)
        sqlString = "alter table %s.%s add tag newt2 binary(16)"%(paraDict["dbName"],paraDict['stbName'])
        tdSql.execute(sqlString)
        sqlString = "alter table %s.%s add tag newt3 double"%(paraDict["dbName"],paraDict['stbName'])
        tdSql.execute(sqlString)
        sqlString = "alter table %s.%s rename tag newt1 newt1n"%(paraDict["dbName"],paraDict['stbName'])
        tdSql.execute(sqlString)
        sqlString = "alter table %s.%s modify tag newt2 binary(32)"%(paraDict["dbName"],paraDict['stbName'])
        tdSql.execute(sqlString)
        sqlString = "alter table %s.%s drop tag newt3"%(paraDict["dbName"],paraDict['stbName'])
        tdSql.execute(sqlString)
        sqlString = "alter table %s.%s0 set tag newt2='new tag'"%(paraDict["dbName"],paraDict['ctbPrefix'])
        tdSql.execute(sqlString)

        tdLog.info("check the consume result")
        tdSql.query(queryStringList[0])
        expectRowsList.append(tdSql.getRows())

        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)

        tdLog.info("expect consume rows: %d"%(expectRowsList[0]))
        tdLog.info("act consume rows: %d"%(resultList[0]))

        if expectRowsList[0] != resultList[0]:
            tdLog.exit("0 tmq consume rows error!")

        tmqCom.checkTmqConsumeFileContent(consumerId, dstFile)

        time.sleep(10)
        for i in range(len(topicNameList)):
            tdSql.query("drop topic %s"%topicNameList[i])

        tdLog.printNoPrefix("======== test case 1 end ...... ")

    def tmqCase2(self):
        tdLog.printNoPrefix("======== test case 2: topic: select * from ntb, while consume, add column int-A/bianry-B/float-C, and then rename A, modify B, drop C")
        paraDict = {'dbName':     'db1',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    4,
                    'stbName':    'stb',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count':2}, {'type': 'binary', 'len':20, 'count':2}],
                    'tagSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                    'ctbPrefix':  'ctb',
                    'ctbStartIdx': 0,
                    'ctbNum':     1,
                    'rowsPerTbl': 10000,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  10,
                    'showMsg':    1,
                    'showRow':    1}

        ntbName = 'ntb'

        topicNameList = ['topic1']
        expectRowsList = []
        queryStringList = []
        tmqCom.initConsumerTable()
        tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], vgroups=4,replica=1)
        tdLog.info("create stb")
        tdCom.create_stable(tdSql, dbname=paraDict["dbName"],stbname=paraDict["stbName"], column_elm_list=paraDict['colSchema'], tag_elm_list=paraDict['tagSchema'])
        tdLog.info("create ntb")
        tdCom.create_table(tdSql, dbname=paraDict["dbName"],tbname=ntbName,column_elm_list=paraDict['colSchema'],count=1)
        tdLog.info("start insert data ....")
        # pThread = tmqCom.asyncInsertData(paraDict)
        tdCom.insert_rows(tdSql, dbname=paraDict["dbName"], tbname=ntbName, column_ele_list=paraDict['colSchema'], start_ts_value=paraDict["startTs"], count=paraDict["rowsPerTbl"])
        tdLog.info("insert data end")

        tdLog.info("create topics from ntb with filter")
        queryStringList.append("select * from %s.%s" %(paraDict['dbName'], ntbName))
        sqlString = "create topic %s as %s" %(topicNameList[0], queryStringList[0])
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)
        tdSql.query(queryStringList[0])
        expectRowsList.append(tdSql.getRows())

        # init consume info, and start tmq_sim, then check consume result
        tdLog.info("insert consume info to consume processor")
        consumerId   = 0
        expectrowcnt = paraDict["rowsPerTbl"] * paraDict["ctbNum"]
        topicList    = topicNameList[0]
        ifcheckdata  = 1
        ifManualCommit = 1
        keyList      = 'group.id:cgrp1, enable.auto.commit:true, auto.commit.interval.ms:6000, auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        dstFile = tmqCom.getResultFileByTaosShell(consumerId, queryStringList[0])

        tdLog.info("start consume processor")
        tmqCom.startTmqSimProcess(paraDict['pollDelay'],paraDict["dbName"],paraDict['showMsg'], paraDict['showRow'])

        tdLog.info("wait the notify info of start consume, then alter schema")
        tmqCom.getStartConsumeNotifyFromTmqsim()

        # add column double-A/bianry-B/double-C, and then rename A, modify B, drop C
        sqlString = "alter table %s.%s add column newc1 double"%(paraDict["dbName"],ntbName)
        tdSql.execute(sqlString)
        sqlString = "alter table %s.%s add column newc2 binary(16)"%(paraDict["dbName"],ntbName)
        tdSql.execute(sqlString)
        sqlString = "alter table %s.%s add column newc3 double"%(paraDict["dbName"],ntbName)
        tdSql.execute(sqlString)
        sqlString = "alter table %s.%s rename column newc1 newc1n"%(paraDict["dbName"],ntbName)
        tdSql.execute(sqlString)
        sqlString = "alter table %s.%s modify column newc2 binary(32)"%(paraDict["dbName"],ntbName)
        tdSql.execute(sqlString)
        sqlString = "alter table %s.%s drop column newc3"%(paraDict["dbName"],ntbName)
        tdSql.execute(sqlString)

        tdLog.info("check the consume result")
        tdSql.query(queryStringList[0])
        expectRowsList.append(tdSql.getRows())

        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)

        tdLog.info("expect consume rows: %d"%(expectRowsList[0]))
        tdLog.info("act consume rows: %d"%(resultList[0]))

        if expectRowsList[0] != resultList[0]:
            tdLog.exit("0 tmq consume rows error!")

        tmqCom.checkTmqConsumeFileContent(consumerId, dstFile)

        time.sleep(10)
        for i in range(len(topicNameList)):
            tdSql.query("drop topic %s"%topicNameList[i])

        tdLog.printNoPrefix("======== test case 2 end ...... ")

    def run(self):
        self.tmqCase1()
        self.tmqCase2()


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
