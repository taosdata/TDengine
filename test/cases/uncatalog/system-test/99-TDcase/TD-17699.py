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
    paraDict = {'dbName':     'db1',
                'dropFlag':   1,
                'event':      '',
                'vgroups':    2,
                'stbName':    'stb0',
                'colPrefix':  'c',
                'tagPrefix':  't',
                'colSchema':   [{'type': 'INT', 'count':2}, {'type': 'binary', 'len':16, 'count':1}, {'type': 'timestamp','count':1}],
                'tagSchema':   [{'type': 'INT', 'count':1}, {'type': 'binary', 'len':20, 'count':1}],
                'ctbPrefix':  'ctb',
                'ctbStartIdx': 0,
                'ctbNum':     100,
                'rowsPerTbl': 1000,
                'batchNum':   1000,
                'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                'pollDelay':  20,
                'showMsg':    1,
                'showRow':    1,
                'snapshot':   1}

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

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        logSql = False
        tdSql.init(conn.cursor(), logSql)

    def tmqCase1(self):
        tdLog.printNoPrefix("======== test case 1: ")
        tdLog.info("step 1: create database, stb, ctb and insert data")

        tmqCom.initConsumerTable(self.cdbName)

        tdCom.create_database(tdSql,self.paraDict["dbName"],self.paraDict["dropFlag"])

        self.paraDict["stbName"] = 'stb1'
        tdCom.create_stable(tdSql,dbname=self.paraDict["dbName"],stbname=self.paraDict["stbName"],column_elm_list=self.paraDict["colSchema"],tag_elm_list=self.paraDict["tagSchema"],count=1, default_stbname_prefix=self.paraDict["stbName"])
        tdCom.create_ctable(tdSql,dbname=self.paraDict["dbName"],stbname=self.paraDict["stbName"],tag_elm_list=self.paraDict['tagSchema'],count=self.paraDict["ctbNum"],default_ctbname_prefix=self.paraDict["ctbPrefix"])
        tmqCom.insert_data_2(tdSql,self.paraDict["dbName"],self.paraDict["ctbPrefix"],self.paraDict["ctbNum"],self.paraDict["rowsPerTbl"],self.paraDict["batchNum"],self.paraDict["startTs"],self.paraDict["ctbStartIdx"])
        # pThread1 = tmqCom.asyncInsertData(paraDict=self.paraDict)

        self.paraDict["stbName"] = 'stb2'
        self.paraDict["ctbPrefix"] = 'newctb'
        self.paraDict["batchNum"] = 1000
        tdCom.create_stable(tdSql,dbname=self.paraDict["dbName"],stbname=self.paraDict["stbName"],column_elm_list=self.paraDict["colSchema"],tag_elm_list=self.paraDict["tagSchema"],count=1, default_stbname_prefix=self.paraDict["stbName"])
        tdCom.create_ctable(tdSql,dbname=self.paraDict["dbName"],stbname=self.paraDict["stbName"],tag_elm_list=self.paraDict['tagSchema'],count=self.paraDict["ctbNum"],default_ctbname_prefix=self.paraDict["ctbPrefix"])
        # tmqCom.insert_data_2(tdSql,self.paraDict["dbName"],self.paraDict["ctbPrefix"],self.paraDict["ctbNum"],self.paraDict["rowsPerTbl"],self.paraDict["batchNum"],self.paraDict["startTs"],self.paraDict["ctbStartIdx"])
        pThread2 = tmqCom.asyncInsertData(paraDict=self.paraDict)

        tdLog.info("create topics from db")
        topicName1 = 'UpperCasetopic_%s'%(self.paraDict['dbName'])
        tdSql.execute("create topic %s as database %s" %(topicName1, self.paraDict['dbName']))

        topicList = topicName1 + ',' +topicName1
        keyList = '%s,%s,%s,%s'%(self.groupId,self.autoCommit,self.autoCommitInterval,self.autoOffset)
        self.expectrowcnt = self.paraDict["rowsPerTbl"] * self.paraDict["ctbNum"] * 2
        tmqCom.insertConsumerInfo(self.consumerId, self.expectrowcnt,topicList,keyList,self.ifcheckdata,self.ifManualCommit)

        tdLog.info("start consume processor")
        # tmqCom.startTmqSimProcess(self.pollDelay,self.paraDict["dbName"],self.showMsg, self.showRow,self.cdbName)
        tmqCom.startTmqSimProcess(pollDelay=self.pollDelay,dbName=self.paraDict["dbName"],showMsg=self.showMsg, showRow=self.showRow,snapshot=self.paraDict['snapshot'])

        tmqCom.getStartConsumeNotifyFromTmqsim()
        tdLog.info("drop one stable")
        self.paraDict["stbName"] = 'stb1'
        tdSql.execute("drop table %s.%s" %(self.paraDict['dbName'], self.paraDict['stbName']))
        dropTblNum = int(self.paraDict["ctbNum"] / 4)
        tmqCom.drop_ctable(tdSql, dbname=self.paraDict['dbName'], count=dropTblNum, default_ctbname_prefix=self.paraDict["ctbPrefix"])

        # pThread2.join()

        tdLog.info("wait result from consumer, then check it")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)

        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        if not (totalConsumeRows >= self.expectrowcnt*3/8 and totalConsumeRows <= self.expectrowcnt):
            tdLog.info("act consume rows: %d, expect consume rows: between %d and %d"%(totalConsumeRows, self.expectrowcnt/2, self.expectrowcnt))
            tdLog.exit("tmq consume rows error!")

        time.sleep(10)
        tdSql.query("drop topic %s"%topicName1)

        tdLog.printNoPrefix("======== test case 1 end ...... ")

    def run(self):
        tdSql.prepare()
        self.tmqCase1()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
