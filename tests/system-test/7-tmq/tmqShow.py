
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
    def insertConsumerInfo(self,consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifmanualcommit,offset=1,cdbName='cdb'):
        sql = "insert into %s.consumeinfo values "%cdbName
        sql += "(now+%ds, %d, '%s', '%s', %d, %d, %d)"%(offset,consumerId, topicList, keyList, expectrowcnt, ifcheckdata, ifmanualcommit)
        tdLog.info("consume info sql: %s"%sql)
        tdSql.query(sql)

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
                    'ctbNum':     100,
                    'rowsPerTbl': 4000,
                    'batchNum':   15,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  30,
                    'showMsg':    1,
                    'showRow':    1}

        topicNameList = ['topic1', 'topic2', 'topic3', 'topic4']
        consumeGroupIdList = ['cgrp1', 'cgrp1', 'cgrp3', 'cgrp4']
        consumerIdList = [0, 1, 2, 3]
        tmqCom.initConsumerTable()
        tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], vgroups=paraDict['vgroups'],replica=1)
        tdLog.info("create stb")
        tdCom.create_stable(tdSql, dbname=paraDict["dbName"],stbname=paraDict["stbName"], column_elm_list=paraDict['colSchema'], tag_elm_list=paraDict['tagSchema'])
        tdLog.info("create ctb")
        tdCom.create_ctable(tdSql, dbname=paraDict["dbName"],stbname=paraDict["stbName"],tag_elm_list=paraDict['tagSchema'],count=paraDict["ctbNum"], default_ctbname_prefix=paraDict['ctbPrefix'])
        # tdLog.info("insert data")
        # tmqCom.insert_data(tdSql,paraDict["dbName"],paraDict["ctbPrefix"],paraDict["ctbNum"],paraDict["rowsPerTbl"],paraDict["batchNum"],paraDict["startTs"])

        tdLog.info("create 4 topics")
        sqlString = "create topic %s as database %s" %(topicNameList[0], paraDict['dbName'])
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)

        sqlString = "create topic %s as stable %s.%s" %(topicNameList[1], paraDict['dbName'], paraDict['stbName'])
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)

        queryString = "select * from %s.%s where c1 %% 7 == 0" %(paraDict['dbName'], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicNameList[2], queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)

        queryString = "select ts, log(c1), ceil(pow(c1,3)) from %s.%s where c1 %% 7 == 0" %(paraDict['dbName'], paraDict['stbName'])
        sqlString = "create topic %s as %s " %(topicNameList[3], queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)

        tdSql.query("show topics")
        tdLog.debug(tdSql.queryResult)
        rows = tdSql.getRows()
        if rows != len(consumerIdList):
            tdLog.exit("topic rows error")

        for i in range (rows):
            topicName = tdSql.getData(i,0)
            matchFlag = 0
            while matchFlag == 0:
                for j in range(len(topicNameList)):
                    if topicName == topicNameList[j]:
                        matchFlag = 1
                        break
                if matchFlag == 0:
                    tdLog.exit("topic name: %s is error", topicName)

        # init consume info, and start tmq_sim, then check consume result
        tdLog.info("insert consume info to consume processor")
        expectrowcnt = paraDict["rowsPerTbl"] * paraDict["ctbNum"]
        topicList    = topicNameList[0]
        ifcheckdata  = 0
        ifManualCommit = 0
        keyList      = 'group.id:%s, enable.auto.commit:false, auto.commit.interval.ms:6000, auto.offset.reset:earliest'%consumeGroupIdList[0]
        tsOffset=1
        self.insertConsumerInfo(consumerIdList[0], expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit,tsOffset)

        topicList    = topicNameList[1]
        keyList      = 'group.id:%s, enable.auto.commit:false, auto.commit.interval.ms:6000, auto.offset.reset:earliest'%consumeGroupIdList[1]
        tsOffset=2
        self.insertConsumerInfo(consumerIdList[1], expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit,tsOffset)

        topicList    = topicNameList[2]
        keyList      = 'group.id:%s, enable.auto.commit:false, auto.commit.interval.ms:6000, auto.offset.reset:earliest'%consumeGroupIdList[2]
        tsOffset=3
        self.insertConsumerInfo(consumerIdList[2], expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit,tsOffset)

        topicList    = topicNameList[3]
        keyList      = 'group.id:%s, enable.auto.commit:false, auto.commit.interval.ms:6000, auto.offset.reset:earliest'%consumeGroupIdList[3]
        tsOffset=4
        self.insertConsumerInfo(consumerIdList[3], expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit,tsOffset)

        tdLog.info("start consume processor")
        tmqCom.startTmqSimProcess(paraDict['pollDelay'],paraDict["dbName"],paraDict['showMsg'], paraDict['showRow'])

        tdLog.info("async insert data")
        pThread = tmqCom.asyncInsertData(paraDict)

        tmqCom.getStartConsumeNotifyFromTmqsim()

        for i in range(0, 10, 1):
            tdLog.info("check show consumers")
            tdSql.query("show consumers")
            # tdLog.info(tdSql.queryResult)
            rows = tdSql.getRows()
            tdLog.info("show consumers rows: %d" % rows)

            if rows == len(topicNameList):
                tdLog.info("show consumers rows not match %d:%d" %
                           (rows, len(topicNameList)))
                time.sleep(1)
                break
            if (rows == 9):
                tdLog.exit("show consumers rows error")

        for i in range(0, 10, 1):
            tdLog.info("check show subscriptions")
            tdSql.query("show subscriptions")
            tdLog.debug(tdSql.queryResult)
            rows = tdSql.getRows()
            expectSubscriptions = paraDict['vgroups'] * len(topicNameList)
            tdLog.info("show subscriptions rows: %d, expect Subscriptions: %d"%(rows,expectSubscriptions))
            if rows != expectSubscriptions:
                # tdLog.exit("show subscriptions rows error")
                tdLog.info("continue retry[%d] to show subscriptions"%(i))
                time.sleep(1)
                continue
            else: 
                break

        if rows != expectSubscriptions:
            tdLog.exit("show subscriptions rows error")

        pThread.join()

        tdLog.info("insert process end, and start to check consume result")
        expectRows = len(consumerIdList)
        _ = tmqCom.selectConsumeResult(expectRows)

        time.sleep(10)
        for i in range(len(topicNameList)):
            tdSql.query("drop topic %s"%topicNameList[i])

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
