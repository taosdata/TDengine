
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
from util.cluster import *
sys.path.append("./7-tmq")
from tmqCommon import *

class TDTestCase:
    def __init__(self):
        self.vgroups    = 1
        self.ctbNum     = 10
        self.rowsPerTbl = 1000

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    def getDataPath(self):
        selfPath = tdCom.getBuildPath()

        return selfPath + '/../sim/dnode%d/data/vnode/vnode%d/wal/*';

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
                    'rowsPerTbl': 1000,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  60,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        tdCom.drop_all_db()
        tmqCom.initConsumerTable()
        tdCom.create_database(tdSql, paraDict["dbName"],paraDict["dropFlag"], wal_retention_period=36000,vgroups=paraDict["vgroups"],replica=self.replicaVar)
        tdLog.info("create stb")
        tmqCom.create_stable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"])
        # tdLog.info("create ctb")
        # tmqCom.create_ctable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"],ctbPrefix=paraDict['ctbPrefix'],
        #                      ctbNum=paraDict["ctbNum"],ctbStartIdx=paraDict['ctbStartIdx'])
        # tdLog.info("insert data")
        # tmqCom.insert_data_interlaceByMultiTbl(tsql=tdSql,dbName=paraDict["dbName"],ctbPrefix=paraDict["ctbPrefix"],
        #                                        ctbNum=paraDict["ctbNum"],rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],
        #                                        startTs=paraDict["startTs"],ctbStartIdx=paraDict['ctbStartIdx'])

        # tdLog.info("restart taosd to ensure that the data falls into the disk")
        # tdDnodes.stop(1)
        # tdDnodes.start(1)
        # tdSql.query("flush database %s"%(paraDict['dbName']))
        return

    def restartAndRemoveWal(self):
        tdDnodes = cluster.dnodes
        tdSql.query("select * from information_schema.ins_vnodes")
        for result in tdSql.queryResult:
            if result[2] == 'dbt':
                tdLog.debug("dnode is %d"%(result[0]))
                dnodeId = result[0]
                vnodeId = result[1]

                tdDnodes[dnodeId - 1].stoptaosd()
                time.sleep(1)
                dataPath = self.getDataPath()
                dataPath = dataPath%(dnodeId,vnodeId)
                os.system('rm -rf ' + dataPath)
                tdLog.debug("dataPath:%s"%dataPath)
                tdDnodes[dnodeId - 1].starttaosd()
                time.sleep(1)
                break
        tdLog.debug("restart dnode ok")

    def redistributeVgroups(self):
        dnodesList = []
        tdSql.query("show dnodes")
        for result in tdSql.queryResult:
            dnodesList.append(result[0])
        print("dnodeList:",dnodesList)
        tdSql.query("select * from information_schema.ins_vnodes")
        vnodeId = 0
        for result in tdSql.queryResult:
            if result[2] == 'dbt':
                tdLog.debug("dnode is %d"%(result[0]))
                dnodesList.remove(result[0])
                vnodeId = result[1]
        print("its all data",dnodesList)
        # if self.replicaVar == 1:
        #     redistributeSql = "redistribute vgroup %d dnode %d" %(vnodeId, dnodesList[0])
        # else:
        redistributeSql = f"redistribute vgroup {vnodeId} " 
        for vgdnode in dnodesList:
            redistributeSql += f"dnode {vgdnode} "
        print(redistributeSql)
        
        tdLog.debug(f"redistributeSql:{redistributeSql}")
        tdSql.query(redistributeSql)
        tdLog.debug("redistributeSql ok")

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
                    'rowsPerTbl': 1000,
                    'batchNum':   10,
                    'startTs':    1640966400000,  # 2022-01-01 00:00:00.000
                    'pollDelay':  60,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        topicNameList = ['topic1']
        # expectRowsList = []
        tmqCom.initConsumerTable()

        tdLog.info("create topics from stb with filter")
        queryString = "select * from %s.%s"%(paraDict['dbName'], paraDict['stbName'])
        # sqlString = "create topic %s as stable %s" %(topicNameList[0], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicNameList[0], queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)
        # tdSql.query(queryString)
        # expectRowsList.append(tdSql.getRows())

        # init consume info, and start tmq_sim, then check consume result
        tdLog.info("insert consume info to consume processor")
        consumerId   = 0
        expectrowcnt = paraDict["rowsPerTbl"] * paraDict["ctbNum"] * 2
        topicList    = topicNameList[0]
        ifcheckdata  = 1
        ifManualCommit = 1
        keyList      = 'group.id:cgrp1, enable.auto.commit:true, auto.commit.interval.ms:200, auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        tmqCom.startTmqSimProcess(pollDelay=paraDict['pollDelay'],dbName=paraDict["dbName"],showMsg=paraDict['showMsg'], showRow=paraDict['showRow'],snapshot=paraDict['snapshot'])
        tdLog.info("wait the consume result")

        tdLog.info("create ctb1")
        tmqCom.create_ctable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"],ctbPrefix=paraDict['ctbPrefix'],
                             ctbNum=paraDict["ctbNum"],ctbStartIdx=paraDict['ctbStartIdx'])
        tdLog.info("insert data")
        pInsertThread = tmqCom.asyncInsertDataByInterlace(paraDict)

        tmqCom.getStartConsumeNotifyFromTmqsim()
        tmqCom.getStartCommitNotifyFromTmqsim()

        #restart dnode & remove wal
        self.restartAndRemoveWal()

        # redistribute vgroup
        self.redistributeVgroups();

        tdLog.info("create ctb2")
        paraDict['ctbPrefix'] = "ctbn"
        tmqCom.create_ctable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"],ctbPrefix=paraDict['ctbPrefix'],
                             ctbNum=paraDict["ctbNum"],ctbStartIdx=paraDict['ctbStartIdx'])
        tdLog.info("insert data")
        pInsertThread1 = tmqCom.asyncInsertDataByInterlace(paraDict)
        pInsertThread.join()
        pInsertThread1.join()

        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)

        if expectrowcnt / 2 > resultList[0]:
            tdLog.info("expect consume rows: %d, act consume rows: %d"%(expectrowcnt / 2, resultList[0]))
            tdLog.exit("%d tmq consume rows error!"%consumerId)

        # tmqCom.checkFileContent(consumerId, queryString)

        time.sleep(10)
        for i in range(len(topicNameList)):
            tdSql.query("drop topic %s"%topicNameList[i])

        tdLog.printNoPrefix("======== test case 1 end ...... ")

    def tmqCase2(self):
        tdLog.printNoPrefix("======== test case 2: ")
        paraDict = {'dbName':'dbt'}

        ntbName = "ntb"

        topicNameList = ['topic2']
        tmqCom.initConsumerTable()

        sqlString = "create table %s.%s(ts timestamp, i nchar(8))" %(paraDict['dbName'], ntbName)
        tdLog.info("create nomal table sql: %s"%sqlString)
        tdSql.execute(sqlString)

        tdLog.info("create topics from nomal table")
        queryString = "select * from %s.%s"%(paraDict['dbName'], ntbName)
        sqlString = "create topic %s as %s" %(topicNameList[0], queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)
        tdSql.query("flush database %s"%(paraDict['dbName']))
        #restart dnode & remove wal
        self.restartAndRemoveWal()

        # redistribute vgroup
        self.redistributeVgroups();

        sqlString = "alter table %s.%s modify column i nchar(16)" %(paraDict['dbName'], ntbName)
        tdLog.info("alter table sql: %s"%sqlString)
        tdSql.error(sqlString)
        expectRows = 0
        resultList = tmqCom.selectConsumeResult(expectRows)
        time.sleep(1)
        for i in range(len(topicNameList)):
            tdSql.query("drop topic %s"%topicNameList[i])

        tdLog.printNoPrefix("======== test case 2 end ...... ")

    def tmqCase3(self):
        tdLog.printNoPrefix("======== test case 3: ")
        paraDict = {'dbName':     'dbt',
                    'dropFlag':   1,
                    'event':      '',
                    'vgroups':    1,
                    'stbName':    'stbn',
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
                    'pollDelay':  2,
                    'showMsg':    1,
                    'showRow':    1,
                    'snapshot':   0}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = self.ctbNum
        paraDict['rowsPerTbl'] = self.rowsPerTbl

        topicNameList = ['topic3']
        tmqCom.initConsumerTable()

        tdLog.info("create stb")
        tmqCom.create_stable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"])

        tdLog.info("create ctb")
        tmqCom.create_ctable(tdSql, dbName=paraDict["dbName"],stbName=paraDict["stbName"],ctbPrefix=paraDict['ctbPrefix'],
                             ctbNum=paraDict["ctbNum"],ctbStartIdx=paraDict['ctbStartIdx'])
        tdLog.info("insert data")
        tmqCom.insert_data_interlaceByMultiTbl(tsql=tdSql,dbName=paraDict["dbName"],ctbPrefix=paraDict["ctbPrefix"],
                                               ctbNum=paraDict["ctbNum"],rowsPerTbl=paraDict["rowsPerTbl"],batchNum=paraDict["batchNum"],
                                               startTs=paraDict["startTs"],ctbStartIdx=paraDict['ctbStartIdx'])

        tdLog.info("create topics from stb with filter")
        queryString = "select * from %s.%s where c2 > 0 "%(paraDict['dbName'], paraDict['stbName'])
        sqlString = "create topic %s as %s" %(topicNameList[0], queryString)
        tdLog.info("create topic sql: %s"%sqlString)
        tdSql.execute(sqlString)

        # init consume info, and start tmq_sim, then check consume result
        tdLog.info("insert consume info to consume processor")
        consumerId   = 0
        expectrowcnt = paraDict["rowsPerTbl"] * paraDict["ctbNum"]
        topicList    = topicNameList[0]
        ifcheckdata  = 1
        ifManualCommit = 1
        keyList      = 'group.id:cgrp1, enable.auto.commit:true, auto.commit.interval.ms:200, auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        tmqCom.startTmqSimProcess(pollDelay=paraDict['pollDelay'],dbName=paraDict["dbName"],showMsg=paraDict['showMsg'], showRow=paraDict['showRow'],snapshot=paraDict['snapshot'])
        tdLog.info("wait the consume result")

        time.sleep(5)
        #restart dnode & remove wal
        self.restartAndRemoveWal()

        # redistribute vgroup
        self.redistributeVgroups()

        tdLog.info("start consume processor")
        tmqCom.startTmqSimProcess(pollDelay=paraDict['pollDelay'],dbName=paraDict["dbName"],showMsg=paraDict['showMsg'], showRow=paraDict['showRow'],snapshot=paraDict['snapshot'])
        tdLog.info("wait the consume result")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows)
        
        time.sleep(20)
        for i in range(len(topicNameList)):
            tdSql.query("drop topic %s"%topicNameList[i])

        tdLog.printNoPrefix("======== test case 3 end ...... ")

    def run(self):
        self.prepareTestEnv()
        self.tmqCase1()
        self.tmqCase2()
        self.prepareTestEnv()
        self.tmqCase3()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
