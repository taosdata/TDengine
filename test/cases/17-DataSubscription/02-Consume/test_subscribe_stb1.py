
import taos
import time
import socket
import os
import threading
from enum import Enum
import platform

from new_test_framework.utils import tdLog, tdSql, tdDnodes, tdCom, tmqCom

class actionType(Enum):
    CREATE_DATABASE = 0
    CREATE_STABLE   = 1
    CREATE_CTABLE   = 2
    INSERT_DATA     = 3

class TestCase:
    hostname = socket.gethostname()
    #rpcDebugFlagVal = '143'
    #clientCfgDict = {'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    #clientCfgDict["rpcDebugFlag"]  = rpcDebugFlagVal
    #updatecfgDict = {'clientCfg': {}, 'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    #updatecfgDict["rpcDebugFlag"] = rpcDebugFlagVal
    #print ("===================: ", updatecfgDict)

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")


    def newcur(self,cfg,host,port):
        user = "root"
        password = "taosdata"
        con=taos.connect(host=host, user=user, password=password, config=cfg ,port=port)
        cur=con.cursor()
        print(cur)
        return cur

    def create_database(self,tsql, dbName,dropFlag=1,vgroups=4,replica=1):
        if dropFlag == 1:
            tsql.execute("drop database if exists %s"%(dbName))

        tsql.execute("create database if not exists %s vgroups %d replica %d wal_retention_period 3600"%(dbName, vgroups, replica))
        tdLog.debug("complete to create database %s"%(dbName))
        return

    def create_stable(self,tsql, dbName,stbName):
        tsql.execute("create table if not exists %s.%s (ts timestamp, c1 bigint, c2 binary(16)) tags(t1 int)"%(dbName, stbName))
        tdLog.debug("complete to create %s.%s" %(dbName, stbName))
        return

    def create_ctables(self,tsql, dbName,stbName,ctbNum):
        tsql.execute("use %s" %dbName)
        pre_create = "create table"
        sql = pre_create
        #tdLog.debug("doing create one  stable %s and %d  child table in %s  ..." %(stbname, count ,dbname))
        for i in range(ctbNum):
            sql += " %s_%d using %s tags(%d)"%(stbName,i,stbName,i+1)
            if (i > 0) and (i%100 == 0):
                tsql.execute(sql)
                sql = pre_create
        if sql != pre_create:
            tsql.execute(sql)

        tdLog.debug("complete to create %d child tables in %s.%s" %(ctbNum, dbName, stbName))
        return

    def insert_data(self,tsql,dbName,stbName,ctbNum,rowsPerTbl,batchNum,startTs=0):
        tdLog.debug("start to insert data ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        if startTs == 0:
            t = time.time()
            startTs = int(round(t * 1000))

        #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        rowsOfSql = 0
        for i in range(ctbNum):
            sql += " %s_%d values "%(stbName,i)
            for j in range(rowsPerTbl):
                sql += "(%d, %d, 'tmqrow_%d') "%(startTs + j, j, j)
                rowsOfSql += 1
                if (j > 0) and ((rowsOfSql == batchNum) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    rowsOfSql = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s_%d values " %(stbName,i)
                    else:
                        sql = "insert into "
        #end sql
        if sql != pre_insert:
            #print("insert sql:%s"%sql)
            tsql.execute(sql)
        tdLog.debug("insert data ............ [OK]")
        return

    def prepareEnv(self, **parameterDict):
        # create new connector for my thread
        tsql=self.newcur(parameterDict['cfg'], 'localhost', 6030)

        if parameterDict["actionType"] == actionType.CREATE_DATABASE:
            self.create_database(tsql, parameterDict["dbName"])
        elif parameterDict["actionType"] == actionType.CREATE_STABLE:
            self.create_stable(tsql, parameterDict["dbName"], parameterDict["stbName"])
        elif parameterDict["actionType"] == actionType.CREATE_CTABLE:
            self.create_ctables(tsql, parameterDict["dbName"], parameterDict["stbName"], parameterDict["ctbNum"])
        elif parameterDict["actionType"] == actionType.INSERT_DATA:
            self.insert_data(tsql, parameterDict["dbName"], parameterDict["stbName"], parameterDict["ctbNum"],\
                            parameterDict["rowsPerTbl"],parameterDict["batchNum"])
        else:
            tdLog.exit("not support's action: ", parameterDict["actionType"])

        return

    def tmqCase6(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 6: ")

        tmqCom.initConsumerTable(walRetentionPeriod=3600, includeNotifyInfo=False)

        # create and start thread
        parameterDict = {'cfg':        '',       \
                         'actionType': 0,        \
                         'dbName':     'db6',    \
                         'dropFlag':   1,        \
                         'vgroups':    4,        \
                         'replica':    1,        \
                         'stbName':    'stb1',   \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 10000,    \
                         'batchNum':   100,      \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        self.create_database(tdSql, parameterDict["dbName"])
        self.create_stable(tdSql, parameterDict["dbName"], parameterDict["stbName"])
        self.create_ctables(tdSql, parameterDict["dbName"], parameterDict["stbName"], parameterDict["ctbNum"])
        self.insert_data(tdSql,\
                         parameterDict["dbName"],\
                         parameterDict["stbName"],\
                         parameterDict["ctbNum"],\
                         parameterDict["rowsPerTbl"],\
                         parameterDict["batchNum"])

        tdLog.info("create topics from stb1")
        topicFromStb1 = 'topic_stb1'

        tdSql.execute("create topic %s as select ts, c1, c2 from %s.%s" %(topicFromStb1, parameterDict['dbName'], parameterDict['stbName']))
        consumerId     = 0
        expectrowcnt   = parameterDict["rowsPerTbl"] * parameterDict["ctbNum"]
        topicList      = topicFromStb1
        ifcheckdata    = 0
        ifManualCommit = 1
        keyList        = 'group.id:cgrp1,\
                        enable.auto.commit:false,\
                        auto.commit.interval.ms:6000,\
                        auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt/4,topicList,keyList,ifcheckdata,ifManualCommit, useConsumerIdTimestamp=False)

        tdLog.info("start consume processor")
        pollDelay = 5
        showMsg   = 1
        showRow   = 1
        tmqCom.startTmqSimProcess(pollDelay,parameterDict["dbName"],showMsg, showRow)

        tdLog.info("start to check consume result")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows, pollInterval=5)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        tdLog.info("act consume rows: %d, expect consume rows greater than or equal to: %d"%(totalConsumeRows, expectrowcnt/4))
        if totalConsumeRows < expectrowcnt/4:            
            tdLog.exit("tmq consume rows error!")

        tmqCom.initConsumerInfoTable()
        consumerId = 1
        keyList        = 'group.id:cgrp1,\
                        enable.auto.commit:false,\
                        auto.commit.interval.ms:6000,\
                        auto.offset.reset:latest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit, useConsumerIdTimestamp=False)

        tdLog.info("again start consume processor")
        tmqCom.startTmqSimProcess(pollDelay,parameterDict["dbName"],showMsg, showRow)

        tdLog.info("again check consume result")
        expectRows = 2
        resultList = tmqCom.selectConsumeResult(expectRows, pollInterval=5)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        tdLog.info("act consume rows: %d, expect consume rows greater than or equal to: %d"%(totalConsumeRows, expectrowcnt))
        if totalConsumeRows < expectrowcnt:            
            tdLog.exit("tmq consume rows error!")

        tdSql.query("drop topic %s"%topicFromStb1)

        tdLog.printNoPrefix("======== test case 6 end ...... ")

    def tmqCase7(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 7: ")

        tmqCom.initConsumerTable(walRetentionPeriod=3600, includeNotifyInfo=False)

        # create and start thread
        parameterDict = {'cfg':        '',       \
                         'actionType': 0,        \
                         'dbName':     'db7',    \
                         'dropFlag':   1,        \
                         'vgroups':    4,        \
                         'replica':    1,        \
                         'stbName':    'stb1',   \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 10000,    \
                         'batchNum':   100,      \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        self.create_database(tdSql, parameterDict["dbName"])
        self.create_stable(tdSql, parameterDict["dbName"], parameterDict["stbName"])
        self.create_ctables(tdSql, parameterDict["dbName"], parameterDict["stbName"], parameterDict["ctbNum"])
        self.insert_data(tdSql,\
                         parameterDict["dbName"],\
                         parameterDict["stbName"],\
                         parameterDict["ctbNum"],\
                         parameterDict["rowsPerTbl"],\
                         parameterDict["batchNum"])

        tdLog.info("create topics from stb1")
        topicFromStb1 = 'topic_stb1'

        tdSql.execute("create topic %s as select ts, c1, c2 from %s.%s" %(topicFromStb1, parameterDict['dbName'], parameterDict['stbName']))
        consumerId     = 0
        expectrowcnt   = parameterDict["rowsPerTbl"] * parameterDict["ctbNum"]
        topicList      = topicFromStb1
        ifcheckdata    = 0
        ifManualCommit = 1
        keyList        = 'group.id:cgrp1,\
                        enable.auto.commit:false,\
                        auto.commit.interval.ms:6000,\
                        auto.offset.reset:latest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit, useConsumerIdTimestamp=False)

        tdLog.info("start consume processor")
        pollDelay = 5
        showMsg   = 1
        showRow   = 1
        tmqCom.startTmqSimProcess(pollDelay,parameterDict["dbName"],showMsg, showRow)

        tdLog.info("start to check consume result")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows, pollInterval=5)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        if totalConsumeRows != 0:
            tdLog.info("act consume rows: %d, expect consume rows: %d"%(totalConsumeRows, 0))
            tdLog.exit("tmq consume rows error!")

        tmqCom.initConsumerInfoTable()
        consumerId = 1
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit, useConsumerIdTimestamp=False)

        tdLog.info("again start consume processor")
        tmqCom.startTmqSimProcess(pollDelay,parameterDict["dbName"],showMsg, showRow)

        tdLog.info("again check consume result")
        expectRows = 2
        resultList = tmqCom.selectConsumeResult(expectRows, pollInterval=5)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        if totalConsumeRows != 0:
            tdLog.info("act consume rows: %d, expect consume rows: %d"%(totalConsumeRows, 0))
            tdLog.exit("tmq consume rows error!")

        tdSql.query("drop topic %s"%topicFromStb1)

        tdLog.printNoPrefix("======== test case 7 end ...... ")

    def test_subscribeStb1(self):
        """Subscribe stable1
        
        1. Create database and stable
        2. Create topics from stable with manual commit
        3. Test auto commit functionality
        4. Insert consume info to consume processor
        5. Start consume processor
        6. Check consume result
        7. Clean up environment
        
        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_subscribeStb1.py

        """
        tdSql.prepare()

        buildPath = tdCom.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        cfgPath = os.path.join(tdDnodes.sim.path,"psim","cfg")
        tdLog.info("cfgPath: %s" % cfgPath)

        self.tmqCase6(cfgPath, buildPath)
        self.tmqCase7(cfgPath, buildPath)
