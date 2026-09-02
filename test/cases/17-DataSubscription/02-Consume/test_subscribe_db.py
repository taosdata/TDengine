
import taos
import sys
import time
import socket
import os
import threading
import platform

from new_test_framework.utils import tdLog, tdSql, tdDnodes, tdCom, tmqCom

class TestCase:
    hostname = socket.gethostname()
    # rpcDebugFlagVal = '143'
    #clientCfgDict = {'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    #clientCfgDict["rpcDebugFlag"]  = rpcDebugFlagVal
    #updatecfgDict = {'clientCfg': {}, 'serverPort': '', 'firstEp': '', 'secondEp':'', 'rpcDebugFlag':'135', 'fqdn':''}
    # updatecfgDict["rpcDebugFlag"] = rpcDebugFlagVal
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

    def create_tables(self,tsql, dbName,vgroups,stbName,ctbNum):
        tsql.execute("create database if not exists %s vgroups %d wal_retention_period 3600"%(dbName, vgroups))
        tsql.execute("use %s" %dbName)
        tsql.execute("create table  if not exists %s (ts timestamp, c1 bigint, c2 binary(16)) tags(t1 int)"%stbName)
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

        event.set()
        tdLog.debug("complete to create database[%s], stable[%s] and %d child tables" %(dbName, stbName, ctbNum))
        return

    def insert_data(self,tsql,dbName,stbName,ctbNum,rowsPerTbl,batchNum,startTs):
        tdLog.debug("start to insert data ............")
        tsql.execute("use %s" %dbName)
        pre_insert = "insert into "
        sql = pre_insert

        t = time.time()
        startTs = int(round(t * 1000))
        #tdLog.debug("doing insert data into stable:%s rows:%d ..."%(stbName, allRows))
        for i in range(ctbNum):
            sql += " %s_%d values "%(stbName,i)
            for j in range(rowsPerTbl):
                sql += "(%d, %d, 'tmqrow_%d') "%(startTs + j, j, j)
                if (j > 0) and ((j%batchNum == 0) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
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
        print ("input parameters:")
        print (parameterDict)
        # create new connector for my thread
        tsql=self.newcur(parameterDict['cfg'], 'localhost', 6030)
        self.create_tables(tsql,\
                           parameterDict["dbName"],\
                           parameterDict["vgroups"],\
                           parameterDict["stbName"],\
                           parameterDict["ctbNum"])

        self.insert_data(tsql,\
                         parameterDict["dbName"],\
                         parameterDict["stbName"],\
                         parameterDict["ctbNum"],\
                         parameterDict["rowsPerTbl"],\
                         parameterDict["batchNum"],\
                         parameterDict["startTs"])
        return

    def tmqCase1(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 1: Produce while one consume to subscribe one db, inclue 1 stb")
        tdLog.info("step 1: create database, stb, ctb and insert data")
        # create and start thread
        parameterDict = {'cfg':        '',       \
                         'dbName':     'db1',    \
                         'vgroups':    4,        \
                         'stbName':    'stb',    \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 5000,    \
                         'batchNum':   100,      \
                         'replica':   self.replicaVar,      \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        tmqCom.initConsumerTable(walRetentionPeriod=3600, includeNotifyInfo=False)

        tdSql.execute("create database if not exists %s vgroups %d replica %d wal_retention_period 3600" %(parameterDict['dbName'], parameterDict['vgroups'], parameterDict['replica']))

        prepareEnvThread = threading.Thread(target=self.prepareEnv, kwargs=parameterDict)
        prepareEnvThread.start()

        tdLog.info("create topics from db")
        topicName1 = 'topic_db1'

        tdSql.execute("create topic %s as database %s" %(topicName1, parameterDict['dbName']))
        consumerId   = 0
        expectrowcnt = parameterDict["rowsPerTbl"] * parameterDict["ctbNum"]
        topicList    = topicName1
        ifcheckdata  = 0
        ifManualCommit = 0
        keyList      = 'group.id:cgrp1,\
                        enable.auto.commit:false,\
                        auto.commit.interval.ms:6000,\
                        auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        event.wait()

        tdLog.info("start consume processor")
        pollDelay = 100
        showMsg   = 1
        showRow   = 1
        tmqCom.startTmqSimProcess(pollDelay,parameterDict["dbName"],showMsg, showRow)

        # wait for data ready
        prepareEnvThread.join()

        tdLog.info("1-insert process end, and start to check consume result")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows, pollInterval=5)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        if totalConsumeRows != expectrowcnt:
            tdLog.info("act consume rows: %d, expect consume rows: %d"%(totalConsumeRows, expectrowcnt))
            tdLog.exit("tmq consume rows error!")

        tdSql.query("drop topic %s"%topicName1)

        tdLog.info("creat the same topic name , and start to consume")
        tmqCom.initConsumerTable(walRetentionPeriod=3600, includeNotifyInfo=False)
        tdLog.info("create topics from db")
        topicName1 = 'topic_db1'

        tdSql.execute("create topic %s as database %s" %(topicName1, parameterDict['dbName']))
        consumerId   = 0
        expectrowcnt = parameterDict["rowsPerTbl"] * parameterDict["ctbNum"]
        topicList    = topicName1
        ifcheckdata  = 0
        ifManualCommit = 0
        keyList      = 'group.id:cgrp1,\
                        enable.auto.commit:false,\
                        auto.commit.interval.ms:6000,\
                        auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        pollDelay = 20
        showMsg   = 1
        showRow   = 1
        tmqCom.startTmqSimProcess(pollDelay,parameterDict["dbName"],showMsg, showRow)

        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows, pollInterval=5)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        if totalConsumeRows != expectrowcnt:
            tdLog.info("act consume rows: %d, expect consume rows: %d"%(totalConsumeRows, expectrowcnt))
            tdLog.exit("tmq consume rows error!")

        tdSql.query("drop topic %s"%topicName1)

        tdLog.printNoPrefix("======== test case 1 end ...... ")

    def tmqCase2(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 2: Produce while two consumers to subscribe one db, inclue 1 stb")
        tdLog.info("step 1: create database, stb, ctb and insert data")
        # create and start thread
        parameterDict = {'cfg':        '',       \
                         'dbName':     'db2',    \
                         'vgroups':    4,        \
                         'stbName':    'stb',    \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 5000,    \
                         'batchNum':   100,      \
                         'replica':   self.replicaVar,      \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        tmqCom.initConsumerTable(walRetentionPeriod=3600, includeNotifyInfo=False)

        tdSql.execute("create database if not exists %s vgroups %d replica %d wal_retention_period 3600" %(parameterDict['dbName'], parameterDict['vgroups'], parameterDict['replica']))

        prepareEnvThread = threading.Thread(target=self.prepareEnv, kwargs=parameterDict)
        prepareEnvThread.start()

        tdLog.info("create topics from db")
        topicName1 = 'topic_db1'

        tdSql.execute("create topic %s as database %s" %(topicName1, parameterDict['dbName']))

        consumerId   = 0
        expectrowcnt = parameterDict["rowsPerTbl"] * parameterDict["ctbNum"]
        topicList    = topicName1
        ifcheckdata  = 0
        ifManualCommit = 1
        keyList      = 'group.id:cgrp1,\
                        enable.auto.commit:false,\
                        auto.commit.interval.ms:6000,\
                        auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        consumerId   = 1
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        event.wait()

        tdLog.info("start consume processor")
        pollDelay = 20
        showMsg   = 1
        showRow   = 1
        tmqCom.startTmqSimProcess(pollDelay,parameterDict["dbName"],showMsg, showRow)

        # wait for data ready
        prepareEnvThread.join()

        tdLog.info("2-insert process end, and start to check consume result")
        expectRows = 2
        resultList = tmqCom.selectConsumeResult(expectRows, pollInterval=5)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        tdLog.info("act consume rows: %d, expect consume rows: %d"%(totalConsumeRows, expectrowcnt))
        if not (totalConsumeRows >= expectrowcnt):
            tdLog.exit("tmq consume rows error!")

        tdSql.query("drop topic %s"%topicName1)

        tdLog.printNoPrefix("======== test case 2 end ...... ")

    def tmqCase2a(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 2a: Produce while two consumers to subscribe one db, inclue 1 stb")
        tdLog.info("step 1: create database, stb, ctb and insert data")
        # create and start thread
        parameterDict = {'cfg':        '',       \
                         'dbName':     'db2a',    \
                         'vgroups':    4,        \
                         'stbName':    'stb1',    \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 5000,    \
                         'batchNum':   100,      \
                         'replica':   self.replicaVar,      \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        tmqCom.initConsumerTable(walRetentionPeriod=3600, includeNotifyInfo=False)

        tdSql.execute("create database if not exists %s vgroups %d wal_retention_period 3600" %(parameterDict['dbName'], parameterDict['vgroups']))
        tdSql.execute("create table  if not exists %s.%s (ts timestamp, c1 bigint, c2 binary(16)) tags(t1 int)"%(parameterDict['dbName'], parameterDict['stbName']))

        tdLog.info("create topics from db")
        topicName1 = 'topic_db1'

        tdSql.execute("create topic %s as database %s" %(topicName1, parameterDict['dbName']))

        consumerId   = 0
        expectrowcnt = parameterDict["rowsPerTbl"] * parameterDict["ctbNum"]
        topicList    = topicName1
        ifcheckdata  = 0
        ifManualCommit = 1
        keyList      = 'group.id:cgrp1,\
                        enable.auto.commit:false,\
                        auto.commit.interval.ms:6000,\
                        auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        consumerId   = 1
        keyList      = 'group.id:cgrp2,\
                        enable.auto.commit:false,\
                        auto.commit.interval.ms:6000,\
                        auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        tdLog.info("start consume processor")
        pollDelay = 100
        showMsg   = 1
        showRow   = 1
        tmqCom.startTmqSimProcess(pollDelay,parameterDict["dbName"],showMsg, showRow)

        prepareEnvThread = threading.Thread(target=self.prepareEnv, kwargs=parameterDict)
        prepareEnvThread.start()

        # wait for data ready
        prepareEnvThread.join()

        tdLog.info("3-insert process end, and start to check consume result")
        expectRows = 2
        resultList = tmqCom.selectConsumeResult(expectRows, pollInterval=5)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        if totalConsumeRows != expectrowcnt * 2:
            tdLog.info("act consume rows: %d, expect consume rows: %d"%(totalConsumeRows, expectrowcnt*2))
            tdLog.exit("tmq consume rows error!")

        tdSql.query("drop topic %s"%topicName1)

        tdLog.printNoPrefix("======== test case 2a end ...... ")

    def tmqCase3(self, cfgPath, buildPath):
        tdLog.printNoPrefix("======== test case 3: Produce while one consumers to subscribe one db, include 2 stb")
        tdLog.info("step 1: create database, stb, ctb and insert data")
        # create and start thread
        parameterDict = {'cfg':        '',       \
                         'dbName':     'db3',    \
                         'vgroups':    4,        \
                         'stbName':    'stb',    \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 5000,    \
                         'batchNum':   100,      \
                         'replica':   self.replicaVar,      \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        tmqCom.initConsumerTable(walRetentionPeriod=3600, includeNotifyInfo=False)

        tdSql.execute("create database if not exists %s vgroups %d replica %d wal_retention_period 3600" %(parameterDict['dbName'], parameterDict['vgroups'], parameterDict['replica']))

        prepareEnvThread = threading.Thread(target=self.prepareEnv, kwargs=parameterDict)
        prepareEnvThread.start()

        parameterDict2 = {'cfg':        '',       \
                         'dbName':     'db3',    \
                         'vgroups':    4,        \
                         'stbName':    'stb2',    \
                         'ctbNum':     10,       \
                         'rowsPerTbl': 5000,    \
                         'batchNum':   100,      \
                         'startTs':    1640966400000}  # 2022-01-01 00:00:00.000
        parameterDict['cfg'] = cfgPath

        prepareEnvThread2 = threading.Thread(target=self.prepareEnv, kwargs=parameterDict2)
        prepareEnvThread2.start()

        tdLog.info("create topics from db")
        topicName1 = 'topic_db1'

        tdSql.execute("create topic %s as database %s" %(topicName1, parameterDict['dbName']))

        consumerId   = 0
        expectrowcnt = parameterDict["rowsPerTbl"] * parameterDict["ctbNum"] +  parameterDict2["rowsPerTbl"] * parameterDict2["ctbNum"]
        topicList    = topicName1
        ifcheckdata  = 0
        ifManualCommit = 0
        keyList      = 'group.id:cgrp1,\
                        enable.auto.commit:false,\
                        auto.commit.interval.ms:6000,\
                        auto.offset.reset:earliest'
        tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        # consumerId   = 1
        # tmqCom.insertConsumerInfo(consumerId, expectrowcnt,topicList,keyList,ifcheckdata,ifManualCommit)

        event.wait()

        tdLog.info("start consume processor")
        pollDelay = 100
        showMsg   = 1
        showRow   = 1
        tmqCom.startTmqSimProcess(pollDelay,parameterDict["dbName"],showMsg, showRow)

        # wait for data ready
        prepareEnvThread.join()
        prepareEnvThread2.join()

        tdLog.info("4-insert process end, and start to check consume result")
        expectRows = 1
        resultList = tmqCom.selectConsumeResult(expectRows, pollInterval=5)
        totalConsumeRows = 0
        for i in range(expectRows):
            totalConsumeRows += resultList[i]

        if totalConsumeRows != expectrowcnt:
            tdLog.info("act consume rows: %d, expect consume rows: %d"%(totalConsumeRows, expectrowcnt))
            tdLog.exit("tmq consume rows error!")

        tdSql.query("drop topic %s"%topicName1)

        tdLog.printNoPrefix("======== test case 3 end ...... ")

    def test_subscribeDB(self):
        """Subscribe database basic
        
        1. Create database with tables
        2. Create topics from database
        3. Test database-level subscription
        4. Verify all tables data consumption
        5. Clean up environment
        
        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-12-23 Alex Duan Migrated from uncatalog/system-test/7-tmq/test_subscribeDb.py

        """
        tdSql.prepare()

        buildPath = tdCom.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        cfgPath = os.path.join(tdDnodes.sim.path,"psim","cfg")
        tdLog.info("cfgPath: %s" % cfgPath)

        self.tmqCase1(cfgPath, buildPath)
        self.tmqCase2(cfgPath, buildPath)
        self.tmqCase2a(cfgPath, buildPath)
        self.tmqCase3(cfgPath, buildPath)

event = threading.Event()
